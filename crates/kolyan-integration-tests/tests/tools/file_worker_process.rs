//! Actual macOS process enforcement, not real-model acceptance.
#![cfg(target_os = "macos")]

use std::io::Write;
use std::time::Duration;

use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalEvidence, ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyContext,
    PolicyDecisionKind, PolicyEngine, PreparedCall, PreparedGrant, ToolExecutionScope,
    ToolManifest,
};
use kolyan_sandbox::SandboxCancellation;
use kolyan_tools::{FileOperationLimits, IsolatedFileConfig, IsolatedFileTools};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    name: String,
    arguments: serde_json::Value,
    expected_error: bool,
    expected_error_contains: Option<String>,
    expected_content: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionScopeDataset {
    id: String,
    expected_rows: usize,
    admitted_scope: ToolExecutionScope,
    modes: Vec<ExecutionScopeMode>,
    mutations: Vec<ExecutionScopeMutation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionScopeMode {
    id: String,
    approval: ScopeApproval,
    call: ToolCall,
    check_foreign_approval: bool,
    matching_case_id: String,
    expected_matching: MatchingExpectation,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScopeApproval {
    Allow,
    Confirmed,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MatchingExpectation {
    refused: bool,
    effect_exists: bool,
    content: String,
    result_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionScopeMutation {
    id: String,
    field: ScopeField,
    value: Option<String>,
    expected_refusal: RefusalExpectation,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScopeField {
    SessionId,
    TurnId,
    ExecutionId,
    StepId,
    AgentSnapshotDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefusalExpectation {
    refused: bool,
    effect_exists: bool,
    error_contains: String,
}

impl ExecutionScopeMutation {
    fn apply(&self, admitted: &ToolExecutionScope) -> ToolExecutionScope {
        let mut scope = admitted.clone();
        let required = || {
            self.value
                .clone()
                .expect("identity mutation requires a value")
        };
        match self.field {
            ScopeField::SessionId => scope.execution.session_id = required(),
            ScopeField::TurnId => scope.execution.turn_id = required(),
            ScopeField::ExecutionId => scope.execution.execution_id = required(),
            ScopeField::StepId => scope.step_id = required(),
            ScopeField::AgentSnapshotDigest => scope.agent_snapshot_digest = self.value.clone(),
        }
        scope
    }
}

fn scope() -> ToolExecutionScope {
    ToolExecutionScope {
        execution: kolyan_runtime::ExecutionKey {
            session_id: "file-fixture-session".into(),
            turn_id: "file-fixture-turn".into(),
            execution_id: "file-fixture-execution".into(),
        },
        step_id: "file-fixture-step".into(),
        agent_snapshot_digest: Some("b".repeat(64)),
    }
}

#[tokio::test]
async fn exact_granted_file_worker_process_matrix() {
    let scope = scope();
    let root = tempfile::tempdir().unwrap();
    let reports = tempfile::Builder::new()
        .prefix("kolyan-file-worker-")
        .tempdir()
        .unwrap()
        .keep();
    let mut trace = std::fs::File::create(reports.join("actual.jsonl")).unwrap();
    let tools = IsolatedFileTools::new(IsolatedFileConfig {
        workspace: root.path().into(),
        worker: std::env::var_os("KOLYAN_TOOL_WORKER")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_kolyan-test-tool-worker").into()),
        protected_roots: vec![],
        file_limits: FileOperationLimits {
            max_read_bytes: 1024,
            max_write_bytes: 1024,
        },
        max_output_bytes: 1024 * 1024,
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let mut policy = PolicyEngine::default();
    for (name, capability, effect) in [
        ("file.read", Capability::FilesystemRead, Effect::Read),
        ("file.write", Capability::FilesystemWrite, Effect::Update),
        ("file.edit", Capability::FilesystemWrite, Effect::Update),
    ] {
        policy.register(ToolManifest {
            tool_name: name.into(),
            capabilities: [capability].into_iter().collect(),
            effects: [effect].into_iter().collect(),
            path_scopes: vec![],
            idempotency: Idempotency::Unknown,
            approval: ApprovalMode::Never,
        });
    }
    let cases: Vec<Case> = serde_json::from_str(include_str!("fixtures/file_worker.json")).unwrap();
    for case in cases {
        let call = ToolCall {
            id: case.id.clone(),
            name: case.name,
            arguments: case.arguments,
        };
        let prepared = tools.prepare(call.clone());
        let result = match &prepared {
            Ok(prepared) => {
                let decision = policy.decide_prepared(prepared, &PolicyContext::default());
                let grant = PreparedGrant::issue(
                    prepared,
                    decision,
                    ApprovalEvidence::NotConfirmed,
                    scope.clone(),
                )
                .unwrap();
                tools
                    .execute(
                        prepared,
                        &grant,
                        &policy.revision(),
                        &scope,
                        SandboxCancellation::default(),
                    )
                    .await
                    .map_err(|error| error.to_string())
            }
            Err(error) => Err(error.to_string()),
        };
        let record = json!({"case":case.id, "request":call,
            "prepared":prepared.as_ref().ok(), "expected_scope":scope, "actual":result});
        serde_json::to_writer(&mut trace, &record).unwrap();
        trace.write_all(b"\n").unwrap();
        trace.flush().unwrap();
        assert_eq!(
            result.is_err(),
            case.expected_error,
            "{}: {:?}; trace {}",
            case.id,
            result,
            reports.display()
        );
        match result {
            Ok(result) => assert_eq!(result.content, case.expected_content, "{}", case.id),
            Err(error) => assert!(
                error.contains(
                    case.expected_error_contains
                        .as_deref()
                        .expect("error fixture must identify its semantic failure")
                ),
                "{}: {error}",
                case.id
            ),
        }
    }
    assert_eq!(
        std::fs::read_to_string(root.path().join("note.txt")).unwrap(),
        "hello beta"
    );
    eprintln!(
        "file worker actual process trajectory: {}",
        reports.join("actual.jsonl").display()
    );
}

#[tokio::test]
async fn changed_preparation_policy_and_worker_are_rejected_before_effects() {
    let scope = scope();
    let root = tempfile::tempdir().unwrap();
    let worker_root = tempfile::tempdir().unwrap();
    let worker = worker_root.path().join("trusted-worker");
    std::fs::copy(env!("CARGO_BIN_EXE_kolyan-test-tool-worker"), &worker).unwrap();
    let tools = IsolatedFileTools::new(IsolatedFileConfig {
        workspace: root.path().into(),
        worker: worker.clone(),
        protected_roots: vec![],
        file_limits: FileOperationLimits {
            max_read_bytes: 1024,
            max_write_bytes: 1024,
        },
        max_output_bytes: 1024 * 1024,
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let call = ToolCall {
        id: "write".into(),
        name: "file.write".into(),
        arguments: json!({"path":"must-not-exist","content":"original"}),
    };
    let prepared = tools.prepare(call).unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Update].into_iter().collect(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    let grant = PreparedGrant::issue(
        &prepared,
        policy.decide_prepared(&prepared, &PolicyContext::default()),
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    let report = tempfile::Builder::new()
        .prefix("kolyan-file-bindings-")
        .tempdir()
        .unwrap()
        .keep();
    let mut trace = std::fs::File::create(report.join("actual.jsonl")).unwrap();
    let mut value = serde_json::to_value(&prepared).unwrap();
    value["call"]["arguments"]["content"] = json!("tampered");
    let tampered: PreparedCall = serde_json::from_value(value).unwrap();
    let policy_revision = policy.revision();
    for (name, input, revision) in [
        ("changed-input", &tampered, policy_revision.as_str()),
        ("changed-policy", &prepared, "v2"),
    ] {
        let result = tools
            .execute(
                input,
                &grant,
                revision,
                &scope,
                SandboxCancellation::default(),
            )
            .await;
        let error = result.unwrap_err().to_string();
        serde_json::to_writer(
            &mut trace,
            &json!({"case":name, "prepared":input,
            "grant":grant,"policy_revision":revision,"actual_error":error}),
        )
        .unwrap();
        trace.write_all(b"\n").unwrap();
        trace.flush().unwrap();
        assert!(error.contains("binding mismatch"), "{name}: {error}");
        assert!(!root.path().join("must-not-exist").exists());
    }
    let mut binary = std::fs::OpenOptions::new()
        .append(true)
        .open(worker)
        .unwrap();
    binary.write_all(b"revision-changed").unwrap();
    binary.flush().unwrap();
    let error = tools
        .execute(
            &prepared,
            &grant,
            &policy_revision,
            &scope,
            SandboxCancellation::default(),
        )
        .await
        .unwrap_err()
        .to_string();
    serde_json::to_writer(
        &mut trace,
        &json!({"case":"changed-worker", "prepared":prepared,
        "grant":grant,"actual_error":error}),
    )
    .unwrap();
    trace.write_all(b"\n").unwrap();
    trace.flush().unwrap();
    assert!(error.contains("binding mismatch"), "{error}");
    assert!(!root.path().join("must-not-exist").exists());
    eprintln!(
        "file binding refusal trajectory: {}",
        report.join("actual.jsonl").display()
    );
}

#[tokio::test]
async fn symlink_referent_not_alias_is_the_authorized_resource() {
    let scope = scope();
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let report = tempfile::Builder::new()
        .prefix("kolyan-file-scopes-")
        .tempdir()
        .unwrap()
        .keep();
    let mut trace = std::fs::File::create(report.join("actual.jsonl")).unwrap();
    std::fs::create_dir(root.path().join("public")).unwrap();
    std::fs::create_dir(root.path().join("private")).unwrap();
    std::fs::write(root.path().join("private/secret"), "not-public").unwrap();
    std::fs::write(root.path().join("public/a"), "first").unwrap();
    std::fs::write(root.path().join("public/b"), "second").unwrap();
    symlink("../private/secret", root.path().join("public/alias")).unwrap();
    let tools = IsolatedFileTools::new(IsolatedFileConfig {
        workspace: root.path().into(),
        worker: env!("CARGO_BIN_EXE_kolyan-test-tool-worker").into(),
        protected_roots: vec![],
        file_limits: FileOperationLimits {
            max_read_bytes: 1024,
            max_write_bytes: 1024,
        },
        max_output_bytes: 1024 * 1024,
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let call = |path: &str| ToolCall {
        id: "read".into(),
        name: "file.read".into(),
        arguments: json!({"path":path}),
    };
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.read".into(),
        capabilities: [Capability::FilesystemRead].into_iter().collect(),
        effects: [Effect::Read].into_iter().collect(),
        path_scopes: vec![PathScope::new(
            root.path()
                .join("public")
                .canonicalize()
                .unwrap()
                .to_string_lossy(),
        )],
        idempotency: Idempotency::Idempotent,
        approval: ApprovalMode::Never,
    });
    let prepared = tools.prepare(call("public/alias")).unwrap();
    let decision = policy.decide_prepared(&prepared, &PolicyContext::default());
    serde_json::to_writer(
        &mut trace,
        &json!({"case":"private-referent",
        "prepared":prepared,"actual_decision":decision}),
    )
    .unwrap();
    trace.write_all(b"\n").unwrap();
    trace.flush().unwrap();
    assert_eq!(decision.kind, PolicyDecisionKind::Deny);
    std::fs::remove_file(root.path().join("public/alias")).unwrap();
    symlink("a", root.path().join("public/alias")).unwrap();
    let prepared = tools.prepare(call("public/alias")).unwrap();
    let grant = PreparedGrant::issue(
        &prepared,
        policy.decide_prepared(&prepared, &PolicyContext::default()),
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    std::fs::remove_file(root.path().join("public/alias")).unwrap();
    symlink("b", root.path().join("public/alias")).unwrap();
    let error = tools
        .execute(
            &prepared,
            &grant,
            &policy.revision(),
            &scope,
            SandboxCancellation::default(),
        )
        .await
        .unwrap_err()
        .to_string();
    serde_json::to_writer(
        &mut trace,
        &json!({"case":"changed-referent",
        "prepared":prepared,"grant":grant,"actual_error":error}),
    )
    .unwrap();
    trace.write_all(b"\n").unwrap();
    trace.flush().unwrap();
    assert!(error.contains("binding mismatch"), "{error}");
    eprintln!(
        "file scope refusal trajectory: {}",
        report.join("actual.jsonl").display()
    );
}

#[tokio::test]
async fn scoped_file_grants_and_confirmed_approvals_refuse_foreign_execution_effects() {
    let dataset: ExecutionScopeDataset =
        serde_json::from_str(include_str!("fixtures/execution_scope.json")).unwrap();
    let root = tempfile::tempdir().unwrap();
    let reports = tempfile::Builder::new()
        .prefix("kolyan-file-execution-scopes-")
        .tempdir()
        .unwrap()
        .keep();
    let mut trace = std::fs::File::create(reports.join("actual.jsonl")).unwrap();
    let tools = IsolatedFileTools::new(IsolatedFileConfig {
        workspace: root.path().into(),
        worker: env!("CARGO_BIN_EXE_kolyan-test-tool-worker").into(),
        protected_roots: vec![],
        file_limits: FileOperationLimits {
            max_read_bytes: 1024,
            max_write_bytes: 1024,
        },
        max_output_bytes: 1024 * 1024,
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let admitted_scope = dataset.admitted_scope.clone();
    let mut exported_rows = 0;
    for mode in &dataset.modes {
        let path = mode.call.arguments["path"]
            .as_str()
            .expect("file fixture requires path");
        let prepared = tools.prepare(mode.call.clone()).unwrap();
        let mut policy = PolicyEngine::default();
        policy.register(ToolManifest {
            tool_name: "file.write".into(),
            capabilities: [Capability::FilesystemWrite].into_iter().collect(),
            effects: [Effect::Update].into_iter().collect(),
            path_scopes: vec![],
            idempotency: Idempotency::NonIdempotent,
            approval: match mode.approval {
                ScopeApproval::Allow => ApprovalMode::Never,
                ScopeApproval::Confirmed => ApprovalMode::Always,
            },
        });
        let decision = policy.decide_prepared(&prepared, &PolicyContext::default());
        let evidence = match mode.approval {
            ScopeApproval::Confirmed => ApprovalEvidence::Confirmed {
                scope: admitted_scope.clone(),
                prepared_digest: prepared.digest().into(),
                policy_revision: policy.revision(),
                evidence_id: "verified-fixture-approval".into(),
            },
            ScopeApproval::Allow => ApprovalEvidence::NotConfirmed,
        };
        let grant = PreparedGrant::issue(
            &prepared,
            decision.clone(),
            evidence,
            admitted_scope.clone(),
        )
        .unwrap();
        for mutation in &dataset.mutations {
            let expected_scope = mutation.apply(&admitted_scope);
            let case_id = format!("{}-foreign-{}", mode.id, mutation.id);
            let result = tools
                .execute(
                    &prepared,
                    &grant,
                    &policy.revision(),
                    &expected_scope,
                    SandboxCancellation::default(),
                )
                .await;
            let effect_exists = root.path().join(path).exists();
            serde_json::to_writer(
                &mut trace,
                &json!({
                    "fixture_id":dataset.id, "case_fixture_id":case_id, "case":case_id,
                    "mode_fixture_id":mode.id, "mutation_fixture_id":mutation.id,
                    "prepared":prepared, "grant":grant, "expected_scope":expected_scope,
                    "effect_exists":effect_exists,
                    "actual_error":result.as_ref().err().map(ToString::to_string),
                    "actual_output":result.as_ref().ok(),
                }),
            )
            .unwrap();
            trace.write_all(b"\n").unwrap();
            trace.flush().unwrap();
            exported_rows += 1;
            assert_eq!(
                result.is_err(),
                mutation.expected_refusal.refused,
                "{case_id}"
            );
            assert_eq!(
                effect_exists, mutation.expected_refusal.effect_exists,
                "{case_id}"
            );
            assert!(
                matches!(
                    result,
                    Err(kolyan_tools::IsolatedFileError::Prepared(
                        kolyan_policy::PreparedError::BindingMismatch
                    ))
                ),
                "{case_id}: {result:?}; trace {}",
                reports.display()
            );
            assert!(
                result
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains(&mutation.expected_refusal.error_contains),
                "{case_id}"
            );
            assert!(!effect_exists, "{case_id} caused an unauthorized write");
            if mode.check_foreign_approval {
                let result = PreparedGrant::issue(
                    &prepared,
                    decision.clone(),
                    ApprovalEvidence::Confirmed {
                        scope: expected_scope.clone(),
                        prepared_digest: prepared.digest().into(),
                        policy_revision: policy.revision(),
                        evidence_id: "foreign-fixture-approval".into(),
                    },
                    admitted_scope.clone(),
                );
                let case_id = format!("foreign-approval-{}", mutation.id);
                let effect_exists = root.path().join(path).exists();
                serde_json::to_writer(
                    &mut trace,
                    &json!({
                        "fixture_id":dataset.id, "case_fixture_id":case_id, "case":case_id,
                        "mode_fixture_id":mode.id, "mutation_fixture_id":mutation.id,
                        "expected_scope":admitted_scope, "approval_scope":expected_scope,
                        "effect_exists":effect_exists, "actual_grant":result.as_ref().ok(),
                        "actual_error":result.as_ref().err().map(ToString::to_string),
                    }),
                )
                .unwrap();
                trace.write_all(b"\n").unwrap();
                trace.flush().unwrap();
                exported_rows += 1;
                assert_eq!(
                    result.is_err(),
                    mutation.expected_refusal.refused,
                    "{case_id}"
                );
                assert_eq!(
                    effect_exists, mutation.expected_refusal.effect_exists,
                    "{case_id}"
                );
                assert!(
                    result
                        .as_ref()
                        .unwrap_err()
                        .to_string()
                        .contains(&mutation.expected_refusal.error_contains),
                    "{case_id}"
                );
                assert_eq!(result, Err(kolyan_policy::PreparedError::BindingMismatch));
                assert!(!effect_exists, "{case_id} caused an unauthorized write");
            }
        }
        let result = tools
            .execute(
                &prepared,
                &grant,
                &policy.revision(),
                &admitted_scope,
                SandboxCancellation::default(),
            )
            .await;
        let effect = std::fs::read(root.path().join(path));
        serde_json::to_writer(&mut trace, &json!({
            "fixture_id":dataset.id, "case_fixture_id":mode.matching_case_id, "case":mode.matching_case_id,
            "mode_fixture_id":mode.id, "expected_scope":admitted_scope,
            "actual_output":result.as_ref().ok(), "actual_error":result.as_ref().err().map(ToString::to_string),
            "actual_effect":effect.as_ref().ok(), "effect_exists":root.path().join(path).exists(),
        })).unwrap();
        trace.write_all(b"\n").unwrap();
        trace.flush().unwrap();
        exported_rows += 1;
        assert_eq!(
            result.is_err(),
            mode.expected_matching.refused,
            "{}",
            mode.matching_case_id
        );
        assert_eq!(
            root.path().join(path).exists(),
            mode.expected_matching.effect_exists
        );
        assert_eq!(result.unwrap().bytes, mode.expected_matching.result_bytes);
        assert_eq!(effect.unwrap(), mode.expected_matching.content.as_bytes());
    }
    assert_eq!(exported_rows, dataset.expected_rows);
    eprintln!(
        "file execution scope trajectory: {}",
        reports.join("actual.jsonl").display()
    );
}
