//! Actual macOS process enforcement, not real-model acceptance.
#![cfg(target_os = "macos")]

use std::io::Write;
use std::time::Duration;

use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalEvidence, ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyContext,
    PolicyDecisionKind, PolicyEngine, PreparedCall, PreparedGrant, ToolManifest,
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

#[tokio::test]
async fn exact_granted_file_worker_process_matrix() {
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
                let grant =
                    PreparedGrant::issue(prepared, decision, ApprovalEvidence::NotConfirmed)
                        .unwrap();
                tools
                    .execute(
                        prepared,
                        &grant,
                        &policy.revision(),
                        SandboxCancellation::default(),
                    )
                    .await
                    .map_err(|error| error.to_string())
            }
            Err(error) => Err(error.to_string()),
        };
        let record = json!({"case":case.id, "request":call,
            "prepared":prepared.as_ref().ok(), "actual":result});
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
            .execute(input, &grant, revision, SandboxCancellation::default())
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
    )
    .unwrap();
    std::fs::remove_file(root.path().join("public/alias")).unwrap();
    symlink("b", root.path().join("public/alias")).unwrap();
    let error = tools
        .execute(
            &prepared,
            &grant,
            &policy.revision(),
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
