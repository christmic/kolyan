//! Actual approval suspension and host reconstruction across executable changes.

use std::{
    fs,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    sync::Arc,
};

use kolyan_agent::{
    AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentSelector, EnvironmentTool,
    RootApprovalResumeRequest, RootRunRequest,
};
use kolyan_core::{TurnConfig, TurnOutcome, TurnRequest};
use kolyan_ledger::LedgerEventKind;
use kolyan_model::ModelRef;
use kolyan_policy::ApprovalMode;
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{CancellationPolicy, ExecutionRef, SessionService, TaskLimits};
use kolyan_storage::FileSessionStore;
use serde::Deserialize;
use serde_json::json;

use super::{
    super::{data, evidence::Evidence, harness, matrix, tools},
    host::Host,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    max_steps: usize,
    input: String,
    frames: Vec<data::Frame>,
    cases: Vec<Case>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: String,
    expected_success: bool,
    expected_receipts: usize,
    expected_content: Option<String>,
}

#[tokio::test]
async fn approval_reconstruction_uses_immutable_worker_data_cases() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("../../fixtures/agent/worker_pin.json")).unwrap();
    assert_eq!(dataset.schema_version, 1);
    let mut report = matrix::Matrix::new(dataset.cases.iter().map(|case| case.id.clone()));
    for (index, case) in dataset.cases.iter().enumerate() {
        report.run(index, run(case.clone(), &dataset)).await;
    }
    assert!(
        report.complete(),
        "Worker approval evidence: {}",
        report.directory.display()
    );
}

async fn run(case: Case, dataset: &Dataset) {
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-worker-pin-")
        .tempdir()
        .unwrap()
        .keep();
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("staging"))
        .unwrap();
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    println!(
        "AGENT_WORKER_PIN_TRACE={}",
        root.join("actual.jsonl").display()
    );
    // A test-owned mutable build input, never the real shared Cargo output.
    let source = root.join("build-worker");
    fs::copy(env!("CARGO_BIN_EXE_kolyan-test-tool-worker"), &source).unwrap();
    let installation = tools::worker::WorkerRun::prepare_from(&source).await;
    tools::initialize_worker(&root, &evidence, &installation).unwrap();
    SessionService::new(FileSessionStore::new(root.join("state/sessions")).unwrap())
        .create("logical-session")
        .unwrap();
    let mut script = data::dataset();
    script.turns.truncate(1);
    script.turns[0].input = dataset.input.clone();
    script.turns[0].script = dataset.frames.clone();
    for manifest in &mut script.policy {
        if manifest.tool_name == "file.write" {
            manifest.approval = ApprovalMode::Always;
        }
    }
    let model = ModelRef::new("fixture", "pinned-worker");
    let permissions = AgentPermissions {
        tools: [EnvironmentTool::Write].into(),
        ..Default::default()
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "worker-pin-root".into(),
        revision: "r1".into(),
        display_name: None,
        model: model.clone(),
        instructions: script.instructions.clone(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let host = Host::open(
        &root,
        &script,
        Some(&definition),
        &permissions,
        None,
        evidence.clone(),
        None,
    );
    let execution = ExecutionRef {
        session_id: "logical-session".into(),
        turn_id: "turn-establish".into(),
        execution_id: "worker-pin-execution".into(),
    };
    let stopped = host
        .runner
        .start(RootRunRequest {
            task_id: case.id.clone(),
            invocation_id: "root".into(),
            attempt_id: "attempt-1".into(),
            execution: execution.clone(),
            selector: AgentSelector::Named(definition.key()),
            requested_permissions: permissions.clone(),
            objective: dataset.input.clone(),
            limits: TaskLimits {
                max_depth: 1,
                max_invocations: 1,
                max_attempts: 1,
                max_tokens: None,
                max_steps_per_turn: 24,
            },
            cancellation_policy: CancellationPolicy::AllInvocations,
            turn: TurnRequest {
                turn_id: execution.turn_id.clone(),
                config: TurnConfig {
                    max_steps: dataset.max_steps,
                    ..TurnConfig::default()
                },
                model_request: harness::request(
                    &script.turns[0],
                    &model,
                    script.output_reserve_tokens,
                ),
            },
        })
        .await;
    host.export(&execution.execution_id, &case.id, &evidence);
    evidence.append(json!({"event":"initial_result","error":stopped.as_ref().err().map(ToString::to_string)})).unwrap();
    let stopped = stopped.unwrap();
    let DurableTurnResult::Suspended { suspension, .. } = stopped.execution else {
        panic!("must await actual approval")
    };
    assert_eq!(suspension.waiting.approvals.len(), 1);
    let prepared = suspension.checkpoint.calls[0]
        .prepared
        .as_ref()
        .unwrap()
        .clone();
    let confirmation = RootApprovalResumeRequest {
        task_id: case.id.clone(),
        invocation_id: "root".into(),
        logical_session_id: "logical-session".into(),
        attempt_id: "attempt-1".into(),
        approval_id: suspension.waiting.approvals[0].approval_id.clone(),
    };
    evidence
        .append(json!({"event":"saved_authority","prepared":prepared,"suspension":suspension}))
        .unwrap();
    assert!(!root.join("workspace/safe/proof.txt").exists());
    drop(suspension);
    drop(stopped.task);
    drop(host);
    let changed = match case.mutation.as_str() {
        "build" => source,
        "pinned" => tools::worker::verified_worker(&root, &evidence).unwrap(),
        other => panic!("unknown mutation {other}"),
    };
    fs::set_permissions(&changed, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(&changed, b"replaced test executable bytes").unwrap();
    evidence
        .append(json!({"event":"executable_replaced","path":changed,"mutation":case.mutation}))
        .unwrap();
    let host = Host::open(
        &root,
        &script,
        None,
        &permissions,
        None,
        evidence.clone(),
        Some(("establish", 1)),
    );
    let resumed = host.runner.resume_approval(confirmation).await;
    let events = host.export(&execution.execution_id, &case.id, &evidence);
    let content = fs::read_to_string(root.join("workspace/safe/proof.txt")).ok();
    evidence.append(json!({"event":"resume_result","error":resumed.as_ref().err().map(ToString::to_string),"physical_content":content})).unwrap();
    assert_eq!(resumed.is_ok(), case.expected_success);
    let receipts: Vec<_> = events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
        .collect();
    assert_eq!(receipts.len(), case.expected_receipts);
    assert_eq!(content, case.expected_content);
    if case.expected_success {
        assert_eq!(
            receipts[0].payload["input"]["prepared"]["digest"],
            prepared.digest()
        );
        let resumed = resumed.unwrap();
        assert!(
            matches!(resumed.execution, DurableTurnResult::Completed(execution, _) if matches!(execution.result.outcome, TurnOutcome::FinalAnswer { .. }))
        );
    } else {
        assert!(
            resumed
                .err()
                .unwrap()
                .to_string()
                .contains("snapshot digest mismatch")
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == LedgerEventKind::EffectStarted)
                .count(),
            0
        );
    }
}
