//! Root approval restarts reconstruct actual services and isolated OS workers.
//! The saved snapshot/checkpoint, not the catalog or Provider memory, owns resume.

mod host;
mod worker_pin;

use std::{fs, sync::Arc};

use kolyan_agent::{
    AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentSelector, EnvironmentTool,
    RootApprovalResumeRequest, RootRunRequest,
};
use kolyan_core::{TurnConfig, TurnOutcome, TurnRequest};
use kolyan_ledger::LedgerEventKind;
use kolyan_model::{ModelProvider, ModelRef, ModelRequest};
use kolyan_policy::ApprovalMode;
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{CancellationPolicy, ExecutionRef, SessionService, TaskLimits};
use kolyan_storage::{FileSessionStore, SessionStore};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{data, evidence::Evidence, harness};
use host::Host;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    schema_version: u32,
    id: String,
    selectors: Vec<String>,
    approval_tools: Vec<String>,
    max_suspensions: usize,
    turns: Vec<ExpectedTurn>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedTurn {
    id: String,
    model_requests: usize,
    waits: Vec<ExpectedWait>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedWait {
    tool: String,
    model_requests: usize,
    receipts: usize,
    file_before: Option<String>,
}

pub async fn run(
    selector: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &super::tools::worker::WorkerRun,
) {
    let case: Case =
        serde_json::from_str(include_str!("../fixtures/agent/approval_restart.json")).unwrap();
    assert_eq!(case.schema_version, 1);
    assert!(case.selectors.iter().any(|item| item == selector));
    let mut dataset = data::dataset();
    for manifest in &mut dataset.policy {
        if case.approval_tools.contains(&manifest.tool_name) {
            manifest.approval = ApprovalMode::Always;
        }
    }
    let offline = live.is_none();
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-restart-")
        .tempdir()
        .unwrap()
        .keep();
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join("staging"))
            .unwrap();
    }
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    super::tools::initialize_worker(&root, &evidence, installation).unwrap();
    println!(
        "Agent restart evidence: {}",
        root.join("actual.jsonl").display()
    );
    evidence
        .append(
            json!({"event":"plan","case":case.id,"selector":selector,"model":model,
        "budget_mode":"Inspect","counter":"Unsupported","token_estimate":null,
        "approval_tools":case.approval_tools,"shell_policy_scope":dataset.shell_policy_scope}),
        )
        .unwrap();
    SessionService::new(FileSessionStore::new(root.join("state/sessions")).unwrap())
        .create("logical-session")
        .unwrap();
    let permissions = AgentPermissions {
        tools: [
            EnvironmentTool::Read,
            EnvironmentTool::Write,
            EnvironmentTool::Edit,
            EnvironmentTool::Shell,
        ]
        .into_iter()
        .collect(),
        delegation: Default::default(),
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "restart-root".into(),
        revision: "r1".into(),
        display_name: (selector == "named").then(|| "Restart root".into()),
        model: model.clone(),
        instructions: dataset.instructions.clone(),
        permissions: permissions.clone(),
    })
    .unwrap();
    for turn in &dataset.turns {
        let evidence_start = evidence.rows().len();
        let expected = case.turns.iter().find(|item| item.id == turn.id).unwrap();
        let task_id = format!("task-{}", turn.id);
        let execution = ExecutionRef {
            session_id: "logical-session".into(),
            turn_id: format!("turn-{}", turn.id),
            execution_id: format!("execution-{}", turn.id),
        };
        let mut host = Host::open(
            &root,
            &dataset,
            Some(&definition),
            &permissions,
            live.clone(),
            evidence.clone(),
            None,
        );
        evidence
            .append(json!({"event":"root_started","execution":execution,"task_id":task_id}))
            .unwrap();
        let mut outcome = host
            .runner
            .start(RootRunRequest {
                goals: vec![],
                task_id: task_id.clone(),
                invocation_id: "root".into(),
                attempt_id: "attempt-1".into(),
                execution: execution.clone(),
                selector: match selector {
                    "named" => AgentSelector::Named(definition.key()),
                    "inline" => AgentSelector::Inline(definition.clone()),
                    other => panic!("unknown selector {other}"),
                },
                requested_permissions: permissions.clone(),
                objective: turn.input.clone(),
                limits: TaskLimits {
                    max_depth: 1,
                    max_invocations: 1,
                    max_attempts: 1,
                    max_tokens: None,
                    max_steps_per_turn: u32::try_from(dataset.max_steps).unwrap(),
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
                turn: TurnRequest {
                    turn_id: execution.turn_id.clone(),
                    config: TurnConfig {
                        max_steps: dataset.max_steps,
                        max_tool_calls: Some(dataset.max_tool_calls),
                        deadline: None,
                    },
                    model_request: harness::request(turn, &model, dataset.output_reserve_tokens),
                },
            })
            .await;
        let mut waits = 0;
        let mut original_snapshot = None;
        let mut approved_digests = Vec::new();
        loop {
            let events = host.export(&execution.execution_id, &task_id, &evidence);
            match &outcome {
                Ok(result) => {
                    let execution = match &result.execution {
                        DurableTurnResult::Completed(completed, _) => {
                            json!({"state":"completed","turn_id":completed.result.turn_id,
                                "steps":completed.result.steps,"end_reason":format!("{:?}",completed.result.end_reason),
                                "trajectory_source":"ledger"})
                        }
                        DurableTurnResult::Suspended { suspension, .. } => {
                            json!({"state":"suspended","suspension":suspension,"trajectory_source":"ledger"})
                        }
                    };
                    evidence
                        .append(json!({"event":"root_observed","snapshot":result.snapshot,
                        "task":result.task,"execution":execution}))
                        .unwrap();
                }
                Err(error) => evidence
                    .append(json!({"event":"root_failed","error":error.to_string()}))
                    .unwrap(),
            }
            let result = outcome.unwrap();
            if let Some(snapshot) = &original_snapshot {
                assert_eq!(&result.snapshot, snapshot);
            } else {
                original_snapshot = Some(result.snapshot.clone());
            }
            match result.execution {
                DurableTurnResult::Completed(completed, _) => {
                    evidence.append(json!({"event":"root_completed","snapshot":result.snapshot,"task":result.task})).unwrap();
                    let actual_requests: Vec<Value> = evidence.rows()[evidence_start..]
                        .iter()
                        .filter(|row| row["event"] == "request")
                        .map(|row| row["request"].clone())
                        .collect();
                    let recorded_requests: Vec<Value> = events
                        .iter()
                        .filter(|event| event.kind == LedgerEventKind::ModelRequested)
                        .map(|event| event.payload["request"].clone())
                        .collect();
                    assert_eq!(
                        actual_requests, recorded_requests,
                        "Core input must equal every actual neutral Provider request"
                    );
                    for digest in &approved_digests {
                        assert_eq!(
                            events
                                .iter()
                                .filter(|event| event.kind == LedgerEventKind::EffectReceipt
                                    && event.payload["input"]["prepared"]["digest"] == *digest)
                                .count(),
                            1,
                            "confirmed pending effect must have exactly one receipt"
                        );
                    }
                    assert!(matches!(
                        completed.result.outcome,
                        TurnOutcome::FinalAnswer { .. }
                    ));
                    let observed = super::usage::observe(
                        completed
                            .result
                            .steps
                            .iter()
                            .map(|step| &step.response.usage),
                    );
                    evidence.append(json!({"event":"usage_comparison","task":result.task.usage,"observed":observed})).unwrap();
                    let observed =
                        observed.expect("Task usage comparison must reject arithmetic overflow");
                    assert_eq!(
                        result.task.usage.input_tokens,
                        observed.observed_input_tokens
                    );
                    assert_eq!(
                        result.task.usage.output_tokens,
                        observed.observed_output_tokens
                    );
                    assert_eq!(
                        result.task.usage.unreported_steps,
                        observed.unreported_steps
                    );
                    if offline {
                        assert_eq!(waits, expected.waits.len());
                        assert_eq!(
                            events
                                .iter()
                                .filter(|event| event.kind == LedgerEventKind::ModelRequested)
                                .count(),
                            expected.model_requests
                        );
                    }
                    let initial: ModelRequest = serde_json::from_value(
                        events
                            .iter()
                            .find(|event| event.kind == LedgerEventKind::ModelRequested)
                            .unwrap()
                            .payload["request"]
                            .clone(),
                    )
                    .unwrap();
                    let history = serde_json::to_string(&initial.messages).unwrap();
                    for phrase in &turn.expected.history_contains {
                        assert!(history.contains(phrase));
                    }
                    for (path, content) in &turn.expected.file {
                        assert_eq!(
                            fs::read_to_string(root.join("workspace").join(path)).unwrap(),
                            *content
                        );
                    }
                    for (name, minimum) in &turn.expected.tool_minimums {
                        assert!(
                            events
                                .iter()
                                .filter(|event| event.kind == LedgerEventKind::EffectReceipt
                                    && event.payload["input"]["prepared"]["call"]["name"] == *name)
                                .count()
                                >= *minimum
                        );
                    }
                    assert!(waits > 0, "approval case must actually suspend");
                    break;
                }
                DurableTurnResult::Suspended { suspension, .. } => {
                    evidence.append(json!({"event":"approval_suspended","suspension":suspension,"snapshot":result.snapshot})).unwrap();
                    assert!(
                        waits < case.max_suspensions,
                        "bounded approval scenario exhausted"
                    );
                    assert_eq!(suspension.waiting.approvals.len(), 1);
                    assert!(suspension.waiting.external_waits.is_empty());
                    let approval = &suspension.waiting.approvals[0];
                    let effect_id = format!(
                        "{}/{}",
                        suspension.checkpoint.scope.step_id, approval.call_id
                    );
                    assert!(
                        !events
                            .iter()
                            .any(|event| event.kind == LedgerEventKind::EffectStarted
                                && event.payload["effect_id"] == effect_id),
                        "pending effect must not even start before confirmation"
                    );
                    let prepared = suspension
                        .checkpoint
                        .calls
                        .iter()
                        .find(|call| call.call.id == approval.call_id)
                        .unwrap()
                        .prepared
                        .as_ref()
                        .expect("approval must bind a prepared call");
                    assert_eq!(
                        events
                            .iter()
                            .filter(|event| event.kind == LedgerEventKind::EffectReceipt
                                && event.payload["input"]["prepared"]["digest"]
                                    == prepared.digest())
                            .count(),
                        0,
                        "pending effect must not execute before confirmation"
                    );
                    let consumed = events
                        .iter()
                        .filter(|event| event.kind == LedgerEventKind::ModelRequested)
                        .count();
                    if offline {
                        let wait = &expected.waits[waits];
                        assert_eq!(approval.tool_name, wait.tool);
                        assert_eq!(consumed, wait.model_requests);
                        assert_eq!(
                            events
                                .iter()
                                .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
                                .count(),
                            wait.receipts
                        );
                        let path = root.join("workspace/safe/proof.txt");
                        match &wait.file_before {
                            Some(content) => {
                                assert_eq!(fs::read_to_string(path).unwrap(), *content)
                            }
                            None => assert!(!path.exists()),
                        }
                    }
                    let confirmation = RootApprovalResumeRequest {
                        task_id: task_id.clone(),
                        invocation_id: "root".into(),
                        logical_session_id: "logical-session".into(),
                        attempt_id: "attempt-1".into(),
                        approval_id: approval.approval_id.clone(),
                    };
                    approved_digests.push(prepared.digest().to_owned());
                    // Drop the actual result (including checkpoint), Runner, service,
                    // Provider and all opened stores. Resume only accepts host IDs.
                    drop(suspension);
                    drop(host);
                    evidence
                        .append(json!({"event":"host_dropped","request":confirmation}))
                        .unwrap();
                    host = Host::open(
                        &root,
                        &dataset,
                        None,
                        &permissions,
                        live.clone(),
                        evidence.clone(),
                        Some((&turn.id, consumed)),
                    );
                    evidence.append(json!({"event":"host_restored","catalog":"empty","offline_script_offset_from_ledger":consumed})).unwrap();
                    evidence
                        .append(json!({"event":"approval_confirmed","request":confirmation}))
                        .unwrap();
                    outcome = host.runner.resume_approval(confirmation).await;
                    waits += 1;
                }
            }
        }
    }
    let session = FileSessionStore::new(root.join("state/sessions"))
        .unwrap()
        .load("logical-session")
        .unwrap();
    evidence
        .append(json!({"event":"session","value":session}))
        .unwrap();
    assert_eq!(session.turns.len(), dataset.turns.len());
    let rows = evidence.rows();
    let mut offset = 0;
    for line in include_str!("../expected/agent/approval_restart.jsonl").lines() {
        let expected: Value = serde_json::from_str(line).unwrap();
        offset += rows[offset..]
            .iter()
            .position(|row| row["event"] == expected["event"])
            .unwrap_or_else(|| panic!("missing ordered evidence {expected}"))
            + 1;
    }
}
