//! Offline call-only replay of three retained network failures, not new acceptance.

mod coverage;

use std::{fs, os::unix::fs::DirBuilderExt, sync::Arc};

use kolyan_agent::{AgentChildDriveResult, DelegationOwner, EnvironmentTool, RootRunRequest};
use kolyan_core::{CheckpointCallState, TurnConfig, TurnRequest, TurnSuspension};
use kolyan_ledger::{LedgerEventKind, LedgerStore};
use kolyan_model::ModelRef;
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{CancellationPolicy, ExecutionRef, TaskLimits};
use serde::Deserialize;
use serde_json::json;

use super::super::{data, evidence::Evidence, harness, tools};
use super::{
    Case as SingleCase,
    host::{ChildPlan, Host},
    overlap::ProviderOverlap,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    source_trace: String,
    source_projection: String,
    input: String,
    initial_content: String,
    approval_tools: Vec<String>,
    frames: Vec<data::Frame>,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    classification: String,
    error_contains: Option<String>,
    root_approvals: usize,
    root_external_waits: usize,
    child_invocations: usize,
    drive_stages: usize,
}

struct Actual {
    approvals: usize,
    external_waits: usize,
    child_invocations: usize,
    error: Option<String>,
    environment_receipts: usize,
    requests: usize,
    issued_calls: Vec<serde_json::Value>,
}

#[tokio::test]
async fn observed_authority_failures_replay_through_actual_runner_before_effects() {
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../fixtures/agent/observed_authority_replay.json"
    ))
    .unwrap();
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut observations = Vec::new();
    for case in cases {
        let observed = observe(&case, &installation).await;
        observations.push((case, observed));
    }
    // Every case's complete trace exists before the first semantic comparison.
    for (case, actual) in observations {
        assert_eq!(
            actual.approvals, case.expected.root_approvals,
            "{}",
            case.id
        );
        assert_eq!(
            actual.external_waits, case.expected.root_external_waits,
            "{}",
            case.id
        );
        assert_eq!(
            actual.child_invocations, case.expected.child_invocations,
            "{}",
            case.id
        );
        assert_eq!(
            actual.environment_receipts, 0,
            "refusal/approval precedes OS effects"
        );
        assert_eq!(
            actual.requests,
            case.frames.len(),
            "no hidden recovery/retry model calls"
        );
        match &case.expected.error_contains {
            Some(expected) => assert!(
                actual
                    .error
                    .as_ref()
                    .is_some_and(|error| error.contains(expected)),
                "{}: {:?}",
                case.id,
                actual.error
            ),
            None => assert!(actual.error.is_none()),
        }
        // Typed denial provenance is confirmed against the actual generated calls.
        match case.expected.classification.as_str() {
            "invalid_environment_tool" => assert_eq!(
                actual.issued_calls.last().unwrap()["children"][0]["permissions"]["tools"][0],
                "agent.invoke"
            ),
            "effective_ceiling_expansion" => {
                assert_eq!(
                    actual.issued_calls[1]["children"][0]["permissions"]["tools"],
                    json!([])
                );
                assert_eq!(
                    actual.issued_calls[2]["children"][0]["permissions"]["tools"],
                    json!(["file.read"])
                );
            }
            "parent_read_instead_of_child_wait" => assert_eq!(
                actual.issued_calls[0],
                json!({"path":"safe/child-proof.txt"})
            ),
            other => panic!("unsupported data classification {other}"),
        }
    }
}

async fn observe(case: &Case, installation: &tools::worker::WorkerRun) -> Actual {
    let root = tempfile::Builder::new()
        .prefix("kolyan-authority-replay-")
        .tempdir()
        .unwrap()
        .keep();
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("staging"))
        .unwrap();
    fs::write(
        root.join("workspace/safe/child-proof.txt"),
        &case.initial_content,
    )
    .unwrap();
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    println!(
        "AUTHORITY_REPLAY_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let frames: Vec<_> = case.frames.iter().map(|frame|json!({"reasoning":frame.reasoning,"content":frame.content,"usage":frame.usage})).collect();
    evidence.append(json!({"event":"observed_replay_source","case":case.id,"source_trace":case.source_trace,"projection":case.source_projection,"frames":frames,"expected_classification":case.expected.classification,"network_invoked":false})).unwrap();
    tools::initialize_worker(&root, &evidence, installation).unwrap();
    let model = ModelRef::new("fixture", "observed-authority");
    let single = SingleCase {
        id: case.id.clone(),
        target: "self".into(),
        parent_input: case.input.clone(),
        fault: "none".into(),
        restart: true,
        live: false,
        expected_terminal: "diagnostic".into(),
        expected_error: false,
        expected_reads: 0,
    };
    // Parent Frame is a complete data object, not a manufactured ToolInvocation.
    let open = |stage: usize| {
        Host::open_plan(
            &root,
            &single,
            &model,
            None,
            evidence.clone(),
            Some(ChildPlan {
                tools: vec![EnvironmentTool::Read],
                instructions: "Follow the explicit private input; use only admitted permissions."
                    .into(),
                parent_frames: vec![
                    json!({"reasoning":case.frames[0].reasoning,"content":case.frames[0].content,"usage":case.frames[0].usage}),
                ],
                child_frames: vec![],
                recursive_frames: case.frames.get(stage + 1).map(|frame| vec![frame.clone()]),
                approval_tools: case.approval_tools.clone(),
                overlap: ProviderOverlap::new(false, evidence.clone()),
            }),
        )
    };
    let mut host = open(0);
    let execution = ExecutionRef {
        session_id: "logical-session".into(),
        turn_id: "replay-root-turn".into(),
        execution_id: "replay-root-execution".into(),
    };
    let mut turn = data::dataset().turns.remove(0);
    turn.input = case.input.clone();
    let request = TurnRequest {
        turn_id: execution.turn_id.clone(),
        config: TurnConfig {
            max_steps: 24,
            max_tool_calls: Some(24),
            deadline: None,
        },
        model_request: harness::request(&turn, &model, 8192),
    };
    let started = host
        .runner
        .start(RootRunRequest {
            goals: vec![],
            task_id: case.id.clone(),
            invocation_id: "root".into(),
            attempt_id: "root-attempt".into(),
            execution: execution.clone(),
            selector: host.selector(),
            requested_permissions: host.permissions.clone(),
            objective: case.input.clone(),
            limits: TaskLimits {
                max_depth: 4,
                max_invocations: 5,
                max_attempts: 5,
                max_tokens: None,
                max_steps_per_turn: 24,
            },
            cancellation_policy: CancellationPolicy::AllInvocations,
            turn: request.clone(),
        })
        .await;
    host.export(&case.id, &evidence);
    evidence.append(json!({"event":"replay_root_started","error":started.as_ref().err().map(ToString::to_string)})).unwrap();
    let started = started.unwrap();
    let DurableTurnResult::Suspended { suspension, .. } = started.execution else {
        panic!("observed root must stop at a real suspension")
    };
    let approvals = suspension.waiting.approvals.len();
    let external_waits = suspension.waiting.external_waits.len();
    let mut owner = DelegationOwner {
        task_id: case.id.clone(),
        logical_session_id: execution.session_id.clone(),
        parent: started.task.attempts["root-attempt"].binding.clone(),
        scope: suspension.checkpoint.scope.clone(),
    };
    let mut suspension = suspension;
    let mut template = request;
    template.model_request.messages.clear();
    template.model_request.tools.clear();
    let mut error = None;
    for stage in 0..case.expected.drive_stages {
        let (issued, wait) = awaiting(&suspension);
        let driven = host
            .runner
            .drive_agent_children(owner.clone(), issued, wait, template.clone())
            .await;
        host.export(&case.id, &evidence);
        evidence.append(json!({"event":"replay_children_driven","stage":stage,"error":driven.as_ref().err().map(ToString::to_string)})).unwrap();
        let mut next = None;
        for result in driven.unwrap() {
            match result {
                AgentChildDriveResult::Terminal {
                    result,
                    dispatch_error,
                } => {
                    error = dispatch_error.map(|error| error.to_string());
                    evidence.append(json!({"event":"replay_child_terminal","result":result,"dispatch_error":error})).unwrap();
                }
                AgentChildDriveResult::Waiting { child, execution } => {
                    let DurableTurnResult::Suspended {
                        suspension: child_suspension,
                        ..
                    } = *execution
                    else {
                        panic!("waiting requires actual suspension")
                    };
                    next = Some((
                        DelegationOwner {
                            task_id: case.id.clone(),
                            logical_session_id: "logical-session".into(),
                            parent: child.attempt,
                            scope: child_suspension.checkpoint.scope.clone(),
                        },
                        child_suspension,
                    ));
                }
            }
        }
        if let Some((next_owner, next_suspension)) = next {
            owner = next_owner;
            suspension = next_suspension;
            drop(host);
            host = open(stage + 1);
        }
    }
    host.export(&case.id, &evidence);
    let snapshot = kolyan_server::TaskCoordinator::new(host.journal.clone())
        .snapshot(&case.id)
        .unwrap();
    let mut environment_receipts = 0;
    let mut requests = 0;
    for attempt in snapshot.attempts.values() {
        for event in host
            .ledger
            .execution_events_after(&attempt.binding.execution.execution_id, 0)
            .unwrap()
        {
            requests += usize::from(event.kind == LedgerEventKind::ModelRequested);
            if event.kind == LedgerEventKind::EffectReceipt
                && event.payload["input"]["prepared"]["call"]["name"] != "agent.invoke"
            {
                environment_receipts += 1;
            }
        }
    }
    let child_invocations = snapshot.invocations.len() - 1;
    let issued_calls = evidence
        .rows()
        .iter()
        .filter(|row| row["event"] == "model_event")
        .flat_map(|row| {
            row["content"]["Completed"]["content"]
                .as_array()
                .into_iter()
                .flatten()
        })
        .filter(|block| block["type"] == "tool_call")
        .map(|block| block["call"]["arguments"].clone())
        .collect();
    evidence.append(json!({"event":"replay_classification","classification":case.expected.classification,"approvals":approvals,"external_waits":external_waits,"children":child_invocations,"error":error,"environment_receipts":environment_receipts,"requests":requests,"task_state":snapshot.state})).unwrap();
    Actual {
        approvals,
        external_waits,
        child_invocations,
        error,
        environment_receipts,
        requests,
        issued_calls,
    }
}

fn awaiting(
    suspension: &TurnSuspension,
) -> (kolyan_core::IssuedToolAuthority, kolyan_core::ExternalWait) {
    suspension
        .checkpoint
        .calls
        .iter()
        .find_map(|call| match &call.state {
            CheckpointCallState::AwaitingExternal { issued, wait } => {
                Some((issued.clone(), wait.clone()))
            }
            _ => None,
        })
        .expect("data-declared delegated stage requires an actual external wait")
}
