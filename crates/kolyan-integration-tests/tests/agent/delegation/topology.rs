//! Data-owned recursive calls and independently recoverable child approvals.
//! Network runs never consume scripted frames or manufacture model calls.

mod comparison;
mod minimax_live;

use std::{collections::BTreeSet, fs, os::unix::fs::DirBuilderExt, sync::Arc};

use kolyan_agent::{
    AgentChildDriveResult, AgentChildrenPumpResult, ChildApprovalResumeRequest, DelegationOwner,
    EnvironmentTool, RootRunRequest,
};
use kolyan_core::{
    CheckpointCallState, ExternalWait, IssuedToolAuthority, TurnConfig, TurnOutcome, TurnRequest,
    TurnSuspension,
};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore};
use kolyan_model::{ModelProvider, ModelRef};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{CancellationPolicy, ExecutionRef, TaskLimits, TaskState};
use serde::Deserialize;
use serde_json::{Value, json};

use super::super::{data, evidence::Evidence, harness, matrix, tools};
use super::{
    Case as SingleCase,
    host::{ChildPlan, Host},
    overlap::ProviderOverlap,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    target: String,
    input: String,
    initial_content: String,
    tools: Vec<EnvironmentTool>,
    approval_tools: Vec<String>,
    expected_approvals: usize,
    expected_simultaneous_waits: usize,
    expected_file_receipts: usize,
    expected_delegate_receipts: usize,
    expected_instances: usize,
    max_transitions: usize,
    parent_frames: Vec<Value>,
    child_frames: Vec<data::Frame>,
    recursive_frames: Option<Vec<data::Frame>>,
}

fn cases() -> Vec<Case> {
    serde_json::from_str(include_str!("../../fixtures/agent/topology.json")).unwrap()
}

pub(super) fn policy_inventory() -> Vec<String> {
    cases().into_iter().map(|case| case.id).collect()
}

pub(super) async fn run_policy_case(
    id: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &tools::worker::WorkerRun,
    policy: kolyan_core::ToolErrorPolicy,
) {
    let case = cases()
        .into_iter()
        .find(|case| case.id == id)
        .expect("registered topology case");
    run_with_policy(case, model, live, installation, policy).await;
}

#[tokio::test]
async fn recursive_self_and_multiple_child_approvals_offline() {
    execute(None).await;
}

#[tokio::test]
#[ignore = "actual root/recursive/approval child model calls across every configured deployment"]
async fn actual_model_recursive_self_and_multiple_child_approvals_matrix() {
    execute(Some(super::super::deployments())).await;
}

async fn execute(deployments: Option<Vec<super::super::Deployment>>) {
    let cases = cases();
    let installation = tools::worker::WorkerRun::prepare().await;
    if let Some(deployments) = deployments {
        let labels = deployments
            .iter()
            .flat_map(|deployment| {
                cases.iter().map(move |case| {
                    format!(
                        "agent/topology/{}/{}/{}/{}",
                        deployment.family, deployment.surface, deployment.model, case.id
                    )
                })
            })
            .collect::<Vec<_>>();
        let mut report = matrix::Matrix::new(labels);
        let mut index = 0;
        for deployment in deployments {
            for case in &cases {
                report
                    .run(
                        index,
                        run(
                            case.clone(),
                            ModelRef::new(deployment.family, &deployment.model),
                            Some(deployment.build()),
                            &installation,
                        ),
                    )
                    .await;
                index += 1;
            }
        }
        assert!(
            report.complete(),
            "Topology evidence: {}",
            report.directory.display()
        );
    } else {
        let mut report = matrix::Matrix::new(cases.iter().map(|case| case.id.clone()));
        for (index, case) in cases.into_iter().enumerate() {
            report
                .run(
                    index,
                    run(
                        case,
                        ModelRef::new("fixture", "topology"),
                        None,
                        &installation,
                    ),
                )
                .await;
        }
        assert!(
            report.complete(),
            "Topology evidence: {}",
            report.directory.display()
        );
    }
}

struct Pending {
    owner: DelegationOwner,
    issued: IssuedToolAuthority,
    wait: ExternalWait,
    checkpoint: String,
}

fn pending(owner: DelegationOwner, suspension: &TurnSuspension) -> Pending {
    let (issued, wait) = suspension
        .checkpoint
        .calls
        .iter()
        .find_map(|call| match &call.state {
            CheckpointCallState::AwaitingExternal { issued, wait } => {
                Some((issued.clone(), wait.clone()))
            }
            _ => None,
        })
        .expect("actual model-generated child wait");
    Pending {
        owner,
        issued,
        wait,
        checkpoint: suspension.checkpoint.checkpoint_id.clone(),
    }
}

async fn run(
    case: Case,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &tools::worker::WorkerRun,
) {
    run_with_policy(
        case,
        model,
        live,
        installation,
        kolyan_core::ToolErrorPolicy::FailTurn,
    )
    .await;
}

async fn run_with_policy(
    case: Case,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &tools::worker::WorkerRun,
    policy: kolyan_core::ToolErrorPolicy,
) {
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-topology-")
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
        "AGENT_TOPOLOGY_TRACE={}",
        root.join("actual.jsonl").display()
    );
    tools::initialize_worker(&root, &evidence, installation).unwrap();
    let single = SingleCase {
        id: case.id.clone(),
        target: case.target.clone(),
        parent_input: case.input.clone(),
        fault: "none".into(),
        restart: true,
        live: live.is_some(),
        expected_terminal: "completed".into(),
        expected_error: false,
        expected_reads: case.expected_file_receipts,
    };
    let open = || {
        Host::open_plan_with_policy(&root,&single,&model,live.clone(),evidence.clone(),Some(ChildPlan {
        tools:case.tools.clone(),instructions:"Follow the explicit private input; use only actual admitted tools and report actual results.".into(),
        parent_frames:case.parent_frames.clone(),child_frames:case.child_frames.clone(),
        recursive_frames:case.recursive_frames.clone(),approval_tools:case.approval_tools.clone(),
        overlap:ProviderOverlap::new(false,evidence.clone()),
    }), policy)
    };
    let mut host = open();
    let execution = ExecutionRef {
        session_id: "logical-session".into(),
        turn_id: "topology-root-turn".into(),
        execution_id: "topology-root-execution".into(),
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
                max_depth: 2,
                max_invocations: 3,
                max_attempts: 3,
                max_tokens: None,
                max_steps_per_turn: 24,
            },
            cancellation_policy: CancellationPolicy::AllInvocations,
            turn: request.clone(),
        })
        .await;
    host.export(&case.id, &evidence);
    evidence.append(json!({"event":"topology_start","error":started.as_ref().err().map(ToString::to_string)})).unwrap();
    let started = started.unwrap();
    let DurableTurnResult::Suspended { suspension, .. } = started.execution else {
        panic!("root must actually delegate")
    };
    let owner = DelegationOwner {
        task_id: case.id.clone(),
        logical_session_id: execution.session_id.clone(),
        parent: started.task.attempts["root-attempt"].binding.clone(),
        scope: suspension.checkpoint.scope.clone(),
    };
    let mut stack = vec![pending(owner, &suspension)];
    drop(suspension);
    let mut template = request;
    template.model_request.messages.clear();
    template.model_request.tools.clear();
    let mut confirmations = 0;
    let mut maximum_waits = 0;
    let mut completed = false;
    for transition in 0..case.max_transitions {
        let node = stack.last().expect("nonempty progress stack");
        for actual in node.issued.prepared.call().arguments["children"]
            .as_array()
            .unwrap()
        {
            let kind = if case.target == "self" {
                "self_call"
            } else {
                "named"
            };
            assert_eq!(
                actual["target"]["kind"], kind,
                "actual model must choose the declared topology"
            );
        }
        let driven = host
            .runner
            .drive_agent_children(
                node.owner.clone(),
                node.issued.clone(),
                node.wait.clone(),
                template.clone(),
            )
            .await;
        host.export(&case.id, &evidence);
        evidence.append(json!({"event":"topology_drive","transition":transition,"depth":stack.len(),"error":driven.as_ref().err().map(ToString::to_string)})).unwrap();
        let driven = driven.unwrap();
        let mut approvals = Vec::new();
        let mut descendants = Vec::new();
        for child in driven {
            match child {
                AgentChildDriveResult::Terminal {
                    result,
                    dispatch_error,
                } => {
                    evidence.append(json!({"event":"topology_child_terminal","result":result,"dispatch_error":dispatch_error})).unwrap();
                    assert!(dispatch_error.is_none());
                }
                AgentChildDriveResult::Waiting { child, execution } => {
                    let DurableTurnResult::Suspended { suspension, .. } = *execution else {
                        panic!("waiting child needs physical suspension")
                    };
                    evidence.append(json!({"event":"topology_child_wait","child":child,"suspension":suspension})).unwrap();
                    for approval in &suspension.waiting.approvals {
                        approvals.push(ChildApprovalResumeRequest {
                            owner: node.owner.clone(),
                            issued: node.issued.clone(),
                            wait: node.wait.clone(),
                            child_invocation_id: child.attempt.invocation_id.clone(),
                            checkpoint_id: suspension.checkpoint.checkpoint_id.clone(),
                            approval_id: approval.approval_id.clone(),
                        });
                    }
                    if !suspension.waiting.external_waits.is_empty() {
                        descendants.push(pending(
                            DelegationOwner {
                                task_id: case.id.clone(),
                                logical_session_id: "logical-session".into(),
                                parent: child.attempt,
                                scope: suspension.checkpoint.scope.clone(),
                            },
                            &suspension,
                        ));
                    }
                }
            }
        }
        maximum_waits = maximum_waits.max(approvals.len());
        if !approvals.is_empty() {
            for approval in &approvals {
                let children = host
                    .runner
                    .verify_agent_child_wait(
                        approval.owner.clone(),
                        approval.issued.clone(),
                        approval.wait.clone(),
                    )
                    .await
                    .unwrap();
                let child = children
                    .iter()
                    .find(|child| child.attempt.invocation_id == approval.child_invocation_id)
                    .unwrap();
                let events = host
                    .ledger
                    .execution_events_after(&child.attempt.execution.execution_id, 0)
                    .unwrap();
                evidence.append(json!({"event":"child_before_confirmation","invocation_id":child.attempt.invocation_id,"events":events})).unwrap();
                assert!(
                    !events.iter().any(|event| matches!(
                        event.kind,
                        LedgerEventKind::EffectStarted | LedgerEventKind::EffectReceipt
                    )),
                    "approval must precede every physical child effect"
                );
            }
            let before = host
                .ledger
                .execution_events_after("topology-root-execution", 0)
                .unwrap()
                .len();
            // Rebuild every service and factory, retaining only serializable host input.
            let saved = serde_json::to_vec(&approvals).unwrap();
            drop(approvals);
            drop(host);
            host = open();
            let restored: Vec<ChildApprovalResumeRequest> = serde_json::from_slice(&saved).unwrap();
            for approval in restored {
                let events = host
                    .ledger
                    .execution_events_after(&approval.issued.scope.execution.execution_id, 0)
                    .unwrap();
                evidence.append(json!({"event":"approval_restart_input","request":approval,"parent_event_count":events.len()})).unwrap();
                let resumed = host.runner.resume_agent_child_approval(approval).await;
                host.export(&case.id, &evidence);
                evidence.append(json!({"event":"child_approval_resumed","error":resumed.as_ref().err().map(ToString::to_string)})).unwrap();
                assert!(matches!(
                    resumed.unwrap(),
                    AgentChildDriveResult::Terminal {
                        dispatch_error: None,
                        ..
                    }
                ));
                confirmations += 1;
            }
            assert_eq!(
                host.ledger
                    .execution_events_after("topology-root-execution", 0)
                    .unwrap()
                    .len(),
                before,
                "confirming children cannot poll the suspended parent"
            );
            continue;
        }
        if !descendants.is_empty() {
            assert_eq!(
                descendants.len(),
                1,
                "this dataset defines a chain, not a sibling DAG"
            );
            stack.extend(descendants);
            continue;
        }
        let node = stack.pop().unwrap();
        let pumped = host
            .runner
            .pump_agent_children(
                node.owner,
                node.checkpoint,
                node.issued.prepared.call().id.clone(),
                template.clone(),
            )
            .await;
        host.export(&case.id, &evidence);
        evidence.append(json!({"event":"topology_pump","remaining_depth":stack.len(),"error":pumped.as_ref().err().map(ToString::to_string)})).unwrap();
        let AgentChildrenPumpResult::Resumed(result) = pumped.unwrap() else {
            panic!("verified terminals must join")
        };
        assert!(
            matches!(result.execution,DurableTurnResult::Completed(ref execution,_) if matches!(execution.result.outcome,TurnOutcome::FinalAnswer {..}))
        );
        if stack.is_empty() {
            assert_eq!(result.task.state, TaskState::Completed);
            completed = true;
            break;
        }
    }
    host.export(&case.id, &evidence);
    assert!(completed, "bounded topology progress exhausted");
    let state = kolyan_server::TaskCoordinator::new(host.journal.clone())
        .snapshot(&case.id)
        .unwrap();
    let instances: BTreeSet<_> = state
        .attempts
        .values()
        .map(|attempt| attempt.binding.agent.instance_id.clone())
        .collect();
    let sessions: BTreeSet<_> = state
        .attempts
        .values()
        .map(|attempt| attempt.binding.execution.session_id.clone())
        .collect();
    let mut file_receipts = 0;
    let mut delegate_receipts = 0;
    let mut invocation_observations = Vec::new();
    for attempt in state.attempts.values() {
        let mut invocation_file_receipts = 0;
        for event in host
            .ledger
            .execution_events_after(&attempt.binding.execution.execution_id, 0)
            .unwrap()
        {
            if event.kind == LedgerEventKind::EffectReceipt {
                match event.payload["input"]["prepared"]["call"]["name"]
                    .as_str()
                    .unwrap()
                {
                    "agent.invoke" => delegate_receipts += 1,
                    "file.read" => {
                        file_receipts += 1;
                        invocation_file_receipts += 1;
                    }
                    other => panic!("unexpected topology effect {other}"),
                }
            }
        }
        invocation_observations.push(comparison::InvocationObservation {
            row: json!({"event":"invocation_effect_comparison","binding":attempt.binding,"file_receipts":invocation_file_receipts}),
            file_receipts: invocation_file_receipts,
            requires_receipt: !case.approval_tools.is_empty()
                && attempt.binding.invocation_id != "root",
        });
    }
    let consumptions = host
        .journal
        .read(&case.id, 0, 512)
        .unwrap()
        .iter()
        .filter(|fact| fact.draft.kind == "task.terminal_result_consumed")
        .count();
    comparison::export_then_compare(
        &evidence,
        &invocation_observations,
        json!({"event":"topology_comparison","instances":instances,"sessions":sessions,"file_receipts":file_receipts,"delegate_receipts":delegate_receipts,"confirmations":confirmations,"maximum_waits":maximum_waits,"consumptions":consumptions}),
    );
    assert_eq!(instances.len(), case.expected_instances);
    assert_eq!(sessions.len(), case.expected_instances);
    assert_eq!(file_receipts, case.expected_file_receipts);
    assert_eq!(delegate_receipts, case.expected_delegate_receipts);
    assert_eq!(confirmations, case.expected_approvals);
    assert_eq!(maximum_waits, case.expected_simultaneous_waits);
    assert_eq!(consumptions, case.expected_instances - 1);
    assert_eq!(
        fs::read_to_string(root.join("workspace/safe/child-proof.txt")).unwrap(),
        case.initial_content
    );
}
