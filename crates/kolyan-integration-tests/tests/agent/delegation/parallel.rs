//! The same two-child cases run offline and with actual matrix Providers.

pub(super) mod explicit_contract;
mod minimax_live;

use std::{collections::BTreeSet, fs, os::unix::fs::DirBuilderExt, sync::Arc};

use kolyan_agent::{AgentChildrenPumpResult, DelegationOwner, EnvironmentTool, RootRunRequest};
use kolyan_core::{CheckpointCallState, TurnConfig, TurnOutcome, TurnRequest};
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    slots: usize,
    cases: Vec<Case>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    input: String,
    child_instructions: String,
    tools: Vec<EnvironmentTool>,
    initial_content: String,
    expected_content: String,
    expected_peak: usize,
    expected_children: usize,
    expected_receipts: usize,
    expected_receipts_per_child: usize,
    expected_consumptions: usize,
    expected_parent_requests: usize,
    parent_frames: Vec<Value>,
    child_frames: Vec<data::Frame>,
}

fn dataset() -> Dataset {
    serde_json::from_str(include_str!("../../fixtures/agent/parallel_runtime.json")).unwrap()
}

pub(super) fn policy_inventory() -> Vec<String> {
    dataset().cases.into_iter().map(|case| case.id).collect()
}

pub(super) async fn run_policy_case(
    id: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &tools::worker::WorkerRun,
    policy: kolyan_core::ToolErrorPolicy,
) {
    let case = dataset()
        .cases
        .into_iter()
        .find(|case| case.id == id)
        .expect("registered scheduling case");
    run_with_policy(case, model, live, installation, policy).await;
}

#[tokio::test]
async fn two_children_actual_readonly_overlap_and_writable_serialization() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let dataset = dataset();
    assert_eq!(dataset.schema_version, 1);
    assert_eq!(dataset.slots, 2);
    let mut report = matrix::Matrix::new(dataset.cases.iter().map(|case| case.id.clone()));
    for (index, case) in dataset.cases.iter().enumerate() {
        report
            .run(
                index,
                run(
                    case.clone(),
                    ModelRef::new("fixture", "parallel"),
                    None,
                    &installation,
                ),
            )
            .await;
    }
    assert!(
        report.complete(),
        "Actual scheduling evidence: {}",
        report.directory.display()
    );
}

#[tokio::test]
#[ignore = "actual models must generate both child calls; all configured credentials required"]
async fn actual_model_parallel_and_writable_serial_matrix() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let cases = dataset().cases;
    let deployments = super::super::deployments();
    let labels = deployments
        .iter()
        .flat_map(|deployment| {
            cases.iter().map(move |case| {
                format!(
                    "agent/scheduling/{}/{}/{}/{}",
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
        "Actual model scheduling evidence: {}",
        report.directory.display()
    );
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
    let offline = live.is_none();
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-parallel-")
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
        "AGENT_PARALLEL_TRACE={}",
        root.join("actual.jsonl").display()
    );
    tools::initialize_worker(&root, &evidence, installation).unwrap();
    let observation = ProviderOverlap::new(case.expected_peak == 2, evidence.clone());
    let single = SingleCase {
        id: case.id.clone(),
        target: "named".into(),
        parent_input: case.input.clone(),
        fault: "none".into(),
        restart: false,
        live: !offline,
        expected_terminal: "completed".into(),
        expected_error: false,
        expected_reads: 0,
    };
    let host = Host::open_plan_with_policy(
        &root,
        &single,
        &model,
        live,
        evidence.clone(),
        Some(ChildPlan {
            tools: case.tools.clone(),
            instructions: case.child_instructions.clone(),
            parent_frames: case.parent_frames.clone(),
            child_frames: case.child_frames.clone(),
            overlap: observation.clone(),
            recursive_frames: None,
            approval_tools: Vec::new(),
        }),
        policy,
    );
    let execution = ExecutionRef {
        session_id: "logical-session".into(),
        turn_id: "parallel-parent-turn".into(),
        execution_id: "parallel-parent-execution".into(),
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
            task_id: case.id.clone(),
            invocation_id: "root".into(),
            attempt_id: "parent-attempt".into(),
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
    evidence
        .append(
            json!({"event":"start_result","error":started.as_ref().err().map(ToString::to_string)}),
        )
        .unwrap();
    let started = started.unwrap();
    let DurableTurnResult::Suspended { suspension, .. } = started.execution else {
        panic!("parent must wait for both actual children")
    };
    evidence
        .append(json!({"event":"parent_wait","suspension":suspension,"snapshot":started.snapshot}))
        .unwrap();
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
        .expect("actual child admission");
    let owner = DelegationOwner {
        task_id: case.id.clone(),
        logical_session_id: execution.session_id.clone(),
        parent: started.task.attempts["parent-attempt"].binding.clone(),
        scope: suspension.checkpoint.scope.clone(),
    };
    let children = host
        .runner
        .verify_agent_child_wait(owner.clone(), issued.clone(), wait.clone())
        .await
        .unwrap();
    evidence
        .append(json!({"event":"actual_admission","children":children,"issued":issued,"wait":wait}))
        .unwrap();
    assert_eq!(children.len(), case.expected_children);
    assert_eq!(issued.prepared.call().arguments["parallel"], true);
    for actual in issued.prepared.call().arguments["children"]
        .as_array()
        .unwrap()
    {
        assert_eq!(
            actual["target"],
            json!({"kind":"named","value":host.child.key()})
        );
    }
    assert_eq!(
        children
            .iter()
            .map(|child| child.attempt.agent.instance_id.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        case.expected_children
    );
    assert_eq!(
        children
            .iter()
            .map(|child| child.attempt.execution.session_id.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        case.expected_children
    );
    let mut template = request;
    template.model_request.messages.clear();
    template.model_request.tools.clear();
    let driven = host
        .runner
        .drive_agent_children(
            owner.clone(),
            issued.clone(),
            wait.clone(),
            template.clone(),
        )
        .await;
    host.export(&case.id, &evidence);
    evidence.append(json!({"event":"children_driven","error":driven.as_ref().err().map(ToString::to_string),"observed_peak":observation.peak(),"active":observation.active()})).unwrap();
    let driven = driven.unwrap();
    for result in &driven {
        match result {
            kolyan_agent::AgentChildDriveResult::Terminal {result,dispatch_error} => evidence.append(json!({"event":"child_terminal","result":result,"dispatch_error":dispatch_error})).unwrap(),
            kolyan_agent::AgentChildDriveResult::Waiting {child,execution} => { evidence.append(json!({"event":"child_waiting","child":child,"execution":format!("{}",matches!(execution.as_ref(),DurableTurnResult::Suspended{..}))})).unwrap(); panic!("unexpected child wait") }
        }
    }
    let checkpoint = suspension.checkpoint.checkpoint_id.clone();
    let call_id = issued.prepared.call().id.clone();
    drop(suspension);
    drop(driven);
    let resumed = host
        .runner
        .pump_agent_children(owner, checkpoint, call_id, template)
        .await;
    host.export(&case.id, &evidence);
    let actual_content = fs::read_to_string(root.join("workspace/safe/child-proof.txt")).unwrap();
    evidence.append(json!({"event":"parent_resume","error":resumed.as_ref().err().map(ToString::to_string),"physical_content":actual_content})).unwrap();
    let AgentChildrenPumpResult::Resumed(result) = resumed.unwrap() else {
        panic!("all children must join before final parent completion")
    };
    assert!(
        matches!(result.execution,DurableTurnResult::Completed(ref execution,_) if matches!(execution.result.outcome,TurnOutcome::FinalAnswer{..}))
    );
    assert_eq!(result.task.state, TaskState::Completed);
    let mut receipt_count = 0;
    let mut intervals = Vec::new();
    for child in &children {
        let events = host
            .ledger
            .execution_events_after(&child.attempt.execution.execution_id, 0)
            .unwrap();
        let start = events
            .iter()
            .find(|event| event.kind == LedgerEventKind::TurnStarted)
            .expect("actual child Turn start")
            .cursor;
        let end = events
            .iter()
            .rev()
            .find(|event| event.kind == LedgerEventKind::TurnCompleted)
            .expect("actual child Turn completion")
            .cursor;
        intervals.push((start, end));
        let child_receipts: Vec<_> = events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
            .collect();
        evidence
            .append(
                json!({"event":"child_effect_comparison","execution":child.attempt.execution,
            "receipts":child_receipts,"expected_receipts":case.expected_receipts_per_child}),
            )
            .unwrap();
        assert_eq!(child_receipts.len(), case.expected_receipts_per_child);
        for receipt in &child_receipts {
            assert_eq!(
                receipt.payload["input"]["prepared"]["call"]["name"],
                case.tools[0].name()
            );
        }
        receipt_count += child_receipts.len();
    }
    let overlapping = intervals[0].0 < intervals[1].1 && intervals[1].0 < intervals[0].1;
    let consumptions = host
        .journal
        .read(&case.id, 0, 512)
        .unwrap()
        .iter()
        .filter(|fact| fact.draft.kind == "task.terminal_result_consumed")
        .count();
    let parent_events = host
        .ledger
        .execution_events_after(&execution.execution_id, 0)
        .unwrap();
    evidence.append(json!({"event":"scheduling_comparison","intervals":intervals,"actual_overlap":overlapping,"provider_invocation_peak":observation.peak(),"receipts":receipt_count,"consumptions":consumptions,"expected_peak":case.expected_peak,"slots":2})).unwrap();
    assert_eq!(overlapping, case.expected_peak == 2);
    assert_eq!(observation.peak(), case.expected_peak);
    assert!(observation.peak() <= 2);
    assert_eq!(observation.active(), 0);
    assert_eq!(receipt_count, case.expected_receipts);
    assert_eq!(consumptions, case.expected_consumptions);
    assert_eq!(actual_content, case.expected_content);
    assert_eq!(
        parent_events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
            .count(),
        1
    );
    if offline {
        assert_eq!(
            parent_events
                .iter()
                .filter(|event| event.kind == LedgerEventKind::ModelRequested)
                .count(),
            case.expected_parent_requests
        );
    }
}
