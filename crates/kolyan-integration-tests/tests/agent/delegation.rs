//! Real Runner delegation with data-owned model output and durable OS effects.
//! Offline scripts are not evidence that a network model generated agent.invoke.

mod advertisement_wire;
mod definition;
mod error_feedback;
mod explicit_contract;
mod host;
mod minimax_live;
mod observed_replay;
mod overlap;
mod parallel;
mod topology;

use std::sync::Arc;

use kolyan_agent::{AgentChildrenPumpResult, DelegationOwner, RootRunRequest};
use kolyan_core::{CheckpointCallState, TurnConfig, TurnOutcome, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore};
use kolyan_model::{ModelProvider, ModelRef};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{CancellationPolicy, ExecutionRef, TaskLimits};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{data, evidence::Evidence, harness, matrix};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    expected_parent_read_receipts: usize,
    expected_result_consumptions: usize,
    expected_offline_parent_requests: usize,
    child_input: String,
    instructions: String,
    parent_frames: Vec<Value>,
    child_frames: Vec<data::Frame>,
    cases: Vec<Case>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    target: String,
    parent_input: String,
    fault: String,
    restart: bool,
    live: bool,
    expected_terminal: String,
    expected_error: bool,
    expected_reads: usize,
}

fn dataset() -> Dataset {
    serde_json::from_str(include_str!("../fixtures/agent/delegation.json")).unwrap()
}

#[tokio::test]
async fn delegation_offline_actual_runner_and_workers() {
    let installation = super::tools::worker::WorkerRun::prepare().await;
    let dataset = dataset();
    assert_eq!(dataset.schema_version, 1);
    assert!(!dataset.child_input.trim().is_empty());
    let mut report = matrix::Matrix::new(dataset.cases.iter().map(|case| case.id.clone()));
    for (index, case) in dataset.cases.iter().enumerate() {
        report
            .run(
                index,
                run(
                    case.clone(),
                    ModelRef::new("fixture", "delegation"),
                    None,
                    &installation,
                ),
            )
            .await;
    }
    assert!(
        report.complete(),
        "Delegation evidence: {}",
        report.directory.display()
    );
}

#[tokio::test]
#[ignore = "actual root and child model delegation; requires all configured credentials"]
async fn actual_model_delegation_matrix() {
    let installation = super::tools::worker::WorkerRun::prepare().await;
    let cases: Vec<_> = dataset()
        .cases
        .into_iter()
        .filter(|case| case.live)
        .collect();
    let deployments = super::deployments();
    let labels = deployments
        .iter()
        .flat_map(|deployment| {
            cases.iter().map(move |case| {
                format!(
                    "agent/delegation/{}/{}/{}/{}",
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
        "Delegation live evidence: {}",
        report.directory.display()
    );
}

async fn run(
    case: Case,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &super::tools::worker::WorkerRun,
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
    installation: &super::tools::worker::WorkerRun,
    policy: kolyan_core::ToolErrorPolicy,
) {
    let offline = live.is_none();
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-delegation-")
        .tempdir()
        .unwrap()
        .keep();
    std::fs::create_dir_all(root.join("workspace/safe")).unwrap();
    std::fs::create_dir(root.join("state")).unwrap();
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join("staging"))
            .unwrap();
    }
    std::fs::write(
        root.join("workspace/safe/child-proof.txt"),
        "CHILD-REAL-PROOF",
    )
    .unwrap();
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    super::tools::initialize_worker(&root, &evidence, installation).unwrap();
    println!(
        "AGENT_DELEGATION_TRACE={}",
        root.join("actual.jsonl").display()
    );
    evidence.append(json!({"event":"plan","case":case.id,"live":live.is_some(),"parallel_acceptance":false,"strict_token_budget":false})).unwrap();
    let mut host = host::Host::open_plan_with_policy(
        &root,
        &case,
        &model,
        live.clone(),
        evidence.clone(),
        None,
        policy,
    );
    let execution = ExecutionRef {
        session_id: "logical-session".into(),
        turn_id: "parent-turn".into(),
        execution_id: "parent-execution".into(),
    };
    let mut input = case.parent_input.clone();
    if case.target == "inline" {
        input.push_str(&format!(
            " Inline definition: {}",
            serde_json::to_string(&host.child).unwrap()
        ));
    }
    let mut source = data::dataset().turns.remove(0);
    source.input = input.clone();
    let request = TurnRequest {
        turn_id: execution.turn_id.clone(),
        config: TurnConfig {
            max_steps: 24,
            max_tool_calls: Some(24),
            deadline: None,
        },
        model_request: harness::request(&source, &model, 8192),
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
            objective: input,
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
    let snapshot = started.snapshot.clone();
    let DurableTurnResult::Suspended { suspension, .. } = started.execution else {
        panic!("parent must suspend for actual child admission")
    };
    evidence
        .append(json!({"event":"parent_suspended","suspension":suspension,"snapshot":snapshot}))
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
        .expect("real child wait");
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
        .append(
            json!({"event":"admitted_children","children":children,"issued":issued,"wait":wait}),
        )
        .unwrap();
    let actual_input = &issued.prepared.call().arguments;
    evidence.append(json!({"event":"delegation_target_expectation","expected":case.target,"actual":actual_input})).unwrap();
    assert_eq!(issued.prepared.call().name, kolyan_agent::AGENT_INVOKE_NAME);
    assert_eq!(actual_input["parallel"], false);
    assert_eq!(actual_input["children"].as_array().unwrap().len(), 1);
    assert_eq!(
        actual_input["children"][0]["target"]["kind"],
        if case.target == "self" {
            "self_call"
        } else {
            case.target.as_str()
        }
    );
    if case.target == "named" {
        assert_eq!(
            actual_input["children"][0]["target"]["value"],
            json!(host.child.key())
        );
    } else if case.target == "inline" {
        assert_eq!(
            definition::validated(actual_input["children"][0]["target"]["value"].clone()).unwrap(),
            host.child
        );
    }
    for fact in host
        .journal
        .read(
            wait.binding["admission"]["stream_id"].as_str().unwrap(),
            0,
            512,
        )
        .unwrap()
    {
        evidence
            .append(json!({"event":"child_admission_journal","value":fact}))
            .unwrap();
    }
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
    evidence
        .append(
            json!({"event":"drive_result","error":driven.as_ref().err().map(ToString::to_string)}),
        )
        .unwrap();
    let driven = driven.unwrap();
    for child in &driven {
        match child {
            kolyan_agent::AgentChildDriveResult::Terminal{result,dispatch_error}=>evidence.append(json!({"event":"child_terminal","result":result,"dispatch_error":dispatch_error})).unwrap(),
            kolyan_agent::AgentChildDriveResult::Waiting{child,..}=>{evidence.append(json!({"event":"child_waiting","child":child})).unwrap();panic!("unexpected child suspension")}
        }
    }
    let consumed = host
        .runner
        .consume_agent_children(owner.clone(), issued.clone(), wait.clone())
        .await;
    host.export(&case.id, &evidence);
    evidence.append(json!({"event":"consumed_result","result":consumed.as_ref().ok(),"error":consumed.as_ref().err().map(ToString::to_string)})).unwrap();
    let consumed = consumed.unwrap();
    evidence.append(json!({"event":"child_expectation","terminal":case.expected_terminal,"is_error":case.expected_error,"read_receipts":case.expected_reads})).unwrap();
    assert_eq!(consumed.is_error, case.expected_error);
    let actual_terminal: Value = serde_json::from_str(&consumed.content).unwrap();
    assert_eq!(
        actual_terminal["children"][0]["outcome"]["status"],
        case.expected_terminal
    );
    // The checkpoint and child result are reloaded from durable stores below.
    // No old suspension, executor or Provider future survives reconstruction.
    let checkpoint_id = suspension.checkpoint.checkpoint_id.clone();
    drop(suspension);
    drop(started.task);
    drop(driven);
    if case.restart {
        drop(host);
        host = host::Host::open_plan_with_policy(
            &root,
            &case,
            &model,
            live,
            evidence.clone(),
            None,
            policy,
        );
        evidence.append(json!({"event":"host_reopened"})).unwrap();
    }
    let recovered = host
        .runner
        .recover_agent_child_wait(owner.clone(), issued.clone())
        .await
        .unwrap();
    evidence
        .append(json!({"event":"recovered_wait","wait":recovered}))
        .unwrap();
    let pumped = host
        .runner
        .pump_agent_children(
            owner,
            checkpoint_id,
            issued.prepared.call().id.clone(),
            template,
        )
        .await;
    host.export(&case.id, &evidence);
    evidence
        .append(
            json!({"event":"pump_result","error":pumped.as_ref().err().map(ToString::to_string)}),
        )
        .unwrap();
    // Verify committed Runtime feedback even when subsequent Task finalization
    // fails. The pump error below remains a failing test, never a substitute pass.
    let feedback_events = host
        .ledger
        .execution_events_after(&execution.execution_id, 0)
        .unwrap();
    let feedback_requests: Vec<_> = feedback_events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::ModelRequested)
        .collect();
    let source = feedback_requests
        .last()
        .expect("parent resume must open an actual model request");
    let feedback = source.payload["request"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .filter(|block| {
            block["type"] == "tool_result"
                && block["result"]["call_id"] == issued.prepared.call().id
        })
        .map(|block| block["result"].clone())
        .collect::<Vec<_>>();
    evidence.append(json!({"event":"parent_feedback_comparison","expected_is_error":case.expected_error,"actual":feedback,"requests":feedback_requests.len()})).unwrap();
    assert_eq!(feedback, vec![json!(consumed)]);
    let expectation = dataset();
    let parent_reads = feedback_events
        .iter()
        .filter(|event| {
            event.kind == LedgerEventKind::EffectReceipt
                && event.payload["input"]["prepared"]["call"]["name"] == "file.read"
        })
        .count();
    assert_eq!(
        parent_reads, expectation.expected_parent_read_receipts,
        "parent cannot substitute its own file read"
    );
    if offline {
        assert_eq!(
            feedback_requests.len(),
            expectation.expected_offline_parent_requests
        );
    }
    let AgentChildrenPumpResult::Resumed(resumed) = pumped.unwrap() else {
        panic!("terminal child must resume parent")
    };
    let DurableTurnResult::Completed(completed, _) = resumed.execution else {
        panic!("parent did not complete")
    };
    evidence
        .append(json!({"event":"parent_completed","outcome":format!("{:?}",completed.result.outcome),"steps":completed.result.steps,"task":resumed.task}))
        .unwrap();
    if case.expected_error {
        assert_ne!(
            resumed.task.state,
            kolyan_server::TaskState::Completed,
            "failed or cancelled child is not task success"
        );
    } else {
        assert_eq!(resumed.task.state, kolyan_server::TaskState::Completed);
    }
    let rows = evidence.rows();
    assert_eq!(children.len(), 1);
    assert_ne!(
        children[0].attempt.agent.instance_id,
        snapshot.identity().instance_id
    );
    assert_ne!(
        children[0].attempt.execution.session_id,
        execution.session_id
    );
    assert_eq!(recovered, Some(wait));
    assert_eq!(consumed.is_error, case.expected_error);
    let terminal: Value = serde_json::from_str(&consumed.content).unwrap();
    assert_eq!(
        terminal["children"][0]["outcome"]["status"],
        case.expected_terminal
    );
    assert!(matches!(
        completed.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    let events = host
        .ledger
        .execution_events_after(&children[0].attempt.execution.execution_id, 0)
        .unwrap();
    let reads = events
        .iter()
        .filter(|event| {
            event.kind == LedgerEventKind::EffectReceipt
                && event.payload["input"]["prepared"]["call"]["name"] == "file.read"
        })
        .count();
    assert_eq!(reads, case.expected_reads);
    let parent_events = host
        .ledger
        .execution_events_after(&execution.execution_id, 0)
        .unwrap();
    let parent_requests: Vec<_> = rows
        .iter()
        .filter(|row| {
            row["event"] == "request" && row["execution"]["execution_id"] == execution.execution_id
        })
        .map(|row| row["request"].clone())
        .collect();
    let committed_requests: Vec<_> = parent_events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::ModelRequested)
        .map(|event| event.payload["request"].clone())
        .collect();
    assert_eq!(
        parent_requests, committed_requests,
        "actual Provider input must equal Core recorded input"
    );
    assert_eq!(
        parent_events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::EffectReceipt
                && event.payload["input"]["prepared"]["call"]["name"] == "agent.invoke")
            .count(),
        1
    );
    assert_eq!(
        parent_events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::ModelRequested)
            .count(),
        completed.result.steps.len()
    );
    for event in events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::ModelRequested)
    {
        assert!(
            !serde_json::to_string(&event.payload["request"]["messages"])
                .unwrap()
                .contains("PARENT-PRIVATE-MARKER")
        );
    }
    assert!(rows.iter().any(|row| row["event"] == "child_terminal"));
    let facts = host.journal.read(&case.id, 0, 512).unwrap();
    assert_eq!(
        facts
            .iter()
            .filter(|fact| fact.draft.kind == "task.terminal_result_consumed")
            .count(),
        expectation.expected_result_consumptions
    );
}
