//! Actual native execution and durable recovery; Provider mode never changes faults.

use std::{fs, os::unix::fs::DirBuilderExt, path::Path, sync::Arc, time::Duration};

use kolyan_agent::{
    AgentSelector, DelegationOwner, RootRunRequest, TaskFinalizationPolicy, TaskFinalizationRequest,
};
use kolyan_core::{CheckpointCallState, TurnConfig, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{CancellationPolicy, ExecutionRef, SessionService};
use kolyan_storage::FileSessionStore;
use serde_json::{Value, json};

use super::super::{data, evidence::Evidence, harness, tools};
use super::{Case, Dataset, Source, host::Host, ports::State};

pub(super) async fn observe(
    root: &Path,
    evidence: &Arc<Evidence>,
    installation: &tools::worker::WorkerRun,
    dataset: &Dataset,
    case: &Case,
    source: &Source,
    row: &mut Value,
) -> Result<(), String> {
    fs::create_dir_all(root.join("workspace/safe")).map_err(|error| error.to_string())?;
    fs::create_dir(root.join("state")).map_err(|error| error.to_string())?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("staging"))
        .map_err(|error| error.to_string())?;
    tools::initialize_worker(root, evidence, installation)?;
    SessionService::new(
        FileSessionStore::new(root.join("state/sessions")).map_err(|error| error.to_string())?,
    )
    .create("logical-session")
    .map_err(|error| error.to_string())?;
    let state = State::new(
        case.fault.clone(),
        dataset.write_arguments.clone(),
        evidence.clone(),
        dataset.gate_timeout_ms,
    );
    let host = Host::open(root, dataset, state.clone(), source)?;
    evidence.append(json!({"event":"native_plan","case":case,"unapproved_gap":dataset.unapproved_gap,"network_mode":source.live.is_some(),"model":source.model,"native_tool_timeout_ms":30000,"inspection_window_assumption_tokens":dataset.inspection_window_assumption_tokens,"token_counter":"unsupported-no-estimate"}))?;
    let execution = ExecutionRef {
        session_id: "logical-session".into(),
        turn_id: "native-turn".into(),
        execution_id: "native-root-execution".into(),
    };
    if case.fault != "late_child" {
        state.admit(execution.clone())?;
    }
    let mut input = data::dataset().turns.remove(0);
    input.input = if case.fault == "late_child" {
        dataset.parent_input.clone()
    } else {
        dataset.input.clone()
    };
    let turn = TurnRequest {
        turn_id: execution.turn_id.clone(),
        config: TurnConfig {
            max_steps: dataset.max_steps,
            ..Default::default()
        },
        model_request: harness::request(&input, &source.model, 8192),
    };
    let result = host
        .runner
        .start(RootRunRequest {
            task_id: case.id.clone(),
            invocation_id: "root".into(),
            attempt_id: "root-attempt".into(),
            execution: execution.clone(),
            selector: AgentSelector::Named(host.root_definition.key()),
            requested_permissions: host.permissions.clone(),
            objective: input.input,
            limits: kolyan_server::TaskLimits {
                max_depth: 2,
                max_invocations: 2,
                max_attempts: 2,
                max_tokens: None,
                max_steps_per_turn: u32::try_from(dataset.max_steps)
                    .map_err(|error| error.to_string())?,
            },
            cancellation_policy: CancellationPolicy::RootOnly,
            turn: turn.clone(),
        })
        .await;
    row["start_error"] = json!(result.as_ref().err().map(ToString::to_string));
    let execution_error = match result.as_ref().err() {
        Some(kolyan_agent::RunnerError::Execution(error)) => Some(error),
        Some(kolyan_agent::RunnerError::ExecutionAndFinalization { execution, .. }) => {
            Some(execution)
        }
        _ => None,
    };
    row["returned_typed_cancelled"] = json!(matches!(
        execution_error,
        Some(kolyan_server::TaskExecutionError::Server(
            kolyan_server::ServerError::Runtime(kolyan_runtime::RuntimeError::Turn(
                kolyan_core::TurnError::Cancelled
            ))
        ))
    ));
    if case.fault == "late_child" {
        let started = result.map_err(|error| error.to_string())?;
        let DurableTurnResult::Suspended { suspension, .. } = started.execution else {
            return Err("parent has no committed child wait".into());
        };
        let CheckpointCallState::AwaitingExternal { issued, wait } =
            &suspension.checkpoint.calls[0].state
        else {
            return Err("parent checkpoint lacks admitted child".into());
        };
        let owner = DelegationOwner {
            task_id: case.id.clone(),
            logical_session_id: execution.session_id.clone(),
            parent: started.task.attempts["root-attempt"].binding.clone(),
            scope: suspension.checkpoint.scope.clone(),
        };
        late_child(
            &host,
            &state,
            case,
            &owner,
            ChildDispatch {
                issued: issued.clone(),
                wait: wait.clone(),
                template: turn,
                admission_wait_ms: if source.live.is_some() {
                    dataset.live_drive_timeout_ms
                } else {
                    dataset.gate_timeout_ms
                },
            },
            if source.live.is_some() {
                dataset.live_drive_timeout_ms
            } else {
                dataset.drive_timeout_ms
            },
            row,
        )
        .await?;
    }
    host.export(&case.id, evidence)?;
    row["executions"] = json!(state.effects());
    row["completions"] = json!(state.completions());
    row["injected_faults"] = json!(state.injected());
    row["selection_refusals"] = json!(state.refusals());
    let events = host
        .ledger
        .events_after(0)
        .map_err(|error| error.to_string())?;
    let receipts = events
        .iter()
        .filter(|event| {
            event.kind == LedgerEventKind::EffectReceipt
                && event.payload["input"]["prepared"]["call"]["name"] == "file.write"
        })
        .count();
    row["receipts"] = json!(receipts);
    row["uncertain"] = json!(
        events
            .iter()
            .any(|event| event.kind == LedgerEventKind::EffectUncertain)
    );
    row["effects_reconciled"] = json!(
        events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::EffectReconciled)
            .count()
    );
    row["model_requests"] = json!(
        events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::ModelRequested)
            .count()
    );
    row["counts_before_rebuild"] = counts(&host, &state)?;
    row["native_receipt_cursor"] = json!(
        events
            .iter()
            .find(|event| event.kind == LedgerEventKind::EffectReceipt
                && state
                    .selected_call()
                    .is_some_and(|call| event.payload["input"]["prepared"]["call"] == json!(call)))
            .map(|event| event.cursor)
    );
    row["cancel_cursor"] = json!(
        events
            .iter()
            .find(|event| event.kind == LedgerEventKind::ExecutionCancelled
                && event.execution_id == execution.execution_id)
            .map(|event| event.cursor)
    );
    row["stopped_turn_cancelled"] = json!(
        events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::TurnCancelled
                && event.execution_id == execution.execution_id)
            .count()
    );
    row["stopped_cursor"] = json!(
        events
            .iter()
            .find(|event| event.kind == LedgerEventKind::TurnCancelled
                && event.execution_id == execution.execution_id)
            .map(|event| event.cursor)
    );
    for (field, kind) in [
        ("effect_started", LedgerEventKind::EffectStarted),
        ("effect_uncertain", LedgerEventKind::EffectUncertain),
        ("turn_completed", LedgerEventKind::TurnCompleted),
    ] {
        row[field] = json!(events.iter().filter(|event| event.kind == kind).count());
    }
    row["started_cursor"] = json!(
        events
            .iter()
            .find(|event| event.kind == LedgerEventKind::EffectStarted)
            .map(|event| event.cursor)
    );
    row["uncertain_cursor"] = json!(
        events
            .iter()
            .find(|event| event.kind == LedgerEventKind::EffectUncertain)
            .map(|event| event.cursor)
    );
    let restored_state = State::new(
        "none".into(),
        state.expected_write.clone(),
        evidence.clone(),
        dataset.gate_timeout_ms,
    );
    drop(host);
    let restored = Host::open(root, dataset, restored_state.clone(), source)?;
    tools::worker::verified_worker(root, evidence)?;
    if case.fault != "late_child" {
        row["recovery_error"] = json!(
            restored
                .service
                .sessions()
                .execution()
                .server()
                .recover(execution.clone())
                .err()
                .map(|error| error.to_string())
        );
        restored
            .service
            .reconcile(&case.id, "root-attempt")
            .map_err(|error| error.to_string())?;
    }
    let request = TaskFinalizationRequest {
        task_id: case.id.clone(),
        logical_session_id: "logical-session".into(),
        root_invocation_id: "root".into(),
        root_attempt_id: "root-attempt".into(),
        policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
    };
    let mut states = Vec::new();
    for _ in 0..2 {
        let finalized = restored.runner.finalize_task(request.clone()).await;
        evidence.append(json!({"event":"native_finalize","result":finalized.as_ref().ok(),"error":finalized.as_ref().err().map(ToString::to_string)}))?;
        if let Ok(task) = finalized {
            states.push(json!(task.state));
        }
    }
    row["finalize_states"] = json!(states);
    row["task_state"] = json!(
        restored
            .service
            .coordinator()
            .snapshot(&case.id)
            .map_err(|error| error.to_string())?
            .state
    );
    row["result_consumptions"] = json!(
        restored
            .service
            .coordinator()
            .snapshot(&case.id)
            .map_err(|error| error.to_string())?
            .invocations
            .values()
            .map(|inv| inv.consumed_results.len() + inv.terminal_consumed_results.len())
            .sum::<usize>()
    );
    row["counts_after_rebuild"] = counts(&restored, &state)?;
    if restored_state.effects() != 0 {
        return Err("reconstruction entered native effect".into());
    }
    restored.export(&case.id, evidence)?;
    evidence.append(json!({"event":"native_reconstructed","value":row}))?;
    Ok(())
}

fn counts(host: &Host, state: &State) -> Result<Value, String> {
    let events = host
        .ledger
        .events_after(0)
        .map_err(|error| error.to_string())?;
    Ok(
        json!({"effects":state.effects(),"model_requests":events.iter().filter(|event| event.kind == LedgerEventKind::ModelRequested).count(),"effect_entries":events.iter().filter(|event| event.kind == LedgerEventKind::EffectStarted).count(),"authorizations":events.iter().filter(|event| event.kind == LedgerEventKind::EffectAuthorized).count(),"receipts":events.iter().filter(|event| event.kind == LedgerEventKind::EffectReceipt).count()}),
    )
}

struct ChildDispatch {
    issued: kolyan_core::IssuedToolAuthority,
    wait: kolyan_core::ExternalWait,
    template: TurnRequest,
    admission_wait_ms: u64,
}

async fn late_child(
    host: &Host,
    state: &Arc<State>,
    case: &Case,
    owner: &DelegationOwner,
    dispatch: ChildDispatch,
    timeout_ms: u64,
    row: &mut Value,
) -> Result<(), String> {
    let ChildDispatch {
        issued,
        wait,
        mut template,
        admission_wait_ms,
    } = dispatch;
    let original: kolyan_agent::AgentInvokeInput =
        serde_json::from_value(issued.prepared.call().arguments.clone())
            .map_err(|error| error.to_string())?;
    let expected_permissions = kolyan_agent::AgentPermissions {
        tools: [kolyan_agent::EnvironmentTool::Write].into(),
        ..Default::default()
    };
    if original.parallel
        || original.children.len() != 1
        || original.children[0].input != host.child_input
        || original.children[0].permissions != expected_permissions
        || original.children[0].target
            != kolyan_agent::InvocationTarget::Named(host.child_key.clone())
    {
        return Err(
            "original generated child invocation differs from declared private input/ceiling"
                .into(),
        );
    }
    state.evidence.append(json!({"event":"native_original_child_invocation","call":issued.prepared.call(),"owner":owner,"scope":issued.scope,"input":original}))?;
    let admitted = host
        .runner
        .verify_agent_child_wait(owner.clone(), issued.clone(), wait.clone())
        .await
        .map_err(|error| error.to_string())?;
    if admitted.len() != 1 {
        return Err("native scenario requires exactly one admitted child".into());
    }
    row["admitted_child_execution"] = json!(admitted[0].attempt.execution);
    state.admit(admitted[0].attempt.execution.clone())?;
    template.model_request.messages.clear();
    template.model_request.tools.clear();
    let active = host.runner.clone();
    let drive_owner = owner.clone();
    let drive_issued = issued.clone();
    let drive_wait = wait.clone();
    let task = tokio::spawn(async move {
        active
            .drive_agent_children(drive_owner, drive_issued, drive_wait, template)
            .await
    });
    let guard = ReleaseOnDrop {
        state: state.clone(),
        abort: task.abort_handle(),
    };
    tokio::time::timeout(
        Duration::from_millis(admission_wait_ms),
        state.entered.notified(),
    )
    .await
    .map_err(|_| "child native admission acknowledgement expired")?;
    // Session cancellation is the existing stopped-suspension path; Task intent
    // alone cannot stand in for its physical Turn terminal.
    host.service
        .sessions()
        .cancel(&owner.parent.execution)
        .map_err(|error| error.to_string())?;
    host.service
        .reconcile(&case.id, &owner.parent.attempt_id)
        .map_err(|error| error.to_string())?;
    let stopped = host
        .ledger
        .execution_events_after(&owner.parent.execution.execution_id, 0)
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|event| event.kind == LedgerEventKind::TurnCancelled);
    row["parent_stopped_before_task_cancel"] = json!(stopped.is_some());
    let observed = host
        .journal
        .read(&case.id, 0, 512)
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|fact| {
            fact.draft.kind == "task.attempt_observed"
                && stopped.as_ref().is_some_and(|event| {
                    fact.draft.payload["AttemptObserved"]["source"]["event_id"] == event.event_id
                        && fact.draft.payload["AttemptObserved"]["source"]["cursor"] == event.cursor
                })
        });
    row["parent_stop_observation_position"] = json!(observed.as_ref().map(|fact| fact.position));
    host.service
        .cancel(
            &case.id,
            "native-root-only-cancel",
            "cancel root while an admitted native child is held",
        )
        .map_err(|error| error.to_string())?;
    let cancelled = host
        .journal
        .read(&case.id, 0, 512)
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|fact| fact.draft.kind == "task.cancelled");
    row["task_cancel_position"] = json!(cancelled.as_ref().map(|fact| fact.position));
    row["task_cancel_causes_parent_stop"] =
        json!(
            cancelled
                .as_ref()
                .is_some_and(|fact| observed.as_ref().is_some_and(|stop| fact
                    .draft
                    .causes
                    .iter()
                    .any(|cause| cause.stream_id == stop.stream_id
                        && cause.position == stop.position
                        && cause.fact_id == stop.draft.fact_id)))
        );
    state.evidence.append(json!({"event":"parent_stopped_before_child_release","task":host.service.coordinator().snapshot(&case.id).map_err(|error| error.to_string())?}))?;
    state.release.notify_one();
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), task)
        .await
        .map_err(|_| "child drive did not settle within dataset host bound")?
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    state.evidence.append(json!({"event":"late_native_child_terminal","children":result.iter().map(|child| match child { kolyan_agent::AgentChildDriveResult::Terminal { result, dispatch_error }=>json!({"result":result,"dispatch_error":dispatch_error}), kolyan_agent::AgentChildDriveResult::Waiting { .. }=>json!({"unexpected_wait":true}) }).collect::<Vec<_>>()}))?;
    drop(guard);
    let child_events = host
        .ledger
        .execution_events_after(&admitted[0].attempt.execution.execution_id, 0)
        .map_err(|error| error.to_string())?;
    row["child_receipt_matches_admitted_scope"] = json!(child_events.iter().any(|event| {
        event.kind == LedgerEventKind::EffectReceipt
            && event.turn_id == admitted[0].attempt.execution.turn_id
            && state
                .selected_scope()
                .is_some_and(|scope| event.payload["input"]["scope"] == json!(scope))
            && event.payload["input"]["scope"]["execution"]
                == json!({
                    "session_id":admitted[0].attempt.execution.session_id,
                    "execution_id":admitted[0].attempt.execution.execution_id,
                    "turn_id":admitted[0].attempt.execution.turn_id
                })
    }));
    row["consume_error"] = json!(
        host.runner
            .consume_agent_children(owner.clone(), issued, wait)
            .await
            .err()
            .map(|error| error.to_string())
    );
    let parent = host
        .ledger
        .execution_events_after(&owner.parent.execution.execution_id, 0)
        .map_err(|error| error.to_string())?;
    row["parent_turn_cancelled"] = json!(
        parent
            .iter()
            .filter(|event| event.kind == LedgerEventKind::TurnCancelled)
            .count()
    );
    row["parent_receipts"] = json!(
        parent
            .iter()
            .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
            .count()
    );
    row["parent_requests"] = json!(
        parent
            .iter()
            .filter(|event| event.kind == LedgerEventKind::ModelRequested)
            .count()
    );
    Ok(())
}

struct ReleaseOnDrop {
    state: Arc<State>,
    abort: tokio::task::AbortHandle,
}
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.state.release.notify_one();
        self.abort.abort();
    }
}
