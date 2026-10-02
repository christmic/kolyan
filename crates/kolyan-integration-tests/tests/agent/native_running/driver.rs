//! Host control follows exact scoped pipe evidence; dropped roots stay uncertain.

use super::super::{evidence::Evidence, harness, tools};
use super::{Case, Dataset, Mode, clocks::Clocks, host::Host, model::Selection, ports::State};
use kolyan_agent::{
    AgentSelector, RootRunRequest, TaskFinalizationPolicy, TaskFinalizationRequest,
};
use kolyan_core::{TurnConfig, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore};
use kolyan_runtime::DurableTurnDriver;
use kolyan_sandbox::{SandboxProcessObservation, sandbox_process_observation_channel};
use kolyan_server::{CancellationPolicy, ExecutionRef, SessionService, TaskLimits};
use kolyan_storage::FileSessionStore;
use kolyan_tools::ToolProcessObservationContext;
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::DirBuilderExt,
    path::Path,
    sync::{Arc, atomic::Ordering},
};

pub(super) async fn run(
    root: &Path,
    evidence: &Arc<Evidence>,
    installation: &tools::worker::WorkerRun,
    dataset: &Dataset,
    case: &Case,
    row: &mut Value,
) -> Result<(), String> {
    run_selected(
        root,
        evidence,
        installation,
        dataset,
        case,
        row,
        &Selection::offline(),
    )
    .await
}

pub(super) async fn run_selected(
    root: &Path,
    evidence: &Arc<Evidence>,
    installation: &tools::worker::WorkerRun,
    dataset: &Dataset,
    case: &Case,
    row: &mut Value,
    selection: &Selection,
) -> Result<(), String> {
    fs::create_dir_all(root.join("workspace/safe")).map_err(|e| e.to_string())?;
    fs::create_dir(root.join("state")).map_err(|e| e.to_string())?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("staging"))
        .map_err(|e| e.to_string())?;
    tools::initialize_worker(root, evidence, installation)?;
    SessionService::new(
        FileSessionStore::new(root.join("state/sessions")).map_err(|e| e.to_string())?,
    )
    .create("logical-session")
    .map_err(|e| e.to_string())?;
    let state = State::new(evidence.clone());
    let (sender, receiver) = sandbox_process_observation_channel(128).map_err(|e| e.to_string())?;
    let mut receiver = Some(receiver);
    let host = Host::open(
        root,
        case,
        state.clone(),
        sender.clone(),
        selection,
        dataset.model_timeout_ms,
    )?;
    let execution = ExecutionRef {
        session_id: "logical-session".into(),
        turn_id: format!("turn-{}", case.id),
        execution_id: format!("execution-{}", case.id),
    };
    let request = RootRunRequest {
        goals: vec![],
        task_id: case.id.clone(),
        invocation_id: "root".into(),
        attempt_id: "root-attempt".into(),
        execution: execution.clone(),
        selector: AgentSelector::Named(host.definition.key()),
        requested_permissions: host.permissions.clone(),
        objective: case.input.clone(),
        limits: TaskLimits {
            max_depth: 1,
            max_invocations: 1,
            max_attempts: 1,
            max_tokens: None,
            max_steps_per_turn: u32::try_from(dataset.max_steps).map_err(|e| e.to_string())?,
        },
        cancellation_policy: CancellationPolicy::RootOnly,
        turn: TurnRequest {
            turn_id: execution.turn_id.clone(),
            config: TurnConfig {
                max_steps: dataset.max_steps,
                ..Default::default()
            },
            model_request: harness::request(&host.dataset.turns[0], host.definition.model(), 8192),
        },
    };
    evidence.append(json!({"event":"native_running_plan","case":case.id,"network_executed":selection.live.is_some(),"file_native":"not_proven","tool_timeout_ms":30000,
        "model_timeout_ms":dataset.model_timeout_ms,"phase_timeout_ms":dataset.gate_timeout_ms,"cleanup_timeout_ms":dataset.cleanup_timeout_ms}))?;
    let mut running = Box::pin(host.runner.start(request));
    let clocks = Clocks::new(dataset, tokio::time::Instant::now());
    let mut captured_at = state.capture_time.subscribe();
    let mut observations = Vec::new();
    let mut stdout = Vec::new();
    let mut launch: Option<(String, u32)> = None;
    let mut phase = false;
    let mut stopped = None;
    let mut observation_errors = Vec::new();
    let mut phase_clock_recorded = false;
    match case.mode {
        Mode::Loss => {
            drop(receiver.take());
            stopped = Some(running.as_mut().await);
        }
        Mode::Natural => {
            stopped = Some(running.as_mut().await);
        }
        Mode::Cancel | Mode::Drop => loop {
            let captured = *captured_at.borrow_and_update();
            if captured.is_some() && !phase_clock_recorded {
                phase_clock_recorded = true;
                evidence.append(json!({"event":"phase_clock_started","origin":"original_invocation_capture","phase_timeout_ms":dataset.gate_timeout_ms}))?;
            }
            let deadline = captured.map_or(clocks.model_deadline, |at| clocks.phase_deadline(at));
            tokio::select! {
                biased;
                changed=captured_at.changed()=>{
                    if changed.is_err() {
                        observation_errors.push("invocation capture channel closed".into());
                        break;
                    }
                },
                ()=tokio::time::sleep_until(deadline)=>{
                    row["phase_failure"]=json!(if captured.is_some() { "phase deadline exceeded" } else { "model invocation wait exceeded" });
                    evidence.append(json!({"event":"fixture_deadline_exceeded","clock":if captured.is_some() { "phase" } else { "model_invocation_wait" },"meaning":"test_bound_not_provider_diagnosis"}))?;
                    break;
                },
                event=receiver.as_mut().ok_or("receiver missing")?.recv()=>{
                    let Some(event)=event else {
                        observation_errors.push("observer closed before phase".to_string());
                        break;
                    };
                    if let Err(error)=observe(event,evidence,&state,&mut observations,&mut stdout,&mut launch) {
                        observation_errors.push(error);
                        break;
                    }
                    if stdout.windows(case.marker.len()).any(|bytes|bytes==case.marker.as_bytes()) {
                        phase=true;
                        evidence.append(json!({"event":"exact_shell_phase","launch":launch,"marker":case.marker}))?;
                        break;
                    }
                },
                result=&mut running=>{stopped=Some(result); break;},
            }
        },
    }
    let cleanup_deadline = clocks.cleanup_deadline(tokio::time::Instant::now());
    evidence.append(json!({"event":"cleanup_clock_started","origin":"host_cleanup_request","cleanup_timeout_ms":dataset.cleanup_timeout_ms}))?;
    if matches!(case.mode, Mode::Cancel) && stopped.is_none() {
        if phase {
            DurableTurnDriver::new(host.ledger.clone(), NoopTraceSink)
                .cancel(&execution.execution_id, &execution.turn_id)
                .map_err(|e| e.to_string())?;
            evidence.append(json!({"event":"durable_cancel_published","execution":execution}))?;
        }
        {
            let captured = state.captured.lock().map_err(|e| e.to_string())?;
            if let Some(captured) = captured.as_ref() {
                captured.control.cancel();
                evidence
                    .append(json!({"event":"original_control_delivered","execution":execution}))?;
            }
        }
        match tokio::time::timeout_at(cleanup_deadline, running.as_mut()).await {
            Ok(result) => stopped = Some(result),
            Err(_) => observation_errors
                .push("original root cleanup wait exceeded: cleanup unproven".into()),
        }
    }
    drop(running);
    if matches!(case.mode, Mode::Drop) || (!observation_errors.is_empty() && stopped.is_none()) {
        evidence
            .append(json!({"event":"root_future_dropped","meaning":"not_durable_TaskStopped"}))?;
        // Cleanup starts its own clock; a failed phase deadline must not consume it.
        loop {
            let value = serde_json::to_value(&observations).map_err(|e| e.to_string())?;
            let values = value.as_array().ok_or("observation array invalid")?;
            if values
                .iter()
                .any(|event| event["event"]["kind"] == "reaped")
                && values
                    .iter()
                    .any(|event| event["event"]["kind"] == "group_cleanup")
                && values
                    .iter()
                    .filter(|event| event["event"]["kind"] == "capture_completed")
                    .count()
                    == 2
            {
                break;
            }
            let received = tokio::time::timeout_at(
                cleanup_deadline,
                receiver.as_mut().ok_or("receiver missing")?.recv(),
            )
            .await;
            let event = match received {
                Ok(Some(event)) => event,
                Ok(None) => {
                    observation_errors
                        .push("drop cleanup observer closed: cleanup unproven".into());
                    break;
                }
                Err(_) => {
                    observation_errors.push("drop cleanup deadline: cleanup unproven".into());
                    break;
                }
            };
            if let Err(error) = observe(
                event,
                evidence,
                &state,
                &mut observations,
                &mut stdout,
                &mut launch,
            ) {
                observation_errors.push(error);
            }
        }
    }
    if let Some(receiver) = receiver.as_mut() {
        while let Ok(event) = receiver.try_recv() {
            if let Err(error) = observe(
                event,
                evidence,
                &state,
                &mut observations,
                &mut stdout,
                &mut launch,
            ) {
                observation_errors.push(error);
            }
        }
    }
    let values = serde_json::to_value(&observations).map_err(|e| e.to_string())?;
    let events = values.as_array().ok_or("observation array invalid")?;
    row["phase_verified"] = json!(phase);
    row["observation_errors"] = json!(observation_errors);
    evidence.append(json!({"event":"cleanup_observation_outcome","errors":observation_errors,"cleanup_timeout_ms":dataset.cleanup_timeout_ms}))?;
    row["marker_in_actual_pipe"] = json!(
        stdout
            .windows(case.marker.len())
            .any(|bytes| bytes == case.marker.as_bytes())
    );
    row["loss"] = json!(sender.dropped());
    row["reaped"] = json!(
        events
            .iter()
            .any(|event| event["event"]["kind"] == "reaped")
    );
    row["group_cleanup_ok"] = json!(events.iter().any(|event| event["event"]["kind"]
        == "group_cleanup"
        && event["event"]["error"].is_null()));
    row["capture_count"] = json!(
        events
            .iter()
            .filter(|event| event["event"]["kind"] == "capture_completed")
            .count()
    );
    row["scope_failure"] = json!(if !observation_errors.is_empty() {
        Some("observation_validation_or_cleanup_failed")
    } else if row["phase_failure"] == "model invocation wait exceeded" {
        Some("model_invocation_wait_exceeded")
    } else if row["phase_failure"].is_string() {
        Some("phase_deadline_exceeded")
    } else {
        match case.mode {
            Mode::Cancel if phase => None,
            Mode::Drop if phase => None,
            Mode::Loss => Some("observation_loss"),
            _ => Some("natural_exit_before_control"),
        }
    });
    row["start_error"] = json!(
        stopped
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(ToString::to_string)
    );
    evidence.append(json!({"event":"original_root_outcome","task":stopped.as_ref().and_then(|result|result.as_ref().ok()).map(|result|&result.task),"error":row["start_error"],"dropped":matches!(case.mode,Mode::Drop)}))?;
    let before = host.ledger.events_after(0).map_err(|e| e.to_string())?;
    let requests_before = evidence
        .rows()
        .iter()
        .filter(|record| record["event"] == "request")
        .count();
    let effects_before = state.executions.load(Ordering::SeqCst);
    let restored = Host::open(
        root,
        case,
        state.clone(),
        sender,
        selection,
        dataset.model_timeout_ms,
    )?;
    let reconcile = restored.service.reconcile(&case.id, "root-attempt");
    evidence.append(json!({"event":"read_only_reconcile","task":reconcile.as_ref().ok(),"error":reconcile.as_ref().err().map(ToString::to_string)}))?;
    let mut finalizations = Vec::new();
    for _ in 0..2 {
        let result = restored
            .runner
            .finalize_task(TaskFinalizationRequest {
                task_id: case.id.clone(),
                logical_session_id: execution.session_id.clone(),
                root_invocation_id: "root".into(),
                root_attempt_id: "root-attempt".into(),
                policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
            })
            .await;
        let value = json!({"task":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string)});
        evidence.append(json!({"event":"proof_only_finalize","value":value}))?;
        finalizations.push(value);
    }
    host.export(root, &case.id, evidence)?;
    let after = host.ledger.events_after(0).map_err(|e| e.to_string())?;
    row["finalizations"] = json!(finalizations);
    row["root_cancelled"] = json!(
        after
            .iter()
            .any(|event| event.kind == LedgerEventKind::TurnCancelled)
    );
    row["durable_cancel"] = json!(
        after
            .iter()
            .any(|event| event.kind == LedgerEventKind::ExecutionCancelled)
    );
    row["requests"] = json!(requests_before);
    row["executions"] = json!(effects_before);
    row["new_requests"] = json!(
        evidence
            .rows()
            .iter()
            .filter(|record| record["event"] == "request")
            .count()
            - requests_before
    );
    row["new_executions"] = json!(state.executions.load(Ordering::SeqCst) - effects_before);
    row["new_model_facts"] = json!(
        after
            .iter()
            .filter(|event| event.kind == LedgerEventKind::ModelRequested)
            .count()
            - before
                .iter()
                .filter(|event| event.kind == LedgerEventKind::ModelRequested)
                .count()
    );
    row["task_state"] = json!(format!(
        "{:?}",
        restored
            .service
            .coordinator()
            .snapshot(&case.id)
            .map_err(|e| e.to_string())?
            .state
    ));
    row["task_journal"] = json!(
        restored
            .journal
            .read(&case.id, 0, 1024)
            .map_err(|e| e.to_string())?
    );
    let bytes = fs::read(root.join("workspace/safe/running.txt")).map_err(|e| e.to_string())?;
    row["physical_sha256"] = json!(format!("{:x}", Sha256::digest(&bytes)));
    row["physical_matches"] = json!(bytes == case.expected_content.as_bytes());
    row["process_observations"] = values;
    Ok(())
}

fn observe(
    event: SandboxProcessObservation,
    evidence: &Evidence,
    state: &State,
    observations: &mut Vec<SandboxProcessObservation>,
    stdout: &mut Vec<u8>,
    launch: &mut Option<(String, u32)>,
) -> Result<(), String> {
    evidence.append(json!({"event":"sandbox_process","value":event}))?;
    let context: ToolProcessObservationContext =
        serde_json::from_str(&event.context).map_err(|e| e.to_string())?;
    let captured = state.captured.lock().map_err(|e| e.to_string())?;
    let captured = captured
        .as_ref()
        .ok_or("process event before original invocation capture")?;
    if context.scope != captured.scope
        || context.prepared_digest != captured.prepared.digest()
        || context.tool_name != captured.prepared.call().name
    {
        return Err("process scope/digest mismatch".into());
    }
    if let Some((id, pid)) = launch {
        if *id != event.launch_id || *pid != event.pid {
            return Err("unexpected process launch substitution".into());
        }
    } else {
        *launch = Some((event.launch_id.clone(), event.pid));
    }
    if let kolyan_sandbox::SandboxProcessEvent::OutputObserved {
        stream: kolyan_sandbox::SandboxOutputStream::Stdout,
        bytes,
    } = &event.event
    {
        stdout.extend_from_slice(bytes);
    }
    observations.push(event);
    Ok(())
}
