//! Actual service commit-gap recovery; exports precede comparisons.

mod late_children;
pub(super) mod provider;

use std::io::Write;

use kolyan_ledger::{LedgerStore, MemoryFactJournal};
use serde_json::{Value, json};

use super::*;
use crate::runner::tests::support::{Harness, Tools};

pub(super) type Runner = AgentRunner<
    MemoryFactJournal,
    kolyan_ledger::InMemoryLedger,
    kolyan_trace::NoopTraceSink,
    kolyan_storage::FileSessionStore,
    provider::Factory,
    Tools,
>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: Mode,
    mutation: Mutation,
    expected_state: String,
    expected_error: bool,
    expected_requests: usize,
    expected_root: Option<String>,
    expected_terminal_facts: usize,
    max_tokens: Option<u64>,
    script: provider::Script,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Gap,
    Automatic,
    MissingTerminal,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    HostRevoked,
    WrongSession,
    WrongAttempt,
    WholeTaskCancel,
    ContradictoryVerdict,
    HostContinuation,
}

pub(super) fn runner(harness: &Harness, script: &provider::Script, revoked: bool) -> Arc<Runner> {
    let mut host = harness.runner.host.clone();
    if revoked {
        host.tools.clear();
    }
    Arc::new(
        AgentRunner::new(
            harness.service.clone(),
            harness.runner.instances.clone(),
            harness.bindings.clone(),
            crate::AgentCatalog::new(8).unwrap(),
            host,
            (
                provider::Factory {
                    observations: harness.observations.clone(),
                    script: script.clone(),
                    execution: harness.service.sessions().execution().clone(),
                },
                Tools(harness.observations.clone(), false),
            ),
            harness.runner.input_artifacts.clone(),
        )
        .unwrap(),
    )
}

async fn scenario(case: &Case, harness: &Harness, row: &mut Value) -> Result<(), String> {
    let active = runner(harness, &case.script, false);
    let mut request = harness.request(&case.id, false);
    request.limits.max_tokens = case.max_tokens;
    if matches!(case.mutation, Mutation::HostContinuation) {
        request.limits.max_invocations = 2;
        request.limits.max_attempts = 2;
    }
    let mut finalization = TaskFinalizationRequest {
        task_id: case.id.clone(),
        logical_session_id: "session".into(),
        root_invocation_id: "root".into(),
        root_attempt_id: "attempt".into(),
        policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
    };
    match case.mode {
        Mode::Automatic => {
            row["initial_result"] = match active.start(request).await {
                Ok(result) => {
                    json!({"task": result.task, "execution_debug":format!("{:?}",result.execution)})
                }
                Err(error) => json!({"error":error.to_string()}),
            };
        }
        Mode::Gap | Mode::MissingTerminal => {
            let prepare = active.clone();
            let (task, _, binding, turn, executor) =
                tokio::task::spawn_blocking(move || prepare.admit(request))
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())?;
            if matches!(case.mode, Mode::Gap) {
                let stopped = Box::pin(harness.service.run(&task, binding, executor, turn)).await;
                row["gap_execution"] = match stopped {
                    Ok((task, execution)) => {
                        json!({"task":task,"execution_debug":format!("{execution:?}")})
                    }
                    Err(error) => json!({"error":error.to_string()}),
                };
            }
        }
    }
    match case.mutation {
        Mutation::HostContinuation => {
            let state = harness
                .service
                .coordinator()
                .snapshot(&case.id)
                .map_err(|error| error.to_string())?;
            let root = &state.attempts["attempt"].binding;
            let admitted_input = kolyan_runtime::verified_execution_input(
                harness
                    .service
                    .sessions()
                    .execution()
                    .server()
                    .coordinator()
                    .ledger(),
                &kolyan_runtime::ExecutionKey {
                    session_id: root.execution.session_id.clone(),
                    turn_id: root.execution.turn_id.clone(),
                    execution_id: root.execution.execution_id.clone(),
                },
                super::super::input::MAX_INPUT_DOCUMENT_BYTES,
            )
            .map_err(|error| error.to_string())?;
            let artifact = super::super::input::archive(
                harness.runner.input_artifacts.as_ref(),
                &admitted_input.model_request,
            )
            .map_err(|error| error.to_string())?;
            let input = harness
                .service
                .coordinator()
                .publish_invocation_input_source(
                    kolyan_server::InvocationInputEnvelope {
                        kind: kolyan_server::InvocationInputKind::Derived,
                        scope: kolyan_server::InvocationInputScope {
                            task_id: case.id.clone(),
                            invocation_id: "z-continuation".into(),
                            agent: root.agent.clone(),
                            constraints_digest: root.constraints_digest.clone(),
                        },
                        body: json!({"artifact":artifact}),
                    },
                    vec![
                        state.invocations["root"]
                            .terminal_fact
                            .clone()
                            .ok_or("root terminal is absent")?,
                    ],
                )
                .map_err(|error| error.to_string())?;
            harness
                .service
                .coordinator()
                .admit_invocation(
                    &case.id,
                    &format!("{}/continuation-admitted", case.id),
                    kolyan_server::InvocationDefinition {
                        invocation_id: "z-continuation".into(),
                        agent: root.agent.clone(),
                        constraints_digest: root.constraints_digest.clone(),
                        role: kolyan_server::InvocationRole::Continuation,
                        parent_invocation_id: Some(root.invocation_id.clone()),
                        dependencies: vec![root.invocation_id.clone()],
                        input_source: kolyan_server::InvocationInputSource::Derived {
                            fact: input.reference,
                        },
                    },
                )
                .map_err(|error| error.to_string())?;
        }
        Mutation::WholeTaskCancel => {
            harness
                .service
                .coordinator()
                .cancel_task(
                    &case.id,
                    &format!("{}/user-cancel", case.id),
                    "explicit whole Task cancellation",
                )
                .map_err(|error| error.to_string())?;
        }
        Mutation::ContradictoryVerdict => {
            harness
                .service
                .coordinator()
                .fail_task(
                    &case.id,
                    &format!("{}/foreign-verdict", case.id),
                    "different host terminal decision",
                )
                .map_err(|error| error.to_string())?;
        }
        Mutation::WrongSession => finalization.logical_session_id = "foreign-session".into(),
        Mutation::WrongAttempt => finalization.root_attempt_id = "foreign-attempt".into(),
        Mutation::None | Mutation::HostRevoked => {}
    }
    let restored = runner(
        harness,
        &case.script,
        matches!(case.mutation, Mutation::HostRevoked),
    );
    let service = harness.service.clone();
    let before = service
        .coordinator()
        .journal()
        .read(&case.id, 0, 1024)
        .map_err(|error| error.to_string())?;
    let requests_before = harness.observations.requests.lock().unwrap().clone();
    let effects_before = harness.observations.effects.lock().unwrap().clone();
    let first = restored.finalize_task(finalization.clone()).await;
    let after_first = service
        .coordinator()
        .journal()
        .read(&case.id, 0, 1024)
        .map_err(|error| error.to_string())?;
    let second = runner(
        harness,
        &case.script,
        matches!(case.mutation, Mutation::HostRevoked),
    )
    .finalize_task(finalization)
    .await;
    let after_second = service
        .coordinator()
        .journal()
        .read(&case.id, 0, 1024)
        .map_err(|error| error.to_string())?;
    row["first"] = outcome(first);
    row["second"] = outcome(second);
    row["facts_before"] = json!(before);
    row["facts_after_first"] = json!(after_first);
    row["facts_after_second"] = json!(after_second);
    row["requests_before"] = json!(requests_before);
    row["requests_after"] = json!(harness.observations.requests.lock().unwrap().clone());
    row["effects_before"] = json!(effects_before);
    row["effects_after"] = json!(harness.observations.effects.lock().unwrap().clone());
    let state = service
        .coordinator()
        .snapshot(&case.id)
        .map_err(|error| error.to_string())?;
    let mut ledgers = BTreeMap::new();
    for (attempt, saved) in &state.attempts {
        ledgers.insert(
            attempt,
            service
                .sessions()
                .execution()
                .server()
                .coordinator()
                .ledger()
                .execution_events_after(&saved.binding.execution.execution_id, 0)
                .map_err(|error| error.to_string())?,
        );
    }
    row["ledgers"] = json!(ledgers);
    row["task"] = json!(state);
    Ok(())
}

pub(super) fn outcome(result: Result<TaskSnapshot, RunnerError>) -> Value {
    match result {
        Ok(task) => json!({"task":task}),
        Err(error) => {
            let kind = match &error {
                RunnerError::UnsupportedFinalizationRole { .. } => "unsupported_role",
                _ => "refused",
            };
            json!({"error":error.to_string(),"kind":kind})
        }
    }
}

#[tokio::test]
async fn finalization_gap_and_reconstruction_export_before_assertions() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-finalization-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("AGENT_FINALIZATION_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let harness = Harness::new();
        let mut row = json!({"id":case.id});
        if let Err(error) = scenario(case, &harness, &mut row).await {
            row["scenario_error"] = json!(error);
        }
        capture(case, &harness, &mut row);
        writeln!(output, "{}", serde_json::to_string(&row).unwrap()).unwrap();
        output.sync_all().unwrap();
    }
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert!(row["scenario_error"].is_null(), "{}: {row}", case.id);
        assert_eq!(
            row["first"]["error"].is_string(),
            case.expected_error,
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["second"]["error"].is_string(),
            case.expected_error,
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["task"]["state"], case.expected_state,
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["requests_after"].as_array().unwrap().len(),
            case.expected_requests,
            "{}: {row}",
            case.id
        );
        assert_eq!(row["requests_before"], row["requests_after"]);
        assert_eq!(row["effects_before"], row["effects_after"]);
        assert_eq!(row["facts_after_first"], row["facts_after_second"]);
        if matches!(case.mutation, Mutation::HostContinuation) {
            assert_eq!(row["first"]["kind"], "unsupported_role");
            assert_eq!(row["second"]["kind"], "unsupported_role");
        }
        let terminal_facts = row["captured_facts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|fact| {
                matches!(
                    fact["draft"]["kind"].as_str(),
                    Some("task.completed" | "task.failed" | "task.cancelled")
                )
            })
            .count();
        assert_eq!(
            terminal_facts, case.expected_terminal_facts,
            "{}: {row}",
            case.id
        );
        if let Some(root) = case.expected_root {
            assert_eq!(
                row["root_terminal"]["outcome"]["status"], root,
                "{}: {row}",
                case.id
            );
        }
        if case.expected_error || matches!(case.mutation, Mutation::WholeTaskCancel) {
            assert_eq!(row["facts_before"], row["facts_after_first"]);
        }
    }
}

fn capture(case: &Case, harness: &Harness, row: &mut Value) {
    capture_evidence(&case.id, harness, &harness.service, row);
}

pub(super) fn capture_evidence(
    task_id: &str,
    harness: &Harness,
    service: &crate::runner::tests::support::Service,
    row: &mut Value,
) {
    row["requests_after"] = json!(harness.observations.requests.lock().unwrap().clone());
    row["effects_after"] = json!(harness.observations.effects.lock().unwrap().clone());
    row["preparations"] = json!(harness.observations.records.lock().unwrap().iter().map(|record| match record {
        crate::provider::ContextRecord::Prepared { source, prepared } => json!({"source":source,"prepared":prepared}),
        crate::provider::ContextRecord::Rejected { source, failure, preparation } => json!({"source":source,"failure":failure.to_string(),"preparation":preparation}),
    }).collect::<Vec<_>>());
    match service.coordinator().journal().read(task_id, 0, 1024) {
        Ok(facts) => row["captured_facts"] = json!(facts),
        Err(error) => row["facts_capture_error"] = json!(error.to_string()),
    }
    match service.coordinator().snapshot(task_id) {
        Ok(task) => {
            if let Some(attempt) = task.attempts.get("attempt") {
                match service.load_verified_historical_result(
                    task_id,
                    &attempt.binding,
                    1024 * 1024,
                ) {
                    Ok(terminal) => row["root_terminal"] = json!(terminal),
                    Err(error) => row["root_terminal_capture_error"] = json!(error.to_string()),
                }
            }
            let mut ledgers = BTreeMap::new();
            for (id, attempt) in &task.attempts {
                let value = match service
                    .sessions()
                    .execution()
                    .server()
                    .coordinator()
                    .ledger()
                    .execution_events_after(&attempt.binding.execution.execution_id, 0)
                {
                    Ok(events) => json!(events),
                    Err(error) => json!({"capture_error":error.to_string()}),
                };
                ledgers.insert(id, value);
            }
            row["ledgers"] = json!(ledgers);
            row["task"] = json!(task);
        }
        Err(error) => row["task_capture_error"] = json!(error.to_string()),
    }
}
