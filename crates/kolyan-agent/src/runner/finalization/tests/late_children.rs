//! Cancellation verdict and physical quiescence are independent durable facts.

mod provider;

use std::io::Write;

use kolyan_core::CheckpointCallState;
use kolyan_model::ToolCall;
use kolyan_policy::ApprovalMode;
use kolyan_server::{
    CancellationPolicy, ExecutionService, SessionExecutionService, TaskCoordinator,
};
use serde_json::{Value, json};

use super::*;
use crate::{
    AgentChildDriveResult, AgentChildWaitVerifier, AgentDefinition, AgentDefinitionInput,
    AgentDelegationConfig, AgentSelector, DelegationOwner,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    whole_task: bool,
    #[serde(default)]
    signal_only: bool,
    hold_child: bool,
    child_answer: String,
    call: ToolCall,
    permissions: crate::AgentPermissions,
    expected_state: String,
    expected_mid_refused: bool,
    #[serde(default)]
    expected_final_refused: bool,
}

type LateRunner = AgentRunner<
    MemoryFactJournal,
    kolyan_ledger::InMemoryLedger,
    kolyan_trace::NoopTraceSink,
    kolyan_storage::FileSessionStore,
    provider::Factory,
    Tools,
>;

struct Fixture {
    harness: Harness,
    service: Arc<crate::runner::tests::support::Service>,
    runner: Arc<LateRunner>,
    definition: AgentDefinition,
}

struct AbortOnDrop(tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn setup(case: &Case) -> Result<Fixture, String> {
    let harness = Harness::new();
    let verifier = Arc::new(AgentChildWaitVerifier::default());
    let service = Arc::new(kolyan_server::TaskExecutionService::new(
        TaskCoordinator::new(harness.service.coordinator().journal().clone()),
        SessionExecutionService::new(
            ExecutionService::new(
                kolyan_ledger::InMemoryLedger::default(),
                kolyan_trace::NoopTraceSink,
            )
            .with_external_wait_verifier(verifier.clone()),
            harness.service.sessions().sessions().clone(),
        ),
    ));
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "late-root".into(),
        revision: "1".into(),
        display_name: None,
        model: kolyan_model::ModelRef::new("unit", "late-child"),
        instructions: "Late-child fixture".into(),
        permissions: case.permissions.clone(),
    })
    .map_err(|error| error.to_string())?;
    let factory = provider::Factory {
        observations: harness.observations.clone(),
        call: case.call.clone(),
        child_answer: case.child_answer.clone(),
        hold_child: case.hold_child,
        opened: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let runner = Arc::new(
        AgentRunner::new(
            service.clone(),
            harness.runner.instances.clone(),
            harness.bindings.clone(),
            crate::AgentCatalog::new(8).unwrap(),
            case.permissions.clone(),
            (factory, Tools(harness.observations.clone(), false)),
            harness.runner.input_artifacts.clone(),
        )
        .map_err(|error| error.to_string())?
        .with_delegation(AgentDelegationConfig {
            limits: crate::InvokePrepareLimits {
                max_children: 2,
                max_parallel: 1,
                max_child_input_bytes: 4096,
                max_output_bytes: 65536,
                admission_timeout_ms: 10000,
            },
            approval: ApprovalMode::Never,
        })
        .map_err(|error| error.to_string())?,
    );
    verifier
        .attach(&runner)
        .map_err(|error| error.to_string())?;
    Ok(Fixture {
        harness,
        service,
        runner,
        definition,
    })
}

async fn scenario(case: &Case, fixture: &Fixture, row: &mut Value) -> Result<(), String> {
    let Fixture {
        harness,
        service,
        runner,
        definition,
    } = fixture;
    let mut request = harness.request(&case.id, false);
    request.selector = AgentSelector::Inline(definition.clone());
    request.requested_permissions = case.permissions.clone();
    request.limits.max_tokens = None;
    request.limits.max_invocations = 2;
    request.limits.max_attempts = 2;
    request.cancellation_policy = CancellationPolicy::RootOnly;
    let mut template = request.turn.clone();
    template.model_request.messages.clear();
    template.model_request.tools.clear();
    let started = runner
        .start(request)
        .await
        .map_err(|error| error.to_string())?;
    let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = started.execution else {
        return Err("root did not suspend".into());
    };
    let parent = started.task.attempts["attempt"].binding.clone();
    let CheckpointCallState::AwaitingExternal { issued, wait } =
        &suspension.checkpoint.calls[0].state
    else {
        return Err("root has no external wait".into());
    };
    let owner = DelegationOwner {
        task_id: case.id.clone(),
        logical_session_id: "session".into(),
        parent: parent.clone(),
        scope: suspension.checkpoint.scope.clone(),
    };
    let active = runner.clone();
    let issued = issued.clone();
    let wait = wait.clone();
    let drive = tokio::spawn(async move {
        active
            .drive_agent_children(owner, issued, wait, template)
            .await
    });
    // Preserve failure capture without leaving a held child future alive when
    // any intervening host operation refuses the scenario.
    let _drive_guard = AbortOnDrop(drive.abort_handle());
    if tokio::time::timeout(
        std::time::Duration::from_secs(5),
        runner.providers.opened.notified(),
    )
    .await
    .is_err()
    {
        drive.abort();
        return Err("child did not open model".into());
    }
    if case.whole_task {
        service
            .cancel(
                &case.id,
                &format!("{}/user-cancel", case.id),
                "explicit whole Task cancellation",
            )
            .map_err(|error| error.to_string())?;
    } else {
        if case.signal_only {
            service.sessions().execution().cancel(&parent.execution)
        } else {
            // Session coordination publishes the stopped Turn and Session commit;
            // an execution signal alone leaves uncertain work, not a terminal.
            service.sessions().cancel(&parent.execution)
        }
        .map_err(|error| error.to_string())?;
        service
            .reconcile(&case.id, &parent.attempt_id)
            .map_err(|error| error.to_string())?;
    }
    let finalize = TaskFinalizationRequest {
        task_id: case.id.clone(),
        logical_session_id: "session".into(),
        root_invocation_id: parent.invocation_id.clone(),
        root_attempt_id: parent.attempt_id.clone(),
        policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
    };
    row["mid_before"] = json!(
        service
            .coordinator()
            .snapshot(&case.id)
            .map_err(|error| error.to_string())?
    );
    row["mid_facts_before"] = json!(
        service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["mid_finalization"] = outcome(runner.finalize_task(finalize.clone()).await);
    row["mid_facts_after"] = json!(
        service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    runner.providers.release.notify_one();
    row["child_drive"] = match drive.await {
        Ok(Ok(children)) => json!(
            children
                .iter()
                .map(|child| match child {
                    AgentChildDriveResult::Terminal {
                        result,
                        dispatch_error,
                    } => json!({"terminal":result,"dispatch_error":dispatch_error}),
                    AgentChildDriveResult::Waiting { .. } => json!({"waiting":true}),
                })
                .collect::<Vec<_>>()
        ),
        Ok(Err(error)) => json!({"error":error.to_string()}),
        Err(error) => json!({"error":error.to_string()}),
    };
    row["before"] = json!(
        service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["first"] = outcome(runner.finalize_task(finalize.clone()).await);
    row["after_first"] = json!(
        service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    let restored = Arc::new(
        AgentRunner::new(
            service.clone(),
            runner.instances.clone(),
            runner.bindings.clone(),
            crate::AgentCatalog::new(8).unwrap(),
            runner.host.clone(),
            (
                runner.providers.clone(),
                Tools(harness.observations.clone(), false),
            ),
            harness.runner.input_artifacts.clone(),
        )
        .map_err(|error| error.to_string())?,
    );
    row["second"] = outcome(restored.finalize_task(finalize).await);
    row["after_second"] = json!(
        service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["requests"] = json!(harness.observations.requests.lock().unwrap().clone());
    row["effects"] = json!(harness.observations.effects.lock().unwrap().clone());
    row["task"] = json!(
        service
            .coordinator()
            .snapshot(&case.id)
            .map_err(|error| error.to_string())?
    );
    let task = service
        .coordinator()
        .snapshot(&case.id)
        .map_err(|error| error.to_string())?;
    let mut ledgers = Vec::new();
    for attempt in task.attempts.values() {
        ledgers.push(json!({"binding":attempt.binding,"events":service.sessions().execution().server().coordinator().ledger().execution_events_after(&attempt.binding.execution.execution_id,0).map_err(|error|error.to_string())?}));
    }
    row["ledgers"] = json!(ledgers);
    Ok(())
}

#[tokio::test]
async fn late_child_after_parent_cancellation_never_consumes_or_reenters() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("late_children/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-finalization-late-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("AGENT_FINALIZATION_LATE_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let mut row = json!({"id":case.id});
        match setup(case) {
            Ok(fixture) => {
                if let Err(error) = scenario(case, &fixture, &mut row).await {
                    row["error"] = json!(error);
                }
                capture_evidence(&case.id, &fixture.harness, &fixture.service, &mut row);
            }
            Err(error) => row["error"] = json!(error),
        }
        writeln!(output, "{}", row).unwrap();
        output.sync_all().unwrap();
    }
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert!(row["error"].is_null(), "{row}");
        assert_eq!(
            row["mid_finalization"]["error"].is_string(),
            case.expected_mid_refused,
            "{row}"
        );
        assert_eq!(row["mid_facts_before"], row["mid_facts_after"]);
        for verdict in ["first", "second"] {
            assert_eq!(
                row[verdict]["error"].is_string(),
                case.expected_final_refused,
                "{row}"
            );
        }
        if case.expected_final_refused {
            assert_eq!(row["before"], row["after_first"]);
        }
        assert_eq!(row["task"]["state"], case.expected_state);
        assert_eq!(row["after_first"], row["after_second"]);
        assert_eq!(row["requests"].as_array().unwrap().len(), 2);
        assert!(row["effects"].as_array().unwrap().is_empty());
        assert_eq!(
            row["child_drive"][0]["terminal"]["outcome"]["status"], "completed",
            "{row}"
        );
        assert!(
            row["after_second"]
                .as_array()
                .unwrap()
                .iter()
                .all(|fact| !matches!(
                    fact["draft"]["kind"].as_str(),
                    Some("task.result_consumed" | "task.terminal_result_consumed")
                ))
        );
    }
}
