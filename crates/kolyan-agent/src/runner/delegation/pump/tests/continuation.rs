//! Deep self and multiple durable waits, restored through actual Runner ports.

mod provider;
mod recovery;
mod root_only;

use super::super::*;
use crate::runner::tests::support::{Harness, Observations, Service, TestExecutor, Tools};
use crate::{
    AgentChildDriveResult, AgentChildWaitVerifier, AgentChildrenPumpResult, AgentDefinition,
    AgentDefinitionInput, AgentDelegationConfig, AgentExecutionBudget, AgentRunner, AgentSelector,
    AgentSnapshot, ChildApprovalResumeRequest, DelegationOwner, EnvironmentToolFactory,
    RunnerError, RunnerToolSet,
};
use kolyan_core::{TurnRequest, TurnSuspension};
use kolyan_ledger::{InMemoryLedger, MemoryFactJournal};
use kolyan_server::{
    AttemptBinding, ExecutionService, SessionExecutionService, TaskCoordinator,
    TaskExecutionService,
};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

type Runner = AgentRunner<
    MemoryFactJournal,
    InMemoryLedger,
    NoopTraceSink,
    FileSessionStore,
    provider::Factory,
    ReadTools,
>;

#[derive(Clone)]
struct ReadTools {
    observations: Arc<Observations>,
    approval: bool,
}
impl EnvironmentToolFactory for ReadTools {
    type Executor = TestExecutor;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &kolyan_server::ExecutionRef,
    ) -> Result<RunnerToolSet<TestExecutor>, RunnerError> {
        Tools(self.observations.clone(), self.approval).build(snapshot, execution)
    }
    fn enforces_read_only_parallel(&self, snapshot: &AgentSnapshot) -> bool {
        snapshot
            .permissions()
            .tools
            .iter()
            .all(|tool| *tool == crate::EnvironmentTool::Read)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: Mode,
    max_depth: u32,
    max_steps: u32,
    slots: Option<usize>,
    scripts: Vec<provider::Script>,
    approval_order: Vec<usize>,
    expected: Vec<Value>,
    #[serde(default)]
    delay_ms: u64,
    #[serde(default)]
    max_tokens: Option<u64>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Recursive,
    MultipleApprovals,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    owner: DelegationOwner,
    checkpoint_id: String,
    call_id: String,
    issued: kolyan_core::IssuedToolAuthority,
    wait: kolyan_core::ExternalWait,
}
fn cursor(
    task: &str,
    parent: AttemptBinding,
    suspension: &TurnSuspension,
) -> Result<Cursor, String> {
    let call = suspension
        .checkpoint
        .calls
        .first()
        .ok_or("empty checkpoint")?;
    let kolyan_core::CheckpointCallState::AwaitingExternal { issued, wait } = &call.state else {
        return Err("not an external checkpoint".into());
    };
    Ok(Cursor {
        owner: DelegationOwner {
            task_id: task.into(),
            logical_session_id: "session".into(),
            parent,
            scope: suspension.checkpoint.scope.clone(),
        },
        checkpoint_id: suspension.checkpoint.checkpoint_id.clone(),
        call_id: call.call.id.clone(),
        issued: issued.clone(),
        wait: wait.clone(),
    })
}
async fn pump(
    runner: &Arc<Runner>,
    cursor: &Cursor,
    template: &TurnRequest,
) -> Result<AgentChildrenPumpResult, String> {
    runner
        .pump_agent_children(
            cursor.owner.clone(),
            cursor.checkpoint_id.clone(),
            cursor.call_id.clone(),
            template.clone(),
        )
        .await
        .map_err(|error| error.to_string())
}
fn restore(runner: &Arc<Runner>) -> Result<Arc<Runner>, String> {
    restore_with_host(runner, runner.host.clone())
}
fn restore_with_host(
    runner: &Arc<Runner>,
    host: crate::AgentPermissions,
) -> Result<Arc<Runner>, String> {
    let verifier = Arc::new(AgentChildWaitVerifier::default());
    let service = Arc::new(TaskExecutionService::new(
        TaskCoordinator::new(runner.service.coordinator().journal().clone()),
        SessionExecutionService::new(
            ExecutionService::new(
                runner
                    .service
                    .sessions()
                    .execution()
                    .server()
                    .coordinator()
                    .ledger()
                    .clone(),
                NoopTraceSink,
            )
            .with_external_wait_verifier(verifier.clone()),
            runner.service.sessions().sessions().clone(),
        ),
    ));
    let mut restored = AgentRunner::new(
        service,
        runner.instances.clone(),
        runner.bindings.clone(),
        crate::AgentCatalog::new(8).map_err(|error| error.to_string())?,
        host,
        (runner.providers.clone(), runner.tools.clone()),
        runner.input_artifacts.clone(),
    )
    .map_err(|error| error.to_string())?
    .with_delegation(
        runner
            .delegation
            .clone()
            .ok_or("missing delegation configuration")?,
    )
    .map_err(|error| error.to_string())?;
    restored = restored.with_execution_budget(runner.execution_budget.clone());
    let restored = Arc::new(restored);
    verifier
        .attach(&restored)
        .map_err(|error| error.to_string())?;
    Ok(restored)
}
fn phase(
    row: &mut Value,
    name: &str,
    runner: &Runner,
    observations: &Observations,
    waiting: usize,
) {
    let value = json!({"name":name,"requests":observations.requests.lock().unwrap().len(),
        "effects":observations.effects.lock().unwrap().len(),"waiting":waiting,
        "available_slots":if row["configured_slots"].is_null() { Value::Null } else { json!(runner.execution_budget.slots.available_permits()) }});
    row["phases"].as_array_mut().unwrap().push(value);
}
fn waiting(result: AgentChildrenPumpResult) -> Result<Vec<AgentChildDriveResult>, String> {
    match result {
        AgentChildrenPumpResult::Waiting(children) => Ok(children),
        _ => Err("expected child waiting".into()),
    }
}
fn count_waiting(children: &[AgentChildDriveResult]) -> usize {
    children
        .iter()
        .filter(|child| matches!(child, AgentChildDriveResult::Waiting { .. }))
        .count()
}

struct Fixture {
    _root: Arc<tempfile::TempDir>,
    runner: Arc<Runner>,
    observations: Arc<Observations>,
    service: Arc<Service>,
    request: crate::RootRunRequest,
    template: TurnRequest,
}

fn setup(case: &Case) -> Result<Fixture, String> {
    let harness = Harness::new();
    let observations = harness.observations.clone();
    let verifier = Arc::new(AgentChildWaitVerifier::default());
    let service: Arc<Service> = Arc::new(TaskExecutionService::new(
        TaskCoordinator::new(harness.service.coordinator().journal().clone()),
        SessionExecutionService::new(
            ExecutionService::new(InMemoryLedger::default(), NoopTraceSink)
                .with_external_wait_verifier(verifier.clone()),
            harness.service.sessions().sessions().clone(),
        ),
    ));
    let mut permissions = harness.runner.host.clone();
    permissions.delegation.allow_self = true;
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "saved-recursive-agent".into(),
        revision: "1".into(),
        display_name: None,
        model: kolyan_model::ModelRef::new("unit", "recursive"),
        instructions: "Saved self definition".into(),
        permissions: permissions.clone(),
    })
    .map_err(|error| error.to_string())?;
    let mut runner = AgentRunner::new(
        service.clone(),
        harness.runner.instances.clone(),
        harness.bindings.clone(),
        crate::AgentCatalog::new(8).map_err(|error| error.to_string())?,
        permissions.clone(),
        (
            provider::Factory {
                observations: observations.clone(),
                scripts: case.scripts.clone(),
                concurrency: Arc::new(provider::Concurrency {
                    delay_ms: case.delay_ms,
                    ..Default::default()
                }),
            },
            ReadTools {
                observations: observations.clone(),
                approval: matches!(case.mode, Mode::MultipleApprovals),
            },
        ),
        harness.runner.input_artifacts.clone(),
    )
    .map_err(|error| error.to_string())?
    .with_delegation(AgentDelegationConfig {
        limits: crate::InvokePrepareLimits {
            max_children: 4,
            max_parallel: 2,
            max_child_input_bytes: 4096,
            max_output_bytes: 65536,
            admission_timeout_ms: 10000,
        },
        approval: kolyan_policy::ApprovalMode::Never,
    })
    .map_err(|error| error.to_string())?;
    if let Some(bound) = case.slots {
        runner = runner.with_execution_budget(
            AgentExecutionBudget::new(bound).map_err(|error| error.to_string())?,
        );
    }
    let runner = Arc::new(runner);
    verifier
        .attach(&runner)
        .map_err(|error| error.to_string())?;
    let mut request = harness.request(&case.id, false);
    request.selector = AgentSelector::Inline(definition);
    request.requested_permissions = permissions;
    request.turn.model_request.messages[0].content = vec![kolyan_model::ContentBlock::Text {
        text: case.scripts[0].input.clone(),
    }];
    request.turn.config.max_steps =
        usize::try_from(case.max_steps).map_err(|error| error.to_string())?;
    request.limits.max_depth = case.max_depth;
    request.limits.max_steps_per_turn = case.max_steps;
    request.limits.max_invocations = 8;
    request.limits.max_attempts = 8;
    // Explicitly unlimited tokens, never a fallback from a configured ceiling.
    request.limits.max_tokens = case.max_tokens;
    let mut template = request.turn.clone();
    template.model_request.messages.clear();
    template.model_request.tools.clear();
    Ok(Fixture {
        _root: Arc::new(harness._root),
        runner,
        observations,
        service,
        request,
        template,
    })
}

async fn execute(case: &Case, row: &mut Value, fixture: Fixture) -> Result<(), String> {
    let Fixture {
        _root,
        mut runner,
        observations,
        service,
        request,
        template,
    } = fixture;
    let started = runner
        .start(request)
        .await
        .map_err(|error| error.to_string())?;
    let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = started.execution else {
        return Err("root did not suspend".into());
    };
    let root = cursor(
        &case.id,
        started.task.attempts["attempt"].binding.clone(),
        &suspension,
    )?;
    row["root_cursor"] = json!(root);
    phase(row, "root_waiting", &runner, &observations, 1);
    let children = waiting(pump(&runner, &root, &template).await?)?;
    phase(
        row,
        "children_waiting",
        &runner,
        &observations,
        count_waiting(&children),
    );
    runner = restore(&runner)?;
    let restored_root: Cursor =
        serde_json::from_value(row["root_cursor"].clone()).map_err(|error| error.to_string())?;
    let repeated = waiting(pump(&runner, &restored_root, &template).await?)?;
    phase(
        row,
        "repeated_waiting",
        &runner,
        &observations,
        count_waiting(&repeated),
    );
    match case.mode {
        Mode::Recursive => {
            let AgentChildDriveResult::Waiting { child, execution } =
                repeated.first().ok_or("missing self child")?
            else {
                return Err("self child did not wait".into());
            };
            let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } =
                execution.as_ref()
            else {
                return Err("missing middle checkpoint".into());
            };
            let middle = cursor(&case.id, child.attempt.clone(), suspension)?;
            row["middle_cursor"] = json!(middle);
            let AgentChildrenPumpResult::Resumed(result) =
                pump(&runner, &middle, &template).await?
            else {
                return Err("middle did not complete".into());
            };
            row["middle_task"] = json!(result.task);
            phase(row, "middle_completed", &runner, &observations, 0);
            row["duplicate_middle_error"] = json!(pump(&runner, &middle, &template).await.err());
            phase(row, "duplicate_middle", &runner, &observations, 0);
        }
        Mode::MultipleApprovals => {
            let mut approvals = Vec::new();
            for result in repeated {
                let AgentChildDriveResult::Waiting { child, execution } = result else {
                    return Err("child already terminal before approval".into());
                };
                let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } =
                    execution.as_ref()
                else {
                    return Err("missing approval checkpoint".into());
                };
                let approval = suspension
                    .waiting
                    .approvals
                    .first()
                    .ok_or("missing approval")?;
                approvals.push(ChildApprovalResumeRequest {
                    owner: root.owner.clone(),
                    issued: root.issued.clone(),
                    wait: root.wait.clone(),
                    child_invocation_id: child.attempt.invocation_id,
                    checkpoint_id: suspension.checkpoint.checkpoint_id.clone(),
                    approval_id: approval.approval_id.clone(),
                });
            }
            row["approval_requests"] = json!(approvals);
            for (position, index) in case.approval_order.iter().enumerate() {
                runner = restore(&runner)?;
                let request = approvals
                    .get(*index)
                    .ok_or("invalid approval order")?
                    .clone();
                let restored = serde_json::from_value(
                    serde_json::to_value(&request).map_err(|error| error.to_string())?,
                )
                .map_err(|error| error.to_string())?;
                let terminal = runner
                    .resume_agent_child_approval(restored)
                    .await
                    .map_err(|error| error.to_string())?;
                let AgentChildDriveResult::Terminal { result, .. } = terminal else {
                    return Err("approved child did not finish".into());
                };
                row[format!("approved_child_{position}")] = json!(result);
                phase(
                    row,
                    &format!("approved_{position}"),
                    &runner,
                    &observations,
                    approvals.len() - position - 1,
                );
                row[format!("duplicate_approval_{position}")] = json!(
                    runner
                        .resume_agent_child_approval(request)
                        .await
                        .err()
                        .map(|error| error.to_string())
                );
                phase(
                    row,
                    &format!("duplicate_approval_{position}"),
                    &runner,
                    &observations,
                    approvals.len() - position - 1,
                );
                if position + 1 < approvals.len() {
                    let partial = waiting(pump(&runner, &root, &template).await?)?;
                    phase(
                        row,
                        "partial_join",
                        &runner,
                        &observations,
                        count_waiting(&partial),
                    );
                }
            }
        }
    }
    let AgentChildrenPumpResult::Resumed(result) = pump(&runner, &root, &template).await? else {
        return Err("root did not finish".into());
    };
    row["final_task"] = json!(result.task);
    phase(row, "root_completed", &runner, &observations, 0);
    row["duplicate_root_error"] = json!(pump(&runner, &root, &template).await.err());
    phase(row, "duplicate_root", &runner, &observations, 0);
    row["requests"] = json!(observations.requests.lock().unwrap().clone());
    row["effects"] = json!(observations.effects.lock().unwrap().clone());
    row["facts"] = json!(
        service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["ledger"] = json!(
        service
            .sessions()
            .execution()
            .server()
            .coordinator()
            .ledger()
            .execution_events_after(&root.owner.parent.execution.execution_id, 0)
            .map_err(|error| error.to_string())?
    );
    Ok(())
}

fn capture(row: &mut Value, observations: &Observations, service: &Service, task: &str) {
    row["requests"] = json!(observations.requests.lock().unwrap().clone());
    row["effects"] = json!(observations.effects.lock().unwrap().clone());
    row["facts"] = match service.coordinator().journal().read(task, 0, 1024) {
        Ok(facts) => json!(facts),
        Err(error) => json!({"capture_error":error.to_string()}),
    };
    row["ledger_by_execution"] = match service.coordinator().snapshot(task) {
        Ok(snapshot) => json!(
            snapshot
                .attempts
                .values()
                .map(|attempt| {
                    let binding = &attempt.binding;
                    match service
                        .sessions()
                        .execution()
                        .server()
                        .coordinator()
                        .ledger()
                        .execution_events_after(&binding.execution.execution_id, 0)
                    {
                        Ok(events) => json!({"execution":binding.execution,"events":events}),
                        Err(error) => {
                            json!({"execution":binding.execution,"capture_error":error.to_string()})
                        }
                    }
                })
                .collect::<Vec<_>>()
        ),
        Err(error) => json!({"capture_error":error.to_string()}),
    };
}

#[tokio::test]
async fn recursive_and_multiple_waits_restore_without_reexecution() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("continuation/cases.json")).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        let mut row = json!({"id":case.id,"phases":[],"configured_slots":case.slots});
        match setup(case) {
            Ok(fixture) => {
                let _root = fixture._root.clone();
                let observations = fixture.observations.clone();
                let service = fixture.service.clone();
                if let Err(error) = Box::pin(execute(case, &mut row, fixture)).await {
                    row["error"] = json!(error);
                }
                capture(&mut row, &observations, &service, &case.id);
            }
            Err(error) => row["error"] = json!(error),
        }
        rows.push(row);
    }
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-continuation-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    use std::io::Write;
    let mut output = std::fs::File::create(&path).unwrap();
    for row in rows {
        writeln!(output, "{}", serde_json::to_string(&row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    println!("AGENT_CONTINUATION_TRACE={}", path.display());
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert!(row["error"].is_null(), "{}: {row}", case.id);
        assert_eq!(row["phases"], json!(case.expected), "{}: {row}", case.id);
        assert_eq!(row["final_task"]["state"], "Completed");
        assert!(row["duplicate_root_error"].is_string());
        assert_eq!(row["final_task"]["attempts"].as_object().unwrap().len(), 3);
        let identities: std::collections::BTreeSet<_> = row["final_task"]["invocations"]
            .as_object()
            .unwrap()
            .values()
            .map(|value| {
                value["definition"]["agent"]["instance_id"]
                    .as_str()
                    .unwrap()
            })
            .collect();
        assert_eq!(identities.len(), 3);
        match case.mode {
            Mode::Recursive => assert!(row["duplicate_middle_error"].is_string()),
            Mode::MultipleApprovals => {
                assert!(row["duplicate_approval_0"].is_string());
                assert!(row["duplicate_approval_1"].is_string());
            }
        }
    }
}
