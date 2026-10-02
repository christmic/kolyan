//! Real routed parent suspension and same-checkpoint continuation, scripted model.

mod continuation;
mod finalization;
mod input_source;
mod provider;

use super::*;
use crate::runner::tests::support::*;
use crate::{
    AgentChildWaitVerifier, AgentDefinition, AgentDefinitionInput, AgentDelegationConfig,
    AgentSelector,
};
use kolyan_model::ToolCall;
use kolyan_policy::ApprovalMode;
use kolyan_server::{
    ExecutionService, SessionExecutionService, TaskCoordinator, TaskExecutionService,
};
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    call_id: String,
    arguments: serde_json::Value,
    max_steps: u32,
    #[serde(default)]
    child_call: Option<ToolCall>,
    #[serde(default = "three_requests")]
    expected_requests: usize,
    #[serde(default)]
    fault: Option<provider::Fault>,
    #[serde(default)]
    finalization: Option<finalization::Check>,
}
fn three_requests() -> usize {
    3
}

#[tokio::test]
async fn native_call_ids_restore_exact_parent_checkpoint() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/native_ids.json")).unwrap();
    run_cases(cases).await;
}

#[tokio::test]
async fn unsuccessful_children_close_failed_task_without_changing_typed_outcomes() {
    let cases = serde_json::from_str(include_str!("tests/terminal_tasks.json")).unwrap();
    run_cases(cases).await;
}

async fn run_cases(cases: Vec<Case>) {
    let mut rows = Vec::new();
    for case in &cases {
        let harness = Harness::new();
        let verifier = Arc::new(AgentChildWaitVerifier::default());
        let service = Arc::new(TaskExecutionService::new(
            TaskCoordinator::new(harness.service.coordinator().journal().clone()),
            SessionExecutionService::new(
                ExecutionService::new(kolyan_ledger::InMemoryLedger::default(), NoopTraceSink)
                    .with_external_wait_verifier(verifier.clone()),
                harness.service.sessions().sessions().clone(),
            ),
        ));
        let mut permissions = harness.runner.host.clone();
        permissions.delegation.allow_self = true;
        let definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "pump-root".into(),
            revision: "1".into(),
            display_name: None,
            model: kolyan_model::ModelRef::new("unit", "pump"),
            instructions: "Pump fixture".into(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let runner = Arc::new(
            crate::AgentRunner::new(
                service.clone(),
                harness.runner.instances.clone(),
                harness.bindings.clone(),
                harness.runner.catalog.clone(),
                permissions.clone(),
                (
                    provider::Factory {
                        observations: harness.observations.clone(),
                        call: ToolCall {
                            id: case.call_id.clone(),
                            name: crate::AGENT_INVOKE_NAME.into(),
                            arguments: case.arguments.clone(),
                        },
                        child_call: case.child_call.clone(),
                        fault: case.fault,
                        execution_service: service.sessions().execution().clone(),
                    },
                    Tools(harness.observations.clone(), case.child_call.is_some()),
                ),
                harness.runner.input_artifacts.clone(),
            )
            .unwrap()
            .with_delegation(AgentDelegationConfig {
                limits: crate::InvokePrepareLimits {
                    max_children: 2,
                    max_parallel: 2,
                    max_child_input_bytes: 1024,
                    max_output_bytes: 65536,
                    admission_timeout_ms: 10000,
                },
                approval: ApprovalMode::Never,
            })
            .unwrap(),
        );
        verifier.attach(&runner).unwrap();
        let mut request = harness.request(&case.id, false);
        request.selector = AgentSelector::Inline(definition);
        request.requested_permissions = permissions;
        request.limits.max_tokens = None;
        request.limits.max_depth = 2;
        request.limits.max_invocations = 3;
        request.limits.max_attempts = 3;
        request.limits.max_steps_per_turn = case.max_steps;
        request.turn.config.max_steps = usize::try_from(case.max_steps).unwrap();
        let mut template = request.turn.clone();
        template.model_request.messages.clear();
        template.model_request.tools.clear();
        let started = runner.start(request).await;
        let mut row = json!({"id":case.id,"call_id":case.call_id});
        match started {
            Err(error) => row["start_error"] = json!(error.to_string()),
            Ok(started) => {
                row["start_task"] = json!(started.task);
                match started.execution {
                    kolyan_runtime::DurableTurnResult::Completed(..) => {
                        row["start_error"] = json!("expected actual suspension")
                    }
                    kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } => {
                        row["checkpoint"] = json!(suspension.checkpoint);
                        let parent = started.task.attempts["attempt"].binding.clone();
                        let owner = DelegationOwner {
                            task_id: case.id.clone(),
                            logical_session_id: "session".into(),
                            parent,
                            scope: suspension.checkpoint.scope.clone(),
                        };
                        let mut active_runner = runner.clone();
                        let mut pumped = active_runner
                            .pump_agent_children(
                                owner.clone(),
                                suspension.checkpoint.checkpoint_id.clone(),
                                case.call_id.clone(),
                                template.clone(),
                            )
                            .await;
                        if let Ok(AgentChildrenPumpResult::Waiting(children)) = &pumped
                            && case.child_call.is_some()
                        {
                            let AgentChildDriveResult::Waiting { child, execution } = &children[0]
                            else {
                                panic!("expected child approval suspension");
                            };
                            let kolyan_runtime::DurableTurnResult::Suspended {
                                suspension: child_suspension,
                                ..
                            } = execution.as_ref()
                            else {
                                panic!("expected durable checkpoint");
                            };
                            let checkpoint_call = &suspension.checkpoint.calls[0];
                            let CheckpointCallState::AwaitingExternal { issued, wait } =
                                &checkpoint_call.state
                            else {
                                panic!("expected parent external wait");
                            };
                            let resume = crate::ChildApprovalResumeRequest {
                                owner: owner.clone(),
                                issued: issued.clone(),
                                wait: wait.clone(),
                                child_invocation_id: child.attempt.invocation_id.clone(),
                                checkpoint_id: child_suspension.checkpoint.checkpoint_id.clone(),
                                approval_id: child_suspension.waiting.approvals[0]
                                    .approval_id
                                    .clone(),
                            };
                            row["child_resume_request"] = json!(resume);
                            row["effects_before_approval"] =
                                json!(harness.observations.effects.lock().unwrap().clone());
                            let new_verifier = Arc::new(AgentChildWaitVerifier::default());
                            let restored_service = Arc::new(TaskExecutionService::new(
                                TaskCoordinator::new(service.coordinator().journal().clone()),
                                SessionExecutionService::new(
                                    ExecutionService::new(
                                        service
                                            .sessions()
                                            .execution()
                                            .server()
                                            .coordinator()
                                            .ledger()
                                            .clone(),
                                        NoopTraceSink,
                                    )
                                    .with_external_wait_verifier(new_verifier.clone()),
                                    service.sessions().sessions().clone(),
                                ),
                            ));
                            active_runner = Arc::new(
                                crate::AgentRunner::new(
                                    restored_service,
                                    runner.instances.clone(),
                                    runner.bindings.clone(),
                                    crate::AgentCatalog::new(8).unwrap(),
                                    runner.host.clone(),
                                    (
                                        runner.providers.clone(),
                                        Tools(harness.observations.clone(), true),
                                    ),
                                    harness.runner.input_artifacts.clone(),
                                )
                                .unwrap()
                                .with_delegation(runner.delegation.clone().unwrap())
                                .unwrap(),
                            );
                            new_verifier.attach(&active_runner).unwrap();
                            // All host continuation data survives a serde round trip.
                            let restored =
                                serde_json::from_value(row["child_resume_request"].clone())
                                    .unwrap();
                            match active_runner.resume_agent_child_approval(restored).await {
                                Err(error) => row["child_resume_error"] = json!(error.to_string()),
                                Ok(AgentChildDriveResult::Waiting { .. }) => {
                                    row["child_resume_error"] =
                                        json!("approval did not complete child")
                                }
                                Ok(AgentChildDriveResult::Terminal { result, .. }) => {
                                    row["child_terminal"] = json!(result)
                                }
                            }
                            pumped = active_runner
                                .pump_agent_children(
                                    owner,
                                    suspension.checkpoint.checkpoint_id.clone(),
                                    case.call_id.clone(),
                                    template,
                                )
                                .await;
                        }
                        match pumped {
                            Err(error) => row["pump_error"] = json!(error.to_string()),
                            Ok(AgentChildrenPumpResult::Waiting(_)) => {
                                row["pump_error"] = json!("child unexpectedly waiting")
                            }
                            Ok(AgentChildrenPumpResult::Resumed(result)) => {
                                row["resumed_task"] = json!(result.task);
                                row["completed"] = json!(matches!(
                                    result.execution,
                                    kolyan_runtime::DurableTurnResult::Completed(..)
                                ));
                            }
                        }
                    }
                }
            }
        }
        if let Some(check) = &case.finalization {
            finalization::exercise(&runner, &service, &case.id, check, &mut row).await;
        }
        row["requests"] = json!(harness.observations.requests.lock().unwrap().clone());
        row["effects_after"] = json!(harness.observations.effects.lock().unwrap().clone());
        row["facts"] = json!(
            service
                .coordinator()
                .journal()
                .read(&case.id, 0, 1024)
                .unwrap()
        );
        rows.push(row);
    }
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-pump-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    use std::io::Write;
    let mut output = std::fs::File::create(&path).unwrap();
    for row in rows {
        writeln!(output, "{}", serde_json::to_string(&row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    println!("AGENT_PUMP_TRACE={}", path.display());
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(
            row["start_error"].is_null() && row["pump_error"].is_null(),
            "{}: {row}",
            case.id
        );
        assert_eq!(row["checkpoint"]["calls"][0]["call"]["id"], case.call_id);
        if let Some(check) = &case.finalization {
            finalization::compare(&row, check);
        }
        assert_eq!(row["completed"], true);
        if let Some(fault) = case.fault {
            assert_eq!(row["resumed_task"]["state"], "Failed");
            let child_state = match fault {
                provider::Fault::Failed => "Failed",
                provider::Fault::Cancelled => "Cancelled",
            };
            let invocations = row["resumed_task"]["invocations"].as_object().unwrap();
            assert_eq!(invocations.len(), 2);
            assert!(
                invocations
                    .values()
                    .any(|invocation| invocation["state"] == child_state)
            );
            assert!(
                row["resumed_task"]["success_evidence"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            let messages = row["requests"][case.expected_requests - 1]["messages"]
                .as_array()
                .unwrap();
            assert!(
                messages
                    .iter()
                    .flat_map(|message| message["content"].as_array().unwrap())
                    .any(|block| block["type"] == "tool_result"
                        && block["result"]["is_error"] == true)
            );
        } else {
            assert_eq!(row["resumed_task"]["state"], "Completed");
        }
        assert_eq!(
            row["resumed_task"]["attempts"]["attempt"]["state"],
            "Completed"
        );
        assert_eq!(
            row["requests"].as_array().unwrap().len(),
            case.expected_requests
        );
        if case.child_call.is_some() {
            assert!(row["child_resume_error"].is_null(), "{}: {row}", case.id);
            assert!(
                row["effects_before_approval"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(row["effects_after"].as_array().unwrap().len(), 1);
            assert_eq!(row["child_terminal"]["outcome"]["status"], "completed");
        }
        assert_eq!(
            row["resumed_task"]["attempts"]["attempt"]["binding"],
            row["start_task"]["attempts"]["attempt"]["binding"]
        );
        assert!(
            row["requests"][case.expected_requests - 1]["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["content"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|block| block["type"] == "tool_result"
                        && block["result"]["call_id"] == case.call_id))
        );
    }
}
