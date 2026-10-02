use super::*;
use crate::{StepEvent, StepEventRecordError, StepEventRecorder};
use futures_util::{StreamExt, stream};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelRef, ProviderFuture, StopReason, TokenUsage,
    ToolChoice,
};
use serde_json::Value;

mod policy_revision;
pub(crate) mod preparation;
mod prepared_authority;
mod provider_mapping;
mod suspension;
mod window;
use preparation::{fixture_key, fixture_policy, fixture_prepare, fixture_validate};

async fn resume_confirmed<P: ModelProvider, T: ToolExecutor>(
    executor: &TurnExecutor<P, T>,
    suspension: TurnSuspension,
    approval_id: &str,
) -> Result<ResumableTurn, TurnError> {
    let saved = suspension
        .checkpoint
        .approvals
        .iter()
        .find(|saved| saved.approval_id == approval_id)
        .ok_or_else(|| TurnError::InvalidRequest {
            message: "unknown fixture approval".into(),
        })?;
    let confirmation = ApprovalConfirmation {
        approval_id: approval_id.into(),
        prepared_digest: saved.prepared.digest().into(),
        policy_revision: saved.policy_revision.clone(),
        scope: saved.scope.clone(),
        evidence_id: approval_id.into(),
    };
    let scope = saved.scope.clone();
    let checkpoint = executor.merge_resume_with_control(
        suspension,
        ResumeInput::ApprovalConfirmed(confirmation),
        scope.clone(),
        TurnControl::default(),
    )?;
    executor
        .resume_checkpoint_with_control(checkpoint, scope, TurnControl::default())
        .await
}

#[tokio::test]
async fn no_progress_stops_repeated_tool_results_before_max_steps() {
    use kolyan_policy::{
        ApprovalMode, Capability, Effect, Idempotency, PathScope, ProgressPolicy, ToolManifest,
    };
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Update].into_iter().collect(),
        path_scopes: vec![PathScope::new("safe")],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    let policy = policy
        .with_progress_policy(ProgressPolicy {
            repeat_limit: 2,
            polling_tools: Default::default(),
        })
        .unwrap();
    let executor = TurnExecutor::with_tools(
        MockProvider {
            stop_reason: StopReason::ToolUse,
            content: vec![ContentBlock::ToolCall {
                call: ToolCall {
                    id: "reused-id".into(),
                    name: "file.write".into(),
                    arguments: serde_json::json!({"path":"safe/a","content":"x"}),
                },
            }],
        },
        MockTool,
    )
    .with_execution_key(fixture_key("progress"))
    .with_policy_engine(fixture_policy())
    .with_policy_engine(Arc::new(policy));
    let error = executor
        .execute(TurnRequest {
            turn_id: "progress".into(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 20,
                ..Default::default()
            },
        })
        .await
        .unwrap_err();
    assert!(matches!(error, TurnError::NoProgress { .. }));
    assert_eq!(error.end_reason(), TurnEndReason::NoProgress);
}

struct MockProvider {
    stop_reason: StopReason,
    content: Vec<ContentBlock>,
}

struct SlowProvider;

impl ModelProvider for SlowProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let response = kolyan_model::ModelResponse {
            id: "slow-response".into(),
            model: request.model,
            content: vec![ContentBlock::Text {
                text: "slow answer".into(),
            }],
            structured_output: None,
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            let events =
                stream::iter(vec![Ok(ModelEvent::Started)]).chain(stream::once(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(ModelEvent::Completed(response))
                }));
            Ok(Box::pin(events) as ModelEventStream)
        })
    }
}

impl ModelProvider for MockProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let response = kolyan_model::ModelResponse {
            id: "turn-response".into(),
            model: request.model,
            content: self.content.clone(),
            structured_output: None,
            stop_reason: self.stop_reason.clone(),
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

#[tokio::test]
async fn failed_model_stream_recording_prevents_tool_execution() {
    struct DeltaToolProvider;
    impl ModelProvider for DeltaToolProvider {
        fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
            let response = kolyan_model::ModelResponse {
                id: "recording-failure".into(),
                model: request.model,
                content: vec![ContentBlock::ToolCall {
                    call: ToolCall {
                        id: "must-not-run".into(),
                        name: "file.write".into(),
                        arguments: serde_json::json!({"path":"safe/a","content":"x"}),
                    },
                }],
                structured_output: None,
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
                metadata: Value::Null,
            };
            Box::pin(async move {
                Ok(Box::pin(stream::iter(vec![
                    Ok(ModelEvent::Started),
                    Ok(ModelEvent::TextDelta("must persist".into())),
                    Ok(ModelEvent::Completed(response)),
                ])) as ModelEventStream)
            })
        }
    }
    struct BrokenStreamRecorder;
    impl StepEventRecorder for BrokenStreamRecorder {
        fn record(&self, event: &StepEvent) -> Result<(), StepEventRecordError> {
            if matches!(event, StepEvent::TextDelta { .. }) {
                Err(StepEventRecordError {
                    message: "injected stream ledger failure".into(),
                })
            } else {
                Ok(())
            }
        }
    }

    let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let executor = TurnExecutor::with_tools(
        DeltaToolProvider,
        CountingTool {
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("stream-recording-failure"))
    .with_policy_engine(fixture_policy())
    .with_step_event_recorder(Arc::new(BrokenStreamRecorder));
    let error = executor
        .execute(TurnRequest {
            turn_id: "stream-recording-failure".into(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 2,
                ..TurnConfig::default()
            },
        })
        .await
        .unwrap_err();

    assert!(matches!(error, TurnError::Step(StepError::Recording(_))));
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn required_tool_choice_is_preserved_across_model_steps() {
    #[derive(Default)]
    struct RequestChoices(std::sync::Mutex<Vec<ToolChoice>>);
    impl TurnEventRecorder for RequestChoices {
        fn record(&self, _event: &TurnEvent) -> Result<(), TurnError> {
            Ok(())
        }

        fn record_request(&self, request: &ModelRequest) -> Result<(), TurnError> {
            self.0.lock().unwrap().push(request.tool_choice.clone());
            Ok(())
        }
    }

    let choices = Arc::new(RequestChoices::default());
    let mut model_request = request();
    model_request.tool_choice = ToolChoice::Required;
    let result = TurnExecutor::with_tools(
        MockProvider {
            stop_reason: StopReason::ToolUse,
            content: vec![ContentBlock::ToolCall {
                call: ToolCall {
                    id: "required-call".into(),
                    name: "read".into(),
                    arguments: serde_json::json!({}),
                },
            }],
        },
        MockTool,
    )
    .with_execution_key(fixture_key("required-across-steps"))
    .with_policy_engine(fixture_policy())
    .with_event_recorder(choices.clone())
    .execute(TurnRequest {
        turn_id: "required-across-steps".into(),
        model_request,
        config: TurnConfig {
            max_steps: 2,
            ..TurnConfig::default()
        },
    })
    .await
    .unwrap();

    assert_eq!(result.end_reason, TurnEndReason::MaxSteps);
    assert_eq!(
        *choices.0.lock().unwrap(),
        [ToolChoice::Required, ToolChoice::Required]
    );
}

fn request() -> ModelRequest {
    ModelRequest {
        request_id: "turn-request".into(),
        model: ModelRef::new("mock", "model"),
        system: Vec::new(),
        messages: Vec::new(),
        tools: Vec::new(),
        tool_choice: ToolChoice::Auto,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: None,
        extensions: Value::Null,
    }
}

#[tokio::test]
async fn completes_turn_from_one_final_answer_step() {
    let executor = TurnExecutor::new(MockProvider {
        stop_reason: StopReason::EndTurn,
        content: vec![ContentBlock::Text {
            text: "answer".into(),
        }],
    });
    let result = executor
        .execute(TurnRequest {
            turn_id: "turn-1".into(),
            model_request: request(),
            config: TurnConfig::default(),
        })
        .await
        .expect("turn should complete");

    assert_eq!(result.turn_id, "turn-1");
    assert_eq!(result.steps.len(), 1);
    assert!(matches!(result.outcome, TurnOutcome::FinalAnswer { .. }));
}

#[tokio::test]
async fn emits_turn_lifecycle_events_for_a_final_answer() {
    let executor = TurnExecutor::new(MockProvider {
        stop_reason: StopReason::EndTurn,
        content: vec![ContentBlock::Text {
            text: "answer".into(),
        }],
    });
    let execution = executor
        .execute_with_events(
            TurnRequest {
                turn_id: "turn-events".into(),
                model_request: request(),
                config: TurnConfig::default(),
            },
            TurnControl::default(),
        )
        .await
        .expect("turn events should complete");

    assert!(matches!(execution.events[0], TurnEvent::Started { .. }));
    assert!(matches!(execution.events[1], TurnEvent::StepStarted { .. }));
    assert!(matches!(
        execution.events[2],
        TurnEvent::StepCompleted { .. }
    ));
    assert!(matches!(
        execution.events[3],
        TurnEvent::Completed {
            outcome: TurnOutcome::FinalAnswer { .. },
            ..
        }
    ));

    let mut stream = executor
        .execute_event_stream(
            TurnRequest {
                turn_id: "turn-event-stream".into(),
                model_request: request(),
                config: TurnConfig::default(),
            },
            TurnControl::default(),
        )
        .await
        .expect("turn event stream should open");
    let mut count = 0;
    while stream.next().await.is_some() {
        count += 1;
    }
    assert_eq!(count, 4);
}

#[tokio::test]
async fn event_stream_emits_before_the_model_step_finishes() {
    let executor = TurnExecutor::new(SlowProvider);
    let mut events = executor
        .execute_event_stream(
            TurnRequest {
                turn_id: "turn-live-events".into(),
                model_request: request(),
                config: TurnConfig::default(),
            },
            TurnControl::default(),
        )
        .await
        .expect("event stream should open");

    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(20), events.next())
            .await
            .expect("started event should be immediate")
            .expect("stream should contain started")
            .expect("started event should be successful"),
        TurnEvent::Started { .. }
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(20), events.next())
            .await
            .expect("step-started event should be immediate")
            .expect("stream should contain step started")
            .expect("step-started event should be successful"),
        TurnEvent::StepStarted { .. }
    ));

    let remaining = events.collect::<Vec<_>>().await;
    assert!(
        remaining
            .iter()
            .any(|event| matches!(event, Ok(TurnEvent::Completed { .. })))
    );
}

#[tokio::test]
async fn max_steps_is_a_completed_turn_outcome() {
    let call = ToolCall {
        id: "call-max-steps".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let execution = TurnExecutor::with_tools(provider, MockTool)
        .with_execution_key(fixture_key("turn-max-steps"))
        .with_policy_engine(fixture_policy())
        .execute_with_events(
            TurnRequest {
                turn_id: "turn-max-steps".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 1,
                    ..TurnConfig::default()
                },
            },
            TurnControl::default(),
        )
        .await
        .expect("max steps should be a normal turn result");

    assert!(matches!(execution.result.outcome, TurnOutcome::MaxSteps));
    assert!(matches!(
        execution.events.last(),
        Some(TurnEvent::Completed {
            outcome: TurnOutcome::MaxSteps,
            ..
        })
    ));
}

#[tokio::test]
async fn failed_turn_stream_contains_terminal_event_before_error() {
    let call = ToolCall {
        id: "call-stream-failed".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let executor = TurnExecutor::with_tools(provider, FailingTool)
        .with_execution_key(fixture_key("turn-stream-failed"))
        .with_policy_engine(fixture_policy());
    let events = executor
        .execute_event_stream(
            TurnRequest {
                turn_id: "turn-stream-failed".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..TurnConfig::default()
                },
            },
            TurnControl::default(),
        )
        .await
        .expect("event stream should open");
    let events = events.collect::<Vec<_>>().await;

    assert!(
        events
            .iter()
            .any(|event| matches!(event, Ok(TurnEvent::Failed { .. })))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Err(TurnError::Tool(ToolError::Failed { .. }))))
    );
}

#[tokio::test]
async fn returns_tool_unavailable_instead_of_dropping_tool_calls() {
    let call = ToolCall {
        id: "call-1".into(),
        name: "search".into(),
        arguments: serde_json::json!({"query": "kolyan"}),
    };
    let executor = TurnExecutor::new(MockProvider {
        stop_reason: StopReason::ToolUse,
        content: vec![ContentBlock::ToolCall { call: call.clone() }],
    })
    .with_execution_key(fixture_key("turn-tool"))
    .with_policy_engine(fixture_policy());
    let error = executor
        .execute(TurnRequest {
            turn_id: "turn-tool".into(),
            model_request: request(),
            config: TurnConfig::default(),
        })
        .await
        .expect_err("v0 should not execute tools");

    assert!(matches!(
        error,
        TurnError::Tool(ToolError::Unavailable { name }) if name == "search"
    ));
}

#[derive(Clone)]
struct ScriptedProvider {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    saw_tool_result: std::sync::Arc<std::sync::atomic::AtomicBool>,
    tool_call: ToolCall,
}

impl ModelProvider for ScriptedProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let index = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if index == 1 {
            let has_tool_result = request.messages.iter().any(|message| {
                message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
            });
            self.saw_tool_result
                .store(has_tool_result, std::sync::atomic::Ordering::SeqCst);
        }
        let (stop_reason, content) = if index == 0 {
            (
                StopReason::ToolUse,
                vec![ContentBlock::ToolCall {
                    call: self.tool_call.clone(),
                }],
            )
        } else {
            (
                StopReason::EndTurn,
                vec![ContentBlock::Text {
                    text: "tool result consumed".into(),
                }],
            )
        };
        let response = kolyan_model::ModelResponse {
            id: format!("response-{index}"),
            model: request.model,
            content,
            structured_output: None,
            stop_reason,
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

struct MultiApprovalProvider {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ModelProvider for MultiApprovalProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let index = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let content = if index == 0 {
            vec![
                ContentBlock::ToolCall {
                    call: ToolCall {
                        id: "call-approval-a".into(),
                        name: "shell.query".into(),
                        arguments: serde_json::json!({"command": "count_lines"}),
                    },
                },
                ContentBlock::ToolCall {
                    call: ToolCall {
                        id: "call-approval-b".into(),
                        name: "shell.query".into(),
                        arguments: serde_json::json!({"command": "count_entries"}),
                    },
                },
            ]
        } else {
            vec![ContentBlock::Text {
                text: "all approvals completed".into(),
            }]
        };
        let response = kolyan_model::ModelResponse {
            id: format!("multi-approval-response-{index}"),
            model: request.model,
            content,
            structured_output: None,
            stop_reason: if index == 0 {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

struct MockTool;

impl ToolExecutor for MockTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move { fixture_prepare(call) })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            let call = fixture_validate(&invocation)?;
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: call.id,
                content: "count=1".into(),
                is_error: false,
            }))
        })
    }
}

#[tokio::test]
async fn executes_tool_result_and_runs_a_second_step() {
    let call = ToolCall {
        id: "call-1".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let executor = TurnExecutor::with_tools(provider.clone(), MockTool)
        .with_execution_key(fixture_key("turn-multi-step"))
        .with_policy_engine(fixture_policy());
    let result = executor
        .execute(TurnRequest {
            turn_id: "turn-multi-step".into(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 2,
                ..TurnConfig::default()
            },
        })
        .await
        .expect("turn should consume the tool result");

    assert_eq!(result.steps.len(), 2);
    assert!(matches!(result.outcome, TurnOutcome::FinalAnswer { .. }));
    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(
        provider
            .saw_tool_result
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[tokio::test]
async fn rejects_cancelled_turn_before_starting_a_step() {
    let executor = TurnExecutor::new(MockProvider {
        stop_reason: StopReason::EndTurn,
        content: Vec::new(),
    });
    let control = TurnControl::default();
    control.cancel();
    let error = executor
        .execute_with_control(
            TurnRequest {
                turn_id: "turn-cancelled".into(),
                model_request: request(),
                config: TurnConfig::default(),
            },
            control,
        )
        .await
        .expect_err("cancelled turn should not start");

    assert!(matches!(error, TurnError::Cancelled));
}

struct FailingTool;

impl ToolExecutor for FailingTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move { fixture_prepare(call) })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            fixture_validate(&invocation)?;
            Err(ToolError::Failed {
                message: "synthetic tool failure".into(),
            })
        })
    }
}

#[tokio::test]
async fn continue_batch_converts_tool_failure_to_error_result() {
    let call = ToolCall {
        id: "call-failed".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let executor = TurnExecutor::with_tools(provider, FailingTool)
        .with_execution_key(fixture_key("turn-continue-batch"))
        .with_policy_engine(fixture_policy())
        .with_tool_dispatch_policy(ToolDispatchPolicy {
            mode: ToolDispatchMode::Serial,
            on_error: ToolErrorPolicy::ContinueBatch,
        });
    let execution = executor
        .execute_with_events(
            TurnRequest {
                turn_id: "turn-continue-batch".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..TurnConfig::default()
                },
            },
            TurnControl::default(),
        )
        .await
        .expect("continue-batch should let the model observe the tool error");

    assert_eq!(execution.result.steps.len(), 2);
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert!(execution.events.iter().any(|event| matches!(
        event,
        TurnEvent::ToolExecutionFailed { call_id, .. } if call_id == "call-failed"
    )));
    assert!(execution.events.iter().any(|event| matches!(
        event,
        TurnEvent::ToolResult { result, .. } if result.call_id == "call-failed" && result.is_error
    )));
}

#[tokio::test]
async fn policy_plan_blocks_denied_calls_before_tool_execution() {
    let call = ToolCall {
        id: "call-policy-denied".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let executed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut policy = PolicyEngine::default();
    policy.deny_tool("shell.query");
    let executor = TurnExecutor::with_tools(
        provider,
        CountingTool {
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("turn-policy-denied"))
    .with_policy_engine(fixture_policy())
    .with_policy_engine(Arc::new(policy))
    .with_tool_dispatch_policy(ToolDispatchPolicy {
        mode: ToolDispatchMode::Serial,
        on_error: ToolErrorPolicy::ContinueBatch,
    });
    let execution = executor
        .execute_with_events(
            TurnRequest {
                turn_id: "turn-policy-denied".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..TurnConfig::default()
                },
            },
            TurnControl::default(),
        )
        .await
        .expect("denied policy result should be returned to the model");

    assert_eq!(execution.result.steps.len(), 2);
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(execution.events.iter().any(|event| matches!(
        event,
        TurnEvent::ToolExecutionFailed {
            error: ToolError::PolicyDenied { .. },
            ..
        }
    )));
}

#[tokio::test]
async fn approval_pauses_turn_until_control_approves_the_grant() {
    let call = ToolCall {
        id: "call-approval".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let mut policy = PolicyEngine::default();
    policy.register(kolyan_policy::ToolManifest {
        tool_name: "shell.query".into(),
        capabilities: [kolyan_policy::Capability::ProcessInspect]
            .into_iter()
            .collect(),
        effects: [kolyan_policy::Effect::Read].into_iter().collect(),
        path_scopes: Vec::new(),
        idempotency: kolyan_policy::Idempotency::Idempotent,
        approval: kolyan_policy::ApprovalMode::Always,
    });
    let control = TurnControl::default();
    let task_control = control.clone();
    let executor = TurnExecutor::with_tools(provider, MockTool)
        .with_execution_key(fixture_key("turn-approval"))
        .with_policy_engine(fixture_policy())
        .with_policy_engine(Arc::new(policy));
    let task = tokio::spawn(async move {
        executor
            .execute_with_events(
                TurnRequest {
                    turn_id: "turn-approval".into(),
                    model_request: request(),
                    config: TurnConfig {
                        max_steps: 2,
                        ..TurnConfig::default()
                    },
                },
                task_control,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished(), "turn should wait for approval");
    control.approve_tool("shell.query");
    let execution = task
        .await
        .expect("approval task should join")
        .expect("approved turn should complete");
    assert!(execution.events.iter().any(|event| matches!(
        event,
        TurnEvent::ApprovalRequested { name, .. } if name == "shell.query"
    )));
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
}

#[tokio::test]
async fn durable_approval_resumes_without_replaying_the_first_model_step() {
    let call = ToolCall {
        id: "call-durable".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let model_calls = provider.calls.clone();
    let mut policy = PolicyEngine::default();
    policy.register(kolyan_policy::ToolManifest {
        tool_name: "shell.query".into(),
        capabilities: [kolyan_policy::Capability::ProcessInspect]
            .into_iter()
            .collect(),
        effects: [kolyan_policy::Effect::Read].into_iter().collect(),
        path_scopes: Vec::new(),
        idempotency: kolyan_policy::Idempotency::Idempotent,
        approval: kolyan_policy::ApprovalMode::Always,
    });
    let executed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let executor = TurnExecutor::with_tools(
        provider,
        CountingTool {
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("turn-durable"))
    .with_policy_engine(fixture_policy())
    .with_policy_engine(Arc::new(policy));
    let awaiting = executor
        .start_resumable(TurnRequest {
            turn_id: "turn-durable".into(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 2,
                ..TurnConfig::default()
            },
        })
        .await
        .expect("start should return a durable boundary");
    let approval = match awaiting {
        ResumableTurn::Suspended(value) => *value,
        ResumableTurn::Completed(_) => panic!("expected approval"),
    };
    assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let persisted = serde_json::to_vec(&approval).expect("checkpoint serializes");
    let restored: TurnSuspension = serde_json::from_slice(&persisted).expect("checkpoint restores");
    let completed = resume_confirmed(
        &executor,
        restored,
        &approval.waiting.approvals[0].approval_id,
    )
    .await
    .expect("resume should complete");
    let execution = match completed {
        ResumableTurn::Completed(value) => *value,
        ResumableTurn::Suspended(_) => panic!("expected final answer"),
    };
    assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert_eq!(execution.result.steps.len(), 2);
}

#[tokio::test]
async fn durable_batch_waits_for_all_approvals_before_executing_tools() {
    let provider = MultiApprovalProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let model_calls = provider.calls.clone();
    let mut policy = PolicyEngine::default();
    policy.register(kolyan_policy::ToolManifest {
        tool_name: "shell.query".into(),
        capabilities: [kolyan_policy::Capability::ProcessInspect]
            .into_iter()
            .collect(),
        effects: [kolyan_policy::Effect::Read].into_iter().collect(),
        path_scopes: Vec::new(),
        idempotency: kolyan_policy::Idempotency::Idempotent,
        approval: kolyan_policy::ApprovalMode::Always,
    });
    let executed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let executor = TurnExecutor::with_tools(
        provider,
        CountingTool {
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("turn-multi-approval"))
    .with_policy_engine(fixture_policy())
    .with_policy_engine(Arc::new(policy));

    let first = executor
        .start_resumable(TurnRequest {
            turn_id: "turn-multi-approval".into(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 2,
                ..TurnConfig::default()
            },
        })
        .await
        .expect("multi-approval start should succeed");
    let first = match first {
        ResumableTurn::Suspended(value) => *value,
        ResumableTurn::Completed(_) => panic!("expected first approval"),
    };
    assert_eq!(first.waiting.approvals[0].call_id, "call-approval-a");
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);

    let id = first.waiting.approvals[0].approval_id.clone();
    let second = resume_confirmed(&executor, first, &id)
        .await
        .expect("first approval should expose the second boundary");
    let second = match second {
        ResumableTurn::Suspended(value) => *value,
        ResumableTurn::Completed(_) => panic!("expected second approval"),
    };
    assert_eq!(second.waiting.approvals[0].call_id, "call-approval-b");
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);

    let id = second.waiting.approvals[0].approval_id.clone();
    let completed = resume_confirmed(&executor, second, &id)
        .await
        .expect("second approval should execute the batch");
    let execution = match completed {
        ResumableTurn::Completed(value) => *value,
        ResumableTurn::Suspended(_) => panic!("all approvals were granted"),
    };
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
}

#[tokio::test]
async fn turn_tool_budget_fails_closed_before_executing_the_batch() {
    let call = ToolCall {
        id: "call-budget".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let executed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let execution = TurnExecutor::with_tools(
        provider,
        CountingTool {
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("turn-tool-budget"))
    .with_policy_engine(fixture_policy())
    .execute_with_events(
        TurnRequest {
            turn_id: "turn-tool-budget".into(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 2,
                max_tool_calls: Some(0),
                ..TurnConfig::default()
            },
        },
        TurnControl::default(),
    )
    .await
    .expect_err("tool budget must fail closed");
    assert!(matches!(execution, TurnError::ToolBudgetExceeded));
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn turn_cancellation_propagates_to_a_running_step() {
    let executor = TurnExecutor::new(SlowProvider);
    let control = TurnControl::default();
    let task_control = control.clone();
    let task = tokio::spawn(async move {
        executor
            .execute_with_control(
                TurnRequest {
                    turn_id: "turn-cancel-running-step".into(),
                    model_request: request(),
                    config: TurnConfig::default(),
                },
                task_control,
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    control.cancel();
    let error = task
        .await
        .expect("cancelled turn should join")
        .expect_err("cancelled running step must fail");
    assert!(matches!(error, TurnError::Cancelled));
}

#[tokio::test]
async fn turn_deadline_stops_a_slow_model() {
    let error = TurnExecutor::new(SlowProvider)
        .execute_with_control(
            TurnRequest {
                turn_id: "turn-deadline".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 1,
                    deadline: Some(Duration::from_millis(5)),
                    ..TurnConfig::default()
                },
            },
            TurnControl::default(),
        )
        .await
        .expect_err("turn deadline must stop a slow model");
    assert!(matches!(error, TurnError::TimedOut));
}

#[tokio::test]
async fn durable_approval_rejects_a_tampered_checkpoint() {
    let call = ToolCall {
        id: "call-tampered".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let mut policy = PolicyEngine::default();
    policy.register(kolyan_policy::ToolManifest {
        tool_name: "shell.query".into(),
        capabilities: [kolyan_policy::Capability::ProcessInspect]
            .into_iter()
            .collect(),
        effects: [kolyan_policy::Effect::Read].into_iter().collect(),
        path_scopes: Vec::new(),
        idempotency: kolyan_policy::Idempotency::Idempotent,
        approval: kolyan_policy::ApprovalMode::Always,
    });
    let executor = TurnExecutor::with_tools(provider, MockTool)
        .with_execution_key(fixture_key("turn-tampered"))
        .with_policy_engine(fixture_policy())
        .with_policy_engine(Arc::new(policy));
    let awaiting = executor
        .start_resumable(TurnRequest {
            turn_id: "turn-tampered".into(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 2,
                ..TurnConfig::default()
            },
        })
        .await
        .expect("start should return a durable boundary");
    let mut approval = match awaiting {
        ResumableTurn::Suspended(value) => *value,
        ResumableTurn::Completed(_) => panic!("expected approval"),
    };
    let id = approval.waiting.approvals[0].approval_id.clone();
    approval.checkpoint.calls[0].call.arguments =
        serde_json::json!({"command": "delete_everything"});
    let error = resume_confirmed(&executor, approval, &id)
        .await
        .expect_err("tampered checkpoint must fail closed");
    assert!(matches!(error, TurnError::InvalidRequest { .. }));
}

struct CountingTool {
    executed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ToolExecutor for CountingTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move { fixture_prepare(call) })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            let call = fixture_validate(&invocation)?;
            self.executed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: call.id,
                content: "unexpected execution".into(),
                is_error: false,
            }))
        })
    }
}

#[tokio::test]
async fn parallel_dispatch_preserves_tool_result_order() {
    let call = ToolCall {
        id: "call-parallel".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command": "count_lines"}),
    };
    let provider = ScriptedProvider {
        calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call,
    };
    let executor = TurnExecutor::with_tools(provider, MockTool)
        .with_execution_key(fixture_key("turn-parallel"))
        .with_policy_engine(fixture_policy())
        .with_tool_dispatch_policy(ToolDispatchPolicy {
            mode: ToolDispatchMode::Parallel,
            on_error: ToolErrorPolicy::FailTurn,
        });
    let execution = executor
        .execute_with_events(
            TurnRequest {
                turn_id: "turn-parallel".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..TurnConfig::default()
                },
            },
            TurnControl::default(),
        )
        .await
        .expect("parallel mode should execute tool calls");

    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert!(execution.events.iter().any(|event| matches!(
        event,
        TurnEvent::ToolResult { result, .. } if result.call_id == "call-parallel"
    )));
}

#[test]
fn tool_call_batch_rejects_empty_calls() {
    let error = ToolCallBatch::try_from(Vec::new()).expect_err("empty batch must fail");

    assert!(matches!(
        error,
        ToolError::InvalidBatch { message } if message == "tool call batch must not be empty"
    ));
}

#[test]
fn tool_call_batch_rejects_duplicate_call_ids() {
    let calls = vec![
        ToolCall {
            id: "duplicate".into(),
            name: "file.read".into(),
            arguments: serde_json::json!({"path": "a.txt"}),
        },
        ToolCall {
            id: "duplicate".into(),
            name: "file.read".into(),
            arguments: serde_json::json!({"path": "b.txt"}),
        },
    ];

    let error = ToolCallBatch::try_from(calls).expect_err("duplicate ids must fail");

    assert!(matches!(
        error,
        ToolError::InvalidBatch { message } if message == "duplicate tool call id: duplicate"
    ));
}

#[test]
fn tool_call_batch_requires_matching_result_ids() {
    let batch = ToolCallBatch::try_from(vec![ToolCall {
        id: "call-1".into(),
        name: "file.read".into(),
        arguments: serde_json::json!({"path": "a.txt"}),
    }])
    .expect("batch should be valid");
    let result = ToolDispatchResult {
        call_id: "unknown".into(),
        result: Ok(ToolResult {
            call_id: "unknown".into(),
            content: "content".into(),
            is_error: false,
        }),
    };

    let error = batch
        .validate_results(&[result])
        .expect_err("unknown result id must fail");

    assert!(matches!(
        error,
        ToolError::InvalidBatch { message } if message == "unknown tool result call id: unknown"
    ));
}

#[test]
fn tool_call_batch_rejects_result_payload_id_mismatch() {
    let batch = ToolCallBatch::try_from(vec![ToolCall {
        id: "call-1".into(),
        name: "file.read".into(),
        arguments: serde_json::json!({"path": "a.txt"}),
    }])
    .expect("batch should be valid");
    let result = ToolDispatchResult {
        call_id: "call-1".into(),
        result: Ok(ToolResult {
            call_id: "call-2".into(),
            content: "content".into(),
            is_error: false,
        }),
    };

    let error = batch
        .validate_results(&[result])
        .expect_err("payload id mismatch must fail");

    assert!(matches!(
        error,
        ToolError::InvalidBatch { message } if message == "tool result call id mismatch: expected call-1, got call-2"
    ));
}

#[test]
fn turn_state_accepts_tool_loop_transitions() {
    let state = TurnState::Pending
        .transition(TurnState::Running)
        .and_then(|state| state.transition(TurnState::WaitingModel))
        .and_then(|state| state.transition(TurnState::WaitingTool))
        .and_then(|state| state.transition(TurnState::ExecutingTools))
        .and_then(|state| state.transition(TurnState::Running))
        .and_then(|state| state.transition(TurnState::WaitingModel))
        .and_then(|state| state.transition(TurnState::Completed))
        .expect("valid turn tool loop should transition");

    assert_eq!(state, TurnState::Completed);
}

#[test]
fn turn_state_rejects_skipping_model_and_tool_phases() {
    let error = TurnState::Running
        .transition(TurnState::ExecutingTools)
        .expect_err("running must not skip waiting phases");

    assert!(matches!(
        error,
        TurnError::InvalidStateTransition {
            from: TurnState::Running,
            to: TurnState::ExecutingTools
        }
    ));
}

#[test]
fn turn_errors_expose_terminal_reason() {
    assert_eq!(TurnError::Cancelled.end_reason(), TurnEndReason::Cancelled);
    assert_eq!(TurnError::TimedOut.end_reason(), TurnEndReason::TimedOut);
    assert_eq!(TurnError::MaxSteps.end_reason(), TurnEndReason::MaxSteps);
    assert_eq!(
        TurnError::Tool(ToolError::Failed {
            message: "failed".into(),
        })
        .end_reason(),
        TurnEndReason::Failed
    );
    assert_eq!(
        TurnError::Tool(ToolError::Cancelled).end_reason(),
        TurnEndReason::Cancelled
    );
    assert_eq!(
        TurnError::Tool(ToolError::TimedOut).end_reason(),
        TurnEndReason::TimedOut
    );
    assert_eq!(
        TurnError::ToolBudgetExceeded.end_reason(),
        TurnEndReason::Failed
    );
}
