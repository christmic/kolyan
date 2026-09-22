use crate::{StepError, StepExecutionOptions, StepExecutor, StepOutcome, StepRequest, StepResult};
use futures_core::Stream;
use futures_util::stream;
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelProvider, ModelRequest, ToolCall, ToolChoice,
    ToolResult,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct TurnRequest {
    pub turn_id: String,
    pub model_request: ModelRequest,
    pub config: TurnConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnConfig {
    pub max_steps: usize,
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self { max_steps: 1 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolDispatchMode {
    Serial,
    Parallel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolErrorPolicy {
    FailTurn,
    ContinueBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolDispatchPolicy {
    pub mode: ToolDispatchMode,
    pub on_error: ToolErrorPolicy,
}

impl Default for ToolDispatchPolicy {
    fn default() -> Self {
        Self {
            mode: ToolDispatchMode::Serial,
            on_error: ToolErrorPolicy::FailTurn,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    MaxSteps,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TurnResult {
    pub turn_id: String,
    pub outcome: TurnOutcome,
    pub steps: Vec<StepResult>,
}

/// Turn-level observation events. Fine-grained model deltas remain owned by
/// the StepEvent stream; TurnEvent describes the lifecycle and orchestration
/// around those Steps.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEvent {
    Started {
        turn_id: String,
    },
    StepStarted {
        turn_id: String,
        step_id: String,
    },
    StepCompleted {
        turn_id: String,
        step: StepResult,
    },
    ToolCallRequested {
        turn_id: String,
        call: ToolCall,
    },
    ToolExecutionStarted {
        turn_id: String,
        call_id: String,
        name: String,
    },
    ToolResult {
        turn_id: String,
        result: ToolResult,
    },
    ToolExecutionFailed {
        turn_id: String,
        call_id: String,
        name: String,
        error: ToolError,
    },
    Completed {
        turn_id: String,
        outcome: TurnOutcome,
    },
}

pub type TurnEventStream = Pin<Box<dyn Stream<Item = Result<TurnEvent, TurnError>> + Send>>;

#[derive(Debug, Clone, PartialEq)]
pub struct TurnExecution {
    pub result: TurnResult,
    pub events: Vec<TurnEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TurnOutcome {
    FinalAnswer {
        response: kolyan_model::ModelResponse,
    },
    Refused {
        response: kolyan_model::ModelResponse,
    },
    Incomplete {
        response: kolyan_model::ModelResponse,
    },
    MaxSteps,
}

#[derive(Debug, Error)]
pub enum TurnError {
    #[error("turn request is invalid: {message}")]
    InvalidRequest { message: String },
    #[error("turn step failed: {0}")]
    Step(#[from] StepError),
    #[error("turn tool execution failed: {0}")]
    Tool(#[from] ToolError),
    #[error("tool dispatch mode is not supported: {mode:?}")]
    UnsupportedToolDispatch { mode: ToolDispatchMode },
    #[error("turn was cancelled")]
    Cancelled,
    #[error("turn timed out")]
    TimedOut,
    #[error("turn reached its maximum step count")]
    MaxSteps,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ToolError {
    #[error("tool is unavailable: {name}")]
    Unavailable { name: String },
    #[error("tool execution failed: {message}")]
    Failed { message: String },
}

pub type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<ToolResult, ToolError>> + Send + 'a>>;

pub trait ToolExecutor: Send + Sync {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopToolExecutor;

impl ToolExecutor for NoopToolExecutor {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        Box::pin(async move { Err(ToolError::Unavailable { name: call.name }) })
    }
}

#[derive(Clone, Default)]
pub struct TurnControl {
    cancelled: Arc<AtomicBool>,
}

impl TurnControl {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

pub struct TurnExecutor<P, T = NoopToolExecutor> {
    step_executor: StepExecutor<P>,
    tool_executor: T,
    tool_dispatch: ToolDispatchPolicy,
}

impl<P> TurnExecutor<P, NoopToolExecutor> {
    pub fn new(provider: P) -> Self {
        Self {
            step_executor: StepExecutor::new(provider),
            tool_executor: NoopToolExecutor,
            tool_dispatch: ToolDispatchPolicy::default(),
        }
    }
}

impl<P, T: ToolExecutor> TurnExecutor<P, T> {
    pub fn with_tools(provider: P, tool_executor: T) -> Self {
        Self {
            step_executor: StepExecutor::new(provider),
            tool_executor,
            tool_dispatch: ToolDispatchPolicy::default(),
        }
    }

    pub fn with_step_executor(step_executor: StepExecutor<P>, tool_executor: T) -> Self {
        Self {
            step_executor,
            tool_executor,
            tool_dispatch: ToolDispatchPolicy::default(),
        }
    }

    pub fn with_tool_dispatch_policy(mut self, policy: ToolDispatchPolicy) -> Self {
        self.tool_dispatch = policy;
        self
    }
}

impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    pub async fn execute(&self, request: TurnRequest) -> Result<TurnResult, TurnError> {
        Ok(self
            .execute_with_events(request, TurnControl::default())
            .await?
            .result)
    }

    pub async fn execute_with_control(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<TurnResult, TurnError> {
        Ok(self.execute_with_events(request, control).await?.result)
    }

    pub async fn execute_with_events(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<TurnExecution, TurnError> {
        validate_request(&request)?;
        if control.is_cancelled() {
            return Err(TurnError::Cancelled);
        }

        let turn_id = request.turn_id.clone();
        let mut model_request = request.model_request;
        let mut steps = Vec::new();
        let mut events = vec![TurnEvent::Started {
            turn_id: turn_id.clone(),
        }];

        for step_index in 0..request.config.max_steps {
            if control.is_cancelled() {
                return Err(TurnError::Cancelled);
            }

            let step_id = format!("{}-step-{step_index}", request.turn_id);
            events.push(TurnEvent::StepStarted {
                turn_id: turn_id.clone(),
                step_id: step_id.clone(),
            });
            model_request.request_id = step_id.clone();
            let step = self
                .step_executor
                .execute(StepRequest {
                    step_id,
                    model_request: model_request.clone(),
                    options: StepExecutionOptions::default(),
                })
                .await
                .map_err(map_step_error)?;
            steps.push(step.clone());
            events.push(TurnEvent::StepCompleted {
                turn_id: turn_id.clone(),
                step: step.clone(),
            });

            match step.outcome {
                StepOutcome::FinalAnswer => {
                    let result = TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::FinalAnswer {
                            response: step.response,
                        },
                        steps,
                    };
                    events.push(TurnEvent::Completed {
                        turn_id: turn_id.clone(),
                        outcome: result.outcome.clone(),
                    });
                    return Ok(TurnExecution { result, events });
                }
                StepOutcome::Refused => {
                    let result = TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::Refused {
                            response: step.response,
                        },
                        steps,
                    };
                    events.push(TurnEvent::Completed {
                        turn_id: turn_id.clone(),
                        outcome: result.outcome.clone(),
                    });
                    return Ok(TurnExecution { result, events });
                }
                StepOutcome::Incomplete => {
                    let result = TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::Incomplete {
                            response: step.response,
                        },
                        steps,
                    };
                    events.push(TurnEvent::Completed {
                        turn_id: turn_id.clone(),
                        outcome: result.outcome.clone(),
                    });
                    return Ok(TurnExecution { result, events });
                }
                StepOutcome::ToolCalls => {
                    if self.tool_dispatch.mode == ToolDispatchMode::Parallel {
                        return Err(TurnError::UnsupportedToolDispatch {
                            mode: ToolDispatchMode::Parallel,
                        });
                    }
                    model_request.messages.push(Message {
                        role: MessageRole::Assistant,
                        content: step.response.content.clone(),
                    });
                    for call in tool_calls(&step.response.content) {
                        events.push(TurnEvent::ToolCallRequested {
                            turn_id: turn_id.clone(),
                            call: call.clone(),
                        });
                        events.push(TurnEvent::ToolExecutionStarted {
                            turn_id: turn_id.clone(),
                            call_id: call.id.clone(),
                            name: call.name.clone(),
                        });
                        match self.tool_executor.execute(call.clone()).await {
                            Ok(result) => {
                                events.push(TurnEvent::ToolResult {
                                    turn_id: turn_id.clone(),
                                    result: result.clone(),
                                });
                                model_request.messages.push(Message {
                                    role: MessageRole::User,
                                    content: vec![ContentBlock::ToolResult { result }],
                                });
                            }
                            Err(error) => {
                                events.push(TurnEvent::ToolExecutionFailed {
                                    turn_id: turn_id.clone(),
                                    call_id: call.id.clone(),
                                    name: call.name.clone(),
                                    error: error.clone(),
                                });
                                match self.tool_dispatch.on_error {
                                    ToolErrorPolicy::FailTurn => {
                                        return Err(TurnError::Tool(error));
                                    }
                                    ToolErrorPolicy::ContinueBatch => {
                                        let result = ToolResult {
                                            call_id: call.id,
                                            content: error.to_string(),
                                            is_error: true,
                                        };
                                        events.push(TurnEvent::ToolResult {
                                            turn_id: turn_id.clone(),
                                            result: result.clone(),
                                        });
                                        model_request.messages.push(Message {
                                            role: MessageRole::User,
                                            content: vec![ContentBlock::ToolResult { result }],
                                        });
                                    }
                                }
                            }
                        }
                    }
                    model_request.tool_choice = ToolChoice::Auto;
                }
            }
        }

        Err(TurnError::MaxSteps)
    }

    pub async fn execute_event_stream(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<TurnEventStream, TurnError> {
        let execution = self.execute_with_events(request, control).await?;
        let events = execution.events.into_iter().map(Ok);
        Ok(Box::pin(stream::iter(events)))
    }
}

fn validate_request(request: &TurnRequest) -> Result<(), TurnError> {
    if request.turn_id.is_empty() {
        return Err(TurnError::InvalidRequest {
            message: "turn_id must not be empty".into(),
        });
    }
    if request.config.max_steps == 0 {
        return Err(TurnError::InvalidRequest {
            message: "max_steps must be greater than zero".into(),
        });
    }
    Ok(())
}

fn map_step_error(error: StepError) -> TurnError {
    match error {
        StepError::Cancelled => TurnError::Cancelled,
        StepError::TimedOut => TurnError::TimedOut,
        error => TurnError::Step(error),
    }
}

fn tool_calls(content: &[ContentBlock]) -> Vec<ToolCall> {
    content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall { call } => Some(call.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{StreamExt, stream};
    use kolyan_model::{
        ContentBlock, ModelEvent, ModelEventStream, ModelRef, ProviderFuture, StopReason,
        TokenUsage, ToolChoice,
    };
    use serde_json::Value;

    struct MockProvider {
        stop_reason: StopReason,
        content: Vec<ContentBlock>,
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
    async fn returns_tool_unavailable_instead_of_dropping_tool_calls() {
        let call = ToolCall {
            id: "call-1".into(),
            name: "search".into(),
            arguments: serde_json::json!({"query": "kolyan"}),
        };
        let executor = TurnExecutor::new(MockProvider {
            stop_reason: StopReason::ToolUse,
            content: vec![ContentBlock::ToolCall { call: call.clone() }],
        });
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

    struct MockTool;

    impl ToolExecutor for MockTool {
        fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
            Box::pin(async move {
                Ok(ToolResult {
                    call_id: call.id,
                    content: "count=1".into(),
                    is_error: false,
                })
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
        let executor = TurnExecutor::with_tools(provider.clone(), MockTool);
        let result = executor
            .execute(TurnRequest {
                turn_id: "turn-multi-step".into(),
                model_request: request(),
                config: TurnConfig { max_steps: 2 },
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
        fn execute(&self, _call: ToolCall) -> ToolFuture<'_> {
            Box::pin(async {
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
        let executor = TurnExecutor::with_tools(provider, FailingTool).with_tool_dispatch_policy(
            ToolDispatchPolicy {
                mode: ToolDispatchMode::Serial,
                on_error: ToolErrorPolicy::ContinueBatch,
            },
        );
        let execution = executor
            .execute_with_events(
                TurnRequest {
                    turn_id: "turn-continue-batch".into(),
                    model_request: request(),
                    config: TurnConfig { max_steps: 2 },
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
    async fn parallel_dispatch_is_rejected_until_parallel_execution_is_enabled() {
        let call = ToolCall {
            id: "call-parallel".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let executor = TurnExecutor::with_tools(
            MockProvider {
                stop_reason: StopReason::ToolUse,
                content: vec![ContentBlock::ToolCall { call }],
            },
            MockTool,
        )
        .with_tool_dispatch_policy(ToolDispatchPolicy {
            mode: ToolDispatchMode::Parallel,
            on_error: ToolErrorPolicy::FailTurn,
        });
        let error = executor
            .execute(TurnRequest {
                turn_id: "turn-parallel".into(),
                model_request: request(),
                config: TurnConfig { max_steps: 1 },
            })
            .await
            .expect_err("parallel mode must not silently run serially");

        assert!(matches!(
            error,
            TurnError::UnsupportedToolDispatch {
                mode: ToolDispatchMode::Parallel
            }
        ));
    }
}
