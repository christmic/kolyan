use crate::{StepError, StepExecutionOptions, StepExecutor, StepOutcome, StepRequest, StepResult};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelProvider, ModelRequest, ToolCall, ToolResult,
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
}

impl<P> TurnExecutor<P, NoopToolExecutor> {
    pub fn new(provider: P) -> Self {
        Self {
            step_executor: StepExecutor::new(provider),
            tool_executor: NoopToolExecutor,
        }
    }
}

impl<P, T: ToolExecutor> TurnExecutor<P, T> {
    pub fn with_tools(provider: P, tool_executor: T) -> Self {
        Self {
            step_executor: StepExecutor::new(provider),
            tool_executor,
        }
    }

    pub fn with_step_executor(step_executor: StepExecutor<P>, tool_executor: T) -> Self {
        Self {
            step_executor,
            tool_executor,
        }
    }
}

impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    pub async fn execute(&self, request: TurnRequest) -> Result<TurnResult, TurnError> {
        self.execute_with_control(request, TurnControl::default())
            .await
    }

    pub async fn execute_with_control(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<TurnResult, TurnError> {
        validate_request(&request)?;
        if control.is_cancelled() {
            return Err(TurnError::Cancelled);
        }

        let mut model_request = request.model_request;
        let mut steps = Vec::new();

        for step_index in 0..request.config.max_steps {
            if control.is_cancelled() {
                return Err(TurnError::Cancelled);
            }

            let step_id = format!("{}-step-{step_index}", request.turn_id);
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

            match step.outcome {
                StepOutcome::FinalAnswer => {
                    return Ok(TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::FinalAnswer {
                            response: step.response,
                        },
                        steps,
                    });
                }
                StepOutcome::Refused => {
                    return Ok(TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::Refused {
                            response: step.response,
                        },
                        steps,
                    });
                }
                StepOutcome::Incomplete => {
                    return Ok(TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::Incomplete {
                            response: step.response,
                        },
                        steps,
                    });
                }
                StepOutcome::ToolCalls => {
                    model_request.messages.push(Message {
                        role: MessageRole::Assistant,
                        content: step.response.content.clone(),
                    });
                    for call in tool_calls(&step.response.content) {
                        let result = self.tool_executor.execute(call).await?;
                        model_request.messages.push(Message {
                            role: MessageRole::User,
                            content: vec![ContentBlock::ToolResult { result }],
                        });
                    }
                }
            }
        }

        Err(TurnError::MaxSteps)
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
    use futures_util::stream;
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
}
