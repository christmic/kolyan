//! Deterministic public-API integration tests for Turn failure contracts.

use futures_util::stream;
use kolyan_core::{
    ToolDispatchMode, ToolDispatchPolicy, ToolError, ToolErrorPolicy, ToolExecutor, ToolFuture,
    TurnConfig, TurnControl, TurnError, TurnExecutor, TurnOutcome, TurnRequest,
};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelEvent, ModelEventStream, ModelProvider, ModelRef,
    ModelRequest, ModelResponse, ProviderError, ProviderErrorKind, ProviderErrorPhase,
    ProviderFuture, StopReason, TokenUsage, ToolCall, ToolChoice, ToolResult,
};
use serde_json::Value;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Clone)]
struct ScriptedProvider {
    responses: Arc<Mutex<Vec<ModelResponse>>>,
    requests: Arc<AtomicUsize>,
    snapshots: Arc<Mutex<Vec<ModelRequest>>>,
}

impl ScriptedProvider {
    fn new(responses: Vec<ModelResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses)),
            requests: Arc::new(AtomicUsize::new(0)),
            snapshots: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    fn snapshots(&self) -> Vec<ModelRequest> {
        self.snapshots
            .lock()
            .expect("snapshot lock must not be poisoned")
            .clone()
    }
}

impl ModelProvider for ScriptedProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.snapshots
            .lock()
            .expect("snapshot lock must not be poisoned")
            .push(request);
        let response = self
            .responses
            .lock()
            .expect("response lock must not be poisoned")
            .remove(0);
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

struct FailingProvider;

impl ModelProvider for FailingProvider {
    fn stream(&self, _request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async {
            Err(ProviderError::new(
                ProviderErrorKind::Transport,
                ProviderErrorPhase::Open,
                "synthetic provider failure",
            ))
        })
    }
}

#[derive(Clone)]
struct SelectiveTool {
    fail_call_id: Option<String>,
}

struct HangingTool;

impl ToolExecutor for HangingTool {
    fn execute(&self, _call: ToolCall) -> ToolFuture<'_> {
        Box::pin(async { std::future::pending::<Result<ToolResult, ToolError>>().await })
    }
}

impl ToolExecutor for SelectiveTool {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        let fail_call_id = self.fail_call_id.clone();
        Box::pin(async move {
            if fail_call_id.as_deref() == Some(call.id.as_str()) {
                return Err(ToolError::Failed {
                    message: format!("synthetic failure for {}", call.id),
                });
            }
            Ok(ToolResult {
                call_id: call.id,
                content: "ok".into(),
                is_error: false,
            })
        })
    }
}

#[tokio::test]
async fn fail_turn_stops_before_the_next_model_request() {
    let provider = ScriptedProvider::new(vec![tool_response(vec![
        tool_call("call-1"),
        tool_call("call-2"),
    ])]);
    let executor = TurnExecutor::with_tools(
        provider.clone(),
        SelectiveTool {
            fail_call_id: Some("call-1".into()),
        },
    );

    let error = executor
        .execute(TurnRequest {
            turn_id: "turn-fail-fast".into(),
            model_request: request(),
            config: TurnConfig { max_steps: 2 },
        })
        .await
        .expect_err("FailTurn must return the tool failure");

    assert!(matches!(error, TurnError::Tool(ToolError::Failed { .. })));
    assert_eq!(provider.request_count(), 1);
    assert!(provider.snapshots()[0].messages.iter().all(|message| {
        !message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
    }));
}

#[tokio::test]
async fn continue_batch_returns_success_and_error_results_together() {
    let provider = ScriptedProvider::new(vec![
        tool_response(vec![tool_call("call-1"), tool_call("call-2")]),
        final_response(),
    ]);
    let executor = TurnExecutor::with_tools(
        provider.clone(),
        SelectiveTool {
            fail_call_id: Some("call-2".into()),
        },
    )
    .with_tool_dispatch_policy(ToolDispatchPolicy {
        mode: ToolDispatchMode::Serial,
        on_error: ToolErrorPolicy::ContinueBatch,
    });

    let result = executor
        .execute(TurnRequest {
            turn_id: "turn-continue-batch".into(),
            model_request: request(),
            config: TurnConfig { max_steps: 2 },
        })
        .await
        .expect("ContinueBatch must continue to the final response");

    assert_eq!(provider.request_count(), 2);
    let second_request = &provider.snapshots()[1];
    let tool_results: Vec<&ToolResult> = second_request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolResult { result } => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(tool_results.len(), 2);
    assert!(tool_results.iter().any(|result| result.call_id == "call-1"));
    assert!(
        tool_results
            .iter()
            .any(|result| result.call_id == "call-2" && result.is_error)
    );
    assert!(matches!(
        result.outcome,
        kolyan_core::TurnOutcome::FinalAnswer { .. }
    ));
}

#[tokio::test]
async fn invalid_empty_batch_is_a_tool_error() {
    let provider = ScriptedProvider::new(vec![tool_response(Vec::new())]);
    let executor = TurnExecutor::new(provider.clone());

    let error = executor
        .execute(TurnRequest {
            turn_id: "turn-empty-batch".into(),
            model_request: request(),
            config: TurnConfig::default(),
        })
        .await
        .expect_err("empty batch must fail");

    assert!(matches!(
        error,
        TurnError::Tool(ToolError::InvalidBatch { message })
            if message == "tool call batch must not be empty"
    ));
    assert_eq!(provider.request_count(), 1);
}

#[tokio::test]
async fn max_steps_stops_an_unfinished_tool_loop() {
    let provider = ScriptedProvider::new(vec![tool_response(vec![tool_call("call-1")])]);
    let executor = TurnExecutor::with_tools(provider.clone(), SelectiveTool { fail_call_id: None });

    let result = executor
        .execute(TurnRequest {
            turn_id: "turn-max-steps".into(),
            model_request: request(),
            config: TurnConfig { max_steps: 1 },
        })
        .await
        .expect("unfinished tool loop must produce a terminal result");

    assert!(matches!(result.outcome, TurnOutcome::MaxSteps));
    assert_eq!(provider.request_count(), 1);
}

#[tokio::test]
async fn cancellation_before_model_request_is_terminal() {
    let provider = ScriptedProvider::new(vec![final_response()]);
    let executor = TurnExecutor::new(provider.clone());
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
        .expect_err("cancelled Turn must not start");

    assert!(matches!(error, TurnError::Cancelled));
    assert_eq!(provider.request_count(), 0);
}

#[tokio::test]
async fn provider_failure_does_not_start_a_tool_or_second_step() {
    let executor = TurnExecutor::new(FailingProvider);

    let error = executor
        .execute(TurnRequest {
            turn_id: "turn-provider-failure".into(),
            model_request: request(),
            config: TurnConfig { max_steps: 2 },
        })
        .await
        .expect_err("provider failure must propagate");

    assert!(matches!(error, TurnError::Step(_)));
}

#[tokio::test]
async fn cancellation_interrupts_a_running_tool() {
    let provider = ScriptedProvider::new(vec![tool_response(vec![tool_call("call-1")])]);
    let executor = TurnExecutor::with_tools(provider, HangingTool);
    let control = TurnControl::default();
    let task_control = control.clone();
    let task = tokio::spawn(async move {
        executor
            .execute_with_control(
                TurnRequest {
                    turn_id: "turn-tool-cancelled".into(),
                    model_request: request(),
                    config: TurnConfig { max_steps: 1 },
                },
                task_control,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    control.cancel();
    let error = task
        .await
        .expect("cancelled tool task must join")
        .expect_err("cancelled tool must fail the Turn");

    assert!(matches!(error, TurnError::Tool(ToolError::Cancelled)));
}

#[tokio::test]
async fn tool_timeout_interrupts_a_running_tool() {
    let provider = ScriptedProvider::new(vec![tool_response(vec![tool_call("call-1")])]);
    let executor = TurnExecutor::with_tools(provider, HangingTool)
        .with_tool_timeout(Duration::from_millis(10));

    let error = executor
        .execute(TurnRequest {
            turn_id: "turn-tool-timeout".into(),
            model_request: request(),
            config: TurnConfig { max_steps: 1 },
        })
        .await
        .expect_err("timed out tool must fail the Turn");

    assert!(matches!(error, TurnError::Tool(ToolError::TimedOut)));
}

fn request() -> ModelRequest {
    ModelRequest {
        request_id: "integration-turn-request".into(),
        model: ModelRef::new("test", "model"),
        system: Vec::new(),
        messages: vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: "test".into(),
            }],
        }],
        tools: vec![],
        tool_choice: ToolChoice::Auto,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: Some(128),
        extensions: Value::Null,
    }
}

fn tool_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "test.tool".into(),
        arguments: serde_json::json!({}),
    }
}

fn tool_response(calls: Vec<ToolCall>) -> ModelResponse {
    ModelResponse {
        id: "test-tool-response".into(),
        model: ModelRef::new("test", "model"),
        content: calls
            .into_iter()
            .map(|call| ContentBlock::ToolCall { call })
            .collect(),
        structured_output: None,
        stop_reason: StopReason::ToolUse,
        usage: TokenUsage::default(),
        metadata: Value::Null,
    }
}

fn final_response() -> ModelResponse {
    ModelResponse {
        id: "test-final-response".into(),
        model: ModelRef::new("test", "model"),
        content: vec![ContentBlock::Text {
            text: "done".into(),
        }],
        structured_output: None,
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::default(),
        metadata: Value::Null,
    }
}
