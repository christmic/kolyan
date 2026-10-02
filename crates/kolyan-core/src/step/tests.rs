use super::*;
use futures_util::stream;
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelRef, ProviderErrorKind, ProviderErrorPhase,
    ProviderFuture, StopReason, TokenUsage, ToolChoice,
};
use serde_json::Value;
use std::sync::Mutex;

mod liveness;

struct MockProvider {
    fail: bool,
}

struct DeltaProvider;

impl ModelProvider for DeltaProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let response = ModelResponse {
            id: "response-delta".into(),
            model: request.model,
            content: vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            structured_output: None,
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::TextDelta("he".into())),
                Ok(ModelEvent::ReasoningDelta("think".into())),
                Ok(ModelEvent::Usage(TokenUsage {
                    output_tokens: Some(2),
                    ..TokenUsage::default()
                })),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

#[derive(Default)]
struct CapturingRecorder {
    events: Mutex<Vec<StepEvent>>,
    fail_on_text: bool,
}

impl StepEventRecorder for CapturingRecorder {
    fn record(&self, event: &StepEvent) -> Result<(), StepEventRecordError> {
        if self.fail_on_text && matches!(event, StepEvent::TextDelta { .. }) {
            return Err(StepEventRecordError {
                message: "injected recorder failure".into(),
            });
        }
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

impl ModelProvider for MockProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let fail = self.fail;
        Box::pin(async move {
            if fail {
                return Err(ProviderError::new(
                    ProviderErrorKind::Unavailable,
                    ProviderErrorPhase::Open,
                    "mock provider unavailable",
                ));
            }
            let response = ModelResponse {
                id: "response-1".into(),
                model: request.model,
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
                structured_output: None,
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(1),
                    ..TokenUsage::default()
                },
                metadata: Value::Null,
            };
            let events = vec![Ok(ModelEvent::Started), Ok(ModelEvent::Completed(response))];
            Ok(Box::pin(stream::iter(events)) as ModelEventStream)
        })
    }
}

fn request() -> ModelRequest {
    ModelRequest {
        request_id: "request-1".into(),
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
async fn executes_one_model_step_to_completion() {
    let executor = StepExecutor::new(MockProvider { fail: false });
    let result = executor
        .execute(StepRequest {
            step_id: "step-1".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await
        .expect("step should complete");

    assert_eq!(result.step_id, "step-1");
    assert_eq!(result.response.id, "response-1");
    assert_eq!(result.response.stop_reason, StopReason::EndTurn);
    assert_eq!(result.outcome, StepOutcome::FinalAnswer);
}

#[tokio::test]
async fn exposes_step_events_and_aggregates_the_same_stream_shape() {
    let executor = StepExecutor::new(MockProvider { fail: false });
    let mut stream = executor
        .execute_stream(StepRequest {
            step_id: "step-stream".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await
        .expect("step stream should open");

    assert!(
        matches!(stream.next().await, Some(Ok(StepEvent::Started { step_id })) if step_id == "step-stream")
    );
    assert!(
        matches!(stream.next().await, Some(Ok(StepEvent::Completed(result))) if result.step_id == "step-stream")
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn records_neutral_stream_events_before_returning_the_step_result() {
    let recorder = Arc::new(CapturingRecorder::default());
    let executor = StepExecutor::new(DeltaProvider).with_event_recorder(recorder.clone());
    executor
        .execute(StepRequest {
            step_id: "step-recorded".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await
        .unwrap();

    let events = recorder.events.lock().unwrap();
    assert!(matches!(events[0], StepEvent::Started { .. }));
    assert!(matches!(&events[1], StepEvent::TextDelta { text, .. } if text == "he"));
    assert!(matches!(&events[2], StepEvent::ReasoningDelta { text, .. } if text == "think"));
    assert!(matches!(events[3], StepEvent::Usage { .. }));
    assert!(matches!(events[4], StepEvent::Completed(_)));
}

#[tokio::test]
async fn recorder_failure_stops_the_step_before_completion() {
    let recorder = Arc::new(CapturingRecorder {
        events: Mutex::new(Vec::new()),
        fail_on_text: true,
    });
    let error = StepExecutor::new(DeltaProvider)
        .with_event_recorder(recorder.clone())
        .execute(StepRequest {
            step_id: "step-recording-fails".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, StepError::Recording(_)));
    let events = recorder.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StepEvent::Started { .. }));
}

#[tokio::test]
async fn cancellation_preserves_started_before_cancelled() {
    let executor = StepExecutor::new(MockProvider { fail: false });
    let execution = executor
        .start(StepRequest {
            step_id: "step-cancelled".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await
        .expect("step should open");
    execution.control.cancel();
    let mut stream = execution.stream;

    assert!(matches!(
        stream.next().await,
        Some(Ok(StepEvent::Started { .. }))
    ));
    assert!(matches!(
        stream.next().await,
        Some(Ok(StepEvent::Cancelled { .. }))
    ));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn execute_converts_cancelled_and_timed_out_terminal_events_to_errors() {
    let executor = StepExecutor::new(MockProvider { fail: false });
    let execution = executor
        .start(StepRequest {
            step_id: "step-timeout".into(),
            model_request: request(),
            options: StepExecutionOptions {
                deadline: Some(Instant::now() - std::time::Duration::from_secs(1)),
                ..StepExecutionOptions::default()
            },
        })
        .await
        .expect("step should open");
    let error = aggregate_step_stream(execution.stream)
        .await
        .expect_err("expired step should time out");
    assert!(matches!(error, StepError::TimedOut));
}

#[tokio::test]
async fn aggregate_rejects_events_after_completed() {
    struct TrailingProvider;

    impl ModelProvider for TrailingProvider {
        fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
            let response = ModelResponse {
                id: "response-trailing".into(),
                model: request.model,
                content: Vec::new(),
                structured_output: None,
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
                metadata: Value::Null,
            };
            Box::pin(async move {
                Ok(Box::pin(stream::iter(vec![
                    Ok(ModelEvent::Started),
                    Ok(ModelEvent::Completed(response)),
                    Ok(ModelEvent::TextDelta("late".into())),
                ])) as ModelEventStream)
            })
        }
    }

    let executor = StepExecutor::new(TrailingProvider);
    let error = executor
        .execute(StepRequest {
            step_id: "step-trailing".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await
        .expect_err("late event should violate the stream contract");
    assert!(matches!(error, StepError::Protocol { .. }));
}

#[tokio::test]
async fn validator_rejects_completed_result_before_it_reaches_the_stream() {
    struct RejectValidator;

    impl StepValidator for RejectValidator {
        fn validate(
            &self,
            _request: &ModelRequest,
            _result: &StepResult,
        ) -> Result<(), StepValidationError> {
            Err(StepValidationError {
                message: "mock validation failure".into(),
            })
        }
    }

    let executor = StepExecutor::with_validator(MockProvider { fail: false }, RejectValidator);
    let error = executor
        .execute(StepRequest {
            step_id: "step-invalid".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await
        .expect_err("validator should reject the result");

    assert!(matches!(error, StepError::Validation(_)));
}

#[tokio::test]
async fn returns_provider_error_without_retrying_or_swallowing() {
    let executor = StepExecutor::new(MockProvider { fail: true });
    let error = executor
        .execute(StepRequest {
            step_id: "step-1".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await
        .expect_err("provider error should fail the step");

    assert!(matches!(error, StepError::Provider(_)));
}
