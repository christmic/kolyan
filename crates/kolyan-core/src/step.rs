use futures_core::Stream;
use futures_util::StreamExt;
use kolyan_model::{
    ModelEvent, ModelProvider, ModelRequest, ModelResponse, ProviderError, ProviderMetadata,
    TokenUsage, ToolCall,
};
use std::pin::Pin;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct StepRequest {
    pub step_id: String,
    pub model_request: ModelRequest,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StepResult {
    pub step_id: String,
    pub response: ModelResponse,
}

/// Step-level events. Provider-specific model events are translated into this
/// type so callers do not need to know which protocol produced the stream.
#[derive(Debug, Clone, PartialEq)]
pub enum StepEvent {
    Started {
        step_id: String,
    },
    TextDelta {
        step_id: String,
        text: String,
    },
    ReasoningDelta {
        step_id: String,
        text: String,
    },
    ToolCallStarted {
        step_id: String,
        id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        step_id: String,
        id: String,
        delta: String,
    },
    ToolCallCompleted {
        step_id: String,
        call: ToolCall,
    },
    Usage {
        step_id: String,
        usage: TokenUsage,
    },
    Provider {
        step_id: String,
        metadata: ProviderMetadata,
    },
    Completed(StepResult),
}

pub type StepEventStream = Pin<Box<dyn Stream<Item = Result<StepEvent, StepError>> + Send>>;

#[derive(Debug, Error)]
pub enum StepError {
    #[error("step provider error: {0}")]
    Provider(#[from] ProviderError),
}

pub struct StepExecutor<P> {
    provider: P,
}

impl<P> StepExecutor<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }
}

impl<P: ModelProvider> StepExecutor<P> {
    pub async fn execute_stream(&self, request: StepRequest) -> Result<StepEventStream, StepError> {
        let step_id = request.step_id;
        let stream = self.provider.stream(request.model_request).await?;
        let events = stream.map(move |event| {
            let step_id = step_id.clone();
            event
                .map(|event| map_model_event(step_id, event))
                .map_err(StepError::from)
        });
        Ok(Box::pin(events))
    }

    pub async fn execute(&self, request: StepRequest) -> Result<StepResult, StepError> {
        let stream = self.execute_stream(request).await?;
        aggregate_step_stream(stream).await
    }
}

pub async fn aggregate_step_stream(mut stream: StepEventStream) -> Result<StepResult, StepError> {
    while let Some(event) = stream.next().await {
        if let StepEvent::Completed(result) = event? {
            return Ok(result);
        }
    }
    Err(StepError::Provider(ProviderError::new(
        kolyan_model::ProviderErrorKind::Protocol,
        kolyan_model::ProviderErrorPhase::Stream,
        "step stream ended without a completed result",
    )))
}

fn map_model_event(step_id: String, event: ModelEvent) -> StepEvent {
    match event {
        ModelEvent::Started => StepEvent::Started { step_id },
        ModelEvent::TextDelta(text) => StepEvent::TextDelta { step_id, text },
        ModelEvent::ReasoningDelta(text) => StepEvent::ReasoningDelta { step_id, text },
        ModelEvent::ToolCallStarted { id, name } => {
            StepEvent::ToolCallStarted { step_id, id, name }
        }
        ModelEvent::ToolCallArgumentsDelta { id, delta } => {
            StepEvent::ToolCallArgumentsDelta { step_id, id, delta }
        }
        ModelEvent::ToolCallCompleted(call) => StepEvent::ToolCallCompleted { step_id, call },
        ModelEvent::Usage(usage) => StepEvent::Usage { step_id, usage },
        ModelEvent::Provider(metadata) => StepEvent::Provider { step_id, metadata },
        ModelEvent::Completed(response) => StepEvent::Completed(StepResult { step_id, response }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use kolyan_model::{
        ContentBlock, ModelEvent, ModelEventStream, ModelRef, ProviderErrorKind,
        ProviderErrorPhase, ProviderFuture, StopReason, TokenUsage, ToolChoice,
    };
    use serde_json::Value;

    struct MockProvider {
        fail: bool,
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
            })
            .await
            .expect("step should complete");

        assert_eq!(result.step_id, "step-1");
        assert_eq!(result.response.id, "response-1");
        assert_eq!(result.response.stop_reason, StopReason::EndTurn);
    }

    #[tokio::test]
    async fn exposes_step_events_and_aggregates_the_same_stream_shape() {
        let executor = StepExecutor::new(MockProvider { fail: false });
        let mut stream = executor
            .execute_stream(StepRequest {
                step_id: "step-stream".into(),
                model_request: request(),
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
    async fn returns_provider_error_without_retrying_or_swallowing() {
        let executor = StepExecutor::new(MockProvider { fail: true });
        let error = executor
            .execute(StepRequest {
                step_id: "step-1".into(),
                model_request: request(),
            })
            .await
            .expect_err("provider error should fail the step");

        assert!(matches!(error, StepError::Provider(_)));
    }
}
