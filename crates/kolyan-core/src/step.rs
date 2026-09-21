use kolyan_model::{ModelProvider, ModelRequest, ModelResponse, ProviderError, aggregate_stream};
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
    pub async fn execute(&self, request: StepRequest) -> Result<StepResult, StepError> {
        let step_id = request.step_id;
        let stream = self.provider.stream(request.model_request).await?;
        let response = aggregate_stream(stream).await?;
        Ok(StepResult { step_id, response })
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
