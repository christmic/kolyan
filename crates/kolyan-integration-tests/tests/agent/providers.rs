//! Actual Provider streams are observed without injecting network tool calls.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use futures_util::{StreamExt, stream};
use kolyan_agent::{
    AgentSnapshot, ProviderFactory, RunnerError,
    context::{BudgetMode, ContextPolicy, ContextTokenCounter, TokenMeasurement},
    provider::ContextPreparingProvider,
};
use kolyan_model::{
    ContentBlock, ModelDescriptor, ModelEvent, ModelEventStream, ModelProvider, ModelRequest,
    ModelResponse, ProviderFuture, StopReason,
};
use kolyan_server::ExecutionRef;
use serde_json::json;

use super::{data::Dataset, evidence::Evidence};

pub struct UnsupportedCounter;
impl ContextTokenCounter for UnsupportedCounter {
    fn id(&self) -> &str {
        "unsupported-model-counter"
    }
    fn revision(&self) -> &str {
        "1"
    }
    fn count(&self, _: &ModelRequest, _: &[u8]) -> Result<TokenMeasurement, String> {
        Ok(TokenMeasurement::Unknown {
            reason: "No trusted provider-specific token counter is installed".into(),
            estimated_input_tokens: None,
        })
    }
}

#[derive(Clone)]
pub struct Observed {
    inner: Arc<dyn ModelProvider>,
    evidence: Arc<Evidence>,
}
impl ModelProvider for Observed {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            self.evidence
                .append(json!({"event":"request","request":request}))
                .unwrap();
            let stream = self.inner.stream(request).await;
            let stream = match stream {
                Ok(stream) => stream,
                Err(error) => {
                    self.evidence
                        .append(json!({"event":"provider_error","error":error.to_string()}))
                        .unwrap();
                    return Err(error);
                }
            };
            let evidence = self.evidence.clone();
            Ok(Box::pin(stream.map(move |event| {
                let record = match &event {
                    Ok(content) => json!({"event":"model_event","content":content}),
                    Err(error) => json!({"event":"model_stream_error","error":error.to_string()}),
                };
                evidence.append(record).unwrap();
                event
            })) as ModelEventStream)
        })
    }
}

struct Scripted(Mutex<VecDeque<super::data::Frame>>);
impl ModelProvider for Scripted {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let frame = self
            .0
            .lock()
            .unwrap()
            .pop_front()
            .expect("script exhausted");
        Box::pin(async move {
            let tools = frame
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolCall { .. }));
            let mut content = vec![ContentBlock::Reasoning {
                text: frame.reasoning.clone(),
                opaque: None,
            }];
            content.extend(frame.content);
            let response = ModelResponse {
                id: request.request_id,
                model: request.model,
                content,
                structured_output: None,
                stop_reason: if tools {
                    StopReason::ToolUse
                } else {
                    StopReason::EndTurn
                },
                usage: frame.usage,
                metadata: json!({"source":"fixture-script"}),
            };
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::ReasoningDelta(frame.reasoning)),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

pub struct Providers {
    pub live: Option<Arc<dyn ModelProvider>>,
    pub dataset: Dataset,
    pub evidence: Arc<Evidence>,
}
impl ProviderFactory for Providers {
    type Provider = Observed;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Observed>, RunnerError> {
        let inner = if let Some(provider) = &self.live {
            provider.clone()
        } else {
            let turn = self
                .dataset
                .turns
                .iter()
                .find(|turn| execution.turn_id.ends_with(&format!("-{}", turn.id)))
                .ok_or_else(|| RunnerError::Host("unknown scripted Turn".into()))?;
            Arc::new(Scripted(Mutex::new(turn.script.clone().into()))) as Arc<dyn ModelProvider>
        };
        Ok(ContextPreparingProvider::new(
            Observed {
                inner,
                evidence: self.evidence.clone(),
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(self.dataset.inspection_window_assumption_tokens),
                max_output_tokens: None,
                features: Default::default(),
            },
            ContextPolicy {
                id: "agent-integration-inspection".into(),
                revision: "1".into(),
                mode: BudgetMode::Inspect,
                max_serialized_bytes: 4 * 1024 * 1024,
                max_messages: 2048,
                max_content_blocks: 8192,
                context_limit_tokens: None,
                output_reserve_tokens: self.dataset.output_reserve_tokens,
            },
            Arc::new(UnsupportedCounter),
            self.evidence.clone(),
        ))
    }
}
