//! Input-keyed scripted outputs, supplied entirely by the case dataset.

use crate::runner::tests::support::Observations;
use crate::{
    AgentSnapshot, ProviderFactory, RunnerError,
    context::{BudgetMode, ContextPolicy, ContextTokenCounter, TokenMeasurement},
    provider::ContextPreparingProvider,
};
use futures_util::stream;
use kolyan_model::*;
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub(super) struct Concurrency {
    pub active: AtomicUsize,
    pub peak: AtomicUsize,
    pub delay_ms: u64,
}
struct InFlight(Arc<Concurrency>);
impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Script {
    pub input: String,
    pub call: Option<ToolCall>,
    pub answer: String,
}
#[derive(Clone)]
pub(super) struct Factory {
    pub observations: Arc<Observations>,
    pub scripts: Vec<Script>,
    pub concurrency: Arc<Concurrency>,
}
pub struct Provider {
    observations: Arc<Observations>,
    scripts: Vec<Script>,
    concurrency: Arc<Concurrency>,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.observations
            .requests
            .lock()
            .unwrap()
            .push(request.clone());
        let seed = request
            .messages
            .iter()
            .find(|message| message.role == MessageRole::User)
            .and_then(|message| {
                message.content.iter().find_map(|block| match block {
                    ContentBlock::Text { text } => Some(text),
                    _ => None,
                })
            });
        let script = seed
            .and_then(|seed| self.scripts.iter().find(|script| &script.input == seed))
            .cloned();
        let completed = request.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
        });
        Box::pin(async move {
            let active = self.concurrency.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.concurrency.peak.fetch_max(active, Ordering::SeqCst);
            let _inflight = InFlight(self.concurrency.clone());
            if self.concurrency.delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(self.concurrency.delay_ms))
                    .await;
            }
            let script = script.ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::InvalidRequest,
                    ProviderErrorPhase::Open,
                    "unknown fixture input",
                )
            })?;
            let call = if completed { None } else { script.call };
            let stop_reason = if call.is_some() {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            };
            let content = if let Some(call) = call {
                vec![ContentBlock::ToolCall { call }]
            } else {
                vec![ContentBlock::Text {
                    text: script.answer,
                }]
            };
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(ModelResponse {
                    id: request.request_id,
                    model: request.model,
                    content,
                    stop_reason,
                    structured_output: None,
                    usage: TokenUsage {
                        input_tokens: Some(1),
                        output_tokens: Some(2),
                        ..Default::default()
                    },
                    metadata: serde_json::json!({"scripted":true}),
                })),
            ])) as ModelEventStream)
        })
    }
}
struct Counter;
impl ContextTokenCounter for Counter {
    fn id(&self) -> &str {
        "unit-continuation-counter"
    }
    fn revision(&self) -> &str {
        "1"
    }
    fn count(&self, _: &ModelRequest, _: &[u8]) -> Result<TokenMeasurement, String> {
        Ok(TokenMeasurement::Trusted { input_tokens: 1 })
    }
}
impl ProviderFactory for Factory {
    type Provider = Provider;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        _: &kolyan_server::ExecutionRef,
    ) -> Result<ContextPreparingProvider<Provider>, RunnerError> {
        Ok(ContextPreparingProvider::new(
            Provider {
                observations: self.observations.clone(),
                scripts: self.scripts.clone(),
                concurrency: self.concurrency.clone(),
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(10000),
                max_output_tokens: Some(100),
                features: Default::default(),
            },
            ContextPolicy {
                id: "continuation-fixture".into(),
                revision: "1".into(),
                mode: BudgetMode::Strict,
                max_serialized_bytes: 131072,
                max_messages: 100,
                max_content_blocks: 200,
                context_limit_tokens: None,
                output_reserve_tokens: 100,
            },
            Arc::new(Counter),
            self.observations.clone(),
        ))
    }
}
