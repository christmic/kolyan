//! Scripted root invocation and child response with an explicit test-only barrier.

use std::sync::Arc;

use futures_util::stream;
use kolyan_model::*;
use kolyan_server::ExecutionRef;
use tokio::sync::Notify;

use crate::runner::tests::support::Observations;
use crate::{
    AgentSnapshot, ProviderFactory, RunnerError, context::*, provider::ContextPreparingProvider,
};

#[derive(Clone)]
pub(super) struct Factory {
    pub observations: Arc<Observations>,
    pub call: ToolCall,
    pub child_answer: String,
    pub hold_child: bool,
    pub opened: Arc<Notify>,
    pub release: Arc<Notify>,
}

pub struct Provider {
    factory: Factory,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.factory
            .observations
            .requests
            .lock()
            .unwrap()
            .push(request.clone());
        let parent = request
            .tools
            .iter()
            .any(|tool| tool.name == crate::AGENT_INVOKE_NAME);
        Box::pin(async move {
            if !parent {
                self.factory.opened.notify_one();
                if self.factory.hold_child {
                    self.factory.release.notified().await;
                }
            }
            let content = if parent {
                vec![ContentBlock::ToolCall {
                    call: self.factory.call.clone(),
                }]
            } else {
                vec![ContentBlock::Text {
                    text: self.factory.child_answer.clone(),
                }]
            };
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(ModelResponse {
                    id: request.request_id,
                    model: request.model,
                    content,
                    structured_output: None,
                    stop_reason: if parent {
                        StopReason::ToolUse
                    } else {
                        StopReason::EndTurn
                    },
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

impl ProviderFactory for Factory {
    type Provider = Provider;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Provider>, RunnerError> {
        Ok(ContextPreparingProvider::new(
            Provider {
                factory: self.clone(),
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(10000),
                max_output_tokens: Some(100),
                features: Default::default(),
            },
            ContextPolicy {
                id: "late-context".into(),
                revision: "1".into(),
                mode: BudgetMode::Strict,
                max_serialized_bytes: 65536,
                max_messages: 100,
                max_content_blocks: 200,
                context_limit_tokens: None,
                output_reserve_tokens: 100,
            },
            Arc::new(super::super::provider::Counter),
            self.observations.clone(),
        ))
    }
}
