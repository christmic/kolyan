//! Scripted model outputs supplied by the case, never actual network acceptance.

use crate::runner::tests::support::Observations;
use crate::{
    AgentSnapshot, ProviderFactory, RunnerError,
    context::{BudgetMode, ContextPolicy, ContextTokenCounter, TokenMeasurement},
    provider::ContextPreparingProvider,
};
use futures_util::stream;
use kolyan_ledger::InMemoryLedger;
use kolyan_model::*;
use kolyan_server::{ExecutionRef, ExecutionService};
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Fault {
    Failed,
    Cancelled,
}

#[derive(Clone)]
pub(super) struct Factory {
    pub observations: Arc<Observations>,
    pub call: ToolCall,
    pub child_call: Option<ToolCall>,
    pub fault: Option<Fault>,
    pub execution_service: ExecutionService<InMemoryLedger, NoopTraceSink>,
}
pub struct Provider {
    observations: Arc<Observations>,
    call: ToolCall,
    child_call: Option<ToolCall>,
    fault: Option<Fault>,
    execution_service: ExecutionService<InMemoryLedger, NoopTraceSink>,
    execution: ExecutionRef,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.observations
            .requests
            .lock()
            .unwrap()
            .push(request.clone());
        let parent = request
            .tools
            .iter()
            .any(|tool| tool.name == crate::AGENT_INVOKE_NAME);
        let has_result = request.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
        });
        let call = if parent {
            Some(self.call.clone())
        } else {
            self.child_call.clone()
        };
        let invoke = !has_result && call.is_some();
        Box::pin(async move {
            if !parent {
                match self.fault {
                    Some(Fault::Failed) => {
                        return Err(ProviderError::new(
                            ProviderErrorKind::InvalidRequest,
                            ProviderErrorPhase::Open,
                            "data-declared child open failure",
                        ));
                    }
                    Some(Fault::Cancelled) => self
                        .execution_service
                        .cancel(&self.execution)
                        .map_err(|error| {
                            ProviderError::new(
                                ProviderErrorKind::Other,
                                ProviderErrorPhase::Open,
                                error.to_string(),
                            )
                        })?,
                    None => {}
                }
            }
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(ModelResponse {
                    id: request.request_id,
                    model: request.model,
                    content: if invoke {
                        vec![ContentBlock::ToolCall {
                            call: call.expect("validated scripted call"),
                        }]
                    } else {
                        vec![ContentBlock::Text {
                            text: "Fixture answer".into(),
                        }]
                    },
                    structured_output: None,
                    stop_reason: if invoke {
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
struct Counter;
impl ContextTokenCounter for Counter {
    fn id(&self) -> &str {
        "unit-pump-counter"
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
        execution: &kolyan_server::ExecutionRef,
    ) -> Result<ContextPreparingProvider<Provider>, RunnerError> {
        Ok(ContextPreparingProvider::new(
            Provider {
                observations: self.observations.clone(),
                call: self.call.clone(),
                child_call: self.child_call.clone(),
                fault: self.fault,
                execution_service: self.execution_service.clone(),
                execution: execution.clone(),
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(10000),
                max_output_tokens: Some(100),
                features: Default::default(),
            },
            ContextPolicy {
                id: "pump-fixture".into(),
                revision: "1".into(),
                mode: BudgetMode::Strict,
                max_serialized_bytes: 65536,
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
