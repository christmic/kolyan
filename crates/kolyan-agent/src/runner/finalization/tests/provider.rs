//! Dataset-controlled model terminal behavior; no model-network acceptance.

use std::sync::Arc;

use futures_util::stream;
use kolyan_ledger::InMemoryLedger;
use kolyan_model::*;
use kolyan_server::{ExecutionRef, ExecutionService};
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;

use crate::runner::tests::support::Observations;
use crate::{
    AgentSnapshot, ProviderFactory, RunnerError, context::*, provider::ContextPreparingProvider,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::runner::finalization) struct Script {
    pub fault: Option<Fault>,
    pub answer: String,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::runner::finalization) enum Fault {
    Failed,
    Cancelled,
}

pub(in crate::runner::finalization) struct Factory {
    pub observations: Arc<Observations>,
    pub script: Script,
    pub execution: ExecutionService<InMemoryLedger, NoopTraceSink>,
}
pub struct Provider {
    observations: Arc<Observations>,
    script: Script,
    execution: ExecutionService<InMemoryLedger, NoopTraceSink>,
    owner: ExecutionRef,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.observations
            .requests
            .lock()
            .unwrap()
            .push(request.clone());
        Box::pin(async move {
            match self.script.fault {
                Some(Fault::Failed) => {
                    return Err(ProviderError::new(
                        ProviderErrorKind::InvalidRequest,
                        ProviderErrorPhase::Open,
                        "dataset-declared root failure",
                    ));
                }
                Some(Fault::Cancelled) => self.execution.cancel(&self.owner).map_err(|error| {
                    ProviderError::new(
                        ProviderErrorKind::Other,
                        ProviderErrorPhase::Open,
                        error.to_string(),
                    )
                })?,
                None => {}
            }
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(ModelResponse {
                    id: request.request_id,
                    model: request.model,
                    content: vec![ContentBlock::Text {
                        text: self.script.answer.clone(),
                    }],
                    structured_output: None,
                    stop_reason: StopReason::EndTurn,
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
pub(in crate::runner::finalization) struct Counter;
impl ContextTokenCounter for Counter {
    fn id(&self) -> &str {
        "finalization-unit-counter"
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
        owner: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Provider>, RunnerError> {
        Ok(ContextPreparingProvider::new(
            Provider {
                observations: self.observations.clone(),
                script: self.script.clone(),
                execution: self.execution.clone(),
                owner: owner.clone(),
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(10000),
                max_output_tokens: Some(100),
                features: Default::default(),
            },
            ContextPolicy {
                id: "finalization-context".into(),
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
