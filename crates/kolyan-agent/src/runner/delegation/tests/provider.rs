//! Deterministic module scripts; not an actual network Provider or tokenizer.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::stream;
use kolyan_ledger::InMemoryLedger;
use kolyan_model::{
    ContentBlock, ModelDescriptor, ModelEvent, ModelEventStream, ModelProvider, ModelRef,
    ModelRequest, ModelResponse, ProviderError, ProviderErrorKind, ProviderErrorPhase,
    ProviderFuture, StopReason, TokenUsage,
};
use kolyan_server::{ExecutionRef, ExecutionService};
use kolyan_trace::NoopTraceSink;
use serde::{Deserialize, Serialize};

use crate::runner::tests::support::Observations;
use crate::{
    AgentSnapshot, ProviderFactory, RunnerError,
    context::{BudgetMode, ContextPolicy, ContextTokenCounter, TokenMeasurement},
    provider::ContextPreparingProvider,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Script {
    Completed,
    Failed,
    Cancelled,
}

pub(super) struct ScriptFactory {
    pub observations: Arc<Observations>,
    pub script: Script,
    pub execution: ExecutionService<InMemoryLedger, NoopTraceSink>,
    pub concurrency: Arc<Concurrency>,
}

pub(super) struct Concurrency {
    pub active: AtomicUsize,
    pub peak: AtomicUsize,
    pub barrier: Option<tokio::sync::Barrier>,
}
struct InFlight(Arc<Concurrency>);
impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct ScriptProvider {
    observations: Arc<Observations>,
    script: Script,
    service: ExecutionService<InMemoryLedger, NoopTraceSink>,
    execution: ExecutionRef,
    concurrency: Arc<Concurrency>,
}

impl ModelProvider for ScriptProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.observations
            .requests
            .lock()
            .unwrap()
            .push(request.clone());
        Box::pin(async move {
            let active = self.concurrency.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.concurrency.peak.fetch_max(active, Ordering::SeqCst);
            let _inflight = InFlight(self.concurrency.clone());
            if let Some(barrier) = &self.concurrency.barrier {
                barrier.wait().await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if self.script == Script::Failed {
                return Err(ProviderError::new(
                    ProviderErrorKind::InvalidRequest,
                    ProviderErrorPhase::Open,
                    "Fixture child failure",
                ));
            }
            if self.script == Script::Cancelled {
                self.service.cancel(&self.execution).map_err(|error| {
                    ProviderError::new(
                        ProviderErrorKind::Other,
                        ProviderErrorPhase::Open,
                        error.to_string(),
                    )
                })?;
            }
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(ModelResponse {
                    id: request.request_id,
                    model: request.model,
                    content: vec![ContentBlock::Text {
                        text: "Fixture child answer".into(),
                    }],
                    structured_output: None,
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage {
                        input_tokens: Some(1),
                        output_tokens: Some(2),
                        ..Default::default()
                    },
                    metadata: serde_json::json!({"fixture":true}),
                })),
            ])) as ModelEventStream)
        })
    }
}

struct UnitCounter;
impl ContextTokenCounter for UnitCounter {
    fn id(&self) -> &str {
        "unit-child-counter"
    }
    fn revision(&self) -> &str {
        "1"
    }
    fn count(&self, _: &ModelRequest, _: &[u8]) -> Result<TokenMeasurement, String> {
        Ok(TokenMeasurement::Trusted { input_tokens: 1 })
    }
}

impl ProviderFactory for ScriptFactory {
    type Provider = ScriptProvider;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<ScriptProvider>, RunnerError> {
        let reference: ModelRef = snapshot.definition().model().clone();
        Ok(ContextPreparingProvider::new(
            ScriptProvider {
                observations: self.observations.clone(),
                script: self.script,
                service: self.execution.clone(),
                execution: execution.clone(),
                concurrency: self.concurrency.clone(),
            },
            ModelDescriptor {
                reference,
                context_window: Some(10000),
                max_output_tokens: Some(100),
                features: Default::default(),
            },
            ContextPolicy {
                id: "child-context".into(),
                revision: "1".into(),
                mode: BudgetMode::Strict,
                max_serialized_bytes: 65536,
                max_messages: 100,
                max_content_blocks: 200,
                context_limit_tokens: None,
                output_reserve_tokens: 50,
            },
            Arc::new(UnitCounter),
            self.observations.clone(),
        ))
    }
}
