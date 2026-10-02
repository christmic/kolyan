//! Bound actual model waits in the fixture, without replacing generated events.

use super::super::evidence::Evidence;
use futures_util::{StreamExt, stream};
use kolyan_model::{
    ModelEventStream, ModelProvider, ModelRef, ModelRequest, ProviderError, ProviderErrorKind,
    ProviderErrorPhase, ProviderFuture,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub(super) struct Selection {
    pub model: ModelRef,
    pub live: Option<Arc<dyn ModelProvider>>,
}

impl Selection {
    pub fn offline() -> Self {
        Self {
            model: ModelRef::new("fixture", "native-running"),
            live: None,
        }
    }
}

pub(super) struct BoundedModel {
    pub inner: Arc<dyn ModelProvider>,
    pub wait: Duration,
    pub evidence: Arc<Evidence>,
}

impl ModelProvider for BoundedModel {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + self.wait;
            self.evidence.append(json!({"event":"model_wait_started","request_id":request.request_id,"model_timeout_ms":self.wait.as_millis()})).expect("retain model wait origin");
            let opened = tokio::time::timeout_at(deadline, self.inner.stream(request)).await;
            let inner = match opened {
                Ok(Ok(inner)) => inner,
                Ok(Err(error)) => {
                    record_error(&self.evidence, &error);
                    return Err(error);
                }
                Err(_) => return Err(timeout(&self.evidence, ProviderErrorPhase::Open)),
            };
            let evidence = self.evidence.clone();
            Ok(Box::pin(stream::unfold(
                (inner, false),
                move |(mut inner, expired)| {
                    let evidence = evidence.clone();
                    async move {
                        if expired {
                            return None;
                        }
                        match tokio::time::timeout_at(deadline, inner.next()).await {
                            Ok(Some(event)) => {
                                if let Err(error) = &event {
                                    record_error(&evidence, error);
                                }
                                Some((event, (inner, false)))
                            }
                            Ok(None) => None,
                            Err(_) => Some((
                                Err(timeout(&evidence, ProviderErrorPhase::Stream)),
                                (inner, true),
                            )),
                        }
                    }
                },
            )) as ModelEventStream)
        })
    }
}

fn record_error(evidence: &Evidence, error: &ProviderError) {
    evidence.append(json!({"event":"original_provider_error","kind":error.kind,
        "phase":error.phase,"message":error.message,"provider":error.provider,"status":error.status}))
        .expect("retain original Provider error fields");
}

fn timeout(evidence: &Evidence, phase: ProviderErrorPhase) -> ProviderError {
    evidence
        .append(json!({"event":"fixture_model_timeout","phase":phase,
        "classification":"test_wait_bound_not_provider_diagnosis"}))
        .expect("retain fixture deadline");
    ProviderError::new(
        ProviderErrorKind::Other,
        phase,
        "native-running fixture model wait exceeded",
    )
}

#[cfg(test)]
mod tests;
