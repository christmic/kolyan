//! Test observations cover Provider invocation/open/stream lifetime, not server timing.

use std::{
    collections::BTreeSet,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures_util::Stream;
use kolyan_model::{
    ModelEvent, ModelEventStream, ProviderError, ProviderErrorKind, ProviderErrorPhase,
};
use kolyan_server::ExecutionRef;
use serde_json::json;

use super::super::evidence::Evidence;

#[derive(Default)]
struct State {
    active: usize,
    peak: usize,
    entered: BTreeSet<String>,
}

pub(super) struct ProviderOverlap {
    state: Mutex<State>,
    rendezvous: Option<tokio::sync::Barrier>,
    evidence: Arc<Evidence>,
}

impl ProviderOverlap {
    pub fn new(parallel: bool, evidence: Arc<Evidence>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            rendezvous: parallel.then(|| tokio::sync::Barrier::new(2)),
            evidence,
        })
    }

    pub async fn enter(self: &Arc<Self>, execution: &ExecutionRef) -> Result<Span, ProviderError> {
        let first = {
            let mut state = self.state.lock().unwrap();
            state.active += 1;
            state.peak = state.peak.max(state.active);
            self.evidence.append(json!({"event":"child_provider_interval","phase":"entered","execution":execution,"active":state.active,"peak":state.peak,"meaning":"provider_invocation_including_open_and_stream"})).unwrap();
            state.entered.insert(execution.execution_id.clone())
        };
        let span = Span {
            observer: self.clone(),
            execution: execution.clone(),
            completed: false,
        };
        if first && let Some(barrier) = &self.rendezvous {
            // Forces the offline driver to expose real scheduling: a serial
            // dispatch cannot pass this rendezvous. It neither fabricates calls
            // nor substitutes child effects or network results.
            tokio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
                .await
                .map_err(|_| {
                    ProviderError::new(
                        ProviderErrorKind::Other,
                        ProviderErrorPhase::Open,
                        "actual child scheduling did not reach the two-peer observation barrier",
                    )
                })?;
        }
        Ok(span)
    }

    pub fn peak(&self) -> usize {
        self.state.lock().unwrap().peak
    }
    pub fn active(&self) -> usize {
        self.state.lock().unwrap().active
    }
}

pub(super) struct Span {
    observer: Arc<ProviderOverlap>,
    execution: ExecutionRef,
    completed: bool,
}

impl Drop for Span {
    fn drop(&mut self) {
        let mut state = self.observer.state.lock().unwrap();
        state.active = state
            .active
            .checked_sub(1)
            .expect("balanced observation span");
        self.observer.evidence.append_from_drop(json!({"event":"child_provider_interval","phase":"released","execution":self.execution,"active":state.active,"completed_event_seen":self.completed,"meaning":"provider_invocation_including_open_and_stream"}));
    }
}

struct ObservedStream {
    inner: ModelEventStream,
    span: Option<Span>,
}
impl Stream for ObservedStream {
    type Item = Result<ModelEvent, ProviderError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll_next(cx);
        if matches!(&result, Poll::Ready(Some(Ok(ModelEvent::Completed(_))))) {
            if let Some(span) = &mut this.span {
                span.completed = true;
            }
            this.span.take();
        } else if matches!(&result, Poll::Ready(None)) {
            this.span.take();
        }
        result
    }
}

pub(super) fn observe_stream(inner: ModelEventStream, span: Option<Span>) -> ModelEventStream {
    Box::pin(ObservedStream { inner, span })
}
