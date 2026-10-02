use super::super::super::*;

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use kolyan_model::{
    ModelEventStream, ProviderErrorKind, ProviderErrorPhase, ProviderFuture, StopReason,
};

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Mode {
    OpeningPending,
    OpeningError,
    Pending,
    CompletedPending,
    Normal,
    StreamError,
    Empty,
    Trailing,
    TrailingStarted,
    TrailingError,
}

#[derive(Default)]
pub(super) struct Observed {
    pub requests: Mutex<Vec<ModelRequest>>,
    pub calls: AtomicUsize,
    pub opening_polls: AtomicUsize,
    pub opening_drops: AtomicUsize,
    pub stream_polls: AtomicUsize,
    pub stream_drops: AtomicUsize,
}

pub(super) struct ScriptProvider {
    pub state: Arc<Observed>,
    pub mode: Mode,
}

impl ModelProvider for ScriptProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.state.requests.lock().unwrap().push(request.clone());
        self.state.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(Opening {
            state: self.state.clone(),
            mode: self.mode,
            request: Some(request),
        })
    }
}

struct Opening {
    state: Arc<Observed>,
    mode: Mode,
    request: Option<ModelRequest>,
}

impl Future for Opening {
    type Output = Result<ModelEventStream, ProviderError>;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.opening_polls.fetch_add(1, Ordering::SeqCst);
        if matches!(self.mode, Mode::OpeningPending) {
            return Poll::Pending;
        }
        if matches!(self.mode, Mode::OpeningError) {
            return Poll::Ready(Err(provider_error(ProviderErrorPhase::Open)));
        }
        let request = self
            .request
            .take()
            .expect("opening polled after completion");
        let completed = ModelEvent::Completed(ModelResponse {
            id: "liveness-response".into(),
            model: request.model,
            content: Vec::new(),
            structured_output: None,
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            metadata: serde_json::Value::Null,
        });
        let events = match self.mode {
            Mode::Normal | Mode::CompletedPending => vec![Ok(completed)],
            Mode::Trailing => vec![Ok(completed), Ok(ModelEvent::TextDelta("late".into()))],
            Mode::TrailingStarted => vec![Ok(completed), Ok(ModelEvent::Started)],
            Mode::TrailingError => {
                vec![
                    Ok(completed),
                    Err(provider_error(ProviderErrorPhase::Stream)),
                ]
            }
            Mode::StreamError => vec![Err(provider_error(ProviderErrorPhase::Stream))],
            _ => Vec::new(),
        };
        Poll::Ready(Ok(Box::pin(ScriptStream {
            state: self.state.clone(),
            events: events.into(),
            pending: matches!(self.mode, Mode::Pending | Mode::CompletedPending),
        })))
    }
}

impl Drop for Opening {
    fn drop(&mut self) {
        self.state.opening_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct ScriptStream {
    state: Arc<Observed>,
    events: VecDeque<Result<ModelEvent, ProviderError>>,
    pending: bool,
}

impl Stream for ScriptStream {
    type Item = Result<ModelEvent, ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.state.stream_polls.fetch_add(1, Ordering::SeqCst);
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(Some(event));
        }
        if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

impl Drop for ScriptStream {
    fn drop(&mut self) {
        self.state.stream_drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn provider_error(phase: ProviderErrorPhase) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Unavailable,
        phase,
        "liveness fixture failure",
    )
}

pub(super) fn event_label(item: Result<StepEvent, StepError>) -> String {
    match item {
        Ok(StepEvent::Started { .. }) => "started",
        Ok(StepEvent::Completed(_)) => "completed",
        Ok(StepEvent::Cancelled { .. }) => "cancelled",
        Ok(StepEvent::TimedOut { .. }) => "timed_out",
        Err(StepError::Cancelled) => "error_cancelled",
        Err(StepError::TimedOut) => "error_timed_out",
        Err(StepError::Recording(_)) => "error_recording",
        Err(StepError::Validation(_)) => "error_validation",
        Err(StepError::Protocol { .. }) => "error_protocol",
        Err(StepError::Provider(error)) => match (error.kind, error.phase) {
            (ProviderErrorKind::Unavailable, ProviderErrorPhase::Open) => {
                "error_provider_unavailable_open"
            }
            (ProviderErrorKind::Unavailable, ProviderErrorPhase::Stream) => {
                "error_provider_unavailable_stream"
            }
            _ => "unexpected_provider_error",
        },
        _ => "unexpected_event",
    }
    .into()
}

pub(super) struct RejectRecorder;

impl StepEventRecorder for RejectRecorder {
    fn record(&self, _: &StepEvent) -> Result<(), StepEventRecordError> {
        Err(StepEventRecordError {
            message: "liveness fixture rejection".into(),
        })
    }
}

pub(super) struct RejectValidator;

impl StepValidator for RejectValidator {
    fn validate(&self, _: &ModelRequest, _: &StepResult) -> Result<(), StepValidationError> {
        Err(StepValidationError {
            message: "liveness fixture rejection".into(),
        })
    }
}
