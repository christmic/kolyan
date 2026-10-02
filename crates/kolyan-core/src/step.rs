//! One bounded model call, its neutral events, and verified stream completion.
//! Cancellation/deadline cover opening and consumption without background work.

mod control;
pub use control::StepControl;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use futures_core::Stream;
use futures_util::StreamExt;
use kolyan_model::{
    ModelEvent, ModelProvider, ModelRequest, ModelResponse, ProviderError, ProviderMetadata,
    TokenUsage, ToolCall,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use control::{Boundary, Stop};

#[derive(Debug, Clone, PartialEq)]
pub struct StepRequest {
    pub step_id: String,
    pub model_request: ModelRequest,
    pub options: StepExecutionOptions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepResult {
    pub step_id: String,
    pub response: ModelResponse,
    pub outcome: StepOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepOutcome {
    FinalAnswer,
    ToolCalls,
    Refused,
    Incomplete,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepExecutionOptions {
    /// One absolute deadline for opening, events, and EOF verification. A future
    /// deadline requires a Tokio runtime with its time driver enabled.
    pub deadline: Option<Instant>,
    pub context_version: Option<String>,
    pub trace_id: Option<String>,
}

pub struct StepExecution {
    pub stream: StepEventStream,
    pub control: StepControl,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("step validation error: {message}")]
pub struct StepValidationError {
    pub message: String,
}

pub trait StepValidator: Send + Sync {
    fn validate(
        &self,
        request: &ModelRequest,
        result: &StepResult,
    ) -> Result<(), StepValidationError>;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("step event recording failed: {message}")]
pub struct StepEventRecordError {
    pub message: String,
}

/// Records neutral model-stream observations before the Step advances.
pub trait StepEventRecorder: Send + Sync {
    fn record(&self, event: &StepEvent) -> Result<(), StepEventRecordError>;
}

#[derive(Debug, Default)]
pub struct NoopStepValidator;

impl StepValidator for NoopStepValidator {
    fn validate(
        &self,
        _request: &ModelRequest,
        _result: &StepResult,
    ) -> Result<(), StepValidationError> {
        Ok(())
    }
}

/// Step-level events. Provider-specific model events are translated into this
/// type so callers do not need to know which protocol produced the stream.
#[derive(Debug, Clone, PartialEq)]
pub enum StepEvent {
    Started {
        step_id: String,
    },
    TextDelta {
        step_id: String,
        text: String,
    },
    ReasoningDelta {
        step_id: String,
        text: String,
    },
    ToolCallStarted {
        step_id: String,
        id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        step_id: String,
        id: String,
        delta: String,
    },
    ToolCallCompleted {
        step_id: String,
        call: ToolCall,
    },
    Usage {
        step_id: String,
        usage: TokenUsage,
    },
    Provider {
        step_id: String,
        metadata: ProviderMetadata,
    },
    Completed(StepResult),
    Cancelled {
        step_id: String,
    },
    TimedOut {
        step_id: String,
    },
}

pub type StepEventStream = Pin<Box<dyn Stream<Item = Result<StepEvent, StepError>> + Send>>;

#[derive(Debug, Error)]
pub enum StepError {
    #[error("step provider error: {0}")]
    Provider(#[from] ProviderError),
    #[error("step protocol error: {message}")]
    Protocol { message: String },
    #[error("step request is invalid: {message}")]
    InvalidRequest { message: String },
    #[error(transparent)]
    Validation(#[from] StepValidationError),
    #[error(transparent)]
    Recording(#[from] StepEventRecordError),
    #[error("step was cancelled")]
    Cancelled,
    #[error("step timed out")]
    TimedOut,
}

pub struct StepExecutor<P> {
    provider: P,
    validator: Arc<dyn StepValidator>,
    event_recorder: Option<Arc<dyn StepEventRecorder>>,
}

impl<P> StepExecutor<P> {
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            validator: Arc::new(NoopStepValidator),
            event_recorder: None,
        }
    }

    pub fn with_validator<V: StepValidator + 'static>(provider: P, validator: V) -> Self {
        Self {
            provider,
            validator: Arc::new(validator),
            event_recorder: None,
        }
    }

    pub fn with_event_recorder(mut self, recorder: Arc<dyn StepEventRecorder>) -> Self {
        self.event_recorder = Some(recorder);
        self
    }
}

impl<P: ModelProvider> StepExecutor<P> {
    /// Open one Step. A local stop yields Started followed by its terminal event;
    /// Provider opening failures remain errors. No retries or background tasks run.
    pub async fn start(&self, request: StepRequest) -> Result<StepExecution, StepError> {
        self.start_with_control(request, StepControl::default())
            .await
    }

    /// Bound Provider opening and consumption with the same control and deadline.
    /// Dropping this future releases the opening future without remote-stop proof.
    pub async fn start_with_control(
        &self,
        request: StepRequest,
        control: StepControl,
    ) -> Result<StepExecution, StepError> {
        if request.step_id.is_empty() {
            return Err(StepError::InvalidRequest {
                message: "step_id must not be empty".into(),
            });
        }

        let step_id = request.step_id;
        let options = request.options;
        let model_request = request.model_request;
        let mut boundary = Boundary::new(control.clone(), options.deadline);
        let (stream, stopped) = match boundary.current_stop() {
            Some(stop) => (None, Some(stop)),
            None => {
                // The borrowed opening future is dropped before constructing the
                // lifecycle stream on either outcome. Its budget is never reset.
                tokio::select! {
                    biased;
                    stop = boundary.stopped() => (None, Some(stop)),
                    result = self.provider.stream(model_request.clone()) => (Some(result?), None),
                }
            }
        };
        let events: StepEventStream = Box::pin(ControlledStepStream {
            inner: stream,
            step_id,
            model_request,
            validator: self.validator.clone(),
            boundary: Some(boundary),
            stopped,
            started: false,
            completed: false,
            terminal: false,
        });
        let events = match &self.event_recorder {
            Some(recorder) => Box::pin(RecordingStepStream {
                inner: Some(events),
                recorder: recorder.clone(),
                terminal: false,
            }) as StepEventStream,
            None => events,
        };

        Ok(StepExecution {
            stream: events,
            control,
        })
    }

    pub async fn execute_stream(&self, request: StepRequest) -> Result<StepEventStream, StepError> {
        Ok(self.start(request).await?.stream)
    }

    pub async fn execute(&self, request: StepRequest) -> Result<StepResult, StepError> {
        let execution = self.start(request).await?;
        aggregate_step_stream(execution.stream).await
    }

    pub async fn execute_with_control(
        &self,
        request: StepRequest,
        control: StepControl,
    ) -> Result<StepResult, StepError> {
        let execution = self.start_with_control(request, control).await?;
        aggregate_step_stream(execution.stream).await
    }
}

struct RecordingStepStream {
    inner: Option<StepEventStream>,
    recorder: Arc<dyn StepEventRecorder>,
    terminal: bool,
}

impl Stream for RecordingStepStream {
    type Item = Result<StepEvent, StepError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        match self
            .inner
            .as_mut()
            .expect("active recording stream")
            .as_mut()
            .poll_next(cx)
        {
            Poll::Ready(Some(Ok(event))) => match self.recorder.record(&event) {
                Ok(()) => Poll::Ready(Some(Ok(event))),
                Err(error) => {
                    self.terminal = true;
                    self.inner = None;
                    Poll::Ready(Some(Err(StepError::Recording(error))))
                }
            },
            Poll::Ready(Some(Err(error))) => {
                self.terminal = true;
                self.inner = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.terminal = true;
                self.inner = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub async fn aggregate_step_stream(mut stream: StepEventStream) -> Result<StepResult, StepError> {
    let mut completed = None;
    while let Some(event) = stream.next().await {
        match event? {
            StepEvent::Completed(result) if completed.is_none() => completed = Some(result),
            StepEvent::Cancelled { .. } => return Err(StepError::Cancelled),
            StepEvent::TimedOut { .. } => return Err(StepError::TimedOut),
            StepEvent::Completed(_) => {
                return Err(StepError::Protocol {
                    message: "multiple completed events".into(),
                });
            }
            _ if completed.is_some() => {
                return Err(StepError::Protocol {
                    message: "event received after completed".into(),
                });
            }
            _ => {}
        }
    }
    completed.ok_or_else(|| StepError::Protocol {
        message: "step stream ended without a terminal event".into(),
    })
}

fn map_model_event(step_id: String, event: ModelEvent) -> StepEvent {
    match event {
        ModelEvent::Started => StepEvent::Started { step_id },
        ModelEvent::TextDelta(text) => StepEvent::TextDelta { step_id, text },
        ModelEvent::ReasoningDelta(text) => StepEvent::ReasoningDelta { step_id, text },
        ModelEvent::ToolCallStarted { id, name } => {
            StepEvent::ToolCallStarted { step_id, id, name }
        }
        ModelEvent::ToolCallArgumentsDelta { id, delta } => {
            StepEvent::ToolCallArgumentsDelta { step_id, id, delta }
        }
        ModelEvent::ToolCallCompleted(call) => StepEvent::ToolCallCompleted { step_id, call },
        ModelEvent::Usage(usage) => StepEvent::Usage { step_id, usage },
        ModelEvent::Provider(metadata) => StepEvent::Provider { step_id, metadata },
        ModelEvent::Completed(response) => {
            let outcome = match response.stop_reason {
                kolyan_model::StopReason::EndTurn => StepOutcome::FinalAnswer,
                kolyan_model::StopReason::ToolUse => StepOutcome::ToolCalls,
                kolyan_model::StopReason::Refusal => StepOutcome::Refused,
                kolyan_model::StopReason::MaxOutputTokens | kolyan_model::StopReason::Other(_) => {
                    StepOutcome::Incomplete
                }
            };
            StepEvent::Completed(StepResult {
                step_id,
                response,
                outcome,
            })
        }
    }
}

struct ControlledStepStream {
    inner: Option<kolyan_model::ModelEventStream>,
    step_id: String,
    model_request: ModelRequest,
    validator: Arc<dyn StepValidator>,
    boundary: Option<Boundary>,
    stopped: Option<Stop>,
    started: bool,
    completed: bool,
    terminal: bool,
}

impl ControlledStepStream {
    fn release(&mut self) {
        self.terminal = true;
        self.inner = None;
        self.boundary = None;
    }

    fn stop_event(&mut self, stop: Stop) -> Result<StepEvent, StepError> {
        self.release();
        match (stop, self.completed) {
            (Stop::Cancelled, true) => Err(StepError::Cancelled),
            (Stop::TimedOut, true) => Err(StepError::TimedOut),
            (Stop::Cancelled, false) => Ok(StepEvent::Cancelled {
                step_id: self.step_id.clone(),
            }),
            (Stop::TimedOut, false) => Ok(StepEvent::TimedOut {
                step_id: self.step_id.clone(),
            }),
        }
    }
}

impl Stream for ControlledStepStream {
    type Item = Result<StepEvent, StepError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal {
            return Poll::Ready(None);
        }

        if !self.started {
            self.started = true;
            return Poll::Ready(Some(Ok(StepEvent::Started {
                step_id: self.step_id.clone(),
            })));
        }

        let stop = self.stopped.or_else(|| {
            match self
                .boundary
                .as_mut()
                .expect("active Step boundary")
                .poll_stop(cx)
            {
                Poll::Ready(stop) => Some(stop),
                Poll::Pending => None,
            }
        });
        if let Some(stop) = stop {
            return Poll::Ready(Some(self.stop_event(stop)));
        }

        match self
            .inner
            .as_mut()
            .expect("active Provider stream")
            .as_mut()
            .poll_next(cx)
        {
            Poll::Ready(Some(Ok(event))) => {
                if self.completed {
                    self.release();
                    return Poll::Ready(Some(Err(StepError::Protocol {
                        message: "event received after completed".into(),
                    })));
                }
                if matches!(event, ModelEvent::Started) {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                let is_terminal = matches!(event, ModelEvent::Completed(_));
                let mapped = map_model_event(self.step_id.clone(), event);
                if is_terminal {
                    if let StepEvent::Completed(result) = &mapped
                        && let Err(error) = self.validator.validate(&self.model_request, result)
                    {
                        self.release();
                        return Poll::Ready(Some(Err(StepError::Validation(error))));
                    }
                    self.completed = true;
                }
                Poll::Ready(Some(Ok(mapped)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.release();
                if self.completed {
                    return Poll::Ready(Some(Err(StepError::Protocol {
                        message: "provider error received after completed".into(),
                    })));
                }
                Poll::Ready(Some(Err(StepError::Provider(error))))
            }
            Poll::Ready(None) => {
                self.release();
                if self.completed {
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(StepError::Protocol {
                    message: "step stream ended without a terminal event".into(),
                })))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests;
