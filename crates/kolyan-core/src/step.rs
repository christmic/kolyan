use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::task::AtomicWaker;
use kolyan_model::{
    ModelEvent, ModelProvider, ModelRequest, ModelResponse, ProviderError, ProviderMetadata,
    TokenUsage, ToolCall,
};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::Instant;
use thiserror::Error;

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
    pub deadline: Option<Instant>,
    pub context_version: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Clone)]
pub struct StepControl {
    state: Arc<StepControlState>,
}

struct StepControlState {
    cancelled: AtomicBool,
    waker: AtomicWaker,
}

impl Default for StepControl {
    fn default() -> Self {
        Self {
            state: Arc::new(StepControlState {
                cancelled: AtomicBool::new(false),
                waker: AtomicWaker::new(),
            }),
        }
    }
}

impl StepControl {
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.waker.wake();
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    fn register(&self, waker: &std::task::Waker) {
        self.state.waker.register(waker);
    }
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
    #[error("step was cancelled")]
    Cancelled,
    #[error("step timed out")]
    TimedOut,
}

pub struct StepExecutor<P> {
    provider: P,
    validator: Arc<dyn StepValidator>,
}

impl<P> StepExecutor<P> {
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            validator: Arc::new(NoopStepValidator),
        }
    }

    pub fn with_validator<V: StepValidator + 'static>(provider: P, validator: V) -> Self {
        Self {
            provider,
            validator: Arc::new(validator),
        }
    }
}

impl<P: ModelProvider> StepExecutor<P> {
    pub async fn start(&self, request: StepRequest) -> Result<StepExecution, StepError> {
        self.start_with_control(request, StepControl::default())
            .await
    }

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
        let stream = self.provider.stream(model_request.clone()).await?;
        let events = ControlledStepStream {
            inner: stream,
            step_id,
            model_request,
            validator: self.validator.clone(),
            options,
            control: control.clone(),
            started: false,
            completed: false,
            terminal: false,
        };

        Ok(StepExecution {
            stream: Box::pin(events),
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
    inner: kolyan_model::ModelEventStream,
    step_id: String,
    model_request: ModelRequest,
    validator: Arc<dyn StepValidator>,
    options: StepExecutionOptions,
    control: StepControl,
    started: bool,
    completed: bool,
    terminal: bool,
}

impl Stream for ControlledStepStream {
    type Item = Result<StepEvent, StepError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal {
            return Poll::Ready(None);
        }

        self.control.register(cx.waker());

        if !self.started {
            self.started = true;
            return Poll::Ready(Some(Ok(StepEvent::Started {
                step_id: self.step_id.clone(),
            })));
        }

        if self.control.is_cancelled() {
            self.terminal = true;
            return Poll::Ready(Some(Ok(StepEvent::Cancelled {
                step_id: self.step_id.clone(),
            })));
        } else if self.options.deadline.is_some_and(is_expired) {
            self.terminal = true;
            return Poll::Ready(Some(Ok(StepEvent::TimedOut {
                step_id: self.step_id.clone(),
            })));
        }

        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => {
                if matches!(event, ModelEvent::Started) {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                let is_terminal = matches!(event, ModelEvent::Completed(_));
                if self.completed {
                    self.terminal = true;
                    return Poll::Ready(Some(Err(StepError::Protocol {
                        message: "event received after completed".into(),
                    })));
                }
                let mapped = map_model_event(self.step_id.clone(), event);
                if is_terminal {
                    if let StepEvent::Completed(result) = &mapped
                        && let Err(error) = self.validator.validate(&self.model_request, result)
                    {
                        self.terminal = true;
                        return Poll::Ready(Some(Err(StepError::Validation(error))));
                    }
                    self.completed = true;
                }
                Poll::Ready(Some(Ok(mapped)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.terminal = true;
                if self.completed {
                    return Poll::Ready(Some(Err(StepError::Protocol {
                        message: "provider error received after completed".into(),
                    })));
                }
                Poll::Ready(Some(Err(StepError::Provider(error))))
            }
            Poll::Ready(None) => {
                self.terminal = true;
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

fn is_expired(deadline: Instant) -> bool {
    Instant::now() >= deadline
}

#[cfg(test)]
mod tests;
