use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use futures_util::Stream;
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelEvent, ModelEventStream, ModelRef, ToolCall,
    ToolChoice, ToolResult,
};
use serde_json::json;

use super::*;
use crate::context::{BudgetMode, BudgetStatus, SerializedByteEstimator, TokenMeasurement};

#[derive(Default)]
struct Observations {
    order: Mutex<Vec<&'static str>>,
    records: Mutex<Vec<ContextRecord>>,
    requests: Mutex<Vec<ModelRequest>>,
    stream_drops: Arc<AtomicUsize>,
}

struct Recorder {
    observations: Arc<Observations>,
    fail: bool,
}
impl ContextRecorder for Recorder {
    fn record(&self, record: &ContextRecord) -> Result<(), ContextRecordError> {
        self.observations.order.lock().unwrap().push(match record {
            ContextRecord::Prepared { .. } => "prepared",
            ContextRecord::Rejected { .. } => "rejected",
        });
        self.observations
            .records
            .lock()
            .unwrap()
            .push(record.clone());
        if self.fail {
            Err(ContextRecordError {
                message: "evidence store refused".into(),
            })
        } else {
            Ok(())
        }
    }
}

struct Counter {
    tokens: u64,
    fail: bool,
}
impl ContextTokenCounter for Counter {
    fn id(&self) -> &str {
        "test-counter"
    }
    fn revision(&self) -> &str {
        "1"
    }
    fn count(&self, _: &ModelRequest, _: &[u8]) -> Result<TokenMeasurement, String> {
        if self.fail {
            Err("counter unavailable".into())
        } else {
            Ok(TokenMeasurement::Trusted {
                input_tokens: self.tokens,
            })
        }
    }
}

struct Provider {
    observations: Arc<Observations>,
    open_failure: bool,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        // Observe method invocation itself, not just polling of the returned future.
        self.observations.order.lock().unwrap().push("provider");
        self.observations.requests.lock().unwrap().push(request);
        Box::pin(async move {
            if self.open_failure {
                return Err(provider_error(ProviderErrorPhase::Open));
            }
            let events = VecDeque::from([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::ReasoningDelta("reasoning chunk".into())),
                Ok(ModelEvent::TextDelta("text chunk".into())),
                Ok(ModelEvent::ToolCallStarted {
                    id: "call".into(),
                    name: "file.read".into(),
                }),
                Ok(ModelEvent::ToolCallArgumentsDelta {
                    id: "call".into(),
                    delta: "{}".into(),
                }),
                Ok(ModelEvent::ToolCallCompleted(ToolCall {
                    id: "call".into(),
                    name: "file.read".into(),
                    arguments: json!({}),
                })),
                Err(provider_error(ProviderErrorPhase::Stream)),
            ]);
            Ok(Box::pin(EventStream {
                events,
                drops: self.observations.stream_drops.clone(),
            }) as ModelEventStream)
        })
    }
}

struct EventStream {
    events: VecDeque<Result<ModelEvent, ProviderError>>,
    drops: Arc<AtomicUsize>,
}
impl Stream for EventStream {
    type Item = Result<ModelEvent, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.events.pop_front())
    }
}
impl Drop for EventStream {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut context = Context::from_waker(Waker::noop());
    match Box::pin(future).as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("unit future must be immediately ready"),
    }
}
fn next(stream: &mut ModelEventStream) -> Option<Result<ModelEvent, ProviderError>> {
    match stream
        .as_mut()
        .poll_next(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("unit stream must be immediately ready"),
    }
}

fn provider_error(phase: ProviderErrorPhase) -> ProviderError {
    let mut error = ProviderError::new(
        ProviderErrorKind::RateLimited,
        phase,
        "original provider error",
    );
    error.provider = Some("test-provider".into());
    error.status = Some(429);
    error
}
fn source() -> ModelRequest {
    ModelRequest {
        request_id: "step-0".into(),
        model: ModelRef::new("test-provider", "test-model"),
        system: Vec::new(),
        messages: vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: "Read the input.".into(),
            }],
        }],
        tools: Vec::new(),
        tool_choice: ToolChoice::Required,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: Some(100),
        extensions: json!({"vendor.option":{"preserve":true}}),
    }
}
fn descriptor() -> ModelDescriptor {
    ModelDescriptor {
        reference: source().model,
        context_window: Some(1000),
        max_output_tokens: Some(100),
        features: Default::default(),
    }
}
fn policy() -> ContextPolicy {
    ContextPolicy {
        id: "recorded-context".into(),
        revision: "1".into(),
        mode: BudgetMode::Strict,
        max_serialized_bytes: 65536,
        max_messages: 100,
        max_content_blocks: 200,
        context_limit_tokens: None,
        output_reserve_tokens: 100,
    }
}
fn wrapper(
    observations: Arc<Observations>,
    model: ModelDescriptor,
    policy: ContextPolicy,
    counter: Arc<dyn ContextTokenCounter + Send + Sync>,
    recorder_fail: bool,
    open_failure: bool,
) -> ContextPreparingProvider<Provider> {
    ContextPreparingProvider::new(
        Provider {
            observations: observations.clone(),
            open_failure,
        },
        model,
        policy,
        counter,
        Arc::new(Recorder {
            observations,
            fail: recorder_fail,
        }),
    )
}

mod admission;
mod streaming;
