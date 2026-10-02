//! Bounded best-effort process telemetry. No callbacks or authorization decisions.

use serde::Serialize;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};
use tokio::sync::mpsc;

use crate::SandboxError;

const MAX_CAPACITY: usize = 1024;
const MAX_CONTEXT_BYTES: usize = 4096;

#[derive(Debug, Clone, Serialize)]
pub struct SandboxProcessObservation {
    pub launch_id: String,
    pub context: String,
    pub pid: u32,
    pub process_group: i32,
    pub event: SandboxProcessEvent,
    pub errors_truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SandboxProcessEvent {
    Spawned,
    OutputObserved {
        stream: SandboxOutputStream,
        bytes: Vec<u8>,
    },
    CancellationObserved {
        cause: SandboxCancellationCause,
    },
    TerminationAttempted {
        error: Option<String>,
    },
    Reaped {
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    GroupCleanup {
        error: Option<String>,
    },
    CaptureCompleted {
        stream: SandboxOutputStream,
        retained_bytes: usize,
        error: Option<String>,
    },
    CleanupFailed {
        error: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxOutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxCancellationCause {
    ExplicitControl,
    DroppedFuture,
}

#[derive(Debug)]
struct Sequenced {
    sequence: u64,
    observation: SandboxProcessObservation,
}

#[derive(Debug, Default)]
struct Transport {
    sequence: Mutex<u64>,
    dropped: AtomicU64,
    lifecycle_dropped: AtomicU64,
    output_dropped: AtomicU64,
}
impl Transport {
    fn record_loss(&self, output: bool) {
        if output {
            &self.output_dropped
        } else {
            &self.lifecycle_dropped
        }
        .fetch_add(1, Ordering::Relaxed);
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Clones share independent bounded queues and loss counters. Context is host metadata,
/// never a model input or a capability. Full/closed queues do not stop execution.
#[derive(Debug, Clone)]
pub struct SandboxProcessObservationSender {
    lifecycle: mpsc::Sender<Sequenced>,
    output: mpsc::Sender<Sequenced>,
    transport: Arc<Transport>,
    context: Arc<str>,
}

pub struct SandboxProcessObservationReceiver {
    lifecycle: mpsc::Receiver<Sequenced>,
    output: mpsc::Receiver<Sequenced>,
    lifecycle_head: Option<Sequenced>,
    output_head: Option<Sequenced>,
    transport: Arc<Transport>,
}

/// Capacity is 1..=1024 per queue, plus at most two merge heads. Output chunks
/// contain only existing retained bytes; neither queue backpressures execution.
pub fn sandbox_process_observation_channel(
    capacity: usize,
) -> Result<
    (
        SandboxProcessObservationSender,
        SandboxProcessObservationReceiver,
    ),
    SandboxError,
> {
    if !(1..=MAX_CAPACITY).contains(&capacity) {
        return Err(SandboxError::Invalid(
            "observation capacity must be 1..=1024".into(),
        ));
    }
    let (life_send, life_receive) = mpsc::channel(capacity);
    let (output_send, output_receive) = mpsc::channel(capacity);
    let transport = Arc::new(Transport::default());
    Ok((
        SandboxProcessObservationSender {
            lifecycle: life_send,
            output: output_send,
            transport: transport.clone(),
            context: "".into(),
        },
        SandboxProcessObservationReceiver {
            lifecycle: life_receive,
            output: output_receive,
            lifecycle_head: None,
            output_head: None,
            transport,
        },
    ))
}

impl SandboxProcessObservationSender {
    /// Refuse oversized correlation without altering the operation. The caller
    /// may omit this sink; the explicit loss counter still reports that omission.
    pub fn bind_context(&self, context: String) -> Option<Self> {
        if context.len() > MAX_CONTEXT_BYTES {
            self.record_loss();
            return None;
        }
        Some(Self {
            context: context.into(),
            ..self.clone()
        })
    }
    pub fn dropped(&self) -> u64 {
        self.transport.dropped.load(Ordering::Relaxed)
    }
    pub fn lifecycle_dropped(&self) -> u64 {
        self.transport.lifecycle_dropped.load(Ordering::Relaxed)
    }
    pub fn output_dropped(&self) -> u64 {
        self.transport.output_dropped.load(Ordering::Relaxed)
    }
    pub fn record_loss(&self) {
        self.transport.record_loss(false);
    }
    pub(crate) fn launch(&self, pid: u32, process_group: i32) -> LaunchObservation {
        LaunchObservation {
            sender: self.clone(),
            launch_id: uuid::Uuid::new_v4().to_string(),
            pid,
            process_group,
        }
    }
}

impl SandboxProcessObservationReceiver {
    pub async fn recv(&mut self) -> Option<SandboxProcessObservation> {
        std::future::poll_fn(|context| self.poll_recv(context)).await
    }
    pub fn try_recv(&mut self) -> Result<SandboxProcessObservation, mpsc::error::TryRecvError> {
        let transport = self.transport.clone();
        let _sequence = transport
            .sequence
            .lock()
            .expect("observation sequence lock");
        let lifecycle_closed = Self::fill_head(&mut self.lifecycle, &mut self.lifecycle_head);
        let output_closed = Self::fill_head(&mut self.output, &mut self.output_head);
        self.take_head()
            .ok_or(if lifecycle_closed && output_closed {
                mpsc::error::TryRecvError::Disconnected
            } else {
                mpsc::error::TryRecvError::Empty
            })
    }
    pub fn dropped(&self) -> u64 {
        self.transport.dropped.load(Ordering::Relaxed)
    }
    pub fn lifecycle_dropped(&self) -> u64 {
        self.transport.lifecycle_dropped.load(Ordering::Relaxed)
    }
    pub fn output_dropped(&self) -> u64 {
        self.transport.output_dropped.load(Ordering::Relaxed)
    }
    fn fill_head(receive: &mut mpsc::Receiver<Sequenced>, head: &mut Option<Sequenced>) -> bool {
        if head.is_some() {
            return false;
        }
        match receive.try_recv() {
            Ok(event) => {
                *head = Some(event);
                false
            }
            Err(mpsc::error::TryRecvError::Disconnected) => true,
            Err(mpsc::error::TryRecvError::Empty) => false,
        }
    }
    fn poll_recv(&mut self, context: &mut Context<'_>) -> Poll<Option<SandboxProcessObservation>> {
        let transport = self.transport.clone();
        let _sequence = transport
            .sequence
            .lock()
            .expect("observation sequence lock");
        // Tokio poll_recv may report Pending solely because its cooperative
        // budget is exhausted. Never choose one head against that false-empty
        // snapshot: inspect both queues without the async budget first.
        let lifecycle_closed = Self::fill_head(&mut self.lifecycle, &mut self.lifecycle_head);
        let output_closed = Self::fill_head(&mut self.output, &mut self.output_head);
        if let Some(event) = self.take_head() {
            return Poll::Ready(Some(event));
        }
        if lifecycle_closed && output_closed {
            return Poll::Ready(None);
        }
        let mut closed = 0;
        for (receive, head) in [
            (&mut self.lifecycle, &mut self.lifecycle_head),
            (&mut self.output, &mut self.output_head),
        ] {
            if head.is_none() {
                match receive.poll_recv(context) {
                    Poll::Ready(Some(event)) => *head = Some(event),
                    Poll::Ready(None) => closed += 1,
                    Poll::Pending => {}
                }
            }
        }
        match self.take_head() {
            Some(event) => Poll::Ready(Some(event)),
            None if closed == 2 => Poll::Ready(None),
            None => Poll::Pending,
        }
    }
    fn take_head(&mut self) -> Option<SandboxProcessObservation> {
        let head = match (&self.lifecycle_head, &self.output_head) {
            (Some(life), Some(output)) if life.sequence < output.sequence => {
                &mut self.lifecycle_head
            }
            (Some(_), Some(_)) => &mut self.output_head,
            (Some(_), None) => &mut self.lifecycle_head,
            (None, _) => &mut self.output_head,
        };
        head.take().map(|event| event.observation)
    }
}

#[derive(Clone)]
pub(crate) struct LaunchObservation {
    sender: SandboxProcessObservationSender,
    launch_id: String,
    pid: u32,
    process_group: i32,
}

impl LaunchObservation {
    pub(crate) fn emit(&self, mut event: SandboxProcessEvent) {
        let error = match &mut event {
            SandboxProcessEvent::TerminationAttempted { error }
            | SandboxProcessEvent::GroupCleanup { error }
            | SandboxProcessEvent::CaptureCompleted { error, .. } => error.as_mut(),
            SandboxProcessEvent::CleanupFailed { error } => Some(error),
            _ => None,
        };
        let errors_truncated = error.is_some_and(truncate_error);
        let row = SandboxProcessObservation {
            launch_id: self.launch_id.clone(),
            context: self.sender.context.to_string(),
            pid: self.pid,
            process_group: self.process_group,
            event,
            errors_truncated,
        };
        let output = matches!(row.event, SandboxProcessEvent::OutputObserved { .. });
        let queue = if output {
            &self.sender.output
        } else {
            &self.sender.lifecycle
        };
        let mut sequence = self
            .sender
            .transport
            .sequence
            .lock()
            .expect("observation sequence lock");
        let Some(next) = sequence.checked_add(1) else {
            self.sender.transport.record_loss(output);
            return;
        };
        if queue
            .try_send(Sequenced {
                sequence: *sequence,
                observation: row,
            })
            .is_err()
        {
            self.sender.transport.record_loss(output);
        } else {
            *sequence = next;
        }
    }
}

fn truncate_error(error: &mut String) -> bool {
    const LIMIT: usize = 1024;
    const MARKER: &str = " [truncated]";
    if error.len() <= LIMIT {
        return false;
    }
    let mut boundary = LIMIT - MARKER.len();
    while !error.is_char_boundary(boundary) {
        boundary -= 1;
    }
    error.truncate(boundary);
    error.push_str(MARKER);
    true
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod transport_tests;
