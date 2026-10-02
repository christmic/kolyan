//! Process-local broadcast cancellation and one absolute Step deadline.
//! Stopping drops local Provider resources; it is not proof of remote termination.

use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::sync::Notify;
use tokio::time::Sleep;

/// Cloneable cooperative control. Cancellation is sticky and wakes every
/// registered Step; dropping one control clone does not cancel other owners.
#[derive(Clone, Default)]
pub struct StepControl {
    state: Arc<ControlState>,
}

#[derive(Default)]
struct ControlState {
    cancelled: AtomicBool,
    changed: Notify,
}

impl StepControl {
    /// Request idempotent local cancellation, including during Provider opening.
    /// This does not acknowledge that the remote model has stopped.
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    async fn wait_cancelled(self) {
        loop {
            let notified = self.state.changed.notified();
            tokio::pin!(notified);
            // Register before checking the sticky flag so a concurrent broadcast
            // cannot fall into a check/register gap.
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Stop {
    Cancelled,
    TimedOut,
}

pub(super) struct Boundary {
    control: StepControl,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    deadline: Option<Instant>,
    timer: Option<Pin<Box<Sleep>>>,
}

impl Boundary {
    pub fn new(control: StepControl, deadline: Option<Instant>) -> Self {
        let timer = deadline
            .filter(|limit| *limit > Instant::now() && !control.is_cancelled())
            .map(|limit| Box::pin(tokio::time::sleep_until(limit.into())));
        Self {
            cancelled: Box::pin(control.clone().wait_cancelled()),
            control,
            deadline,
            timer,
        }
    }

    pub fn current_stop(&self) -> Option<Stop> {
        if self.control.is_cancelled() {
            Some(Stop::Cancelled)
        } else if self.deadline.is_some_and(|limit| limit <= Instant::now()) {
            Some(Stop::TimedOut)
        } else {
            None
        }
    }

    pub fn poll_stop(&mut self, cx: &mut Context<'_>) -> Poll<Stop> {
        if let Some(stop) = self.current_stop() {
            return Poll::Ready(stop);
        }
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Stop::Cancelled);
        }
        if self
            .timer
            .as_mut()
            .is_some_and(|timer| timer.as_mut().poll(cx).is_ready())
        {
            // A cancellation racing the timer has priority if already visible.
            return Poll::Ready(if self.control.is_cancelled() {
                Stop::Cancelled
            } else {
                Stop::TimedOut
            });
        }
        Poll::Pending
    }

    pub async fn stopped(&mut self) -> Stop {
        std::future::poll_fn(|cx| self.poll_stop(cx)).await
    }
}
