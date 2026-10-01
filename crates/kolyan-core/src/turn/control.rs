//! Ephemeral cooperative control. Durable approval/checkpoint authority is separate.
//! Broadcast notification supports multiple concurrently running tool consumers.

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

#[derive(Clone, Default)]
pub struct TurnControl {
    state: Arc<ControlState>,
}

#[derive(Default)]
struct ControlState {
    cancelled: AtomicBool,
    approved_tools: Mutex<HashSet<String>>,
    waiting: Mutex<HashMap<String, usize>>,
    changed: Notify,
}

impl TurnControl {
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// Await cancellation without losing an earlier signal or another consumer's
    /// wakeup. Approval notifications do not complete this cancellation future.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.state.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }

    pub fn approve_tool(&self, name: impl Into<String>) {
        self.state
            .approved_tools
            .lock()
            .expect("approval lock must not be poisoned")
            .insert(name.into());
        self.state.changed.notify_waiters();
    }

    pub fn is_waiting_for_approval(&self, name: &str) -> bool {
        self.state
            .waiting
            .lock()
            .expect("approval lock must not be poisoned")
            .get(name)
            .is_some_and(|count| *count > 0)
    }

    pub(super) async fn wait_cancelled(&self) {
        self.cancelled().await;
    }

    pub(super) async fn wait_for_tool_approval(&self, name: impl Into<String>) {
        let name = name.into();
        let _marker = ApprovalMarker::new(self.state.clone(), name.clone());
        loop {
            let notified = self.state.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let approved = self
                .state
                .approved_tools
                .lock()
                .expect("approval lock must not be poisoned")
                .remove(&name);
            if approved {
                return;
            }
            notified.await;
        }
    }
}

struct ApprovalMarker {
    state: Arc<ControlState>,
    name: String,
}

impl ApprovalMarker {
    fn new(state: Arc<ControlState>, name: String) -> Self {
        *state
            .waiting
            .lock()
            .expect("approval lock must not be poisoned")
            .entry(name.clone())
            .or_default() += 1;
        Self { state, name }
    }
}

impl Drop for ApprovalMarker {
    fn drop(&mut self) {
        let mut waiting = self
            .state
            .waiting
            .lock()
            .expect("approval lock must not be poisoned");
        if let Some(count) = waiting.get_mut(&self.name) {
            *count -= 1;
            if *count == 0 {
                waiting.remove(&self.name);
            }
        }
    }
}

#[cfg(test)]
mod tests;
