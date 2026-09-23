use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerEventKind {
    TurnStarted,
    StepStarted,
    StepCompleted,
    ToolCallRequested,
    ApprovalRequested,
    ApprovalResolved,
    ToolExecutionStarted,
    ToolExecutionCompleted,
    ToolExecutionFailed,
    TurnFailed,
    TurnCancelled,
    TurnTimedOut,
    TurnCompleted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub event_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub cursor: u64,
    pub kind: LedgerEventKind,
    pub idempotency_key: String,
    pub payload: Value,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LedgerError {
    #[error("ledger conflict for idempotency key: {0}")]
    Conflict(String),
    #[error("ledger storage failed: {0}")]
    Storage(String),
}

pub trait LedgerStore: Send + Sync {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError>;
    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError>;
    fn claim(&self, idempotency_key: &str) -> Result<bool, LedgerError>;
}

#[derive(Clone, Default)]
pub struct InMemoryLedger {
    state: Arc<Mutex<LedgerState>>,
}

#[derive(Default)]
struct LedgerState {
    next_cursor: u64,
    events: Vec<LedgerEvent>,
    claims: std::collections::BTreeSet<String>,
}

impl LedgerStore for InMemoryLedger {
    fn append(&self, mut event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        let mut state = self.state.lock().expect("ledger lock must not be poisoned");
        if state
            .events
            .iter()
            .any(|item| item.event_id == event.event_id)
        {
            return Err(LedgerError::Conflict(event.event_id));
        }
        state.next_cursor += 1;
        event.cursor = state.next_cursor;
        state.events.push(event.clone());
        Ok(event)
    }

    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        let state = self.state.lock().expect("ledger lock must not be poisoned");
        Ok(state
            .events
            .iter()
            .filter(|event| event.cursor > cursor)
            .cloned()
            .collect())
    }

    fn claim(&self, idempotency_key: &str) -> Result<bool, LedgerError> {
        let mut state = self.state.lock().expect("ledger lock must not be poisoned");
        Ok(state.claims.insert(idempotency_key.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str, kind: LedgerEventKind) -> LedgerEvent {
        LedgerEvent {
            event_id: id.into(),
            turn_id: "turn-1".into(),
            execution_id: "execution-1".into(),
            cursor: 0,
            kind,
            idempotency_key: id.into(),
            payload: Value::Null,
        }
    }

    #[test]
    fn assigns_monotonic_cursors_and_replays_after_cursor() {
        let ledger = InMemoryLedger::default();
        assert_eq!(
            ledger
                .append(event("e1", LedgerEventKind::TurnStarted))
                .unwrap()
                .cursor,
            1
        );
        assert_eq!(
            ledger
                .append(event("e2", LedgerEventKind::TurnCompleted))
                .unwrap()
                .cursor,
            2
        );
        assert_eq!(ledger.events_after(1).unwrap().len(), 1);
    }

    #[test]
    fn claims_are_idempotent() {
        let ledger = InMemoryLedger::default();
        assert!(ledger.claim("turn-1/step-1/call-1").unwrap());
        assert!(!ledger.claim("turn-1/step-1/call-1").unwrap());
    }
}
