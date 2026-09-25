//! Incremental facts for one attempt; the Ledger cursor orders all attempts.

use std::sync::Mutex;

use kolyan_core::{TurnError, TurnEvent, TurnEventRecorder};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};

use super::RuntimeTurnKey;
use crate::encode_turn_event;

pub(super) struct LedgerRecorder<L> {
    ledger: L,
    key: RuntimeTurnKey,
    attempt: String,
    next: Mutex<u64>,
}

impl<L> LedgerRecorder<L> {
    pub(super) fn new(ledger: L, key: RuntimeTurnKey, attempt: String) -> Self {
        Self {
            ledger,
            key,
            attempt,
            next: Mutex::new(0),
        }
    }
}

impl<L: LedgerStore> TurnEventRecorder for LedgerRecorder<L> {
    fn record(&self, event: &TurnEvent) -> Result<(), TurnError> {
        let (kind, payload) = encode_turn_event(event);
        self.append(kind, payload)
    }

    fn record_request(&self, request: &kolyan_model::ModelRequest) -> Result<(), TurnError> {
        self.append(
            LedgerEventKind::ModelRequested,
            serde_json::json!({"request":request}),
        )
    }
}

impl<L: LedgerStore> LedgerRecorder<L> {
    fn append(&self, kind: LedgerEventKind, payload: serde_json::Value) -> Result<(), TurnError> {
        let mut next = self.next.lock().map_err(|_| TurnError::BoundaryControl {
            message: "event recorder lock poisoned".into(),
        })?;
        let id = format!(
            "{}/turn-event/{}/{}",
            self.key.execution_id, self.attempt, *next
        );
        // A duplicate attempt must fail closed, not silently rerun its model or tools.
        self.ledger
            .append(LedgerEvent {
                event_id: id.clone(),
                turn_id: self.key.turn_id.clone(),
                execution_id: self.key.execution_id.clone(),
                cursor: 0,
                kind,
                idempotency_key: id,
                payload,
            })
            .map_err(|error| TurnError::BoundaryControl {
                message: error.to_string(),
            })?;
        *next += 1;
        Ok(())
    }
}
