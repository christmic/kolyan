//! Incremental facts for one attempt; the Ledger cursor orders all attempts.

use std::sync::Mutex;

use kolyan_core::{
    StepEvent, StepEventRecordError, StepEventRecorder, TurnError, TurnEvent, TurnEventRecorder,
};
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

impl<L: LedgerStore> StepEventRecorder for LedgerRecorder<L> {
    fn record(&self, event: &StepEvent) -> Result<(), StepEventRecordError> {
        let Some(payload) = encode_step_event(event) else {
            return Ok(());
        };
        self.append(LedgerEventKind::ModelStreamEvent, payload)
            .map_err(|error| StepEventRecordError {
                message: error.to_string(),
            })
    }
}

fn encode_step_event(event: &StepEvent) -> Option<serde_json::Value> {
    match event {
        StepEvent::TextDelta { step_id, text } => Some(serde_json::json!({
            "type":"text_delta", "step_id":step_id, "text":text
        })),
        StepEvent::ReasoningDelta { step_id, text } => Some(serde_json::json!({
            "type":"reasoning_delta", "step_id":step_id, "text":text
        })),
        StepEvent::ToolCallStarted { step_id, id, name } => Some(serde_json::json!({
            "type":"tool_call_started", "step_id":step_id, "id":id, "name":name
        })),
        StepEvent::ToolCallArgumentsDelta { step_id, id, delta } => Some(serde_json::json!({
            "type":"tool_call_arguments_delta", "step_id":step_id, "id":id, "delta":delta
        })),
        StepEvent::ToolCallCompleted { step_id, call } => Some(serde_json::json!({
            "type":"tool_call_completed", "step_id":step_id, "call":call
        })),
        StepEvent::Usage { step_id, usage } => Some(serde_json::json!({
            "type":"usage", "step_id":step_id, "usage":usage
        })),
        StepEvent::Started { .. }
        | StepEvent::Provider { .. }
        | StepEvent::Completed(_)
        | StepEvent::Cancelled { .. }
        | StepEvent::TimedOut { .. } => None,
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
