//! Read-only public projection of the existing Runtime-owned admission schema.
use super::{InputAdmission, RuntimeTurnKey};
use crate::RuntimeError;
use kolyan_ledger::LedgerStore;
use kolyan_model::ModelRequest;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VerifiedExecutionInput {
    pub key: RuntimeTurnKey,
    pub model_request: ModelRequest,
    pub agent_snapshot_digest: Option<String>,
    pub event_id: String,
    pub cursor: u64,
}

/// Does not admit, reconcile, execute or decode a second copy of the wire schema.
/// The host ceiling bounds both the saved admission and complete returned envelope.
pub fn verified_execution_input<L: LedgerStore>(
    ledger: &L,
    key: &RuntimeTurnKey,
    max_bytes: usize,
) -> Result<VerifiedExecutionInput, RuntimeError> {
    if max_bytes == 0 || max_bytes > 16 * 1024 * 1024 {
        return Err(RuntimeError::Driver(
            "invalid verified input host ceiling".into(),
        ));
    }
    let (saved, event) = InputAdmission::load_record(ledger, key)?;
    let value = VerifiedExecutionInput {
        key: saved.key,
        model_request: saved.model_request,
        agent_snapshot_digest: saved.agent_snapshot_digest,
        event_id: event.event_id.clone(),
        cursor: event.cursor,
    };
    for bytes in [serde_json::to_vec(&event), serde_json::to_vec(&value)] {
        if bytes
            .map_err(|error| RuntimeError::Driver(error.to_string()))?
            .len()
            > max_bytes
        {
            return Err(RuntimeError::Driver(
                "verified input exceeds host ceiling".into(),
            ));
        }
    }
    Ok(value)
}

#[cfg(test)]
#[path = "verified_input/tests.rs"]
mod tests;
