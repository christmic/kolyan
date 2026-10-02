//! Authenticate only the canonical optional Task binding before execution start.

use kolyan_ledger::{LedgerEvent, LedgerEventKind as Kind};

use super::ModelOpeningProofError as Error;
use super::reader::{binding, decode, invalid};

/// Task membership belongs to Server; here the optional host binding must
/// authenticate the same execution and occupy its sole canonical prestart slot.
pub(super) fn execution_prefix(events: &[LedgerEvent], started: &LedgerEvent) -> Result<(), Error> {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Bound {
        binding: crate::ExecutionBinding,
    }
    let before: Vec<_> = events
        .iter()
        .filter(|event| event.cursor < started.cursor)
        .collect();
    if before.len() > 1
        || before
            .first()
            .is_some_and(|event| event.kind != Kind::ExecutionBound)
    {
        return Err(Error::OrderingMismatch(
            "unexpected execution prestart prefix".into(),
        ));
    }
    for event in events
        .iter()
        .filter(|event| event.kind == Kind::ExecutionBound)
    {
        if event.cursor >= started.cursor {
            return Err(Error::OrderingMismatch(
                "binding after execution start".into(),
            ));
        }
        let expected = format!("{}/task-binding", event.execution_id);
        if event.event_id != expected || event.idempotency_key != expected {
            return Err(binding(
                &event.event_id,
                "noncanonical task binding identity",
            ));
        }
        let value: Bound = decode(&event.payload, &event.event_id)?;
        value
            .binding
            .validate()
            .map_err(|error| invalid(&event.event_id, error))?;
        let key: crate::ExecutionKey = decode(&started.payload, &started.event_id)?;
        if value.binding.execution_id != key.execution_id
            || value.binding.session_id != key.session_id
            || value.binding.turn_id != key.turn_id
        {
            return Err(binding(&event.event_id, "task binding execution differs"));
        }
    }
    Ok(())
}
