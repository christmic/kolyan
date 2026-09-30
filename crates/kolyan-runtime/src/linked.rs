//! Read-only execution evidence exports. Bindings are supplied by the host;
//! this module validates execution membership, not task admission or permission.

use std::collections::HashSet;

use kolyan_ledger::{LedgerError, LedgerEventKind, LedgerQuery, LedgerStore};
use kolyan_trace::{LinkedTraceRecord, TraceKind, TraceRecord};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Host-admitted identity coordinates. Deserialized bindings must be validated
/// before use; `LinkedTrajectory::load` always performs that validation.
pub struct ExecutionBinding {
    pub task_id: String,
    pub invocation_id: String,
    pub attempt_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
}

impl ExecutionBinding {
    /// Identity namespaces are separate; equal spellings across namespaces are
    /// not proof of aliasing. Admission and relationship checks belong to Server.
    pub fn validate(&self) -> Result<(), LinkedTrajectoryError> {
        for id in [
            &self.task_id,
            &self.invocation_id,
            &self.attempt_id,
            &self.session_id,
            &self.turn_id,
            &self.execution_id,
        ] {
            if id.trim().is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
                return Err(LinkedTrajectoryError::InvalidEvidence(
                    "invalid binding identity".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentPolicy {
    /// Export original recorded content; host authorization is required.
    Full,
    /// Export identities, kind, byte count and redaction marker only.
    MetadataOnly,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinkedTrajectoryRecord {
    pub binding: ExecutionBinding,
    pub event_id: String,
    pub cursor: u64,
    pub step_id: Option<String>,
    pub kind: LedgerEventKind,
    pub payload: Value,
}

impl LinkedTrajectoryRecord {
    pub fn trace(&self) -> LinkedTraceRecord<ExecutionBinding> {
        LinkedTraceRecord {
            binding: self.binding.clone(),
            event_id: self.event_id.clone(),
            cursor: self.cursor,
            record: TraceRecord {
                turn_id: self.binding.turn_id.clone(),
                execution_id: self.binding.execution_id.clone(),
                sequence: self.cursor,
                kind: TraceKind::TurnEvent,
                payload: self.payload.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinkedTrajectory {
    pub binding: ExecutionBinding,
    pub content_policy: ContentPolicy,
    pub records: Vec<LinkedTrajectoryRecord>,
}

#[derive(Debug, Error)]
pub enum LinkedTrajectoryError {
    #[error("ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("invalid linked evidence: {0}")]
    InvalidEvidence(String),
}

impl LinkedTrajectory {
    /// Load one bounded page without altering evidence or inferring missing
    /// Step identities. This is not a multi-page concurrent snapshot.
    pub fn load(
        ledger: &(impl LedgerStore + ?Sized),
        binding: &ExecutionBinding,
        after: u64,
        limit: usize,
        content_policy: ContentPolicy,
    ) -> Result<Self, LinkedTrajectoryError> {
        binding.validate()?;
        let query = LedgerQuery {
            execution_id: Some(binding.execution_id.clone()),
            event_id: None,
            after,
            through: None,
            limit,
        };
        query.validate()?;
        let events = ledger.query(&query)?;
        if events.len() > limit {
            return Err(LinkedTrajectoryError::InvalidEvidence(
                "query exceeded page limit".into(),
            ));
        }
        let mut cursor = after;
        let mut identities = HashSet::new();
        let mut records = Vec::with_capacity(events.len());
        for event in events {
            if event.execution_id != binding.execution_id
                || event.turn_id != binding.turn_id
                || event.cursor <= cursor
                || event.event_id.trim().is_empty()
                || event.event_id.len() > 1024
                || event.event_id.chars().any(char::is_control)
                || !identities.insert(event.event_id.clone())
            {
                return Err(LinkedTrajectoryError::InvalidEvidence(
                    "query returned invalid membership or identity".into(),
                ));
            }
            cursor = event.cursor;
            if let Some(recorded) = event.payload.get("binding") {
                let recorded: ExecutionBinding = serde_json::from_value(recorded.clone())
                    .map_err(|error| LinkedTrajectoryError::InvalidEvidence(error.to_string()))?;
                recorded.validate()?;
                if recorded != *binding {
                    return Err(LinkedTrajectoryError::InvalidEvidence(
                        "recorded binding differs from export binding".into(),
                    ));
                }
            }
            let step_id = match event.payload.get("step_id") {
                None => None,
                Some(Value::String(id))
                    if !id.trim().is_empty()
                        && id.len() <= 256
                        && !id.chars().any(char::is_control) =>
                {
                    Some(id.clone())
                }
                Some(_) => {
                    return Err(LinkedTrajectoryError::InvalidEvidence(
                        "invalid recorded step identity".into(),
                    ));
                }
            };
            // Deny-by-default metadata export: do not pass through unknown keys
            // or nested content that may contain prompts, outputs or reasoning.
            let payload = match content_policy {
                ContentPolicy::Full => event.payload,
                ContentPolicy::MetadataOnly => json!({"content_redacted": true,
                    "source_bytes": serde_json::to_vec(&event.payload)
                        .map_err(|error| LinkedTrajectoryError::InvalidEvidence(error.to_string()))?.len()}),
            };
            records.push(LinkedTrajectoryRecord {
                binding: binding.clone(),
                event_id: event.event_id,
                cursor,
                step_id,
                kind: event.kind,
                payload,
            });
        }
        Ok(Self {
            binding: binding.clone(),
            content_policy,
            records,
        })
    }
}

#[cfg(test)]
mod tests;
