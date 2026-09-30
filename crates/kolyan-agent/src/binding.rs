//! Immutable invocation/private-context ownership facts, not task admission or grants.
//! Restoring a binding never re-resolves its saved snapshot through a mutable catalog.

use std::sync::Arc;

use kolyan_ledger::{FactDraft, FactError, FactJournal, FactRecord, FactRef, FactSubject};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AgentSnapshot, digest, identity};

const SUBJECT_KIND: &str = "agent.invocation";
const FACT_KIND: &str = "agent.context.bound";

/// Binding schema v1 has the same 128 KiB ceiling as coordination fact payloads.
pub const MAX_BINDING_PAYLOAD_BYTES: usize = 128 * 1024;

/// A root uses exactly its logical Session; independent root Turns may share it.
/// A child uses its derived private context; forks or other sharing require a
/// distinct kind/policy, never an arbitrary physical Session alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingContextKind {
    Root,
    Child,
}

/// Exact immutable snapshot and physical conversation ownership for an invocation.
/// Fields are validated on save/load; possession is not proof of task admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInvocationBinding {
    pub task_id: String,
    pub invocation_id: String,
    pub logical_session_id: String,
    pub private_session_id: String,
    pub context_kind: BindingContextKind,
    pub snapshot: AgentSnapshot,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BindingError {
    #[error("invalid invocation binding: {0}")]
    Invalid(String),
    #[error("immutable invocation binding conflicts with saved content")]
    Conflict,
    #[error("logical Session does not own this invocation binding")]
    ForeignOwner,
    #[error("corrupt or unsupported invocation binding fact: {0}")]
    Corrupt(String),
    #[error("binding payload is {bytes} bytes; maximum is {limit}")]
    Oversized { bytes: usize, limit: usize },
    #[error("binding journal failed: {0}")]
    Journal(#[from] FactError),
}

impl AgentInvocationBinding {
    pub fn validate(&self) -> Result<(), BindingError> {
        for id in [
            &self.task_id,
            &self.invocation_id,
            &self.logical_session_id,
            &self.private_session_id,
        ] {
            validate_id(id)?;
        }
        if self.context_kind == BindingContextKind::Root
            && self.private_session_id != self.logical_session_id
        {
            return Err(BindingError::Invalid(
                "root private Session must equal its logical Session".into(),
            ));
        }
        if self.context_kind == BindingContextKind::Child
            && (self.private_session_id == self.logical_session_id
                || self.private_session_id
                    != child_private_session_id(
                        &self.logical_session_id,
                        &self.task_id,
                        &self.invocation_id,
                    )?)
        {
            return Err(BindingError::Invalid(
                "child private Session must equal its exact derived invocation context".into(),
            ));
        }
        // AgentSnapshot deserialization independently verifies revision, identity,
        // digest and permission subset. Never trust an opaque persisted digest alone.
        let encoded = serde_json::to_vec(&self.snapshot).map_err(invalid)?;
        serde_json::from_slice::<AgentSnapshot>(&encoded).map_err(invalid)?;
        Ok(())
    }
}

/// Derive a bounded, namespaced child context from the exact logical owner,
/// Task and invocation tuple. JSON tuple encoding prevents delimiter ambiguity.
/// Distinct admitted coordinates obtain collision-resistant independent contexts,
/// without a mutable global registry. This helper does not admit an invocation.
pub fn child_private_session_id(
    logical_session_id: &str,
    task_id: &str,
    invocation_id: &str,
) -> Result<String, BindingError> {
    for id in [logical_session_id, task_id, invocation_id] {
        validate_id(id)?;
    }
    Ok(format!(
        "agent.child.v1.{}",
        digest(&(
            "kolyan.agent.child.context.v1",
            logical_session_id,
            task_id,
            invocation_id
        ))
        .map_err(invalid)?
    ))
}

/// Append-only, single-record namespace backed by the supplied durable journal.
/// Memory and SQLite obey the same CAS contract; durability is the backend's property.
#[derive(Clone)]
pub struct AgentInvocationBindingStore {
    journal: Arc<dyn FactJournal>,
}

impl AgentInvocationBindingStore {
    pub fn new(journal: Arc<dyn FactJournal>) -> Self {
        Self { journal }
    }

    /// Save exact content once. Identical retries return the original coordinate;
    /// changed definitions/permissions/owners fail. No task or permission facts are issued.
    pub fn save(&self, binding: &AgentInvocationBinding) -> Result<FactRef, BindingError> {
        binding.validate()?;
        let payload = encode(binding)?;
        if let Some(existing) = self.load(
            &binding.task_id,
            &binding.invocation_id,
            &binding.logical_session_id,
        )? {
            if &existing != binding {
                return Err(BindingError::Conflict);
            }
            return coordinate(&binding.task_id, &binding.invocation_id);
        }
        let stream = stream_id(&binding.task_id, &binding.invocation_id)?;
        let draft = FactDraft {
            fact_id: fact_id(&binding.task_id, &binding.invocation_id)?,
            subject: FactSubject {
                kind: SUBJECT_KIND.into(),
                id: binding.invocation_id.clone(),
            },
            kind: FACT_KIND.into(),
            schema_version: 1,
            critical: true,
            causes: vec![],
            payload,
        };
        match self.journal.append(&stream, 0, vec![draft]) {
            Ok(records) => {
                if records.len() != 1 {
                    return Err(BindingError::Corrupt(
                        "append returned unexpected record count".into(),
                    ));
                }
                let restored = decode(
                    &records[0],
                    &binding.task_id,
                    &binding.invocation_id,
                    &binding.logical_session_id,
                )?;
                if restored != *binding {
                    return Err(BindingError::Conflict);
                }
            }
            Err(FactError::Conflict(_) | FactError::StalePosition { .. }) => {
                let restored = self.load(
                    &binding.task_id,
                    &binding.invocation_id,
                    &binding.logical_session_id,
                )?;
                if restored.as_ref() != Some(binding) {
                    return Err(BindingError::Conflict);
                }
            }
            Err(error) => return Err(error.into()),
        }
        // Detect unknown/extra facts even after the journal's idempotent append path.
        if self
            .load(
                &binding.task_id,
                &binding.invocation_id,
                &binding.logical_session_id,
            )?
            .as_ref()
            != Some(binding)
        {
            return Err(BindingError::Corrupt(
                "committed binding is missing or changed".into(),
            ));
        }
        coordinate(&binding.task_id, &binding.invocation_id)
    }

    /// Read at most two records to prove this dedicated namespace contains exactly
    /// one known critical record. Extra facts, unknown schemas and foreign owners fail.
    pub fn load(
        &self,
        task_id: &str,
        invocation_id: &str,
        logical_session_id: &str,
    ) -> Result<Option<AgentInvocationBinding>, BindingError> {
        for id in [task_id, invocation_id, logical_session_id] {
            validate_id(id)?;
        }
        let records = self
            .journal
            .read(&stream_id(task_id, invocation_id)?, 0, 2)?;
        match records.as_slice() {
            [] => Ok(None),
            [record] => decode(record, task_id, invocation_id, logical_session_id).map(Some),
            _ => Err(BindingError::Corrupt(
                "unexpected extra facts in binding namespace".into(),
            )),
        }
    }
}

fn decode(
    record: &FactRecord,
    task: &str,
    invocation: &str,
    owner: &str,
) -> Result<AgentInvocationBinding, BindingError> {
    if record.stream_id != stream_id(task, invocation)?
        || record.position != 1
        || record.draft.fact_id != fact_id(task, invocation)?
        || record.draft.subject.kind != SUBJECT_KIND
        || record.draft.subject.id != invocation
        || record.draft.kind != FACT_KIND
        || record.draft.schema_version != 1
        || !record.draft.critical
        || !record.draft.causes.is_empty()
    {
        return Err(BindingError::Corrupt(
            "record coordinate, subject, version or kind mismatch".into(),
        ));
    }
    let bytes = serde_json::to_vec(&record.draft.payload).map_err(corrupt)?;
    check_size(bytes.len())?;
    let binding: AgentInvocationBinding = serde_json::from_slice(&bytes).map_err(corrupt)?;
    binding.validate().map_err(corrupt)?;
    if binding.task_id != task || binding.invocation_id != invocation {
        return Err(BindingError::Corrupt(
            "payload identity differs from exact requested scope".into(),
        ));
    }
    if binding.logical_session_id != owner {
        return Err(BindingError::ForeignOwner);
    }
    Ok(binding)
}

fn encode(binding: &AgentInvocationBinding) -> Result<serde_json::Value, BindingError> {
    let bytes = serde_json::to_vec(binding).map_err(invalid)?;
    check_size(bytes.len())?;
    serde_json::from_slice(&bytes).map_err(invalid)
}

fn check_size(bytes: usize) -> Result<(), BindingError> {
    if bytes > MAX_BINDING_PAYLOAD_BYTES {
        return Err(BindingError::Oversized {
            bytes,
            limit: MAX_BINDING_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

fn stream_id(task: &str, invocation: &str) -> Result<String, BindingError> {
    Ok(format!(
        "agent.binding.{}",
        digest(&("kolyan.agent.binding.stream.v1", task, invocation)).map_err(invalid)?
    ))
}

fn fact_id(task: &str, invocation: &str) -> Result<String, BindingError> {
    Ok(format!(
        "agent.binding.fact.{}",
        digest(&("kolyan.agent.binding.fact.v1", task, invocation)).map_err(invalid)?
    ))
}

fn coordinate(task: &str, invocation: &str) -> Result<FactRef, BindingError> {
    Ok(FactRef {
        stream_id: stream_id(task, invocation)?,
        position: 1,
        fact_id: fact_id(task, invocation)?,
    })
}

fn validate_id(value: &str) -> Result<(), BindingError> {
    identity(value).map_err(invalid)
}
fn invalid(error: impl std::fmt::Display) -> BindingError {
    BindingError::Invalid(error.to_string())
}
fn corrupt(error: impl std::fmt::Display) -> BindingError {
    BindingError::Corrupt(error.to_string())
}

#[cfg(test)]
mod tests;
