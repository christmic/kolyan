//! Server-owned selected projection identity; Agent snapshot remains in its binding SSOT.
use crate::{ServerError, SessionService};
use kolyan_ledger::{FactDraft, FactJournal, FactRef, FactSubject};
use kolyan_model::Message;
use kolyan_storage::{SessionInitialization, SessionRecord, SessionStore, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateContextOwner {
    pub logical_session_id: String,
    pub task_id: String,
    pub invocation_id: String,
    pub private_session_id: String,
    pub snapshot_digest: String,
}

/// Agent supplies an exact read-only binding verifier. There is no permissive
/// default and Server never decodes a second AgentSnapshot schema.
pub trait PrivateContextOwnershipVerifier: Send + Sync {
    fn verify_owner(&self, owner: &PrivateContextOwner, fact: &FactRef) -> Result<(), ServerError>;
}

pub struct PrivateContextService<SS> {
    sessions: SessionService<SS>,
    journal: Arc<dyn FactJournal>,
    ownership: Arc<dyn PrivateContextOwnershipVerifier>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VerifiedPrivateContextInitialization {
    pub initialization: SessionInitialization,
    pub initialization_fact: FactRef,
    pub ownership_fact: FactRef,
}

impl<SS: SessionStore + Clone> PrivateContextService<SS> {
    /// Read original initialization only, preserving all later Session history.
    pub fn load_verified_initialization(
        &self,
        owner: &PrivateContextOwner,
        ownership_fact: &FactRef,
        max_bytes: usize,
    ) -> Result<VerifiedPrivateContextInitialization, ServerError> {
        if max_bytes == 0 || max_bytes > 16 * 1024 * 1024 {
            return Err(conflict("invalid initialization host ceiling"));
        }
        self.ownership.verify_owner(owner, ownership_fact)?;
        let record = self.sessions.store.load(&owner.private_session_id)?;
        if record.session_id != owner.private_session_id {
            return Err(conflict("foreign private Session record"));
        }
        let initial = record
            .initialization
            .ok_or_else(|| conflict("missing owned initialization"))?;
        let canonical = private_context_initialization(owner, initial.messages.clone())?;
        if initial != canonical {
            return Err(conflict("initialization binding differs"));
        }
        let coordinate = digest(&json!([
            "kolyan.server.private-context.owner.v1",
            owner.private_session_id
        ]))?;
        let stream = format!("server.private-context.{coordinate}");
        let rows = self
            .journal
            .read(&stream, 0, 2)
            .map_err(|error| conflict(error.to_string()))?;
        let expected = FactDraft {
            fact_id: stream.clone(),
            subject: FactSubject {
                kind: "server.private-context".into(),
                id: owner.private_session_id.clone(),
            },
            kind: "server.private-context.initialized".into(),
            schema_version: 1,
            critical: true,
            causes: vec![ownership_fact.clone()],
            payload: json!({"owner":owner,"projection_digest":digest(&initial.messages)?,"binding_digest":initial.binding_digest}),
        };
        if rows.len() != 1
            || rows[0].stream_id != stream
            || rows[0].position != 1
            || rows[0].draft != expected
        {
            return Err(conflict("initialization fact differs"));
        }
        let value = VerifiedPrivateContextInitialization {
            initialization: initial,
            initialization_fact: FactRef {
                stream_id: stream,
                position: 1,
                fact_id: expected.fact_id,
            },
            ownership_fact: ownership_fact.clone(),
        };
        if serde_json::to_vec(&value)
            .map_err(StorageError::from)?
            .len()
            > max_bytes
        {
            return Err(conflict("initialization proof exceeds host ceiling"));
        }
        Ok(value)
    }
    pub fn new(
        sessions: SessionService<SS>,
        journal: Arc<dyn FactJournal>,
        ownership: Arc<dyn PrivateContextOwnershipVerifier>,
    ) -> Self {
        Self {
            sessions,
            journal,
            ownership,
        }
    }

    /// Exact retry only. A crash after journal reservation is repaired by the
    /// atomic Storage initializer, not by overwriting an existing conversation.
    pub fn initialize(
        &self,
        owner: &PrivateContextOwner,
        ownership_fact: &FactRef,
        selected_messages: Vec<Message>,
    ) -> Result<SessionRecord, ServerError> {
        let initialization = private_context_initialization(owner, selected_messages)?;
        self.ownership.verify_owner(owner, ownership_fact)?;
        let coordinate = digest(&json!([
            "kolyan.server.private-context.owner.v1",
            owner.private_session_id
        ]))?;
        let stream = format!("server.private-context.{coordinate}");
        let draft = FactDraft {
            fact_id: stream.clone(),
            subject: FactSubject {
                kind: "server.private-context".into(),
                id: owner.private_session_id.clone(),
            },
            kind: "server.private-context.initialized".into(),
            schema_version: 1,
            critical: true,
            causes: vec![ownership_fact.clone()],
            payload: json!({"owner":owner,"projection_digest":digest(&initialization.messages)?,"binding_digest":initialization.binding_digest}),
        };
        let saved = self
            .journal
            .read(&stream, 0, 2)
            .map_err(|error| conflict(error.to_string()))?;
        if saved.is_empty() {
            match self.journal.append(&stream, 0, vec![draft.clone()]) {
                Ok(_) => {}
                Err(
                    kolyan_ledger::FactError::Conflict(_)
                    | kolyan_ledger::FactError::StalePosition { .. },
                ) => {}
                Err(error) => return Err(conflict(error.to_string())),
            }
        }
        let saved = self
            .journal
            .read(&stream, 0, 2)
            .map_err(|error| conflict(error.to_string()))?;
        if saved.len() != 1
            || saved[0].stream_id != stream
            || saved[0].position != 1
            || saved[0].draft != draft
        {
            return Err(conflict(
                "private context ownership/projection fact differs",
            ));
        }
        Ok(self
            .sessions
            .store
            .initialize(&owner.private_session_id, &initialization)?)
    }
}

/// Canonical Server schema v1. Hash serialized tuple/object values, never
/// delimiter-concatenated model identifiers. Selected messages are stored only
/// in SessionInitialization, not duplicated in the ownership journal.
pub fn private_context_initialization(
    owner: &PrivateContextOwner,
    messages: Vec<Message>,
) -> Result<SessionInitialization, ServerError> {
    for id in [
        &owner.logical_session_id,
        &owner.task_id,
        &owner.invocation_id,
        &owner.private_session_id,
    ] {
        if id.is_empty()
            || id.len() > 256
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        {
            return Err(conflict("invalid private context owner identity"));
        }
    }
    if owner.snapshot_digest.len() != 64
        || !owner
            .snapshot_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(conflict("invalid snapshot digest"));
    }
    let projection = digest(&messages)?;
    let binding_digest = digest(
        &json!({"schema_version":1,"domain":"kolyan.server.private-context","owner":owner,"projection_digest":projection}),
    )?;
    let value = SessionInitialization {
        binding_digest,
        messages,
    };
    if serde_json::to_vec(&value)
        .map_err(StorageError::from)?
        .len()
        > 16 * 1024 * 1024
    {
        return Err(conflict(
            "private context initialization exceeds host ceiling",
        ));
    }
    Ok(value)
}

fn digest<T: Serialize>(value: &T) -> Result<String, ServerError> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(StorageError::from)?)
    ))
}
fn conflict(message: impl Into<String>) -> ServerError {
    StorageError::Conflict(message.into()).into()
}

#[cfg(test)]
mod tests;
