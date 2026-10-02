//! Bounded catalog replay with immutable Required artifacts and explicit revocation.

use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};

use kolyan_ledger::{FactDraft, FactError, FactJournal, FactRef, FactSubject};
use kolyan_trace::{ArtifactRef, ArtifactStore, Retention};

use super::{HookError, HookKey, HookManifest, encode, hash, id, reference};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredHook {
    pub manifest: HookManifest,
    pub body: ArtifactRef,
    pub reference: FactRef,
}

#[derive(Clone)]
pub struct HookCatalog {
    pub(super) journal: Arc<dyn FactJournal>,
    pub(super) artifacts: Arc<ArtifactStore>,
    pub(super) namespace: String,
    stream: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    namespace: String,
    manifest: HookManifest,
    body: ArtifactRef,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Revocation {
    key: HookKey,
    registration: FactRef,
    operation_id: String,
    reason: String,
}
pub(super) struct State {
    head: u64,
    pub entries: BTreeMap<HookKey, RegisteredHook>,
    pub revoked: BTreeMap<HookKey, FactRef>,
    operations: BTreeMap<String, (Revocation, FactRef)>,
}

impl HookCatalog {
    pub fn new(
        journal: Arc<dyn FactJournal>,
        artifacts: Arc<ArtifactStore>,
        namespace: String,
    ) -> Result<Self, HookError> {
        id(&namespace)?;
        let stream = format!(
            "agent.hooks.catalog.{}",
            hash(&("kolyan.hooks.catalog.v1", &namespace))?
        );
        Ok(Self {
            journal,
            artifacts,
            namespace,
            stream,
        })
    }
    pub fn stream_id(&self) -> &str {
        &self.stream
    }

    /// Publish body before the fact. Losing a CAS may leave a pinned orphan,
    /// never a false registration. Identical retries never undo a revocation.
    pub fn register(
        &self,
        manifest: HookManifest,
        body: &str,
    ) -> Result<RegisteredHook, HookError> {
        manifest.validate()?;
        if body.is_empty() || body.len() > 32 * 1024 || body.contains('\0') {
            return Err(HookError::Capacity);
        }
        let payload = Registration {
            namespace: self.namespace.clone(),
            manifest,
            body: self.artifacts.put(body.as_bytes(), Retention::Required)?,
        };
        let state = self.replay()?;
        if let Some(old) = state.entries.get(&payload.manifest.key) {
            return if old.manifest == payload.manifest && old.body == payload.body {
                Ok(old.clone())
            } else {
                Err(HookError::Conflict)
            };
        }
        if state.entries.len() >= 128 {
            return Err(HookError::Capacity);
        }
        let draft = self.draft(
            "agent.hook.registered",
            &payload.manifest.key,
            vec![],
            serde_json::to_value(&payload).map_err(invalid)?,
        )?;
        self.append(state.head, draft)?;
        let saved = self
            .replay()?
            .entries
            .remove(&payload.manifest.key)
            .ok_or(HookError::Conflict)?;
        if saved.manifest != payload.manifest || saved.body != payload.body {
            return Err(HookError::Conflict);
        }
        Ok(saved)
    }

    /// Exact operation id and registration cause make identical revocations
    /// idempotent. An alternate operation cannot replace the original reason.
    pub fn revoke(
        &self,
        key: &HookKey,
        registration: &FactRef,
        operation_id: String,
        reason: String,
    ) -> Result<FactRef, HookError> {
        key.validate()?;
        id(&operation_id)?;
        if reason.trim().is_empty() || reason.len() > 1024 {
            return Err(HookError::Invalid("invalid revocation reason".into()));
        }
        let payload = Revocation {
            key: key.clone(),
            registration: registration.clone(),
            operation_id,
            reason,
        };
        let state = self.replay()?;
        if let Some((old, fact)) = state.operations.get(&payload.operation_id) {
            return if old == &payload {
                Ok(fact.clone())
            } else {
                Err(HookError::Conflict)
            };
        }
        let saved = state
            .entries
            .get(key)
            .ok_or_else(|| HookError::Integrity("registration missing".into()))?;
        if saved.reference != *registration {
            return Err(HookError::Integrity("foreign registration".into()));
        }
        if state.revoked.contains_key(key) {
            return Err(HookError::Revoked);
        }
        let mut draft = self.draft(
            "agent.hook.revoked",
            key,
            vec![registration.clone()],
            serde_json::to_value(&payload).map_err(invalid)?,
        )?;
        draft.fact_id = format!(
            "agent.hook.revocation.{}",
            hash(&(&self.namespace, &payload.operation_id))?
        );
        self.append(state.head, draft)?;
        match self.replay()?.operations.get(&payload.operation_id) {
            Some((old, fact)) if old == &payload => Ok(fact.clone()),
            _ => Err(HookError::Conflict),
        }
    }

    pub(super) fn body(&self, hook: &RegisteredHook) -> Result<Vec<u8>, HookError> {
        Ok(self.artifacts.read(&hook.body, 32 * 1024)?)
    }

    pub(super) fn replay(&self) -> Result<State, HookError> {
        let mut state = State {
            head: 0,
            entries: BTreeMap::new(),
            revoked: BTreeMap::new(),
            operations: BTreeMap::new(),
        };
        for row in self.journal.read(&self.stream, 0, 257)? {
            if row.position != state.head + 1
                || row.stream_id != self.stream
                || row.draft.schema_version != 1
                || !row.draft.critical
            {
                return Err(HookError::Integrity("invalid catalog envelope".into()));
            }
            encode(&row.draft.payload)?;
            match row.draft.kind.as_str() {
                "agent.hook.registered" => {
                    let payload: Registration =
                        serde_json::from_value(row.draft.payload.clone()).map_err(invalid)?;
                    payload.manifest.validate()?;
                    if payload.namespace != self.namespace
                        || payload.body.retention != Retention::Required
                        || !super::is_digest(&payload.body.digest)
                        || !(1..=32 * 1024).contains(&payload.body.byte_length)
                        || state.entries.len() >= 128
                        || state.entries.contains_key(&payload.manifest.key)
                        || row.draft
                            != self.draft(
                                "agent.hook.registered",
                                &payload.manifest.key,
                                vec![],
                                row.draft.payload.clone(),
                            )?
                    {
                        return Err(HookError::Integrity("invalid registration".into()));
                    }
                    state.entries.insert(
                        payload.manifest.key.clone(),
                        RegisteredHook {
                            manifest: payload.manifest,
                            body: payload.body,
                            reference: reference(&row),
                        },
                    );
                }
                "agent.hook.revoked" => {
                    let payload: Revocation =
                        serde_json::from_value(row.draft.payload.clone()).map_err(invalid)?;
                    id(&payload.operation_id)?;
                    if payload.reason.trim().is_empty() || payload.reason.len() > 1024 {
                        return Err(HookError::Integrity("invalid revocation reason".into()));
                    }
                    let original = state
                        .entries
                        .get(&payload.key)
                        .ok_or_else(|| HookError::Integrity("missing registration".into()))?;
                    let mut expected = self.draft(
                        "agent.hook.revoked",
                        &payload.key,
                        vec![original.reference.clone()],
                        row.draft.payload.clone(),
                    )?;
                    expected.fact_id = format!(
                        "agent.hook.revocation.{}",
                        hash(&(&self.namespace, &payload.operation_id))?
                    );
                    if payload.registration != original.reference
                        || expected != row.draft
                        || state.revoked.contains_key(&payload.key)
                        || state.operations.contains_key(&payload.operation_id)
                    {
                        return Err(HookError::Integrity("invalid revocation".into()));
                    }
                    state.revoked.insert(payload.key.clone(), reference(&row));
                    state
                        .operations
                        .insert(payload.operation_id.clone(), (payload, reference(&row)));
                }
                _ => return Err(HookError::Integrity("unknown hook fact".into())),
            }
            state.head = row.position;
            if state.head > 256 {
                return Err(HookError::Capacity);
            }
        }
        Ok(state)
    }

    fn draft(
        &self,
        kind: &str,
        key: &HookKey,
        causes: Vec<FactRef>,
        payload: serde_json::Value,
    ) -> Result<FactDraft, HookError> {
        encode(&payload)?;
        let identity = hash(&("kolyan.hooks.definition.v1", &self.namespace, key))?;
        Ok(FactDraft {
            fact_id: format!("agent.hook.registration.{identity}"),
            subject: FactSubject {
                kind: "agent.hook".into(),
                id: identity,
            },
            kind: kind.into(),
            schema_version: 1,
            critical: true,
            causes,
            payload,
        })
    }
    fn append(&self, head: u64, draft: FactDraft) -> Result<(), HookError> {
        match self.journal.append(&self.stream, head, vec![draft]) {
            Ok(_) | Err(FactError::Conflict(_) | FactError::StalePosition { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
fn invalid(error: impl std::fmt::Display) -> HookError {
    HookError::Integrity(error.to_string())
}
