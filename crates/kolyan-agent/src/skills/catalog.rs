//! Bounded replay and compare-and-swap publication in an explicit host namespace.

use std::{collections::BTreeMap, sync::Arc};

use kolyan_ledger::{FactDraft, FactError, FactJournal, FactRecord, FactRef, FactSubject};
use kolyan_trace::{ArtifactStore, Retention};
use serde::{Deserialize, Serialize};

use super::{
    RegisteredSkill, SkillDescriptorInput, SkillError, SkillKey, SkillLimits, SkillMetadata,
    encoding::{encode, hash, id},
    metadata::{MAX_METADATA_BYTES, SavedMetadata},
};

const REGISTERED: &str = "agent.skill.registered";
const REVOKED: &str = "agent.skill.revoked";
const SUBJECT: &str = "agent.skill";

#[derive(Clone)]
pub struct SkillCatalog {
    pub(super) journal: Arc<dyn FactJournal>,
    pub(super) artifacts: Arc<ArtifactStore>,
    pub(super) namespace: String,
    pub(super) limits: SkillLimits,
    stream: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrationPayload {
    namespace: String,
    metadata: SavedMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RevocationPayload {
    namespace: String,
    key: SkillKey,
    registration: FactRef,
    operation_id: String,
    reason: String,
}

pub(super) struct CatalogState {
    pub head: u64,
    pub entries: BTreeMap<SkillKey, RegisteredSkill>,
    pub revoked: BTreeMap<SkillKey, FactRef>,
    operations: BTreeMap<String, (RevocationPayload, FactRef)>,
}

impl SkillCatalog {
    pub fn new(
        journal: Arc<dyn FactJournal>,
        artifacts: Arc<ArtifactStore>,
        host_namespace: String,
        limits: SkillLimits,
    ) -> Result<Self, SkillError> {
        id(&host_namespace)?;
        let stream = format!(
            "agent.skills.catalog.{}",
            hash(&("kolyan.skills.catalog.v1", &host_namespace))?
        );
        Ok(Self {
            journal,
            artifacts,
            namespace: host_namespace,
            limits,
            stream,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn stream_id(&self) -> &str {
        &self.stream
    }

    /// Required content is durably pinned before its immutable registration fact.
    /// A losing CAS may leave a pinned orphan, never a false successful registration.
    /// Identical retries return the original fact even after revocation, without reactivation.
    pub fn register(
        &self,
        descriptor: SkillDescriptorInput,
        body: &str,
    ) -> Result<RegisteredSkill, SkillError> {
        descriptor.validate()?;
        if body.len() > self.limits.body_bytes {
            return Err(SkillError::Capacity);
        }
        let metadata = SavedMetadata::new(
            descriptor,
            self.artifacts.put(body.as_bytes(), Retention::Required)?,
        )?;
        let state = self.replay()?;
        if let Some(saved) = state.entries.get(&metadata.descriptor.key) {
            return if saved.metadata.0 == metadata {
                Ok(saved.clone())
            } else {
                Err(SkillError::Conflict)
            };
        }
        if state.entries.len() >= self.limits.catalog_versions {
            return Err(SkillError::Capacity);
        }
        let payload = RegistrationPayload {
            namespace: self.namespace.clone(),
            metadata: metadata.clone(),
        };
        let draft = FactDraft {
            fact_id: self.registration_id(&metadata.descriptor.key)?,
            subject: self.subject(&metadata.descriptor.key)?,
            kind: REGISTERED.into(),
            schema_version: 1,
            critical: true,
            causes: vec![],
            payload: json(&payload)?,
        };
        self.append_once(state.head, draft)?;
        let saved = self
            .replay()?
            .entries
            .remove(&metadata.descriptor.key)
            .ok_or(SkillError::Conflict)?;
        if saved.metadata.0 != metadata {
            return Err(SkillError::Conflict);
        }
        Ok(saved)
    }

    pub fn revoke(
        &self,
        key: &SkillKey,
        registration: &FactRef,
        operation_id: &str,
        reason: &str,
    ) -> Result<FactRef, SkillError> {
        key.validate()?;
        id(operation_id)?;
        validate_reason(reason)?;
        let payload = RevocationPayload {
            namespace: self.namespace.clone(),
            key: key.clone(),
            registration: registration.clone(),
            operation_id: operation_id.into(),
            reason: reason.into(),
        };
        let state = self.replay()?;
        if let Some((existing, reference)) = state.operations.get(operation_id) {
            return if existing == &payload {
                Ok(reference.clone())
            } else {
                Err(SkillError::Conflict)
            };
        }
        let saved = state
            .entries
            .get(key)
            .ok_or_else(|| SkillError::Provenance("registration missing".into()))?;
        if saved.reference != *registration {
            return Err(SkillError::Provenance(
                "registration coordinate mismatch".into(),
            ));
        }
        if state.revoked.contains_key(key) {
            return Err(SkillError::Revoked);
        }
        let draft = FactDraft {
            fact_id: self.revocation_id(operation_id)?,
            subject: self.subject(key)?,
            kind: REVOKED.into(),
            schema_version: 1,
            critical: true,
            causes: vec![registration.clone()],
            payload: json(&payload)?,
        };
        self.append_once(state.head, draft)?;
        let state = self.replay()?;
        match state.operations.get(operation_id) {
            Some((saved, reference)) if saved == &payload => Ok(reference.clone()),
            _ => Err(SkillError::Conflict),
        }
    }

    // Single append only. A CAS loser may inspect the winner, but never silently
    // retries a different candidate against a new head.
    fn append_once(&self, head: u64, draft: FactDraft) -> Result<(), SkillError> {
        match self.journal.append(&self.stream, head, vec![draft]) {
            Ok(_) | Err(FactError::Conflict(_) | FactError::StalePosition { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn replay(&self) -> Result<CatalogState, SkillError> {
        let mut state = CatalogState {
            head: 0,
            entries: BTreeMap::new(),
            revoked: BTreeMap::new(),
            operations: BTreeMap::new(),
        };
        // Each version admits at most one registration and one revocation.
        let max_records = 2 * self.limits.catalog_versions;
        loop {
            let records = self
                .journal
                .read(&self.stream, state.head, max_records + 1)?;
            if records.is_empty() {
                break;
            }
            for record in records {
                if state.head as usize >= max_records {
                    return Err(SkillError::Capacity);
                }
                if record.stream_id != self.stream
                    || record.position != state.head + 1
                    || record.draft.schema_version != 1
                    || !record.draft.critical
                {
                    return Err(SkillError::Integrity(
                        "unknown schema or noncontiguous catalog".into(),
                    ));
                }
                encode(&record.draft.payload, MAX_METADATA_BYTES)?;
                match record.draft.kind.as_str() {
                    REGISTERED => self.replay_registration(&record, &mut state)?,
                    REVOKED => self.replay_revocation(&record, &mut state)?,
                    _ => return Err(SkillError::Integrity("unknown catalog fact".into())),
                }
                state.head = record.position;
            }
        }
        Ok(state)
    }

    fn replay_registration(
        &self,
        record: &FactRecord,
        state: &mut CatalogState,
    ) -> Result<(), SkillError> {
        let payload: RegistrationPayload = decode(record.draft.payload.clone())?;
        payload.metadata.validate()?;
        let key = &payload.metadata.descriptor.key;
        if payload.namespace != self.namespace
            || record.draft.fact_id != self.registration_id(key)?
            || record.draft.subject != self.subject(key)?
            || !record.draft.causes.is_empty()
            || state.entries.contains_key(key)
        {
            return Err(SkillError::Integrity(
                "invalid registration provenance".into(),
            ));
        }
        if state.entries.len() >= self.limits.catalog_versions
            || payload.metadata.body.byte_length > self.limits.body_bytes as u64
        {
            return Err(SkillError::Capacity);
        }
        state.entries.insert(
            key.clone(),
            RegisteredSkill {
                metadata: SkillMetadata(payload.metadata),
                reference: reference(record),
            },
        );
        Ok(())
    }

    fn replay_revocation(
        &self,
        record: &FactRecord,
        state: &mut CatalogState,
    ) -> Result<(), SkillError> {
        let payload: RevocationPayload = decode(record.draft.payload.clone())?;
        payload.key.validate()?;
        id(&payload.operation_id)?;
        validate_reason(&payload.reason)?;
        let saved = state
            .entries
            .get(&payload.key)
            .ok_or_else(|| SkillError::Integrity("revocation precedes registration".into()))?;
        if payload.namespace != self.namespace
            || saved.reference != payload.registration
            || record.draft.subject != self.subject(&payload.key)?
            || record.draft.fact_id != self.revocation_id(&payload.operation_id)?
            || record.draft.causes != vec![payload.registration.clone()]
            || state.revoked.contains_key(&payload.key)
            || state.operations.contains_key(&payload.operation_id)
        {
            return Err(SkillError::Integrity(
                "invalid revocation provenance".into(),
            ));
        }
        let reference = reference(record);
        state.revoked.insert(payload.key.clone(), reference.clone());
        state
            .operations
            .insert(payload.operation_id.clone(), (payload, reference));
        Ok(())
    }

    fn registration_id(&self, key: &SkillKey) -> Result<String, SkillError> {
        Ok(format!(
            "agent.skill.registration.{}",
            hash(&("kolyan.skill.registration.v1", &self.namespace, key))?
        ))
    }
    fn revocation_id(&self, operation: &str) -> Result<String, SkillError> {
        Ok(format!(
            "agent.skill.revocation.{}",
            hash(&("kolyan.skill.revocation.v1", &self.namespace, operation))?
        ))
    }
    fn subject(&self, key: &SkillKey) -> Result<FactSubject, SkillError> {
        Ok(FactSubject {
            kind: SUBJECT.into(),
            id: hash(&("kolyan.skill.subject.v1", &self.namespace, key))?,
        })
    }
}

pub(super) fn reference(record: &FactRecord) -> FactRef {
    FactRef {
        stream_id: record.stream_id.clone(),
        position: record.position,
        fact_id: record.draft.fact_id.clone(),
    }
}

pub(super) fn decode<T: serde::de::DeserializeOwned + Serialize>(
    value: serde_json::Value,
) -> Result<T, SkillError> {
    let decoded: T = serde_json::from_value(value.clone())
        .map_err(|_| SkillError::Integrity("invalid or unknown payload fields".into()))?;
    // FactRef itself is a shared wire DTO without deny_unknown_fields. Comparing
    // its complete normalized payload rejects unknown nested fields as well.
    if json(&decoded)? != value {
        return Err(SkillError::Integrity(
            "noncanonical or unknown nested payload fields".into(),
        ));
    }
    Ok(decoded)
}

pub(super) fn json<T: Serialize>(value: &T) -> Result<serde_json::Value, SkillError> {
    serde_json::from_slice(&encode(value, MAX_METADATA_BYTES)?)
        .map_err(|_| SkillError::Invalid("invalid JSON".into()))
}

fn validate_reason(reason: &str) -> Result<(), SkillError> {
    if reason.trim().is_empty() || reason.len() > 1024 || reason.chars().any(char::is_control) {
        return Err(SkillError::Invalid("invalid revocation reason".into()));
    }
    Ok(())
}
