//! Exact saved advertisements and independent current-use validation.

use kolyan_ledger::{FactDraft, FactError, FactRef, FactSubject};
use serde::{Deserialize, Serialize};

use super::{
    RegisteredSkill, SkillAccessPolicy, SkillCatalog, SkillError, SkillScope,
    catalog::{decode, json, reference},
    encoding::{encode, hash},
    metadata::SavedMetadata,
    policy::SavedScope,
};
use crate::{AgentInvocationBindingStore, AgentKey, AgentSnapshot, BindingError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedSelection {
    metadata: SavedMetadata,
    registration: FactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedAdvertisement {
    namespace: String,
    scope: SavedScope,
    agent: AgentKey,
    policy_digest: String,
    skills: Vec<SavedSelection>,
}

/// Metadata-only, immutable selection. Serialization is diagnostic, not authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillAdvertisement(SavedAdvertisement);

impl SkillAdvertisement {
    pub fn scope(&self) -> SkillScope {
        SkillScope(self.0.scope.clone())
    }
    pub fn skills(&self) -> Vec<RegisteredSkill> {
        self.0
            .skills
            .iter()
            .map(|s| RegisteredSkill {
                metadata: super::SkillMetadata(s.metadata.clone()),
                reference: s.registration.clone(),
            })
            .collect()
    }
    pub fn policy_digest(&self) -> &str {
        &self.0.policy_digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedBinding {
    advertisement: SavedAdvertisement,
    ownership: FactRef,
}

/// Historical provenance only. Call validate_current before any new use.
/// No Deserialize implementation allows a caller to manufacture a verified binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifiedSkillBinding {
    saved: SavedBinding,
    reference: FactRef,
}

impl VerifiedSkillBinding {
    pub fn reference(&self) -> &FactRef {
        &self.reference
    }
    pub fn ownership(&self) -> &FactRef {
        &self.saved.ownership
    }
    pub fn advertisement(&self) -> SkillAdvertisement {
        SkillAdvertisement(self.saved.advertisement.clone())
    }
}

#[derive(Clone)]
pub struct SkillRuntime {
    catalog: SkillCatalog,
    policy: SkillAccessPolicy,
}

impl SkillRuntime {
    pub fn new(catalog: SkillCatalog, policy: SkillAccessPolicy) -> Self {
        Self { catalog, policy }
    }

    /// No artifact reads. The caller must incorporate this frozen metadata into
    /// its real input source before admission; this foundation does not run a model.
    pub fn discover(
        &self,
        snapshot: &AgentSnapshot,
        scope: &SkillScope,
    ) -> Result<SkillAdvertisement, SkillError> {
        scope.0.validate()?;
        if scope.0.agent_snapshot_digest != snapshot.digest() {
            return Err(SkillError::Provenance(
                "snapshot differs from exact scope".into(),
            ));
        }
        let agent = snapshot.definition().key();
        let state = self.catalog.replay()?;
        let allowed = self.policy.allowed(&agent, scope);
        let mut skills = Vec::new();
        for key in allowed {
            let registered = state.entries.get(&key).ok_or_else(|| {
                SkillError::Provenance("ACL references missing exact revision".into())
            })?;
            if !state.revoked.contains_key(&key) {
                skills.push(SavedSelection {
                    metadata: registered.metadata.0.clone(),
                    registration: registered.reference.clone(),
                });
            }
        }
        if skills.len() > self.catalog.limits.advertised {
            return Err(SkillError::Capacity);
        }
        let advertisement = SavedAdvertisement {
            namespace: self.catalog.namespace.clone(),
            scope: scope.0.clone(),
            agent,
            policy_digest: self.policy.digest().into(),
            skills,
        };
        encode(&advertisement, self.catalog.limits.metadata_bytes)?;
        Ok(SkillAdvertisement(advertisement))
    }

    /// Bind original selection, never refresh it. Actual ownership is reloaded
    /// from the same journal, rather than trusting a supplied snapshot or FactRef.
    pub fn bind(
        &self,
        advertisement: &SkillAdvertisement,
        ownership: &FactRef,
    ) -> Result<VerifiedSkillBinding, SkillError> {
        let snapshot = self.verify_owner(&advertisement.0, ownership)?;
        self.check_current(&advertisement.0, &snapshot)?;
        let saved = SavedBinding {
            advertisement: advertisement.0.clone(),
            ownership: ownership.clone(),
        };
        let (stream, fact_id) = self.coordinates(&saved.advertisement.scope)?;
        let draft = FactDraft {
            fact_id: fact_id.clone(),
            subject: FactSubject {
                kind: "agent.skill.binding".into(),
                id: saved.advertisement.scope.invocation_id.clone(),
            },
            kind: "agent.skill.bound".into(),
            schema_version: 1,
            critical: true,
            causes: causes(&saved),
            payload: json(&saved)?,
        };
        match self.catalog.journal.append(&stream, 0, vec![draft]) {
            Ok(_) | Err(FactError::Conflict(_) | FactError::StalePosition { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let reference = FactRef {
            stream_id: stream,
            position: 1,
            fact_id,
        };
        let restored = self.restore_binding(&reference, &advertisement.scope())?;
        if restored.saved != saved {
            return Err(SkillError::Conflict);
        }
        self.validate_current(&restored)?;
        Ok(restored)
    }

    /// Read-only historical verification. Revocation or changed ACL does not erase
    /// saved provenance, but validate_current will refuse new use of that binding.
    pub fn restore_binding(
        &self,
        reference_value: &FactRef,
        expected_scope: &SkillScope,
    ) -> Result<VerifiedSkillBinding, SkillError> {
        expected_scope.0.validate()?;
        let (stream, fact_id) = self.coordinates(&expected_scope.0)?;
        if reference_value.stream_id != stream
            || reference_value.position != 1
            || reference_value.fact_id != fact_id
        {
            return Err(SkillError::Provenance("foreign binding coordinates".into()));
        }
        let records = self.catalog.journal.read(&stream, 0, 2)?;
        let [record] = records.as_slice() else {
            return Err(SkillError::Integrity(
                "missing or extra binding facts".into(),
            ));
        };
        encode(&record.draft.payload, self.catalog.limits.metadata_bytes)?;
        let saved: SavedBinding = decode(record.draft.payload.clone())?;
        let ad = &saved.advertisement;
        if reference(record) != *reference_value
            || record.draft.kind != "agent.skill.bound"
            || record.draft.schema_version != 1
            || !record.draft.critical
            || record.draft.subject
                != (FactSubject {
                    kind: "agent.skill.binding".into(),
                    id: expected_scope.0.invocation_id.clone(),
                })
            || ad.namespace != self.catalog.namespace
            || ad.scope != expected_scope.0
            || record.draft.causes != causes(&saved)
        {
            return Err(SkillError::Integrity("invalid binding provenance".into()));
        }
        ad.agent
            .validate()
            .map_err(|_| SkillError::Integrity("invalid Agent key".into()))?;
        if !super::encoding::is_digest(&ad.policy_digest)
            || ad.skills.len() > self.catalog.limits.advertised
        {
            return Err(SkillError::Integrity(
                "invalid saved selection bounds".into(),
            ));
        }
        let state = self.catalog.replay()?;
        let mut previous = None;
        for selection in &ad.skills {
            selection.metadata.validate()?;
            let key = &selection.metadata.descriptor.key;
            if previous.as_ref().is_some_and(|p| p >= key) {
                return Err(SkillError::Integrity(
                    "selection not unique and ordered".into(),
                ));
            }
            previous = Some(key.clone());
            let registered = state
                .entries
                .get(key)
                .ok_or_else(|| SkillError::Integrity("selected registration missing".into()))?;
            if registered.reference != selection.registration
                || registered.metadata.0 != selection.metadata
            {
                return Err(SkillError::Integrity(
                    "selected registration changed".into(),
                ));
            }
        }
        self.verify_owner(ad, &saved.ownership)?;
        Ok(VerifiedSkillBinding {
            saved,
            reference: reference_value.clone(),
        })
    }

    /// Rechecks physical provenance and current host policy; this is not a grant
    /// and is not atomic with revocation on another stream or a subsequent send.
    pub fn validate_current(&self, binding: &VerifiedSkillBinding) -> Result<(), SkillError> {
        let restored =
            self.restore_binding(&binding.reference, &binding.advertisement().scope())?;
        if &restored != binding {
            return Err(SkillError::Integrity("binding changed".into()));
        }
        let snapshot = self.verify_owner(&binding.saved.advertisement, &binding.saved.ownership)?;
        self.check_current(&binding.saved.advertisement, &snapshot)
    }

    fn check_current(
        &self,
        ad: &SavedAdvertisement,
        snapshot: &AgentSnapshot,
    ) -> Result<(), SkillError> {
        if ad.namespace != self.catalog.namespace {
            return Err(SkillError::Provenance("foreign namespace".into()));
        }
        if ad.policy_digest != self.policy.digest() {
            return Err(SkillError::Permission);
        }
        let state = self.catalog.replay()?;
        if ad
            .skills
            .iter()
            .any(|s| state.revoked.contains_key(&s.metadata.descriptor.key))
        {
            return Err(SkillError::Revoked);
        }
        let current = self.discover(snapshot, &SkillScope(ad.scope.clone()))?;
        if &current.0 != ad {
            return Err(SkillError::Permission);
        }
        Ok(())
    }

    fn verify_owner(
        &self,
        ad: &SavedAdvertisement,
        ownership: &FactRef,
    ) -> Result<AgentSnapshot, SkillError> {
        if ad.namespace != self.catalog.namespace {
            return Err(SkillError::Provenance("foreign namespace".into()));
        }
        let store = AgentInvocationBindingStore::new(self.catalog.journal.clone());
        let (binding, reference_value) = store
            .load_with_reference(
                &ad.scope.task_id,
                &ad.scope.invocation_id,
                &ad.scope.logical_session_id,
            )
            .map_err(|error| match error {
                BindingError::Journal(error) => SkillError::Journal(error),
                _ => SkillError::Provenance("ownership journal verification failed".into()),
            })?
            .ok_or_else(|| SkillError::Provenance("ownership missing".into()))?;
        if reference_value != *ownership
            || SkillScope::from_binding(&binding)?.0 != ad.scope
            || binding.snapshot.definition().key() != ad.agent
        {
            return Err(SkillError::Provenance("exact ownership mismatch".into()));
        }
        Ok(binding.snapshot)
    }

    fn coordinates(&self, scope: &SavedScope) -> Result<(String, String), SkillError> {
        let digest = hash(&("kolyan.skill.binding.v1", &self.catalog.namespace, scope))?;
        Ok((
            format!("agent.skills.binding.{digest}"),
            format!("agent.skill.binding.{digest}"),
        ))
    }
}

fn causes(saved: &SavedBinding) -> Vec<FactRef> {
    std::iter::once(saved.ownership.clone())
        .chain(
            saved
                .advertisement
                .skills
                .iter()
                .map(|s| s.registration.clone()),
        )
        .collect()
}
