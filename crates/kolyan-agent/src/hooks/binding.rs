//! Opaque saved selection, revalidated against physical ownership and catalog facts.

use serde::{Deserialize, Serialize};

use kolyan_ledger::{FactDraft, FactError, FactRef, FactSubject};

use super::{
    HookAccessPolicy, HookCatalog, HookError, HookKey, HookScope, RegisteredHook, encode, hash,
    reference,
};
use crate::{AgentInvocationBindingStore, AgentKey, BindingError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SavedBinding {
    namespace: String,
    pub scope: HookScope,
    pub agent: AgentKey,
    ownership: FactRef,
    pub hooks: Vec<RegisteredHook>,
    pub policy_digest: String,
    pub native_digest: String,
}

/// Possession alone is not authority. Every dispatch reloads this exact fact,
/// current ACL, revocations and actual ownership; there is no Deserialize impl.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifiedHookBinding {
    pub(super) saved: SavedBinding,
    pub(super) reference: FactRef,
}
impl VerifiedHookBinding {
    pub fn scope(&self) -> &HookScope {
        &self.saved.scope
    }
    pub fn reference(&self) -> &FactRef {
        &self.reference
    }
}

impl HookCatalog {
    /// Restore the explicit saved selection; missing facts never trigger binding.
    pub(super) fn restore_for_scope(
        &self,
        scope: &HookScope,
    ) -> Result<VerifiedHookBinding, HookError> {
        let (stream_id, fact_id) = self.binding_coordinates(scope)?;
        self.restore_binding(
            &FactRef {
                stream_id,
                position: 1,
                fact_id,
            },
            scope,
        )
    }

    /// Freezes explicit exact keys after checking all registered phases against
    /// host ACL. Empty selections are allowed, but never suppress required hooks.
    pub fn bind(
        &self,
        scope: HookScope,
        ownership: FactRef,
        keys: Vec<HookKey>,
        policy: &HookAccessPolicy,
        native_digest: String,
    ) -> Result<VerifiedHookBinding, HookError> {
        scope.validate()?;
        if keys.len() > 8 || !super::is_digest(&native_digest) {
            return Err(HookError::Capacity);
        }
        let (owner, actual) = self.owner(&scope)?;
        if actual != ownership {
            return Err(HookError::Integrity("foreign ownership reference".into()));
        }
        let state = self.replay()?;
        let mut ordered = keys;
        ordered.sort();
        if ordered.windows(2).any(|p| p[0] == p[1]) {
            return Err(HookError::Invalid("duplicate selected hook".into()));
        }
        let mut hooks = Vec::new();
        for key in ordered {
            key.validate()?;
            let saved = state
                .entries
                .get(&key)
                .ok_or_else(|| HookError::Integrity("selected registration missing".into()))?;
            if state.revoked.contains_key(&key) {
                return Err(HookError::Revoked);
            }
            if saved
                .manifest
                .phases
                .iter()
                .any(|p| !policy.permits(&owner, &scope, &key, *p))
            {
                return Err(HookError::Permission);
            }
            hooks.push(saved.clone());
        }
        let saved = SavedBinding {
            namespace: self.namespace.clone(),
            scope,
            agent: owner,
            ownership,
            hooks,
            policy_digest: policy.digest().into(),
            native_digest,
        };
        let (stream, fact_id) = self.binding_coordinates(&saved.scope)?;
        let draft = binding_draft(&saved, fact_id.clone())?;
        match self.journal.append(&stream, 0, vec![draft]) {
            Ok(_) | Err(FactError::Conflict(_) | FactError::StalePosition { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let fact = FactRef {
            stream_id: stream,
            position: 1,
            fact_id,
        };
        let result = self.restore_binding(&fact, &saved.scope)?;
        if result.saved != saved {
            return Err(HookError::Conflict);
        }
        self.validate_current(&result, policy, &result.saved.native_digest)?;
        Ok(result)
    }

    /// Historical proof restoration does not reselect revisions. A revoked
    /// binding may be inspected, but validate_current refuses new dispatch.
    pub fn restore_binding(
        &self,
        fact: &FactRef,
        scope: &HookScope,
    ) -> Result<VerifiedHookBinding, HookError> {
        scope.validate()?;
        let (stream, fact_id) = self.binding_coordinates(scope)?;
        if fact.stream_id != stream || fact.position != 1 || fact.fact_id != fact_id {
            return Err(HookError::Integrity("foreign binding coordinates".into()));
        }
        let rows = self.journal.read(&stream, 0, 2)?;
        let [row] = rows.as_slice() else {
            return Err(HookError::Integrity("missing/extra binding facts".into()));
        };
        encode(&row.draft.payload)?;
        let saved: SavedBinding = serde_json::from_value(row.draft.payload.clone())
            .map_err(|e| HookError::Integrity(e.to_string()))?;
        if saved.namespace != self.namespace
            || &saved.scope != scope
            || saved.hooks.len() > 8
            || !super::is_digest(&saved.policy_digest)
            || !super::is_digest(&saved.native_digest)
            || row.draft != binding_draft(&saved, fact_id)?
            || reference(row) != *fact
        {
            return Err(HookError::Integrity("invalid binding envelope".into()));
        }
        let (agent, ownership) = self.owner(scope)?;
        if agent != saved.agent || ownership != saved.ownership {
            return Err(HookError::Integrity("ownership changed".into()));
        }
        let state = self.replay()?;
        let mut previous = None;
        for hook in &saved.hooks {
            let key = &hook.manifest.key;
            if previous.is_some_and(|old| old >= key) || state.entries.get(key) != Some(hook) {
                return Err(HookError::Integrity("selection changed".into()));
            }
            previous = Some(key);
        }
        Ok(VerifiedHookBinding {
            saved,
            reference: fact.clone(),
        })
    }

    pub fn validate_current(
        &self,
        binding: &VerifiedHookBinding,
        policy: &HookAccessPolicy,
        native_digest: &str,
    ) -> Result<(), HookError> {
        if self.restore_binding(&binding.reference, binding.scope())? != *binding {
            return Err(HookError::Integrity("binding changed".into()));
        }
        if binding.saved.policy_digest != policy.digest()
            || binding.saved.native_digest != native_digest
        {
            return Err(HookError::Permission);
        }
        let state = self.replay()?;
        for hook in &binding.saved.hooks {
            if state.revoked.contains_key(&hook.manifest.key) {
                return Err(HookError::Revoked);
            }
            if hook.manifest.phases.iter().any(|p| {
                !policy.permits(
                    &binding.saved.agent,
                    binding.scope(),
                    &hook.manifest.key,
                    *p,
                )
            }) {
                return Err(HookError::Permission);
            }
        }
        Ok(())
    }

    fn owner(&self, scope: &HookScope) -> Result<(AgentKey, FactRef), HookError> {
        let (binding, fact) = AgentInvocationBindingStore::new(self.journal.clone())
            .load_with_reference(
                &scope.task_id,
                &scope.invocation_id,
                &scope.logical_session_id,
            )
            .map_err(|e| match e {
                BindingError::Journal(error) => HookError::Journal(error),
                other => HookError::Integrity(other.to_string()),
            })?
            .ok_or_else(|| HookError::Integrity("ownership missing".into()))?;
        let expected =
            HookScope::from_binding(&binding, scope.execution_id.clone(), scope.turn_id.clone())?;
        if &expected != scope {
            return Err(HookError::Integrity("foreign private context".into()));
        }
        Ok((binding.snapshot.definition().key(), fact))
    }
    fn binding_coordinates(&self, scope: &HookScope) -> Result<(String, String), HookError> {
        let digest = hash(&("kolyan.hooks.binding.v1", &self.namespace, scope))?;
        Ok((
            format!("agent.hooks.binding.{digest}"),
            format!("agent.hook.binding.{digest}"),
        ))
    }
}

fn binding_draft(saved: &SavedBinding, fact_id: String) -> Result<FactDraft, HookError> {
    let payload = serde_json::to_value(saved).map_err(|e| HookError::Invalid(e.to_string()))?;
    encode(&payload)?;
    Ok(FactDraft {
        fact_id,
        subject: FactSubject {
            kind: "agent.hook.binding".into(),
            id: saved.scope.invocation_id.clone(),
        },
        kind: "agent.hook.bound".into(),
        schema_version: 1,
        critical: true,
        causes: std::iter::once(saved.ownership.clone())
            .chain(saved.hooks.iter().map(|h| h.reference.clone()))
            .collect(),
        payload,
    })
}
