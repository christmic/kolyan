//! Host ACL and immutable ownership coordinates; no model-derived authority.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{HookError, HookKey, HookPhase, hash, id, is_digest};
use crate::{AgentInvocationBinding, AgentKey};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookScope {
    pub logical_session_id: String,
    pub task_id: String,
    pub invocation_id: String,
    pub private_session_id: String,
    pub execution_id: String,
    pub turn_id: String,
    pub agent_snapshot_digest: String,
}
impl HookScope {
    pub fn from_binding(
        binding: &AgentInvocationBinding,
        execution_id: String,
        turn_id: String,
    ) -> Result<Self, HookError> {
        binding
            .validate()
            .map_err(|e| HookError::Integrity(e.to_string()))?;
        let value = Self {
            logical_session_id: binding.logical_session_id.clone(),
            task_id: binding.task_id.clone(),
            invocation_id: binding.invocation_id.clone(),
            private_session_id: binding.private_session_id.clone(),
            execution_id,
            turn_id,
            agent_snapshot_digest: binding.snapshot.digest().into(),
        };
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), HookError> {
        for value in [
            &self.logical_session_id,
            &self.task_id,
            &self.invocation_id,
            &self.private_session_id,
            &self.execution_id,
            &self.turn_id,
        ] {
            id(value)?;
        }
        if !is_digest(&self.agent_snapshot_digest) {
            return Err(HookError::Integrity("invalid snapshot digest".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HookAccessRule {
    pub agent: AgentKey,
    pub logical_session_id: String,
    pub task_id: Option<String>,
    pub invocation_id: Option<String>,
    pub hooks: BTreeSet<HookKey>,
    pub phases: BTreeSet<HookPhase>,
}

/// Explicit immutable host policy. Deserialization cannot manufacture authority.
#[derive(Debug, Clone, Serialize)]
pub struct HookAccessPolicy {
    revision: String,
    rules: Vec<HookAccessRule>,
    digest: String,
}
impl HookAccessPolicy {
    pub fn new(revision: String, rules: Vec<HookAccessRule>) -> Result<Self, HookError> {
        id(&revision)?;
        if rules.len() > 128 {
            return Err(HookError::Capacity);
        }
        for rule in &rules {
            rule.agent
                .validate()
                .map_err(|e| HookError::Invalid(e.to_string()))?;
            id(&rule.logical_session_id)?;
            for value in [&rule.task_id, &rule.invocation_id].into_iter().flatten() {
                id(value)?;
            }
            if rule.invocation_id.is_some() && rule.task_id.is_none() {
                return Err(HookError::Invalid("invocation ACL requires Task".into()));
            }
            if rule.hooks.len() > 128 || rule.phases.is_empty() {
                return Err(HookError::Capacity);
            }
            for key in &rule.hooks {
                key.validate()?;
            }
        }
        let digest = hash(&("kolyan.hooks.policy.v1", &revision, &rules))?;
        Ok(Self {
            revision,
            rules,
            digest,
        })
    }
    pub fn deny_all() -> Self {
        Self::new("deny-1".into(), vec![]).expect("fixed valid empty policy")
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }
    pub(super) fn permits(
        &self,
        agent: &AgentKey,
        scope: &HookScope,
        key: &HookKey,
        phase: HookPhase,
    ) -> bool {
        self.rules.iter().any(|r| {
            &r.agent == agent
                && r.logical_session_id == scope.logical_session_id
                && r.task_id.as_ref().is_none_or(|v| v == &scope.task_id)
                && r.invocation_id
                    .as_ref()
                    .is_none_or(|v| v == &scope.invocation_id)
                && r.hooks.contains(key)
                && r.phases.contains(&phase)
        })
    }
}
