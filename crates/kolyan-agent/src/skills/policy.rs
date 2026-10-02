//! Explicit host ACL and exact physical/private conversation scope.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{
    SkillError, SkillKey,
    encoding::{hash, id, is_digest},
};
use crate::{AgentInvocationBinding, AgentKey};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillAccessRuleInput {
    pub agent: AgentKey,
    pub logical_session_id: String,
    pub task_id: Option<String>,
    pub invocation_id: Option<String>,
    pub skills: BTreeSet<SkillKey>,
}

/// No model input can construct an access policy through deserialization.
#[derive(Debug, Clone, Serialize)]
pub struct SkillAccessPolicy {
    id: String,
    revision: String,
    rules: BTreeSet<SkillAccessRuleInput>,
    digest: String,
}

impl SkillAccessPolicy {
    pub fn new(
        policy_id: String,
        revision: String,
        rules: Vec<SkillAccessRuleInput>,
    ) -> Result<Self, SkillError> {
        id(&policy_id)?;
        id(&revision)?;
        if rules.len() > 128 {
            return Err(SkillError::Capacity);
        }
        for rule in &rules {
            rule.agent
                .validate()
                .map_err(|_| SkillError::Invalid("invalid Agent key".into()))?;
            id(&rule.logical_session_id)?;
            for value in [&rule.task_id, &rule.invocation_id].into_iter().flatten() {
                id(value)?;
            }
            if rule.invocation_id.is_some() && rule.task_id.is_none() {
                return Err(SkillError::Invalid("invocation ACL requires Task".into()));
            }
            if rule.skills.len() > 128 {
                return Err(SkillError::Capacity);
            }
            for key in &rule.skills {
                key.validate()?;
            }
        }
        let normalized: BTreeSet<_> = rules.into_iter().collect();
        let digest = hash(&(
            "kolyan.skills.policy.v1",
            &policy_id,
            &revision,
            &normalized,
        ))?;
        Ok(Self {
            id: policy_id,
            revision,
            rules: normalized,
            digest,
        })
    }

    pub fn deny_all() -> Self {
        Self::new("skills.deny".into(), "1".into(), vec![]).expect("fixed valid empty policy")
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub(super) fn allowed(&self, agent: &AgentKey, scope: &SkillScope) -> BTreeSet<SkillKey> {
        self.rules
            .iter()
            .filter(|r| {
                &r.agent == agent
                    && r.logical_session_id == scope.0.logical_session_id
                    && r.task_id.as_ref().is_none_or(|v| v == &scope.0.task_id)
                    && r.invocation_id
                        .as_ref()
                        .is_none_or(|v| v == &scope.0.invocation_id)
            })
            .flat_map(|r| r.skills.iter().cloned())
            .collect()
    }
}

/// Validated coordinates are still not proof: binding verifies the actual ownership fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SavedScope {
    pub logical_session_id: String,
    pub task_id: String,
    pub invocation_id: String,
    pub private_session_id: String,
    pub agent_snapshot_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillScope(pub(super) SavedScope);

impl SkillScope {
    pub fn from_binding(binding: &AgentInvocationBinding) -> Result<Self, SkillError> {
        binding
            .validate()
            .map_err(|_| SkillError::Provenance("invalid invocation ownership".into()))?;
        Ok(Self(SavedScope {
            logical_session_id: binding.logical_session_id.clone(),
            task_id: binding.task_id.clone(),
            invocation_id: binding.invocation_id.clone(),
            private_session_id: binding.private_session_id.clone(),
            agent_snapshot_digest: binding.snapshot.digest().into(),
        }))
    }
    pub fn logical_session_id(&self) -> &str {
        &self.0.logical_session_id
    }
    pub fn task_id(&self) -> &str {
        &self.0.task_id
    }
    pub fn invocation_id(&self) -> &str {
        &self.0.invocation_id
    }
    pub fn private_session_id(&self) -> &str {
        &self.0.private_session_id
    }
    pub fn agent_snapshot_digest(&self) -> &str {
        &self.0.agent_snapshot_digest
    }
}

impl SavedScope {
    pub(super) fn validate(&self) -> Result<(), SkillError> {
        for value in [
            &self.logical_session_id,
            &self.task_id,
            &self.invocation_id,
            &self.private_session_id,
        ] {
            id(value)?;
        }
        if !is_digest(&self.agent_snapshot_digest) {
            return Err(SkillError::Integrity("invalid snapshot digest".into()));
        }
        Ok(())
    }
}
