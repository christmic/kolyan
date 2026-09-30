//! Static ceilings for four environment operations and independent delegation.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{AgentError, identity};

/// Exact definition identity; display names never serve as authorization keys.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentKey {
    pub definition_id: String,
    pub revision: String,
}

impl AgentKey {
    pub fn new(
        definition_id: impl Into<String>,
        revision: impl Into<String>,
    ) -> Result<Self, AgentError> {
        let key = Self {
            definition_id: definition_id.into(),
            revision: revision.into(),
        };
        key.validate()?;
        Ok(key)
    }

    pub fn validate(&self) -> Result<(), AgentError> {
        identity(&self.definition_id)?;
        identity(&self.revision)
    }
}

/// Exhaustive environment inventory. Agent delegation is deliberately separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EnvironmentTool {
    #[serde(rename = "file.read")]
    Read,
    #[serde(rename = "file.write")]
    Write,
    #[serde(rename = "file.edit")]
    Edit,
    #[serde(rename = "shell")]
    Shell,
}

impl EnvironmentTool {
    pub fn name(self) -> &'static str {
        match self {
            Self::Read => "file.read",
            Self::Write => "file.write",
            Self::Edit => "file.edit",
            Self::Shell => "shell",
        }
    }
}

/// Named targets require exact revisions. Inline and self-call authority are
/// independent switches, not inferred from possession of environment tools.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationCeiling {
    pub named_targets: BTreeSet<AgentKey>,
    pub allow_inline: bool,
    pub allow_self: bool,
}

impl DelegationCeiling {
    pub fn validate(&self) -> Result<(), AgentError> {
        if self.named_targets.len() > 256 {
            return Err(AgentError::Invalid(
                "delegation target count exceeds 256".into(),
            ));
        }
        for target in &self.named_targets {
            target.validate()?;
        }
        Ok(())
    }

    pub fn intersection(&self, other: &Self) -> Result<Self, AgentError> {
        self.validate()?;
        other.validate()?;
        Ok(Self {
            named_targets: self
                .named_targets
                .intersection(&other.named_targets)
                .cloned()
                .collect(),
            allow_inline: self.allow_inline && other.allow_inline,
            allow_self: self.allow_self && other.allow_self,
        })
    }

    fn is_subset_of(&self, other: &Self) -> bool {
        self.named_targets.is_subset(&other.named_targets)
            && (!self.allow_inline || other.allow_inline)
            && (!self.allow_self || other.allow_self)
    }
}

/// Validated static capability ceiling, not a dynamic execution grant.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPermissions {
    pub tools: BTreeSet<EnvironmentTool>,
    pub delegation: DelegationCeiling,
}

impl AgentPermissions {
    pub fn validate(&self) -> Result<(), AgentError> {
        self.delegation.validate()
    }

    /// Intersect all supplied authority layers. Invalid layers fail explicitly.
    pub fn intersection(&self, other: &Self) -> Result<Self, AgentError> {
        self.validate()?;
        other.validate()?;
        Ok(Self {
            tools: self.tools.intersection(&other.tools).copied().collect(),
            delegation: self.delegation.intersection(&other.delegation)?,
        })
    }

    /// Reject an attempted expansion instead of silently clipping a request.
    pub fn require_subset_of(&self, ceiling: &Self) -> Result<(), AgentError> {
        self.validate()?;
        ceiling.validate()?;
        if !self.tools.is_subset(&ceiling.tools)
            || !self.delegation.is_subset_of(&ceiling.delegation)
        {
            return Err(AgentError::PermissionDenied);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
