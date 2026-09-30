//! Immutable definition values; construction and decoding share validation.

use kolyan_model::ModelRef;
use serde::{Deserialize, Serialize};

use crate::{AgentError, AgentKey, AgentPermissions};

/// Bounded construction input. An absent display name does not erase identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDefinitionInput {
    pub definition_id: String,
    pub revision: String,
    pub display_name: Option<String>,
    pub model: ModelRef,
    pub instructions: String,
    pub permissions: AgentPermissions,
}

/// An immutable validated value, including when restored from serialized data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AgentDefinitionInput", into = "AgentDefinitionInput")]
pub struct AgentDefinition(AgentDefinitionInput);

impl AgentDefinition {
    pub fn new(input: AgentDefinitionInput) -> Result<Self, AgentError> {
        AgentKey::new(&input.definition_id, &input.revision)?;
        for value in [&input.model.provider, &input.model.model] {
            if value.trim().is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
                return Err(AgentError::Invalid("invalid model reference".into()));
            }
        }
        if let Some(name) = &input.display_name
            && (name.trim().is_empty() || name.len() > 256 || name.chars().any(char::is_control))
        {
            return Err(AgentError::Invalid("invalid display name".into()));
        }
        if input.instructions.trim().is_empty()
            || input.instructions.len() > 65536
            || input.instructions.contains('\0')
        {
            return Err(AgentError::Invalid(
                "instructions must be 1..65536 non-NUL bytes".into(),
            ));
        }
        input.permissions.validate()?;
        Ok(Self(input))
    }

    pub fn key(&self) -> AgentKey {
        AgentKey {
            definition_id: self.0.definition_id.clone(),
            revision: self.0.revision.clone(),
        }
    }
    pub fn display_name(&self) -> Option<&str> {
        self.0.display_name.as_deref()
    }
    pub fn model(&self) -> &ModelRef {
        &self.0.model
    }
    pub fn instructions(&self) -> &str {
        &self.0.instructions
    }
    pub fn permissions(&self) -> &AgentPermissions {
        &self.0.permissions
    }

    /// SHA-256 of the canonical, versioned definition content.
    pub fn digest(&self) -> Result<String, AgentError> {
        crate::digest(&("kolyan.agent.definition.v1", self))
    }
}

impl TryFrom<AgentDefinitionInput> for AgentDefinition {
    type Error = AgentError;
    fn try_from(input: AgentDefinitionInput) -> Result<Self, Self::Error> {
        Self::new(input)
    }
}

impl From<AgentDefinition> for AgentDefinitionInput {
    fn from(definition: AgentDefinition) -> Self {
        definition.0
    }
}
