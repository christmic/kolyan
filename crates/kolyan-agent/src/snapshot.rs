//! Digest-bound instance snapshots with no implicit authority expansion.

use kolyan_server::AgentIdentity;
use serde::{Deserialize, Serialize};

use crate::{AgentDefinition, AgentError, AgentPermissions, digest, identity};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotInput {
    schema_version: u32,
    definition: AgentDefinition,
    identity: AgentIdentity,
    permissions: AgentPermissions,
    digest: String,
}

/// Content, instance identity and effective static authority are all bound.
/// Dynamic policy must still mediate every actual tool or delegation call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SnapshotInput", into = "SnapshotInput")]
pub struct AgentSnapshot(SnapshotInput);

impl AgentSnapshot {
    pub(crate) fn new(
        definition: AgentDefinition,
        instance_id: String,
        permissions: AgentPermissions,
    ) -> Result<Self, AgentError> {
        let key = definition.key();
        let mut input = SnapshotInput {
            schema_version: 1,
            definition,
            identity: AgentIdentity {
                definition_id: key.definition_id,
                revision: key.revision,
                instance_id,
            },
            permissions,
            digest: String::new(),
        };
        input.digest = binding_digest(&input)?;
        Self::try_from(input)
    }

    pub fn definition(&self) -> &AgentDefinition {
        &self.0.definition
    }
    pub fn identity(&self) -> &AgentIdentity {
        &self.0.identity
    }
    pub fn permissions(&self) -> &AgentPermissions {
        &self.0.permissions
    }
    pub fn digest(&self) -> &str {
        &self.0.digest
    }
}

impl TryFrom<SnapshotInput> for AgentSnapshot {
    type Error = AgentError;

    fn try_from(input: SnapshotInput) -> Result<Self, Self::Error> {
        identity(&input.identity.instance_id)?;
        let key = input.definition.key();
        if input.schema_version != 1
            || key.definition_id != input.identity.definition_id
            || key.revision != input.identity.revision
            || binding_digest(&input)? != input.digest
        {
            return Err(AgentError::SnapshotMismatch);
        }
        input
            .permissions
            .require_subset_of(input.definition.permissions())?;
        Ok(Self(input))
    }
}

impl From<AgentSnapshot> for SnapshotInput {
    fn from(snapshot: AgentSnapshot) -> Self {
        snapshot.0
    }
}

fn binding_digest(input: &SnapshotInput) -> Result<String, AgentError> {
    digest(&(
        "kolyan.agent.snapshot.v1",
        input.schema_version,
        &input.definition,
        &input.identity,
        &input.permissions,
    ))
}

#[cfg(test)]
mod tests;
