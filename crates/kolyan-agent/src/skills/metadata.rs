//! Exact immutable metadata; deserialized configuration is validated on consumption.

use kolyan_ledger::FactRef;
use kolyan_trace::{ArtifactRef, Retention};
use serde::{Deserialize, Serialize};

use super::{
    SkillError,
    encoding::{hash, id, is_digest},
};

pub const MAX_CATALOG_VERSIONS: usize = 128;
pub const MAX_ADVERTISED_SKILLS: usize = 16;
pub const MAX_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_BODY_BYTES: usize = 32 * 1024;
pub const MAX_TOOL_RESULT_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillKey {
    pub id: String,
    pub revision: String,
}

impl SkillKey {
    pub fn new(id_value: String, revision: String) -> Result<Self, SkillError> {
        let key = Self {
            id: id_value,
            revision,
        };
        key.validate()?;
        Ok(key)
    }

    pub(super) fn validate(&self) -> Result<(), SkillError> {
        id(&self.id)?;
        id(&self.revision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillDescriptorInput {
    pub key: SkillKey,
    pub title: String,
    pub description: String,
}

impl SkillDescriptorInput {
    pub(super) fn validate(&self) -> Result<(), SkillError> {
        self.key.validate()?;
        for (value, limit) in [(&self.title, 256), (&self.description, 1024)] {
            if value.trim().is_empty() || value.len() > limit || value.chars().any(char::is_control)
            {
                return Err(SkillError::Invalid("invalid metadata text".into()));
            }
        }
        Ok(())
    }
}

/// Host limits can only tighten the documented hard maxima. No silent truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SkillLimits {
    pub(super) catalog_versions: usize,
    pub(super) advertised: usize,
    pub(super) metadata_bytes: usize,
    pub(super) body_bytes: usize,
}

impl Default for SkillLimits {
    fn default() -> Self {
        Self {
            catalog_versions: MAX_CATALOG_VERSIONS,
            advertised: MAX_ADVERTISED_SKILLS,
            metadata_bytes: MAX_METADATA_BYTES,
            body_bytes: MAX_BODY_BYTES,
        }
    }
}

impl SkillLimits {
    pub fn new(
        catalog_versions: usize,
        advertised: usize,
        metadata_bytes: usize,
        body_bytes: usize,
    ) -> Result<Self, SkillError> {
        for (value, max) in [
            (catalog_versions, MAX_CATALOG_VERSIONS),
            (advertised, MAX_ADVERTISED_SKILLS),
            (metadata_bytes, MAX_METADATA_BYTES),
            (body_bytes, MAX_BODY_BYTES),
        ] {
            if value == 0 || value > max {
                return Err(SkillError::Invalid("invalid host limit".into()));
            }
        }
        Ok(Self {
            catalog_versions,
            advertised,
            metadata_bytes,
            body_bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SavedMetadata {
    pub descriptor: SkillDescriptorInput,
    pub body: ArtifactRef,
    pub content_digest: String,
}

impl SavedMetadata {
    pub fn new(descriptor: SkillDescriptorInput, body: ArtifactRef) -> Result<Self, SkillError> {
        let content_digest = hash(&("kolyan.skill.content.v1", &descriptor, &body))?;
        let value = Self {
            descriptor,
            body,
            content_digest,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), SkillError> {
        self.descriptor.validate()?;
        if self.body.retention != Retention::Required
            || self.body.byte_length > MAX_BODY_BYTES as u64
            || !is_digest(&self.body.digest)
            || !is_digest(&self.content_digest)
            || self.content_digest
                != hash(&("kolyan.skill.content.v1", &self.descriptor, &self.body))?
        {
            return Err(SkillError::Integrity("invalid immutable metadata".into()));
        }
        Ok(())
    }
}

/// Immutable knowledge reference, not authorization to read or execute its body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillMetadata(pub(super) SavedMetadata);

impl SkillMetadata {
    pub fn descriptor(&self) -> &SkillDescriptorInput {
        &self.0.descriptor
    }
    pub fn body(&self) -> &ArtifactRef {
        &self.0.body
    }
    pub fn content_digest(&self) -> &str {
        &self.0.content_digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegisteredSkill {
    pub(super) metadata: SkillMetadata,
    pub(super) reference: FactRef,
}

impl RegisteredSkill {
    pub fn metadata(&self) -> &SkillMetadata {
        &self.metadata
    }
    pub fn reference(&self) -> &FactRef {
        &self.reference
    }
}
