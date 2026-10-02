//! Host-governed immutable knowledge, not grants, executable scripts or an Agent loop.
//! Metadata discovery and historical binding restoration never read Skill bodies.
//! Current ACL/revocation checks do not claim atomicity with subsequent HTTP sends.

mod binding;
mod catalog;
mod encoding;
mod metadata;
mod policy;

pub use binding::{SkillAdvertisement, SkillRuntime, VerifiedSkillBinding};
pub use catalog::SkillCatalog;
pub use metadata::{
    MAX_ADVERTISED_SKILLS, MAX_BODY_BYTES, MAX_CATALOG_VERSIONS, MAX_METADATA_BYTES,
    MAX_TOOL_RESULT_BYTES, RegisteredSkill, SkillDescriptorInput, SkillKey, SkillLimits,
    SkillMetadata,
};
pub use policy::{SkillAccessPolicy, SkillAccessRuleInput, SkillScope};

use kolyan_ledger::FactError;
use kolyan_trace::ArtifactError;
use thiserror::Error;

/// Failures never authorize fallback to a different version or a wider scope.
#[derive(Debug, Error)]
pub enum SkillError {
    #[error("invalid Skill input: {0}")]
    Invalid(String),
    #[error("Skill capacity exceeded")]
    Capacity,
    #[error("immutable Skill content conflicts")]
    Conflict,
    #[error("Skill version was revoked")]
    Revoked,
    #[error("Skill access is not authorized by current host policy")]
    Permission,
    #[error("Skill provenance rejected: {0}")]
    Provenance(String),
    #[error("Skill integrity rejected: {0}")]
    Integrity(String),
    #[error("Skill journal failed: {0}")]
    Journal(#[from] FactError),
    #[error("Skill artifact failed: {0}")]
    Artifact(#[from] ArtifactError),
}

#[cfg(test)]
mod tests;
