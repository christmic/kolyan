//! Validated Agent definitions, exact catalog resolution and authority snapshots.
//! Root execution composes the existing durable task service and host factories.
//! Explicit delegation separates child orchestration from environment tools.
//! Grants remain policy-owned; parallel driving requires host-attested read-only
//! enforcement and immutable bounds. Writable work remains conservatively serial.
//! Invocation bindings use a supplied journal; definitions remain pure values.

pub mod binding;
mod catalog;
pub mod context;
mod definition;
mod goals;
mod invoke;
mod permission;
pub mod provider;
mod runner;
pub mod skills;
mod snapshot;

pub use binding::{
    AgentInvocationBinding, AgentInvocationBindingStore, BindingContextKind, BindingError,
    child_private_session_id,
};
pub use catalog::{AgentCatalog, AgentSelector, Registration, resolve_self};
pub use definition::{AgentDefinition, AgentDefinitionInput};
pub use goals::{FileWriteCommittedChecker, FileWriteCommittedPredicateV1};
pub use invoke::{
    AGENT_INVOKE_NAME, AgentInvokeInput, ChildInvocationInput, InvocationTarget,
    InvokePrepareError, InvokePrepareLimits, PreparedAgentInvocation, ResolvedChildIntent,
    agent_invoke_manifest, prepare_agent_invocation,
};
pub use permission::{AgentKey, AgentPermissions, DelegationCeiling, EnvironmentTool};
pub use runner::{
    AdmittedAgentChild, AgentChildDriveResult, AgentChildWaitVerifier, AgentChildrenPumpResult,
    AgentDelegationConfig, AgentExecutionBudget, AgentRunner, ChildApprovalResumeRequest,
    ContinuationProjectionConfig, ContinuationProjectionRequest, DelegationOwner,
    EnvironmentToolFactory, PreparedRootInput, ProviderFactory, RootApprovalResumeRequest,
    RootInputPreparationRequest, RootRunRequest, RootRunResult, RunnerError, RunnerToolSet,
    TaskFinalizationPolicy, TaskFinalizationRequest,
};
pub use skills::{
    RegisteredSkill, SkillAccessPolicy, SkillAccessRuleInput, SkillAdvertisement, SkillCatalog,
    SkillDescriptorInput, SkillError, SkillKey, SkillLimits, SkillMetadata, SkillRuntime,
    SkillScope, VerifiedSkillBinding,
};
pub use snapshot::AgentSnapshot;

use thiserror::Error;

/// Explicit admission failures; none permit fallback to a different definition.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentError {
    #[error("invalid Agent input: {0}")]
    Invalid(String),
    #[error("definition content conflicts with its registered revision")]
    Conflict,
    #[error("exact definition revision is not registered")]
    NotFound,
    #[error("catalog capacity exceeded")]
    Capacity,
    #[error("delegation or requested permissions exceed the effective ceiling")]
    PermissionDenied,
    #[error("snapshot digest or identity does not match its definition")]
    SnapshotMismatch,
}

pub(crate) fn identity(value: &str) -> Result<(), AgentError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
    {
        return Err(AgentError::Invalid(
            "identity must be 1..256 ASCII identifier bytes".into(),
        ));
    }
    Ok(())
}

pub(crate) fn digest<T: serde::Serialize>(value: &T) -> Result<String, AgentError> {
    use sha2::{Digest, Sha256};

    let bytes =
        serde_json::to_vec(value).map_err(|error| AgentError::Invalid(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
