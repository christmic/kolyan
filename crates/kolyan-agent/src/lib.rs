//! Validated Agent definitions, exact catalog resolution and authority snapshots.
//! This library performs no model calls, scheduling or grant creation.
//! Invocation bindings use a supplied journal; definitions remain pure values.

pub mod binding;
mod catalog;
pub mod context;
mod definition;
mod permission;
pub mod provider;
mod snapshot;

pub use catalog::{AgentCatalog, AgentSelector, Registration, resolve_self};
pub use definition::{AgentDefinition, AgentDefinitionInput};
pub use permission::{AgentKey, AgentPermissions, DelegationCeiling, EnvironmentTool};
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
