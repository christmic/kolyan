//! Physical file bindings and descriptor-relative execution for trusted workers.
//! No policy decisions, alias resolution, ambient fallback or concurrent-writer CAS.

mod binding;
mod execution;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::file_operations::FileOperation;

pub(crate) use binding::OpenedBinding;
pub use execution::execute_exact;
pub(crate) use execution::validate_operation;

/// Object identity, not a content revision or a persistent inode generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactFileIdentity {
    pub dev: u64,
    pub ino: u64,
}

/// Physical path plus ordered identities, including `/` and the final directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactDirectoryBinding {
    pub physical_path: PathBuf,
    pub identity_chain: Vec<ExactFileIdentity>,
}

/// Host-prepared target. Model paths never select the execution resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactFileBinding {
    pub workspace: ExactDirectoryBinding,
    pub parent: ExactDirectoryBinding,
    pub leaf: String,
    #[serde(deserialize_with = "deserialize_nullable")]
    pub target_identity: Option<ExactFileIdentity>,
    pub protected_roots: Vec<PathBuf>,
}

/// One absent leaf in a host-private, same-filesystem staging directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactFileStaging {
    pub parent: ExactDirectoryBinding,
    pub leaf: String,
}

/// Bounded by the worker transport; all fields are revalidated before effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactFileWorkerRequest {
    pub operation: FileOperation,
    pub binding: ExactFileBinding,
    #[serde(deserialize_with = "deserialize_nullable")]
    pub staging: Option<ExactFileStaging>,
}

// Nullable fields still require their wire key; omission is not an old protocol.
fn deserialize_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests;
