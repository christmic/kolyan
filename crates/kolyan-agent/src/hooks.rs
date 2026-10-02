//! Revision-pinned host hooks. Script decisions can only narrow execution.
//! This foundation has no Runner/Runtime consumer and never opens a model stream.

mod binding;
mod catalog;
mod definition;
mod native;
mod policy;
mod protocol;
mod runtime;

pub use binding::VerifiedHookBinding;
pub use catalog::{HookCatalog, RegisteredHook};
pub use definition::{HookKey, HookManifest, HookPhase};
pub use native::NativeHookHost;
pub use policy::{HookAccessPolicy, HookAccessRule, HookScope};
pub use protocol::{HookDecision, HookEvent, HookPayload, HookReply};
pub use runtime::{HookDispatchResult, HookExecutionWindow, HookRuntime};

use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use kolyan_ledger::{FactError, FactRecord, FactRef};
use kolyan_trace::ArtifactError;

/// Host failures are separate from a script's policy denial. Neither is a grant.
#[derive(Debug, Error)]
pub enum HookError {
    #[error("invalid hook contract: {0}")]
    Invalid(String),
    #[error("hook host policy denied access")]
    Permission,
    #[error("hook version is revoked")]
    Revoked,
    #[error("immutable hook content conflicts")]
    Conflict,
    #[error("hook provenance or integrity failed: {0}")]
    Integrity(String),
    #[error("hook capacity exceeded")]
    Capacity,
    #[error("hook protocol failed: {0}")]
    Protocol(String),
    #[error("hook native execution failed: {0}")]
    Native(String),
    #[error("hook execution cancelled")]
    Cancelled,
    #[error("hook execution window expired")]
    Expired,
    #[error("hook execution is interrupted; implicit replay is forbidden")]
    Interrupted,
    #[error("hook journal failed: {0}")]
    Journal(#[from] FactError),
    #[error("hook artifact failed: {0}")]
    Artifact(#[from] ArtifactError),
    #[error("hook host I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

pub(super) fn id(value: &str) -> Result<(), HookError> {
    crate::identity(value).map_err(|e| HookError::Invalid(e.to_string()))
}

pub(super) fn encode(value: &impl Serialize) -> Result<Vec<u8>, HookError> {
    let bytes = serde_json::to_vec(value).map_err(|e| HookError::Invalid(e.to_string()))?;
    if bytes.len() > 64 * 1024 {
        return Err(HookError::Capacity);
    }
    Ok(bytes)
}

pub(super) fn hash(value: &impl Serialize) -> Result<String, HookError> {
    Ok(bytes_hash(&encode(value)?))
}

pub(super) fn bytes_hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

pub(super) fn reference(row: &FactRecord) -> FactRef {
    FactRef {
        stream_id: row.stream_id.clone(),
        position: row.position,
        fact_id: row.draft.fact_id.clone(),
    }
}
