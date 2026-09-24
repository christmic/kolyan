use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    Authentication,
    InvalidRequest,
    RateLimited,
    Unavailable,
    Transport,
    Protocol,
    Unsupported,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorPhase {
    Open,
    Stream,
    Decode,
}

#[derive(Debug, Error)]
#[error("{phase:?} provider error ({kind:?}): {message}")]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub phase: ProviderErrorPhase,
    pub message: String,
    pub provider: Option<String>,
    pub status: Option<u16>,
}

impl ProviderError {
    /// Preserve the causal chain without requiring callers to retain transport types.
    pub fn describe(error: &(dyn std::error::Error + 'static)) -> String {
        let mut message = error.to_string();
        let mut source = error.source();
        while let Some(cause) = source {
            message.push_str("; caused by: ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        message
    }

    pub fn new(
        kind: ProviderErrorKind,
        phase: ProviderErrorPhase,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            phase,
            message: message.into(),
            provider: None,
            status: None,
        }
    }
}
