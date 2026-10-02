//! Attempt-bound accounting and one-shot durable permission for a prepared GEN.
//! Guard wrappers bind this capability without acquiring a second Core interface.

mod attempt;
mod consumer;
mod writer;

pub use attempt::{ModelOpeningAttempt, ModelOpeningAttemptConfig, ModelOpeningCompletionSources};
pub use consumer::OpeningModelProvider;

use std::time::Duration;

use kolyan_ledger::{FactError, LedgerError};
use kolyan_model::{ModelProvider, ProviderError};
use thiserror::Error;

use crate::ModelOpeningProofError;

/// Implement explicitly at the raw provider and each preserving guard wrapper.
/// No default, blanket adapter or already-bound rebinding is provided.
pub trait BindOpeningAttempt: ModelProvider + Sized {
    type Bound: ModelProvider;
    fn bind_opening(
        self,
        attempt: ModelOpeningAttempt,
    ) -> Result<Self::Bound, OpeningAdmissionError>;
}

#[derive(Debug, Clone)]
pub enum OpeningAccountingPolicy {
    ProviderReported {
        max_input_tokens: u64,
        count_timeout: Duration,
    },
    WireBytes {
        policy_revision: String,
        max_generation_wire_bytes: u64,
    },
}

#[derive(Debug, Error)]
pub enum OpeningAdmissionError {
    #[error("invalid opening configuration: {0}")]
    InvalidConfiguration(String),
    #[error("opening binding differs: {0}")]
    BindingMismatch(String),
    #[error("opening state rejects operation: {0}")]
    InvalidState(String),
    #[error("opening cancelled")]
    Cancelled,
    #[error("opening deadline exhausted")]
    DeadlineExceeded,
    #[error("configured provider count timeout exhausted")]
    CountTimedOut,
    #[error("accounting rejected: {0}")]
    AccountingRejected(String),
    #[error("provider failed: {0}")]
    Provider(#[from] ProviderError),
    #[error("opening Ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("opening Journal failed: {0}")]
    Fact(#[from] FactError),
    #[error("opening proof failed: {0}")]
    Proof(#[from] ModelOpeningProofError),
    #[error("opening storage worker did not return: {0}")]
    Worker(String),
}

#[cfg(test)]
mod tests;
