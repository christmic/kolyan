//! Bounded file worker protocol. The trusted host supplies workspace and limits;
//! this helper neither authorizes effects nor establishes its own OS isolation.

use std::io::Read;
use std::path::PathBuf;

use kolyan_tools::{FileOperation, FileOperationLimits, FileOperationResult, FileOperations};
use thiserror::Error;

const MAX_PROTOCOL_BYTES: usize = 64 * 1024 * 1024;

/// Host-selected command-line configuration, never part of model arguments.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub workspace: PathBuf,
    pub max_input_bytes: usize,
    pub file_limits: FileOperationLimits,
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("invalid worker configuration")]
    Configuration,
    #[error("worker input exceeds its byte limit")]
    InputLimit,
    #[error("invalid worker request: {0}")]
    Request(#[from] serde_json::Error),
    #[error("worker input I/O failed: {0}")]
    Input(#[from] std::io::Error),
    #[error("file operation failed: {0}")]
    Operation(#[from] kolyan_tools::FileOperationError),
}

impl WorkerConfig {
    /// Validate before consuming stdin or touching workspace files.
    pub fn validate(&self) -> Result<(), WorkerError> {
        if !self.workspace.is_absolute()
            || [
                self.max_input_bytes,
                self.file_limits.max_read_bytes,
                self.file_limits.max_write_bytes,
            ]
            .iter()
            .any(|limit| *limit == 0 || *limit > MAX_PROTOCOL_BYTES)
        {
            return Err(WorkerError::Configuration);
        }
        Ok(())
    }
}

/// Execute exactly one strict typed request. Oversized input and unknown fields
/// fail before any operation. Callers launch this inside the admitted sandbox.
pub fn execute_request(
    config: &WorkerConfig,
    input: impl Read,
) -> Result<FileOperationResult, WorkerError> {
    config.validate()?;
    let mut bytes = Vec::new();
    input
        .take(config.max_input_bytes as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > config.max_input_bytes {
        return Err(WorkerError::InputLimit);
    }
    let operation: FileOperation = serde_json::from_slice(&bytes)?;
    Ok(FileOperations::new(&config.workspace, config.file_limits).execute(&operation)?)
}

#[cfg(test)]
mod tests;
