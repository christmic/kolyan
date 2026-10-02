//! Check zero-exit worker output without reopening or retrying the target.
//! An invalid replacement receipt cannot establish whether rename committed.

use sha2::{Digest, Sha256};

use super::{FileOperation, FileOperationLimits, FileOperationResult, IsolatedFileError};

pub(super) fn decode(
    operation: &FileOperation,
    stdout: &[u8],
    limits: FileOperationLimits,
) -> Result<FileOperationResult, IsolatedFileError> {
    let invalid = |reason: String| match operation {
        FileOperation::Read(_) => IsolatedFileError::Invalid(reason),
        FileOperation::Write(_) | FileOperation::Edit(_) => IsolatedFileError::Uncertain(reason),
    };
    let result: FileOperationResult = serde_json::from_slice(stdout)
        .map_err(|error| invalid(format!("worker result decoding: {error}")))?;
    if result.path != operation.path() {
        return Err(invalid("worker result path mismatch".into()));
    }
    if result.sha256.len() != 64
        || !result
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            "worker result digest is not canonical SHA-256".into(),
        ));
    }
    match operation {
        FileOperation::Read(_) => {
            let content = result
                .content
                .as_ref()
                .ok_or_else(|| invalid("read result has no content".into()))?;
            if content.len() > limits.max_read_bytes || !matches_content(&result, content) {
                return Err(invalid(
                    "read result content, digest or byte count mismatch".into(),
                ));
            }
        }
        FileOperation::Write(arguments) => {
            if result.content.is_some()
                || result.bytes > limits.max_write_bytes
                || !matches_content(&result, &arguments.content)
            {
                return Err(invalid(
                    "write result content, digest or byte count mismatch".into(),
                ));
            }
        }
        FileOperation::Edit(_) => {
            // new_text is only the replacement segment, not the resulting file.
            if result.content.is_some() || result.bytes > limits.max_write_bytes {
                return Err(invalid("edit result shape or byte ceiling mismatch".into()));
            }
        }
    }
    Ok(result)
}

fn matches_content(result: &FileOperationResult, content: &str) -> bool {
    result.bytes == content.len()
        && result.sha256 == format!("{:x}", Sha256::digest(content.as_bytes()))
}

#[cfg(test)]
mod tests;
