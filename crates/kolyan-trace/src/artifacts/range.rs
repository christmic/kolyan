//! Blocking, whole-object integrity verification followed by a raw byte slice.
//! Content identity is not read authority; the host must authorize independently.

use serde::{Deserialize, Serialize};

use super::{ArtifactError, ArtifactRef, ArtifactStore};

/// Independent whole-object inspection and returned raw-byte ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRangeLimits {
    pub max_verified_bytes: u64,
    pub max_returned_bytes: u64,
}

/// Content only, not an authorization token or an execution receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRange {
    pub offset: u64,
    pub total_bytes: u64,
    /// Complete object bytes verified, not only the returned slice length.
    pub verified_bytes: u64,
    pub bytes: Vec<u8>,
    pub eof: bool,
}

impl ArtifactStore {
    /// Verify the complete bounded object before returning exact raw bytes.
    ///
    /// This performs blocking file I/O. Async hosts must move it off their async
    /// worker and enforce current authority, cancellation and result ceilings.
    /// `limits.max_verified_bytes` bounds the WHOLE object before allocation, in addition
    /// to the store ceiling. Existing `read` verifies length and SHA-256, including
    /// bytes outside the requested slice. Its one-byte overrun detection remains.
    /// Verification errors return no partial content. Verification and the copied
    /// slice retain at most twice the object bytes, plus the existing one-byte
    /// overrun probe; allocator capacity/overhead is not a content-byte budget.
    ///
    /// `length` must be nonzero; offset plus length must not overflow. An offset
    /// at EOF yields an empty slice, beyond EOF is invalid, and ranges extending
    /// past EOF yield the remaining bytes. UTF-8 boundaries have no significance.
    /// Requested length cannot exceed `limits.max_returned_bytes`, even if EOF
    /// would clip the result. Zero or unbounded ceilings are invalid. Required retention
    /// and existing trusted-directory assumptions are unchanged.
    pub fn read_range(
        &self,
        reference: &ArtifactRef,
        offset: u64,
        length: u64,
        limits: ArtifactRangeLimits,
    ) -> Result<ArtifactRange, ArtifactError> {
        if length == 0
            || limits.max_verified_bytes == 0
            || limits.max_verified_bytes == u64::MAX
            || limits.max_returned_bytes == 0
            || limits.max_returned_bytes == u64::MAX
        {
            return Err(ArtifactError::Invalid(
                "invalid range or verification bound".into(),
            ));
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| ArtifactError::Invalid("range addition overflow".into()))?;
        if length > limits.max_returned_bytes {
            return Err(ArtifactError::Invalid(
                "returned byte bound exceeded".into(),
            ));
        }
        if offset > reference.byte_length {
            return Err(ArtifactError::Invalid("range starts beyond EOF".into()));
        }
        let bytes = self.read(reference, limits.max_verified_bytes)?;
        let end = end.min(reference.byte_length);
        let start = usize::try_from(offset)
            .map_err(|_| ArtifactError::Invalid("range offset cannot fit address space".into()))?;
        let stop = usize::try_from(end)
            .map_err(|_| ArtifactError::Invalid("range end cannot fit address space".into()))?;
        Ok(ArtifactRange {
            offset,
            total_bytes: reference.byte_length,
            verified_bytes: reference.byte_length,
            bytes: bytes[start..stop].to_vec(),
            eof: end == reference.byte_length,
        })
    }
}

#[cfg(test)]
mod tests;
