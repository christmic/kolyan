//! Size-first bounded JSON hashing, without constructing a serialized Vec.
use crate::ProviderError;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::{self, Write};

/// Independent compact UTF-8 JSON ceilings for neutral, generation and count input.
/// Includes escaping and all fields, excludes HTTP headers; equality is admitted.
pub const MAX_CONTEXT_JSON_BYTES: u64 = 16 * 1024 * 1024;

struct BudgetWriter {
    used: u64,
    exceeded: bool,
    digest: Sha256,
}
impl BudgetWriter {
    fn new() -> Self {
        Self {
            used: 0,
            exceeded: false,
            digest: Sha256::new(),
        }
    }
}
impl Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = bytes.len() as u64;
        if length > MAX_CONTEXT_JSON_BYTES - self.used {
            self.exceeded = true;
            return Err(io::Error::other("accounting JSON budget exceeded"));
        }
        self.digest.update(bytes);
        self.used += length;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub fn context_json_bytes(value: &impl Serialize) -> Result<u64, ProviderError> {
    Ok(measure_digest(value, "context JSON")?.0)
}
pub(super) fn measure_digest(
    value: &impl Serialize,
    label: &str,
) -> Result<(u64, String), ProviderError> {
    let mut writer = BudgetWriter::new();
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(super::accounting_error(&if writer.exceeded {
            format!("{label} exceeds 16 MiB serialized JSON ceiling")
        } else {
            format!("{label} serialization failed")
        }));
    }
    Ok((writer.used, format!("{:x}", writer.digest.finalize())))
}
pub(super) fn digest_bounded(value: &impl Serialize, label: &str) -> Result<String, ProviderError> {
    // Preserve established Value-based digest ordering, but bound the source first.
    measure_digest(value, label)?;
    let canonical = serde_json::to_value(value)
        .map_err(|_| super::accounting_error("accounting serialization failed"))?;
    Ok(measure_digest(&canonical, label)?.1)
}
