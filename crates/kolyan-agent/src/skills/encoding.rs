//! Bounded canonical JSON encoding for stored metadata and identity hashing.

use std::io::{self, Write};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::SkillError;

struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("Skill encoding bound exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, SkillError> {
    let mut writer = BoundedBytes {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.bytes),
        Err(_) if writer.exceeded => Err(SkillError::Capacity),
        Err(_) => Err(SkillError::Invalid("JSON serialization failed".into())),
    }
}

pub(super) fn hash<T: Serialize>(value: &T) -> Result<String, SkillError> {
    Ok(format!("{:x}", Sha256::digest(encode(value, 128 * 1024)?)))
}

pub(super) fn id(value: &str) -> Result<(), SkillError> {
    crate::identity(value).map_err(|_| SkillError::Invalid("invalid identity".into()))
}

pub(super) fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
