//! Local immutable content addressing. The host owns directory access control.
//! This store rejects symlink objects, but is not a sandbox against a hostile
//! process with write access to the same directory. Required pins survive reopen.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod range;
pub use range::{ArtifactRange, ArtifactRangeLimits};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    Required,
    Optional,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub digest: String,
    pub byte_length: u64,
    pub retention: Retention,
}

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("invalid artifact reference or bound: {0}")]
    Invalid(String),
    #[error("artifact integrity mismatch")]
    Integrity,
    #[error("required artifact cannot be removed")]
    Required,
    #[error("artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

pub struct ArtifactStore {
    root: PathBuf,
    max_bytes: u64,
}

impl ArtifactStore {
    /// Explicit local root; no global paths or invented user permissions.
    pub fn new(root: impl AsRef<Path>, max_bytes: u64) -> Result<Self, ArtifactError> {
        if max_bytes == 0 || max_bytes == u64::MAX {
            return Err(ArtifactError::Invalid("invalid maximum size".into()));
        }
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        Ok(Self { root, max_bytes })
    }

    /// Atomically publish synced bytes. A required pin is durable before
    /// publication; an interrupted write may leave a conservative pin, never
    /// an unprotected required object. Identical writes reuse verified content.
    pub fn put(&self, bytes: &[u8], retention: Retention) -> Result<ArtifactRef, ArtifactError> {
        let _lock = self.lock()?;
        let reference = ArtifactRef {
            digest: digest(bytes),
            byte_length: bytes.len() as u64,
            retention,
        };
        let path = self.path(&reference)?;
        if retention == Retention::Required {
            self.publish(&self.pin_path(&reference), b"required")?;
        }
        self.publish(&path, bytes)?;
        self.read(&reference, self.max_bytes)?;
        Ok(reference)
    }

    /// Size-check before allocation, then hash-check before returning content.
    pub fn read(&self, reference: &ArtifactRef, max_bytes: u64) -> Result<Vec<u8>, ArtifactError> {
        let path = self.path(reference)?;
        if reference.byte_length > max_bytes {
            return Err(ArtifactError::Invalid("read bound exceeded".into()));
        }
        check_regular(&path)?;
        let file = File::open(path)?;
        if file.metadata()?.len() != reference.byte_length {
            return Err(ArtifactError::Integrity);
        }
        let mut bytes = Vec::new();
        file.take(reference.byte_length + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != reference.byte_length || digest(&bytes) != reference.digest {
            return Err(ArtifactError::Integrity);
        }
        Ok(bytes)
    }

    /// Required references and durable pins cannot be removed, even when a
    /// caller changes the reference's retention field. Missing optional content
    /// remains an explicit I/O error rather than a successful empty read.
    pub fn remove(&self, reference: &ArtifactRef) -> Result<(), ArtifactError> {
        let _lock = self.lock()?;
        let path = self.path(reference)?;
        if reference.retention == Retention::Required {
            return Err(ArtifactError::Required);
        }
        match fs::symlink_metadata(self.pin_path(reference)) {
            Ok(_) => return Err(ArtifactError::Required),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.read(reference, self.max_bytes)?;
        fs::remove_file(path)?;
        self.sync_root()
    }

    fn path(&self, reference: &ArtifactRef) -> Result<PathBuf, ArtifactError> {
        if reference.digest.len() != 64
            || !reference
                .digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || reference.byte_length > self.max_bytes
        {
            return Err(ArtifactError::Invalid("digest or size invalid".into()));
        }
        Ok(self.root.join(&reference.digest))
    }

    fn pin_path(&self, reference: &ArtifactRef) -> PathBuf {
        self.root.join(format!("{}.required", reference.digest))
    }

    fn publish(&self, path: &Path, bytes: &[u8]) -> Result<(), ArtifactError> {
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root)?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_all()?;
        match temporary.persist_noclobber(path) {
            Ok(_) => self.sync_root(),
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                check_regular(path)?;
                let file = File::open(path)?;
                if file.metadata()?.len() != bytes.len() as u64 {
                    return Err(ArtifactError::Integrity);
                }
                let mut existing = Vec::new();
                file.take(bytes.len() as u64 + 1)
                    .read_to_end(&mut existing)?;
                if existing != bytes {
                    return Err(ArtifactError::Integrity);
                }
                self.sync_root()
            }
            Err(error) => Err(error.error.into()),
        }
    }

    fn sync_root(&self) -> Result<(), ArtifactError> {
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    // Serialize retention promotion and deletion across local store instances.
    fn lock(&self) -> Result<File, ArtifactError> {
        let path = self.root.join(".lock");
        if path.symlink_metadata().is_ok() {
            check_regular(&path)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.lock()?;
        Ok(file)
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn check_regular(path: &Path) -> Result<(), ArtifactError> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(ArtifactError::Invalid(
            "artifact is not a regular file".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
