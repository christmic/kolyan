//! Validated, bounded file operations under a pinned workspace directory capability.
//!
//! This synchronous adapter does not authorize calls or supply process isolation.
//! Workers must run it inside their granted sandbox; asynchronous hosts must not
//! execute its blocking I/O on an executor thread. Atomic replacement does not
//! provide compare-and-swap against uncooperative external writers.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use cap_std::fs::{Dir, OpenOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::workspace::Workspace;

const DEFAULT_LIMIT: usize = 1024 * 1024;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Strict wire arguments; deserialization rejects unknown fields and tools.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "name", content = "arguments", deny_unknown_fields)]
pub enum FileOperation {
    #[serde(rename = "file.read")]
    Read(ReadArguments),
    #[serde(rename = "file.write")]
    Write(WriteArguments),
    #[serde(rename = "file.edit")]
    Edit(EditArguments),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReadArguments {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WriteArguments {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EditArguments {
    pub path: String,
    pub old_text: String,
    pub new_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_sha256: Option<String>,
}

/// Host limits, never model-selected; byte counts include UTF-8 encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileOperationLimits {
    pub max_read_bytes: usize,
    pub max_write_bytes: usize,
}

impl Default for FileOperationLimits {
    fn default() -> Self {
        Self {
            max_read_bytes: DEFAULT_LIMIT,
            max_write_bytes: DEFAULT_LIMIT,
        }
    }
}

/// A successful observation or atomic replacement, with the resulting digest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileOperationResult {
    pub path: String,
    pub bytes: usize,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FileOperationError {
    InvalidArguments(String),
    Io(String),
    ReadLimitExceeded,
    WriteLimitExceeded,
    InvalidUtf8,
    MissingMatch,
    AmbiguousMatch,
    StaleContent,
}

impl std::fmt::Display for FileOperationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for FileOperationError {}

/// Reuses the existing pinned directory; never reopens the root by ambient path.
#[derive(Debug, Clone)]
pub struct FileOperations {
    workspace: Workspace,
    limits: FileOperationLimits,
}

impl FileOperations {
    pub fn new(root: impl Into<PathBuf>, limits: FileOperationLimits) -> Self {
        Self {
            workspace: Workspace::new(root),
            limits,
        }
    }

    /// Decode tool arguments and preflight their shape and host byte limits
    /// without performing filesystem I/O or acquiring execution authority.
    pub fn parse(
        &self,
        name: &str,
        arguments: &serde_json::Value,
    ) -> Result<FileOperation, FileOperationError> {
        let operation = serde_json::from_value(serde_json::json!({
            "name": name,
            "arguments": arguments,
        }))
        .map_err(|error| FileOperationError::InvalidArguments(error.to_string()))?;
        self.validate(&operation)?;
        Ok(operation)
    }

    /// Validate arguments without file effects. Actual directory access remains
    /// capability-relative and is checked again by the execution operation.
    pub fn validate(&self, operation: &FileOperation) -> Result<(), FileOperationError> {
        relative_file(operation.path())?;
        match operation {
            FileOperation::Write(arguments) => self.check_write(&arguments.content),
            FileOperation::Edit(arguments) => {
                if arguments.old_text.is_empty() {
                    return Err(invalid("old_text must not be empty"));
                }
                if arguments.old_text.len() > self.limits.max_read_bytes {
                    return Err(FileOperationError::ReadLimitExceeded);
                }
                self.check_write(&arguments.new_text)?;
                if let Some(digest) = &arguments.expected_sha256
                    && (digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
                {
                    return Err(invalid(
                        "expected_sha256 must contain 64 hexadecimal characters",
                    ));
                }
                Ok(())
            }
            FileOperation::Read(_) => Ok(()),
        }
    }

    /// Run a validated operation. Failed edits and pre-rename write failures leave
    /// the destination unchanged. Successful rename is the commit point; directory
    /// durability across power loss is not promised by this adapter.
    pub fn execute(
        &self,
        operation: &FileOperation,
    ) -> Result<FileOperationResult, FileOperationError> {
        self.validate(operation)?;
        let path = relative_file(operation.path())?;
        let directory = self.workspace.directory().map_err(FileOperationError::Io)?;
        let (content, read) = match operation {
            FileOperation::Read(_) => (self.read(directory, path)?, true),
            FileOperation::Write(arguments) => {
                atomic_replace(directory, path, &arguments.content)?;
                (arguments.content.clone(), false)
            }
            FileOperation::Edit(arguments) => {
                let original = self.read(directory, path)?;
                if let Some(expected) = &arguments.expected_sha256
                    && !digest(&original).eq_ignore_ascii_case(expected)
                {
                    return Err(FileOperationError::StaleContent);
                }
                // Include overlapping matches; replacing an arbitrarily selected
                // occurrence would hide ambiguity (for example "aa" in "aaa").
                let mut matches = original
                    .char_indices()
                    .filter(|(index, _)| original[*index..].starts_with(&arguments.old_text));
                let index = matches.next().ok_or(FileOperationError::MissingMatch)?.0;
                if matches.next().is_some() {
                    return Err(FileOperationError::AmbiguousMatch);
                }
                let mut replaced = original;
                replaced
                    .replace_range(index..index + arguments.old_text.len(), &arguments.new_text);
                self.check_write(&replaced)?;
                atomic_replace(directory, path, &replaced)?;
                (replaced, false)
            }
        };
        Ok(FileOperationResult {
            path: operation.path().into(),
            bytes: content.len(),
            sha256: digest(&content),
            content: read.then_some(content),
        })
    }

    fn read(&self, directory: &Dir, path: &Path) -> Result<String, FileOperationError> {
        let file = directory.open(path).map_err(io_error)?;
        if !file.metadata().map_err(io_error)?.is_file() {
            return Err(invalid("path must name a regular file"));
        }
        let limit = u64::try_from(self.limits.max_read_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut bytes = Vec::new();
        file.take(limit).read_to_end(&mut bytes).map_err(io_error)?;
        if bytes.len() > self.limits.max_read_bytes {
            return Err(FileOperationError::ReadLimitExceeded);
        }
        String::from_utf8(bytes).map_err(|_| FileOperationError::InvalidUtf8)
    }

    fn check_write(&self, content: &str) -> Result<(), FileOperationError> {
        if content.len() > self.limits.max_write_bytes {
            Err(FileOperationError::WriteLimitExceeded)
        } else {
            Ok(())
        }
    }
}

impl FileOperation {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Read(_) => "file.read",
            Self::Write(_) => "file.write",
            Self::Edit(_) => "file.edit",
        }
    }

    pub fn path(&self) -> &str {
        match self {
            Self::Read(arguments) => &arguments.path,
            Self::Write(arguments) => &arguments.path,
            Self::Edit(arguments) => &arguments.path,
        }
    }
}

fn relative_file(relative: &str) -> Result<&Path, FileOperationError> {
    let path = Workspace::relative(relative).map_err(FileOperationError::InvalidArguments)?;
    if relative.is_empty() || relative.as_bytes().contains(&0) || path.file_name().is_none() {
        return Err(invalid("path must name a workspace-relative file"));
    }
    Ok(path)
}

fn atomic_replace(directory: &Dir, path: &Path, content: &str) -> Result<(), FileOperationError> {
    let parent = directory
        .open_dir(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )
        .map_err(io_error)?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid("missing file name"))?;
    match parent.symlink_metadata(name) {
        Ok(metadata) if !metadata.is_file() => {
            return Err(invalid(
                "replacement target must be a regular file, not a symlink",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    for _ in 0..32 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = format!(".kolyan-write-{}-{sequence}", std::process::id());
        let mut file = match parent.open_with(&temporary, &options) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(io_error(error)),
        };
        let result = (|| {
            file.write_all(content.as_bytes()).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            parent.rename(&temporary, &parent, name).map_err(io_error)
        })();
        if result.is_err() {
            parent.remove_file(&temporary).map_err(io_error)?;
        }
        return result;
    }
    Err(invalid("cannot allocate atomic replacement file"))
}

fn digest(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}
fn invalid(message: &str) -> FileOperationError {
    FileOperationError::InvalidArguments(message.into())
}
fn io_error(error: std::io::Error) -> FileOperationError {
    FileOperationError::Io(error.to_string())
}

#[cfg(test)]
mod tests;
