//! Strict host-declared physical target, excluding the replaced leaf inode.
use std::path::{Component, Path, PathBuf};

use kolyan_server::TaskError;
use kolyan_tools::ExactDirectoryBinding;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileWriteCommittedPredicateV1 {
    pub schema_version: u32,
    pub tool_revision: String,
    pub workspace: ExactDirectoryBinding,
    pub parent: ExactDirectoryBinding,
    pub leaf: String,
    pub expected_bytes: u64,
    pub expected_sha256: String,
}

impl FileWriteCommittedPredicateV1 {
    pub(super) fn validate(&self) -> Result<(), TaskError> {
        if self.schema_version != 1 || !sha256(&self.expected_sha256) {
            return Err(bad("unsupported file goal schema or invalid SHA-256"));
        }
        revision(&self.tool_revision)?;
        directories(&self.workspace, &self.parent)?;
        leaf(&self.leaf)
    }
}

pub(super) fn revision(value: &str) -> Result<(), TaskError> {
    if value.trim().is_empty() || value.len() > 1024 {
        return Err(bad("file adapter revision must be 1..1024 bytes"));
    }
    Ok(())
}

pub(super) fn directories(
    workspace: &ExactDirectoryBinding,
    parent: &ExactDirectoryBinding,
) -> Result<(), TaskError> {
    for directory in [workspace, parent] {
        physical(&directory.physical_path)?;
        if directory.identity_chain.len() != directory.physical_path.components().count() {
            return Err(bad("physical directory identity chain length differs"));
        }
    }
    if workspace.physical_path.parent().is_none()
        || !parent.physical_path.starts_with(&workspace.physical_path)
        || !parent.identity_chain.starts_with(&workspace.identity_chain)
    {
        return Err(bad(
            "parent does not retain workspace path and identity prefix",
        ));
    }
    Ok(())
}

pub(super) fn physical(path: &Path) -> Result<(), TaskError> {
    let text = path
        .to_str()
        .ok_or_else(|| bad("physical path is not UTF-8"))?;
    let components: Vec<_> = path.components().collect();
    if text.contains('\0')
        || !path.is_absolute()
        || components.len() > 128
        || components
            .iter()
            .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
        || components.iter().collect::<PathBuf>().as_os_str() != path.as_os_str()
    {
        return Err(bad(
            "physical path is not a bounded normalized absolute path",
        ));
    }
    Ok(())
}

pub(super) fn leaf(value: &str) -> Result<(), TaskError> {
    let mut components = Path::new(value).components();
    if value.contains('\0')
        || value.is_empty()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
        || value.contains('/')
    {
        return Err(bad("leaf must be one ordinary UTF-8 filename"));
    }
    Ok(())
}

pub(super) fn sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(super) fn bad(message: &str) -> TaskError {
    TaskError::Invalid(message.into())
}
