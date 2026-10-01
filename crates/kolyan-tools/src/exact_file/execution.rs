//! Execute through pinned parents; rename is a commit point, not a CAS.

use std::io::{Read, Write};
use std::path::{Component, Path};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::{Dir, OpenOptions};
use sha2::{Digest, Sha256};

use crate::file_operations::{
    FileOperation, FileOperationError, FileOperationLimits, FileOperationResult,
};

use super::binding::{invalid, io_error, leaf_identity};
use super::{ExactFileIdentity, ExactFileWorkerRequest};

/// Validate a strict request and execute using only its bound physical resource.
/// The model path is a display label. Staging is mandatory for replacements and
/// prohibited for reads. Errors before rename leave the target unchanged; this
/// does not promise directory power-loss durability or concurrent-writer CAS.
pub fn execute_exact(
    request: &ExactFileWorkerRequest,
    limits: FileOperationLimits,
) -> Result<FileOperationResult, FileOperationError> {
    validate_operation(&request.operation, limits)?;
    let read = matches!(request.operation, FileOperation::Read(_));
    if read != request.staging.is_none() {
        return Err(invalid(
            "reads prohibit staging; replacements require staging",
        ));
    }
    let opened = request.binding.open()?;
    let stage = request
        .staging
        .as_ref()
        .map(|stage| stage.open(&request.binding, &opened.parent))
        .transpose()?;
    let content = match &request.operation {
        FileOperation::Read(_) => read_content(&opened.parent, request, limits)?,
        FileOperation::Write(arguments) => arguments.content.clone(),
        FileOperation::Edit(arguments) => {
            let original = read_content(&opened.parent, request, limits)?;
            if let Some(expected) = &arguments.expected_sha256
                && !digest(&original).eq_ignore_ascii_case(expected)
            {
                return Err(FileOperationError::StaleContent);
            }
            let mut matches = original
                .char_indices()
                .filter(|(index, _)| original[*index..].starts_with(&arguments.old_text));
            let index = matches.next().ok_or(FileOperationError::MissingMatch)?.0;
            if matches.next().is_some() {
                return Err(FileOperationError::AmbiguousMatch);
            }
            let mut content = original;
            content.replace_range(index..index + arguments.old_text.len(), &arguments.new_text);
            if content.len() > limits.max_write_bytes {
                return Err(FileOperationError::WriteLimitExceeded);
            }
            content
        }
    };
    if let (Some(directory), Some(binding)) = (stage, &request.staging) {
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .follow(FollowSymlinks::No);
        let mut file = directory
            .open_with(&binding.leaf, &options)
            .map_err(io_error)?;
        let result = (|| {
            file.write_all(content.as_bytes()).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            // This check detects changes already present, not changes racing the
            // rename. Rename never dereferences the final destination symlink.
            if leaf_identity(&opened.parent, &request.binding.leaf)?
                != request.binding.target_identity
            {
                return Err(invalid("target changed before commit"));
            }
            directory
                .rename(&binding.leaf, &opened.parent, &request.binding.leaf)
                .map_err(io_error)
        })();
        if let Err(error) = result {
            // Only remove the exact stage we created, never another leaf or subtree.
            directory.remove_file(&binding.leaf).map_err(io_error)?;
            return Err(error);
        }
    }
    Ok(FileOperationResult {
        path: request.operation.path().into(),
        bytes: content.len(),
        sha256: digest(&content),
        content: read.then_some(content),
    })
}

pub(crate) fn validate_operation(
    operation: &FileOperation,
    limits: FileOperationLimits,
) -> Result<(), FileOperationError> {
    if limits.max_read_bytes == 0
        || limits.max_write_bytes == 0
        || limits.max_read_bytes > 64 * 1024 * 1024
        || limits.max_write_bytes > 64 * 1024 * 1024
    {
        return Err(invalid("invalid host file limits"));
    }
    let label = operation.path();
    if label.is_empty()
        || label.contains('\0')
        || Path::new(label).is_absolute()
        || Path::new(label).file_name().is_none()
        || Path::new(label).components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::Prefix(_) | Component::RootDir
            )
        })
    {
        return Err(invalid("display path must be workspace-relative"));
    }
    match operation {
        FileOperation::Write(arguments) if arguments.content.len() > limits.max_write_bytes => {
            Err(FileOperationError::WriteLimitExceeded)
        }
        FileOperation::Edit(arguments) => {
            if arguments.old_text.is_empty() {
                return Err(invalid("old_text must not be empty"));
            }
            if arguments.old_text.len() > limits.max_read_bytes {
                return Err(FileOperationError::ReadLimitExceeded);
            }
            if arguments.new_text.len() > limits.max_write_bytes {
                return Err(FileOperationError::WriteLimitExceeded);
            }
            if let Some(digest) = &arguments.expected_sha256
                && (digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
            {
                return Err(invalid(
                    "expected_sha256 must contain 64 hexadecimal characters",
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn read_content(
    directory: &Dir,
    request: &ExactFileWorkerRequest,
    limits: FileOperationLimits,
) -> Result<String, FileOperationError> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = directory
        .open_with(&request.binding.leaf, &options)
        .map_err(io_error)?;
    let metadata = file.metadata().map_err(io_error)?;
    if !metadata.is_file()
        || Some(ExactFileIdentity::of(&metadata)) != request.binding.target_identity
    {
        return Err(invalid(
            "opened target does not match prepared regular file identity",
        ));
    }
    let mut bytes = Vec::new();
    file.take(limits.max_read_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > limits.max_read_bytes {
        return Err(FileOperationError::ReadLimitExceeded);
    }
    String::from_utf8(bytes).map_err(|_| FileOperationError::InvalidUtf8)
}

fn digest(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}
