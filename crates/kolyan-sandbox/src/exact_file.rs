//! Exact physical file admission and literal-only resource rendering.
//! Path checks reject already redirected resources; they do not close TOCTOU.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use crate::{SandboxCommand, SandboxError, SandboxRequest, profile::path_text};

/// Host-owned exact resources, never derived from untrusted policy text.
/// Workspace is only a directory metadata/CWD anchor, not a content grant.
/// File paths must be absolute canonical parent + leaf; final symlinks are denied.
/// Missing leaves are allowed, but their parents must exist. Read and write lists
/// are independent. A trusted staging file may be outside the workspace.
#[derive(Clone, Debug)]
pub struct FileSandboxConfig {
    pub workspace: PathBuf,
    pub read_files: Vec<PathBuf>,
    pub write_files: Vec<PathBuf>,
    /// Physical protected paths, including nonexistent final control leaves.
    pub protected_roots: Vec<PathBuf>,
}

pub(crate) fn canonical_config(
    mut config: FileSandboxConfig,
) -> Result<FileSandboxConfig, SandboxError> {
    config.workspace = config.workspace.canonicalize()?;
    if !config.workspace.is_dir() || config.workspace.parent().is_none() {
        return Err(invalid("workspace must be an existing non-root directory"));
    }
    path_text(&config.workspace)?;
    if is_control(&config.workspace) {
        return Err(invalid("workspace is a control path"));
    }
    if config.read_files.is_empty() && config.write_files.is_empty() {
        return Err(invalid("at least one exact file is required"));
    }
    for root in &config.protected_roots {
        validate_physical(root, false)?;
    }
    for path in config.read_files.iter().chain(&config.write_files) {
        validate_physical(path, true)?;
        if is_control(path)
            || config
                .protected_roots
                .iter()
                .any(|root| path.starts_with(root))
        {
            return Err(invalid("exact file overlaps a protected control path"));
        }
    }
    if config
        .protected_roots
        .iter()
        .any(|root| config.workspace.starts_with(root))
    {
        return Err(invalid("workspace overlaps a protected control path"));
    }
    // Protect conventional control names at every directory we must open, including
    // staging ancestors. Canonical alias checks above reject redirected ancestors.
    for directory in directories(&config) {
        for name in [".git", ".kolyan"] {
            config.protected_roots.push(directory.join(name));
        }
    }
    config.protected_roots.sort();
    config.protected_roots.dedup();
    Ok(config)
}

pub(crate) fn admit(
    config: &FileSandboxConfig,
    request: &SandboxRequest,
) -> Result<(), SandboxError> {
    if matches!(request.command, SandboxCommand::Shell(_)) {
        return Err(invalid("exact-file mode requires a trusted executable"));
    }
    if request.cwd != config.workspace || config.workspace.canonicalize()? != config.workspace {
        return Err(invalid("cwd must match the admitted physical workspace"));
    }
    // Detect replacements already present at launch; no claim of atomic pinning.
    for path in config.read_files.iter().chain(&config.write_files) {
        validate_physical(path, true)?;
    }
    Ok(())
}

pub(crate) fn render(
    config: &FileSandboxConfig,
    profile: &mut String,
    parameters: &mut Vec<String>,
) -> Result<(), SandboxError> {
    for (index, directory) in directories(config).iter().enumerate() {
        parameters.push(format!("DIR{index}={}", path_text(directory)?));
        // macOS directory open requires read-data. Literal allows directory names
        // to be listed, never content reads of child files. No directory write grant.
        profile.push_str(&format!(
            "(allow file-read-metadata file-read-data (literal (param \"DIR{index}\")))\n"
        ));
    }
    for (prefix, operation, files) in [
        (
            "READ",
            "file-read-data file-read-metadata",
            &config.read_files,
        ),
        (
            "WRITE",
            "file-write* file-read-metadata",
            &config.write_files,
        ),
    ] {
        for (index, file) in files.iter().enumerate() {
            parameters.push(format!("{prefix}{index}={}", path_text(file)?));
            profile.push_str(&format!(
                "(allow {operation} (literal (param \"{prefix}{index}\")))\n"
            ));
        }
    }
    Ok(())
}

fn directories(config: &FileSandboxConfig) -> BTreeSet<PathBuf> {
    config
        .workspace
        .ancestors()
        .chain(
            config
                .read_files
                .iter()
                .chain(&config.write_files)
                .flat_map(|file| file.parent().into_iter().flat_map(Path::ancestors)),
        )
        .map(Path::to_path_buf)
        .collect()
}

fn validate_physical(path: &Path, file_only: bool) -> Result<(), SandboxError> {
    path_text(path)?;
    if !path.is_absolute()
        || path.file_name().is_none()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(invalid(
            "exact paths must be absolute non-root physical paths",
        ));
    }
    let parent = path.parent().ok_or_else(|| invalid("missing parent"))?;
    let leaf = path.file_name().ok_or_else(|| invalid("missing leaf"))?;
    if parent.canonicalize()?.as_os_str() != parent.as_os_str()
        || parent.join(leaf).as_os_str() != path.as_os_str()
    {
        return Err(invalid(
            "exact path parent differs from its canonical identity",
        ));
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || (file_only && !metadata.is_file()) => {
            Err(invalid(
                "exact path must not redirect or name a directory/special file",
            ))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn is_control(path: &Path) -> bool {
    path.components().any(|component| {
        matches!(component, Component::Normal(name) if name == ".git" || name == ".kolyan")
    })
}

fn invalid(message: &str) -> SandboxError {
    SandboxError::Invalid(message.into())
}

#[cfg(test)]
mod tests;
