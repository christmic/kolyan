//! Nofollow traversal and validation of host-selected physical identities.

use std::path::{Component, Path};

use cap_fs_ext::{DirExt, MetadataExt};
use cap_std::fs::{Dir, Metadata};

use crate::file_operations::FileOperationError;

use super::{ExactDirectoryBinding, ExactFileBinding, ExactFileIdentity, ExactFileStaging};

pub(crate) struct OpenedBinding {
    // Keep the root capability alive for the entire operation.
    pub _workspace: Dir,
    pub parent: Dir,
    pub _staging: Vec<Dir>,
}

impl ExactFileIdentity {
    pub(super) fn of(metadata: &Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
}

impl ExactDirectoryBinding {
    pub(crate) fn capture(path: &Path) -> Result<(Self, Dir), FileOperationError> {
        let (directory, identity_chain) = traverse(path)?;
        Ok((
            Self {
                physical_path: path.into(),
                identity_chain,
            },
            directory,
        ))
    }

    pub(crate) fn open(&self) -> Result<Dir, FileOperationError> {
        let (directory, observed) = traverse(&self.physical_path)?;
        if observed != self.identity_chain {
            return Err(invalid("directory identity changed"));
        }
        Ok(directory)
    }
}

impl ExactFileBinding {
    /// Capture only an already resolved physical target. All directories must
    /// exist and every component must be nonsymlink. No file effects occur.
    pub fn prepare(
        workspace: &Path,
        physical_target: &Path,
        protected_roots: &[std::path::PathBuf],
    ) -> Result<Self, FileOperationError> {
        physical(workspace)?;
        physical(physical_target)?;
        let parent_path = physical_target
            .parent()
            .ok_or_else(|| invalid("target has no parent"))?;
        let leaf = physical_target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("target leaf must be UTF-8"))?;
        let (workspace, root) = ExactDirectoryBinding::capture(workspace)?;
        let (parent, directory) = ExactDirectoryBinding::capture(parent_path)?;
        let binding = Self {
            workspace,
            parent,
            leaf: leaf.into(),
            target_identity: leaf_identity(&directory, leaf)?,
            protected_roots: protected_roots.to_vec(),
        };
        binding.validate()?;
        // Independently traversed chains must agree on the selected root.
        if ExactFileIdentity::of(&root.dir_metadata().map_err(io_error)?)
            != binding.parent.identity_chain[binding.workspace.identity_chain.len() - 1]
        {
            return Err(invalid("workspace changed during preparation"));
        }
        Ok(binding)
    }

    pub(super) fn validate(&self) -> Result<(), FileOperationError> {
        physical(&self.workspace.physical_path)?;
        physical(&self.parent.physical_path)?;
        leaf(&self.leaf)?;
        if self.workspace.physical_path.parent().is_none()
            || !self
                .parent
                .physical_path
                .starts_with(&self.workspace.physical_path)
            || self.workspace.identity_chain.is_empty()
            || !self
                .parent
                .identity_chain
                .starts_with(&self.workspace.identity_chain)
        {
            return Err(invalid(
                "target parent must retain the admitted workspace identity",
            ));
        }
        let target = self.parent.physical_path.join(&self.leaf);
        if control(&target) || control(&self.workspace.physical_path) {
            return Err(invalid("control resources are protected"));
        }
        for protected in &self.protected_roots {
            physical(protected)?;
            if target.starts_with(protected) || self.workspace.physical_path.starts_with(protected)
            {
                return Err(invalid("target overlaps protected resources"));
            }
        }
        Ok(())
    }

    pub(crate) fn open(&self) -> Result<OpenedBinding, FileOperationError> {
        self.validate()?;
        let workspace = self.workspace.open()?;
        let parent = self.parent.open()?;
        if leaf_identity(&parent, &self.leaf)? != self.target_identity {
            return Err(invalid("target identity changed"));
        }
        Ok(OpenedBinding {
            _workspace: workspace,
            parent,
            _staging: Vec::new(),
        })
    }
}

impl ExactFileStaging {
    /// Capture an absent stage leaf outside the workspace. The host owns its
    /// private parent; model arguments may never choose this path.
    pub fn prepare(
        binding: &ExactFileBinding,
        physical_stage: &Path,
    ) -> Result<Self, FileOperationError> {
        physical(physical_stage)?;
        let parent_path = physical_stage
            .parent()
            .ok_or_else(|| invalid("stage has no parent"))?;
        let name = physical_stage
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("stage leaf must be UTF-8"))?;
        let (parent, _) = ExactDirectoryBinding::capture(parent_path)?;
        let stage = Self {
            parent,
            leaf: name.into(),
        };
        let opened = binding.open()?;
        stage.open(binding, &opened.parent)?;
        Ok(stage)
    }

    pub(super) fn open(
        &self,
        binding: &ExactFileBinding,
        target_parent: &Dir,
    ) -> Result<Dir, FileOperationError> {
        leaf(&self.leaf)?;
        let path = self.parent.physical_path.join(&self.leaf);
        if self.parent.physical_path.starts_with(&binding.workspace.physical_path)
            || control(&path)
            // The host-private stage directory is an immediate child of the
            // configured protected staging root. Only its exact leaf is admitted;
            // no model target or directory-wide grant receives this exception.
            || binding.protected_roots.iter().any(|root| path.starts_with(root)
                && self.parent.physical_path.parent() != Some(root.as_path()))
        {
            return Err(invalid(
                "stage must be outside workspace and protected resources",
            ));
        }
        let directory = self.parent.open()?;
        if directory.dir_metadata().map_err(io_error)?.dev()
            != target_parent.dir_metadata().map_err(io_error)?.dev()
        {
            return Err(invalid("stage and target must share a filesystem"));
        }
        #[cfg(unix)]
        {
            use cap_std::fs::PermissionsExt;
            if directory
                .dir_metadata()
                .map_err(io_error)?
                .permissions()
                .mode()
                & 0o077
                != 0
            {
                return Err(invalid("stage parent must be host-private"));
            }
        }
        if leaf_identity(&directory, &self.leaf)?.is_some() {
            return Err(invalid("stage leaf must be absent"));
        }
        Ok(directory)
    }
}

pub(super) fn leaf_identity(
    directory: &Dir,
    name: &str,
) -> Result<Option<ExactFileIdentity>, FileOperationError> {
    match directory.symlink_metadata(name) {
        Ok(metadata) if metadata.is_file() => Ok(Some(ExactFileIdentity::of(&metadata))),
        Ok(_) => Err(invalid(
            "leaf must be a regular file, not a symlink or special file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}

fn traverse(path: &Path) -> Result<(Dir, Vec<ExactFileIdentity>), FileOperationError> {
    physical(path)?;
    let mut directory =
        Dir::open_ambient_dir("/", cap_std::ambient_authority()).map_err(io_error)?;
    let mut identities = vec![ExactFileIdentity::of(
        &directory.dir_metadata().map_err(io_error)?,
    )];
    for component in path.components().skip(1) {
        directory = directory
            .open_dir_nofollow(component.as_os_str())
            .map_err(io_error)?;
        identities.push(ExactFileIdentity::of(
            &directory.dir_metadata().map_err(io_error)?,
        ));
    }
    Ok((directory, identities))
}

fn physical(path: &Path) -> Result<(), FileOperationError> {
    if !path.is_absolute() || path.components().count() > 128 {
        return Err(invalid("physical path must be absolute and bounded"));
    }
    let mut normalized = std::path::PathBuf::from("/");
    for component in path.components().skip(1) {
        match component {
            Component::Normal(name) => normalized.push(name),
            _ => {
                return Err(invalid(
                    "physical path must not contain dot or parent components",
                ));
            }
        }
    }
    if normalized.as_os_str() != path.as_os_str()
        || path.as_os_str().as_encoded_bytes().contains(&0)
    {
        return Err(invalid(
            "physical path must be normalized and contain no NUL",
        ));
    }
    Ok(())
}

fn leaf(name: &str) -> Result<(), FileOperationError> {
    let mut components = Path::new(name).components();
    if name.is_empty()
        || name.contains(['/', '\0'])
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(invalid("leaf must be one normal filename"));
    }
    Ok(())
}

fn control(path: &Path) -> bool {
    path.components()
        .any(|part| matches!(part, Component::Normal(name) if name == ".git" || name == ".kolyan"))
}

pub(super) fn invalid(message: &str) -> FileOperationError {
    FileOperationError::InvalidArguments(message.into())
}

pub(super) fn io_error(error: std::io::Error) -> FileOperationError {
    FileOperationError::Io(error.to_string())
}
