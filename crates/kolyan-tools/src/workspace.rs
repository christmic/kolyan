//! Hold directory authority once; never re-open paths using ambient filesystem APIs.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::fs::Dir;

#[derive(Debug, Clone)]
pub(super) struct Workspace {
    pub(super) path: PathBuf,
    directory: Result<Arc<Dir>, String>,
}

impl Workspace {
    pub(super) fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        // Authority is selected by the host, never by model arguments.
        let directory = Dir::open_ambient_dir(&path, cap_std::ambient_authority())
            .map(Arc::new)
            .map_err(|error| error.to_string());
        Self { path, directory }
    }

    pub(super) fn directory(&self) -> Result<&Dir, String> {
        self.directory.as_deref().map_err(Clone::clone)
    }

    pub(super) fn relative(relative: &str) -> Result<&Path, String> {
        let path = Path::new(relative);
        if path.is_absolute()
            || path.components().any(|part| {
                matches!(
                    part,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err("path must stay below the configured tool root".into());
        }
        Ok(path)
    }
}
