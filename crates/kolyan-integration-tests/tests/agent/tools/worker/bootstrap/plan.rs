//! Distinct trusted native and data-script controls; no selectable fallback.

use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use serde::Serialize;

use super::installation::{self, InstalledWorker};

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(in super::super) enum LaunchPlan {
    NativeBootstrap {
        installation: InstalledWorker,
    },
    InterpreterFixture {
        case_id: String,
        script: PathBuf,
        sha256: String,
        dev: u64,
        ino: u64,
    },
}

impl LaunchPlan {
    pub(super) fn native(installation: &InstalledWorker) -> Self {
        Self::NativeBootstrap {
            installation: installation.clone(),
        }
    }

    pub(in super::super) fn fixture(case_id: &str, script: &Path) -> Result<Self, String> {
        let metadata = script_metadata(script)?;
        Ok(Self::InterpreterFixture {
            case_id: case_id.into(),
            script: script.into(),
            sha256: installation::digest(&fs::read(script).map_err(|error| error.to_string())?),
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        match self {
            Self::NativeBootstrap { installation } => installation::verify(installation),
            Self::InterpreterFixture {
                script,
                sha256,
                dev,
                ino,
                ..
            } => {
                let metadata = script_metadata(script)?;
                if metadata.dev() != *dev
                    || metadata.ino() != *ino
                    || installation::digest(&fs::read(script).map_err(|error| error.to_string())?)
                        != *sha256
                {
                    return Err("interpreter fixture identity changed".into());
                }
                Ok(())
            }
        }
    }

    pub(super) fn is_native(&self) -> bool {
        matches!(self, Self::NativeBootstrap { .. })
    }
    pub(super) fn program(&self) -> PathBuf {
        match self {
            Self::NativeBootstrap { installation } => installation.path.clone(),
            Self::InterpreterFixture { .. } => PathBuf::from("/bin/sh"),
        }
    }
    pub(super) fn arguments(&self) -> Vec<PathBuf> {
        match self {
            Self::NativeBootstrap { .. } => vec!["--bootstrap-check".into()],
            Self::InterpreterFixture { script, .. } => vec![script.clone()],
        }
    }
    pub(super) fn directory(&self) -> &Path {
        match self {
            Self::NativeBootstrap { installation } => &installation.root,
            Self::InterpreterFixture { script, .. } => {
                script.parent().expect("validated physical script")
            }
        }
    }
    pub(super) fn observation_id(&self) -> String {
        match self {
            Self::NativeBootstrap { installation } => {
                format!("{}/bootstrap-once", installation.installation_id)
            }
            Self::InterpreterFixture { case_id, .. } => {
                format!("{case_id}/interpreter-control-once")
            }
        }
    }
    pub(super) fn event(&self, phase: &str) -> String {
        format!(
            "{}_{phase}",
            if self.is_native() {
                "worker_bootstrap"
            } else {
                "interpreter_fixture"
            }
        )
    }
}

fn script_metadata(script: &Path) -> Result<fs::Metadata, String> {
    installation::checked_file(script, true)?;
    let metadata = fs::symlink_metadata(script).map_err(|error| error.to_string())?;
    if metadata.mode() & 0o111 != 0 {
        return Err("interpreter script must not be executable".into());
    }
    Ok(metadata)
}
