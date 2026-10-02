//! Immutable installed artifact and independently readable native-entry proof.

use std::{
    fs,
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::super::Evidence;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstalledWorker {
    pub root: PathBuf,
    pub installation_id: String,
    pub path: PathBuf,
    pub sha256: String,
    pub dev: u64,
    pub ino: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReadyEvidence {
    pub schema_version: u32,
    pub installation: InstalledWorker,
    pub observation_id: String,
    pub pid: u32,
    pub arguments: Vec<String>,
    pub empty_environment: bool,
    pub empty_stdin: bool,
    pub timeout_ms: u64,
    pub elapsed_ms: u64,
    pub exit_code: i32,
    pub marker: Value,
}

impl ReadyEvidence {
    pub fn validate(&self, installed: &InstalledWorker) -> Result<(), String> {
        let expected: Value = serde_json::from_slice(kolyan_tool_worker::BOOTSTRAP_RESPONSE)
            .map_err(|error| error.to_string())?;
        if self.schema_version != 1
            || &self.installation != installed
            || self.pid == 0
            || self.observation_id != format!("{}/bootstrap-once", installed.installation_id)
            || self.arguments != ["--bootstrap-check"]
            || !self.empty_environment
            || !self.empty_stdin
            || self.timeout_ms == 0
            || self.timeout_ms > 600000
            || self.elapsed_ms > self.timeout_ms
            || self.exit_code != 0
            || self.marker != expected
        {
            return Err("invalid worker Ready evidence".into());
        }
        Ok(())
    }
}

pub(super) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn write_private(path: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| error.to_string())?;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .map_err(|error| error.to_string())?;
    fs::File::open(path.parent().ok_or("private file needs parent")?)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())
}

pub(super) fn checked_file(path: &Path, readonly: bool) -> Result<(), String> {
    let physical = path.canonicalize().map_err(|error| error.to_string())?;
    if physical != path {
        return Err("worker evidence path must be physical and nonsymlink".into());
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || (readonly && metadata.mode() & 0o222 != 0)
    {
        return Err("worker evidence must be an immutable regular file".into());
    }
    Ok(())
}

pub(super) fn install(
    root: &Path,
    source: &Path,
    evidence: &Evidence,
) -> Result<InstalledWorker, String> {
    let bytes = fs::read(source).map_err(|error| error.to_string())?;
    let path = root.join("worker");
    evidence.append(json!({"event":"worker_install_intent","source":source,"path":path,"sha256":digest(&bytes),"bytes":bytes.len()}))?;
    write_private(&path, &bytes, 0o500)?;
    let metadata = fs::metadata(&path).map_err(|error| error.to_string())?;
    let installed = InstalledWorker {
        root: root.into(),
        installation_id: root
            .file_name()
            .ok_or("installation ID missing")?
            .to_string_lossy()
            .into(),
        path,
        sha256: digest(&bytes),
        dev: metadata.dev(),
        ino: metadata.ino(),
    };
    write_private(
        &root.join("installation.json"),
        &serde_json::to_vec(&installed).map_err(|error| error.to_string())?,
        0o400,
    )?;
    evidence.append(json!({"event":"worker_installed","installation":installed}))?;
    verify(&installed)?;
    Ok(installed)
}

pub(super) fn verify(installed: &InstalledWorker) -> Result<(), String> {
    if installed.path != installed.root.join("worker")
        || installed.installation_id
            != installed
                .root
                .file_name()
                .ok_or("installation ID missing")?
                .to_string_lossy()
    {
        return Err("installation coordinates mismatch".into());
    }
    let root = fs::symlink_metadata(&installed.root).map_err(|error| error.to_string())?;
    if !root.is_dir()
        || root.file_type().is_symlink()
        || root.mode() & 0o077 != 0
        || installed
            .root
            .canonicalize()
            .map_err(|error| error.to_string())?
            != installed.root
    {
        return Err("installation root must be private and physical".into());
    }
    let metadata = fs::metadata(&installed.path).map_err(|error| error.to_string())?;
    if metadata.uid() != root.uid()
        || metadata.mode() & 0o100 == 0
        || metadata.dev() != installed.dev
        || metadata.ino() != installed.ino
        || digest(&fs::read(&installed.path).map_err(|error| error.to_string())?)
            != installed.sha256
    {
        return Err("trusted worker snapshot digest mismatch".into());
    }
    checked_file(&installed.path, true)?;
    let manifest = installed.root.join("installation.json");
    checked_file(&manifest, true)?;
    let saved: InstalledWorker =
        serde_json::from_slice(&fs::read(manifest).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    if &saved != installed {
        return Err("installed worker manifest mismatch".into());
    }
    Ok(())
}
