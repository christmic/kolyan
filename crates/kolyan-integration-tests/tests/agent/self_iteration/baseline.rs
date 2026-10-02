//! Read-only tracked/nonignored inventory; the host never writes candidate bytes.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::Plan;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct FileState {
    pub kind: String,
    pub mode: u32,
    pub sha256: String,
    pub link_target_sha256: Option<String>,
}
pub(super) type Inventory = BTreeMap<String, FileState>;

pub(super) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn collect(worktree: &Path) -> Result<Inventory, String> {
    let output = Command::new("/usr/bin/git")
        .args(["--no-optional-locks", "-C"])
        .arg(worktree)
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into());
    }
    let mut inventory = Inventory::new();
    for name in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let name = std::str::from_utf8(name).map_err(|error| error.to_string())?;
        safe_relative(name)?;
        let path = worktree.join(name);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.to_string()),
        };
        let state = if metadata.file_type().is_symlink() {
            use std::os::unix::ffi::OsStrExt;
            let target = fs::read_link(&path).map_err(|e| e.to_string())?;
            let physical = path.canonicalize().map_err(|e| e.to_string())?;
            if !physical.starts_with(worktree.canonicalize().map_err(|e| e.to_string())?) {
                return Err(format!(
                    "untrusted symlink referent outside worktree: {name}"
                ));
            }
            FileState {
                kind: "symlink".into(),
                mode: metadata.permissions().mode() & 0o7777,
                sha256: digest(&fs::read(&physical).map_err(|e| e.to_string())?),
                link_target_sha256: Some(digest(target.as_os_str().as_bytes())),
            }
        } else if metadata.is_file() {
            FileState {
                kind: "file".into(),
                mode: metadata.permissions().mode() & 0o7777,
                sha256: digest(&fs::read(path).map_err(|e| e.to_string())?),
                link_target_sha256: None,
            }
        } else {
            return Err(format!("non-file inventory entry: {name}"));
        };
        inventory.insert(name.into(), state);
    }
    Ok(inventory)
}

pub(super) fn verify_initial(plan: &Plan, actual: &Inventory) -> Result<(), String> {
    let snapshot = Command::new("/usr/bin/git")
        .args(["--no-optional-locks", "-C"])
        .arg(&plan.run.worktree)
        .args(["diff", "--binary", "HEAD"])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .map_err(|e| e.to_string())?;
    if !snapshot.status.success() || digest(&snapshot.stdout) != plan.run.source_snapshot_sha256 {
        return Err("actual git diff --binary HEAD SHA differs from RunConfig pin".into());
    }
    if digest(
        &fs::read(plan.run.host_private.join(&plan.run.baseline_manifest))
            .map_err(|e| e.to_string())?,
    ) != plan.run.baseline_manifest_sha256
    {
        return Err("baseline manifest SHA differs from RunConfig pin".into());
    }
    let head = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(&plan.run.worktree)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|e| e.to_string())?;
    if !head.status.success() || String::from_utf8_lossy(&head.stdout).trim() != plan.run.head {
        return Err("worktree HEAD differs from RunConfig pin".into());
    }
    let manifest = fs::read_to_string(plan.run.host_private.join(&plan.run.baseline_manifest))
        .map_err(|e| e.to_string())?;
    let mut expected = BTreeMap::new();
    for line in manifest.lines() {
        let (hash, name) = line
            .split_once("  ")
            .ok_or("invalid host baseline manifest")?;
        safe_relative(name)?;
        if hash.len() != 64
            || !hash.bytes().all(|b| b.is_ascii_hexdigit())
            || expected.insert(name.to_owned(), hash.to_owned()).is_some()
        {
            return Err("invalid/duplicate host baseline hash".into());
        }
    }
    if expected.len() != plan.run.baseline_files || actual.len() != expected.len() {
        return Err("baseline file inventory cardinality differs".into());
    }
    for (path, hash) in expected {
        if actual.get(&path).map(|state| state.sha256.as_str()) != Some(hash.as_str()) {
            return Err(format!("initial source differs: {path}"));
        }
    }
    for absent in &plan.allowlist[2..] {
        match fs::symlink_metadata(plan.run.worktree.join(absent)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
            Ok(_) => {
                return Err(format!(
                    "new candidate file must initially be absent: {absent}"
                ));
            }
        }
    }
    for (path, baseline) in [
        (plan.allowlist[0].as_str(), "types.rs.baseline"),
        (plan.allowlist[1].as_str(), "tests.rs.baseline"),
    ] {
        if fs::read(plan.run.worktree.join(path)).map_err(|e| e.to_string())?
            != fs::read(plan.run.host_private.join(baseline)).map_err(|e| e.to_string())?
        {
            return Err(format!("candidate baseline bytes differ: {path}"));
        }
    }
    let branch = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(&plan.run.worktree)
        .args(["symbolic-ref", "--short", "HEAD"])
        .output()
        .map_err(|e| e.to_string())?;
    if !branch.status.success() || String::from_utf8_lossy(&branch.stdout).trim() != plan.run.branch
    {
        return Err("worktree branch differs".into());
    }
    Ok(())
}

pub(super) fn changed(before: &Inventory, after: &Inventory) -> BTreeSet<String> {
    before
        .keys()
        .chain(after.keys())
        .filter(|name| before.get(*name) != after.get(*name))
        .cloned()
        .collect()
}
pub(super) fn confined(plan: &Plan, before: &Inventory, after: &Inventory) -> Result<(), String> {
    for name in changed(before, after) {
        if !plan.allowlist.contains(&name) {
            return Err(format!("outside candidate allowlist changed: {name}"));
        }
    }
    Ok(())
}
pub(super) fn safe_relative(name: &str) -> Result<(), String> {
    let path = Path::new(name);
    if name.is_empty()
        || path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err("noncanonical workspace-relative path".into());
    }
    Ok(())
}
