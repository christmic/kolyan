use super::*;

use std::path::Path;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::OpenOptions;
use serde_json::json;

use crate::{
    EditArguments, FileOperationError, FileOperationLimits, ReadArguments, WriteArguments,
};

struct Fixture {
    _root: tempfile::TempDir,
    _stage: tempfile::TempDir,
    root: PathBuf,
    stage: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut builder = tempfile::Builder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let stage = builder.tempdir().unwrap();
        Self {
            root: root.path().canonicalize().unwrap(),
            stage: stage.path().canonicalize().unwrap(),
            _root: root,
            _stage: stage,
        }
    }

    fn binding(&self, name: &str) -> ExactFileBinding {
        ExactFileBinding::prepare(&self.root, &self.root.join(name), &[]).unwrap()
    }

    fn request(&self, operation: FileOperation, target: &str) -> ExactFileWorkerRequest {
        let binding = self.binding(target);
        let staging = if matches!(operation, FileOperation::Read(_)) {
            None
        } else {
            Some(ExactFileStaging::prepare(&binding, &self.stage.join("stage")).unwrap())
        };
        ExactFileWorkerRequest {
            operation,
            binding,
            staging,
        }
    }
}

fn read(path: &str) -> FileOperation {
    FileOperation::Read(ReadArguments { path: path.into() })
}
fn write(path: &str, content: &str) -> FileOperation {
    FileOperation::Write(WriteArguments {
        path: path.into(),
        content: content.into(),
    })
}
fn edit(path: &str, old: &str, new: &str, sha: Option<&str>) -> FileOperation {
    FileOperation::Edit(EditArguments {
        path: path.into(),
        old_text: old.into(),
        new_text: new.into(),
        expected_sha256: sha.map(str::to_owned),
    })
}

#[test]
fn writes_reads_edits_exact_resource_and_preserves_display_label() {
    let fixture = Fixture::new();
    let limits = FileOperationLimits::default();
    let written = execute_exact(
        &fixture.request(write("display-alias", "alpha keep"), "physical"),
        limits,
    )
    .unwrap();
    assert_eq!(written.path, "display-alias");
    assert!(!fixture.root.join("display-alias").exists());
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("physical")).unwrap(),
        "alpha keep"
    );
    assert!(!fixture.stage.join("stage").exists());
    let observed =
        execute_exact(&fixture.request(read("display-alias"), "physical"), limits).unwrap();
    assert_eq!(observed.content.as_deref(), Some("alpha keep"));
    execute_exact(
        &fixture.request(
            edit("display-alias", "alpha", "beta", Some(&observed.sha256)),
            "physical",
        ),
        limits,
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("physical")).unwrap(),
        "beta keep"
    );
}

#[test]
fn replacement_is_atomic_and_keeps_old_open_object() {
    let fixture = Fixture::new();
    std::fs::write(fixture.root.join("note"), "old").unwrap();
    let mut old = std::fs::File::open(fixture.root.join("note")).unwrap();
    execute_exact(
        &fixture.request(write("note", "new"), "note"),
        FileOperationLimits::default(),
    )
    .unwrap();
    use std::io::Read;
    let mut content = String::new();
    old.read_to_string(&mut content).unwrap();
    assert_eq!(content, "old");
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("note")).unwrap(),
        "new"
    );
    assert!(!fixture.stage.join("stage").exists());
}

#[test]
fn missing_ambiguous_overlapping_and_stale_edits_leave_no_stage_or_effect() {
    let fixture = Fixture::new();
    for (old, sha, expected) in [
        ("missing", None, FileOperationError::MissingMatch),
        ("aa", None, FileOperationError::AmbiguousMatch),
        (
            "aaa",
            Some("0000000000000000000000000000000000000000000000000000000000000000"),
            FileOperationError::StaleContent,
        ),
    ] {
        std::fs::write(fixture.root.join("note"), "aaa keep").unwrap();
        let request = fixture.request(edit("note", old, "new", sha), "note");
        assert_eq!(
            execute_exact(&request, FileOperationLimits::default()).unwrap_err(),
            expected
        );
        assert_eq!(
            std::fs::read_to_string(fixture.root.join("note")).unwrap(),
            "aaa keep"
        );
        assert!(!fixture.stage.join("stage").exists());
    }
}

#[test]
fn bounded_invalid_utf8_and_missing_reads_fail_without_writes() {
    let fixture = Fixture::new();
    let limits = FileOperationLimits {
        max_read_bytes: 3,
        max_write_bytes: 3,
    };
    std::fs::write(fixture.root.join("note"), "four").unwrap();
    assert_eq!(
        execute_exact(&fixture.request(read("note"), "note"), limits).unwrap_err(),
        FileOperationError::ReadLimitExceeded
    );
    assert_eq!(
        execute_exact(&fixture.request(write("note", "four"), "note"), limits).unwrap_err(),
        FileOperationError::WriteLimitExceeded
    );
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("note")).unwrap(),
        "four"
    );
    std::fs::write(fixture.root.join("note"), [255]).unwrap();
    assert_eq!(
        execute_exact(&fixture.request(read("note"), "note"), limits).unwrap_err(),
        FileOperationError::InvalidUtf8
    );
    assert!(execute_exact(&fixture.request(read("missing"), "missing"), limits).is_err());
    assert!(!fixture.stage.join("stage").exists());
}

#[test]
fn strict_schema_and_malformed_bindings_are_rejected() {
    let fixture = Fixture::new();
    let request = fixture.request(write("note", "new"), "note");
    let mut missing = serde_json::to_value(&request).unwrap();
    missing.as_object_mut().unwrap().remove("staging");
    assert!(serde_json::from_value::<ExactFileWorkerRequest>(missing).is_err());
    let mut missing = serde_json::to_value(&request).unwrap();
    missing["binding"]
        .as_object_mut()
        .unwrap()
        .remove("target_identity");
    assert!(serde_json::from_value::<ExactFileWorkerRequest>(missing).is_err());
    for location in ["request", "binding", "workspace", "identity", "staging"] {
        let mut wire = serde_json::to_value(&request).unwrap();
        let value = match location {
            "binding" => &mut wire["binding"],
            "workspace" => &mut wire["binding"]["workspace"],
            "identity" => &mut wire["binding"]["workspace"]["identity_chain"][0],
            "staging" => &mut wire["staging"],
            _ => &mut wire,
        };
        value
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(true));
        assert!(
            serde_json::from_value::<ExactFileWorkerRequest>(wire).is_err(),
            "{location}"
        );
    }
    for leaf in ["../outside", "/absolute", "", ".", "a/b", "nul\0"] {
        let mut malformed = request.clone();
        malformed.binding.leaf = leaf.into();
        assert!(
            execute_exact(&malformed, FileOperationLimits::default()).is_err(),
            "{leaf:?}"
        );
    }
    let mut malformed = request.clone();
    malformed.binding.workspace.identity_chain.clear();
    assert!(execute_exact(&malformed, FileOperationLimits::default()).is_err());
    let mut malformed = request;
    malformed.binding.parent.physical_path = fixture.stage.clone();
    assert!(execute_exact(&malformed, FileOperationLimits::default()).is_err());
    assert!(!fixture.root.join("note").exists());
}

#[test]
fn staging_must_be_absent_outside_workspace_and_same_filesystem() {
    let fixture = Fixture::new();
    let binding = fixture.binding("note");
    assert!(ExactFileStaging::prepare(&binding, &fixture.root.join("stage")).is_err());
    assert!(ExactFileStaging::prepare(&binding, &fixture.root.join("note")).is_err());
    std::fs::write(fixture.stage.join("stage"), "existing").unwrap();
    assert!(ExactFileStaging::prepare(&binding, &fixture.stage.join("stage")).is_err());
    assert_eq!(
        std::fs::read_to_string(fixture.stage.join("stage")).unwrap(),
        "existing"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_ne!(
            std::fs::metadata("/dev").unwrap().dev(),
            std::fs::metadata(&fixture.root).unwrap().dev()
        );
        assert!(
            ExactFileStaging::prepare(&binding, Path::new("/dev/kolyan-exact-stage-test"))
                .unwrap_err()
                .to_string()
                .contains("filesystem")
        );
    }
}

#[test]
fn protected_resources_and_stage_mode_mismatch_are_refused() {
    let fixture = Fixture::new();
    std::fs::create_dir(fixture.root.join(".git")).unwrap();
    assert!(
        ExactFileBinding::prepare(&fixture.root, &fixture.root.join(".git/config"), &[]).is_err()
    );
    assert!(
        ExactFileBinding::prepare(
            &fixture.root,
            &fixture.root.join("note"),
            &[fixture.root.join("note")]
        )
        .is_err()
    );
    let mut write = fixture.request(write("note", "new"), "note");
    write.staging = None;
    assert!(execute_exact(&write, FileOperationLimits::default()).is_err());
    let mut read = fixture.request(read("note"), "note");
    read.staging =
        Some(ExactFileStaging::prepare(&read.binding, &fixture.stage.join("stage")).unwrap());
    assert!(execute_exact(&read, FileOperationLimits::default()).is_err());
    assert!(!fixture.root.join("note").exists());
}

#[test]
fn deterministic_root_parent_and_leaf_replacement_invalidates_preparation() {
    for replaced in ["root", "parent", "leaf"] {
        let fixture = Fixture::new();
        std::fs::create_dir(fixture.root.join("parent")).unwrap();
        let path = fixture.root.join("parent/note");
        std::fs::write(&path, "original").unwrap();
        let request = fixture.request(read("parent/note"), "parent/note");
        match replaced {
            "root" => {
                std::fs::rename(&fixture.root, fixture.root.with_extension("old")).unwrap();
                std::fs::create_dir_all(fixture.root.join("parent")).unwrap();
            }
            "parent" => {
                std::fs::rename(fixture.root.join("parent"), fixture.root.join("old-parent"))
                    .unwrap();
                std::fs::create_dir(fixture.root.join("parent")).unwrap();
            }
            _ => {
                std::fs::rename(&path, fixture.root.join("old-note")).unwrap();
            }
        }
        std::fs::write(&path, "other").unwrap();
        assert!(
            execute_exact(&request, FileOperationLimits::default()).is_err(),
            "{replaced}"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "other");
        if replaced == "root" {
            std::fs::remove_dir_all(fixture.root.with_extension("old")).unwrap();
        }
    }
}

#[cfg(unix)]
#[test]
fn final_and_parent_symlinks_are_never_followed_after_preparation() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new();
    std::fs::create_dir(fixture.root.join("parent")).unwrap();
    std::fs::write(fixture.root.join("parent/note"), "original").unwrap();
    std::fs::write(fixture.stage.join("outside"), "secret").unwrap();
    let request = fixture.request(read("parent/note"), "parent/note");
    std::fs::rename(fixture.root.join("parent/note"), fixture.root.join("saved")).unwrap();
    symlink(
        fixture.stage.join("outside"),
        fixture.root.join("parent/note"),
    )
    .unwrap();
    assert!(execute_exact(&request, FileOperationLimits::default()).is_err());
    assert!(fixture.binding("saved").target_identity.is_some());
    std::fs::remove_file(fixture.root.join("parent/note")).unwrap();
    std::fs::rename(fixture.root.join("saved"), fixture.root.join("parent/note")).unwrap();
    std::fs::rename(
        fixture.root.join("parent"),
        fixture.root.join("saved-parent"),
    )
    .unwrap();
    symlink(&fixture.stage, fixture.root.join("parent")).unwrap();
    assert!(execute_exact(&request, FileOperationLimits::default()).is_err());
    assert_eq!(
        std::fs::read_to_string(fixture.stage.join("outside")).unwrap(),
        "secret"
    );
}

#[test]
fn held_parent_capability_does_not_rebind_after_rename() {
    let fixture = Fixture::new();
    std::fs::create_dir(fixture.root.join("parent")).unwrap();
    std::fs::write(fixture.root.join("parent/note"), "original").unwrap();
    let binding = fixture.binding("parent/note");
    let pinned = binding.open().unwrap();
    std::fs::rename(
        fixture.root.join("parent"),
        fixture.root.join("original-parent"),
    )
    .unwrap();
    std::fs::create_dir(fixture.root.join("parent")).unwrap();
    std::fs::write(fixture.root.join("parent/note"), "other").unwrap();
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    use std::io::Read;
    let mut original = String::new();
    pinned
        .parent
        .open_with("note", &options)
        .unwrap()
        .read_to_string(&mut original)
        .unwrap();
    assert_eq!(original, "original");
    assert!(binding.open().is_err());
}

#[test]
fn replaced_stage_parent_and_created_stage_leaf_are_refused_without_target_effects() {
    let fixture = Fixture::new();
    let request = fixture.request(write("note", "new"), "note");
    std::fs::write(fixture.stage.join("stage"), "unrelated").unwrap();
    assert!(execute_exact(&request, FileOperationLimits::default()).is_err());
    assert_eq!(
        std::fs::read_to_string(fixture.stage.join("stage")).unwrap(),
        "unrelated"
    );
    assert!(!fixture.root.join("note").exists());
    std::fs::rename(&fixture.stage, fixture.stage.with_extension("old")).unwrap();
    std::fs::create_dir(&fixture.stage).unwrap();
    assert!(execute_exact(&request, FileOperationLimits::default()).is_err());
    assert!(!fixture.root.join("note").exists());
    std::fs::remove_dir_all(fixture.stage.with_extension("old")).unwrap();
}

#[cfg(unix)]
#[test]
fn private_stage_permissions_are_real_and_nonprivate_directories_are_refused() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    assert_eq!(
        std::fs::metadata(&fixture.stage)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let binding = fixture.binding("note");
    let stage_path = fixture.stage.join("stage");
    for mode in [0o755, 0o770, 0o707] {
        std::fs::set_permissions(&fixture.stage, std::fs::Permissions::from_mode(mode)).unwrap();
        assert!(
            ExactFileStaging::prepare(&binding, &stage_path)
                .unwrap_err()
                .to_string()
                .contains("host-private")
        );
        assert!(!fixture.root.join("note").exists());
        assert!(!stage_path.exists());
    }
    std::fs::set_permissions(&fixture.stage, std::fs::Permissions::from_mode(0o700)).unwrap();
    let request = fixture.request(write("note", "private-success"), "note");
    // A permission change after preparation must fail at execution too.
    std::fs::set_permissions(&fixture.stage, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        execute_exact(&request, FileOperationLimits::default())
            .unwrap_err()
            .to_string()
            .contains("host-private")
    );
    assert!(!fixture.root.join("note").exists());
    std::fs::set_permissions(&fixture.stage, std::fs::Permissions::from_mode(0o700)).unwrap();
    execute_exact(&request, FileOperationLimits::default()).unwrap();
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("note")).unwrap(),
        "private-success"
    );
    assert!(!stage_path.exists());
}
