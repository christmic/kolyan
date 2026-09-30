use super::*;

use serde_json::json;
use tempfile::TempDir;

fn fixture() -> (TempDir, FileOperations) {
    let root = tempfile::tempdir().unwrap();
    let operations = FileOperations::new(root.path(), FileOperationLimits::default());
    (root, operations)
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

fn edit(path: &str, old: &str, new: &str, expected: Option<String>) -> FileOperation {
    FileOperation::Edit(EditArguments {
        path: path.into(),
        old_text: old.into(),
        new_text: new.into(),
        expected_sha256: expected,
    })
}

#[test]
fn writes_reads_and_edits_utf8_with_digest() {
    let (_root, operations) = fixture();
    let written = operations
        .execute(&write("note.txt", "before 🦀 after"))
        .unwrap();
    assert_eq!(written.bytes, "before 🦀 after".len());
    assert_eq!(written.content, None);
    let observed = operations.execute(&read("note.txt")).unwrap();
    assert_eq!(observed.sha256, written.sha256);
    assert_eq!(observed.content.as_deref(), Some("before 🦀 after"));
    let changed = operations
        .execute(&edit("note.txt", "🦀", "agent", Some(observed.sha256)))
        .unwrap();
    assert_eq!(changed.sha256, digest("before agent after"));
    assert_eq!(
        operations
            .execute(&read("note.txt"))
            .unwrap()
            .content
            .as_deref(),
        Some("before agent after")
    );
}

#[test]
fn rejects_unknown_fields_and_tools_in_wire_contract() {
    for value in [
        json!({"name":"file.read","arguments":{"path":"a","extra":1}}),
        json!({"name":"file.write","arguments":{"path":"a","content":"x","extra":1}}),
        json!({"name":"file.edit","arguments":{"path":"a","old_text":"x","new_text":"y","extra":1}}),
        json!({"name":"file.read","arguments":{"path":"a"},"extra":1}),
        json!({"name":"shell","arguments":{"path":"a"}}),
    ] {
        assert!(serde_json::from_value::<FileOperation>(value).is_err());
    }
    for operation in [read("a"), write("a", "x"), edit("a", "x", "y", None)] {
        let roundtrip: FileOperation =
            serde_json::from_value(serde_json::to_value(&operation).unwrap()).unwrap();
        assert_eq!(roundtrip, operation);
        assert!(operation.name().starts_with("file."));
    }
}

#[test]
fn parse_preflights_without_creating_or_reading_files() {
    let (root, operations) = fixture();
    let operation = operations
        .parse("file.write", &json!({"path":"a","content":"x"}))
        .unwrap();
    assert_eq!(operation, write("a", "x"));
    assert!(
        operations
            .parse("file.read", &json!({"path":"missing"}))
            .is_ok()
    );
    assert!(
        operations
            .parse(
                "file.edit",
                &json!({"path":"a","old_text":"","new_text":"x"})
            )
            .is_err()
    );
    assert!(
        operations
            .parse("file.read", &json!({"path":"a","extra":1}))
            .is_err()
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn rejects_absolute_parent_empty_and_nul_paths() {
    let (_root, operations) = fixture();
    for path in ["/tmp/kolyan", "../a", "a/../b", "", ".", "a\0b"] {
        for operation in [read(path), write(path, "x"), edit(path, "x", "y", None)] {
            assert!(matches!(
                operations.execute(&operation),
                Err(FileOperationError::InvalidArguments(_))
            ));
        }
    }
}

#[test]
fn missing_ambiguous_overlapping_and_stale_edits_do_not_modify() {
    let (root, operations) = fixture();
    std::fs::write(root.path().join("a"), "aaa ββ").unwrap();
    for (operation, expected) in [
        (
            edit("a", "absent", "x", None),
            FileOperationError::MissingMatch,
        ),
        (
            edit("a", "aa", "x", None),
            FileOperationError::AmbiguousMatch,
        ),
        (
            edit("a", "β", "x", None),
            FileOperationError::AmbiguousMatch,
        ),
        (
            edit("a", "aaa", "x", Some("0".repeat(64))),
            FileOperationError::StaleContent,
        ),
    ] {
        assert_eq!(operations.execute(&operation), Err(expected));
        assert_eq!(
            std::fs::read_to_string(root.path().join("a")).unwrap(),
            "aaa ββ"
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    }
}

#[test]
fn rejects_empty_match_and_malformed_digest_before_effects() {
    let (root, operations) = fixture();
    std::fs::write(root.path().join("a"), "original").unwrap();
    for operation in [
        edit("a", "", "x", None),
        edit("a", "original", "x", Some("invalid".into())),
    ] {
        assert!(matches!(
            operations.execute(&operation),
            Err(FileOperationError::InvalidArguments(_))
        ));
    }
    assert_eq!(
        std::fs::read_to_string(root.path().join("a")).unwrap(),
        "original"
    );
}

#[test]
fn read_and_write_bounds_are_byte_exact_and_leave_original() {
    let root = tempfile::tempdir().unwrap();
    let operations = FileOperations::new(
        root.path(),
        FileOperationLimits {
            max_read_bytes: 4,
            max_write_bytes: 4,
        },
    );
    operations.execute(&write("a", "éé")).unwrap();
    assert_eq!(operations.execute(&read("a")).unwrap().bytes, 4);
    assert_eq!(
        operations.execute(&write("a", "ééx")),
        Err(FileOperationError::WriteLimitExceeded)
    );
    assert_eq!(
        operations
            .execute(&edit("a", "éé", "xxxx", None))
            .unwrap()
            .bytes,
        4
    );
    assert_eq!(
        operations.execute(&edit("a", "x", "yyyy", None)),
        Err(FileOperationError::AmbiguousMatch)
    );
    operations.execute(&write("a", "abcd")).unwrap();
    assert_eq!(
        operations.execute(&edit("a", "a", "xyz", None)),
        Err(FileOperationError::WriteLimitExceeded)
    );
    assert_eq!(std::fs::read(root.path().join("a")).unwrap(), b"abcd");
    std::fs::write(root.path().join("a"), "abcde").unwrap();
    assert_eq!(
        operations.execute(&read("a")),
        Err(FileOperationError::ReadLimitExceeded)
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

#[test]
fn missing_parent_and_directory_target_fail_without_partial_files() {
    let (root, operations) = fixture();
    assert!(operations.execute(&read("missing")).is_err());
    assert!(
        operations
            .execute(&edit("missing", "a", "b", None))
            .is_err()
    );
    assert!(operations.execute(&write("missing/a", "x")).is_err());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    std::fs::create_dir(root.path().join("directory")).unwrap();
    assert!(operations.execute(&write("directory", "x")).is_err());
    assert!(operations.execute(&read("directory")).is_err());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

#[test]
fn edit_can_shrink_readable_content_to_a_smaller_write_limit() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("a"), "abcdef").unwrap();
    let operations = FileOperations::new(
        root.path(),
        FileOperationLimits {
            max_read_bytes: 8,
            max_write_bytes: 3,
        },
    );
    assert_eq!(
        operations
            .execute(&edit("a", "abcdef", "xy", None))
            .unwrap()
            .bytes,
        2
    );
    assert_eq!(std::fs::read(root.path().join("a")).unwrap(), b"xy");
}

#[test]
fn invalid_utf8_and_unavailable_root_fail_closed() {
    let (root, operations) = fixture();
    std::fs::write(root.path().join("a"), [0xff]).unwrap();
    assert_eq!(
        operations.execute(&read("a")),
        Err(FileOperationError::InvalidUtf8)
    );
    let missing = FileOperations::new(root.path().join("missing"), FileOperationLimits::default());
    assert!(matches!(
        missing.execute(&write("a", "x")),
        Err(FileOperationError::Io(_))
    ));
}

#[cfg(unix)]
#[test]
fn atomic_replacement_preserves_old_open_handle_and_hardlink() {
    let (root, operations) = fixture();
    std::fs::write(root.path().join("a"), "original").unwrap();
    std::fs::hard_link(root.path().join("a"), root.path().join("old-link")).unwrap();
    let mut old_handle = std::fs::File::open(root.path().join("a")).unwrap();
    operations.execute(&write("a", "new")).unwrap();
    let mut original = String::new();
    old_handle.read_to_string(&mut original).unwrap();
    assert_eq!(original, "original");
    assert_eq!(
        std::fs::read_to_string(root.path().join("old-link")).unwrap(),
        "original"
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("a")).unwrap(),
        "new"
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 2);
}

#[cfg(unix)]
#[test]
fn rejects_outside_symlink_targets_and_parent_symlinks() {
    use std::os::unix::fs::symlink;
    let (root, operations) = fixture();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), "untouched").unwrap();
    symlink(outside.path().join("secret"), root.path().join("escape")).unwrap();
    symlink(outside.path(), root.path().join("parent")).unwrap();
    for path in ["escape", "parent/secret"] {
        for operation in [
            read(path),
            write(path, "changed"),
            edit(path, "untouched", "changed", None),
        ] {
            assert!(operations.execute(&operation).is_err());
        }
    }
    assert!(operations.execute(&write("parent/new", "x")).is_err());
    assert_eq!(
        std::fs::read_to_string(outside.path().join("secret")).unwrap(),
        "untouched"
    );
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn permits_internal_symlink_reads_but_rejects_symlink_replacements() {
    use std::os::unix::fs::symlink;
    let (root, operations) = fixture();
    operations.execute(&write("a", "original")).unwrap();
    symlink("a", root.path().join("link")).unwrap();
    assert_eq!(
        operations
            .execute(&read("link"))
            .unwrap()
            .content
            .as_deref(),
        Some("original")
    );
    assert!(operations.execute(&write("link", "changed")).is_err());
    assert!(
        operations
            .execute(&edit("link", "original", "changed", None))
            .is_err()
    );
    assert_eq!(
        operations.execute(&read("a")).unwrap().content.as_deref(),
        Some("original")
    );
}

#[cfg(unix)]
#[test]
fn pinned_root_survives_ambient_root_replacement() {
    let container = tempfile::tempdir().unwrap();
    let path = container.path().join("root");
    std::fs::create_dir(&path).unwrap();
    let operations = FileOperations::new(&path, FileOperationLimits::default());
    std::fs::rename(&path, container.path().join("original")).unwrap();
    std::fs::create_dir(&path).unwrap();
    operations.execute(&write("a", "trusted")).unwrap();
    assert!(!path.join("a").exists());
    assert_eq!(
        std::fs::read_to_string(container.path().join("original/a")).unwrap(),
        "trusted"
    );
}
