//! Reconstruction must not reselect the mutable build input.

use std::os::unix::fs::PermissionsExt;

use super::*;

#[tokio::test]
async fn worker_snapshot_survives_build_replacement_and_rejects_changed_pin() {
    let root = tempfile::tempdir().unwrap();
    let build = tempfile::tempdir().unwrap();
    let source = build.path().join("worker");
    fs::copy(env!("CARGO_BIN_EXE_kolyan-test-tool-worker"), &source).unwrap();
    let original = fs::read(&source).unwrap();
    let installation = WorkerRun::prepare_from(&source).await;
    let evidence = Evidence::new(&root.path().join("actual.jsonl"));
    initialize_worker(root.path(), &evidence, &installation).unwrap();
    fs::write(&source, b"rebuilt executable").unwrap();
    let pinned = verified_worker(root.path(), &evidence).unwrap();
    assert_eq!(fs::read(&pinned).unwrap(), original);
    assert!(initialize_worker(root.path(), &evidence, &installation).is_err());
    fs::set_permissions(&pinned, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(&pinned, b"changed pinned executable").unwrap();
    assert_eq!(
        verified_worker(root.path(), &evidence).unwrap_err(),
        "trusted worker snapshot digest mismatch"
    );
    fs::remove_file(&pinned).unwrap();
    assert!(verified_worker(root.path(), &evidence).is_err());
    assert!(evidence.rows().iter().any(|row| !row["error"].is_null()));
}
