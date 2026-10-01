use super::*;

use kolyan_tools::{ExactFileBinding, ExactFileStaging, FileOperation};
use serde_json::json;

fn private_stage() -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    builder.tempdir().unwrap()
}

fn config(root: PathBuf) -> WorkerConfig {
    WorkerConfig {
        workspace: root.canonicalize().unwrap(),
        max_input_bytes: 64 * 1024,
        file_limits: FileOperationLimits {
            max_read_bytes: 100,
            max_write_bytes: 100,
        },
    }
}

fn request(
    config: &WorkerConfig,
    stage: &std::path::Path,
    operation: serde_json::Value,
) -> ExactFileWorkerRequest {
    let operation: FileOperation = serde_json::from_value(operation).unwrap();
    let binding = ExactFileBinding::prepare(
        &config.workspace,
        &config.workspace.join(operation.path()),
        &[],
    )
    .unwrap();
    let staging = if matches!(operation, FileOperation::Read(_)) {
        None
    } else {
        Some(
            ExactFileStaging::prepare(&binding, &stage.canonicalize().unwrap().join("stage"))
                .unwrap(),
        )
    };
    ExactFileWorkerRequest {
        operation,
        binding,
        staging,
    }
}

fn run(
    config: &WorkerConfig,
    request: &ExactFileWorkerRequest,
) -> Result<FileOperationResult, WorkerError> {
    execute_request(config, serde_json::to_vec(request).unwrap().as_slice())
}

#[test]
fn strict_request_writes_then_reads_actual_content() {
    let root = tempfile::tempdir().unwrap();
    let stage = private_stage();
    let config = config(root.path().into());
    let write = request(
        &config,
        stage.path(),
        json!({"name":"file.write","arguments":{"path":"a.txt","content":"hello"}}),
    );
    let result = run(&config, &write).unwrap();
    assert_eq!(result.bytes, 5);
    let read = request(
        &config,
        stage.path(),
        json!({"name":"file.read","arguments":{"path":"a.txt"}}),
    );
    assert_eq!(
        run(&config, &read).unwrap().content.as_deref(),
        Some("hello")
    );
}

#[test]
fn foreign_workspace_and_changed_identities_fail_before_effects() {
    let root = tempfile::tempdir().unwrap();
    let foreign = tempfile::tempdir().unwrap();
    let stage = private_stage();
    let config = config(root.path().into());
    let original = request(
        &config,
        stage.path(),
        json!({"name":"file.write","arguments":{"path":"a","content":"x"}}),
    );
    let mut foreign_config = config.clone();
    foreign_config.workspace = foreign.path().canonicalize().unwrap();
    assert!(matches!(
        run(&foreign_config, &original),
        Err(WorkerError::WorkspaceMismatch)
    ));
    for component in ["workspace", "parent"] {
        let mut value = serde_json::to_value(&original).unwrap();
        value["binding"][component]["identity_chain"][0]["ino"] = json!(0);
        let changed = serde_json::from_value(value).unwrap();
        assert!(matches!(
            run(&config, &changed),
            Err(WorkerError::Operation(_))
        ));
        assert!(!root.path().join("a").exists());
        assert_eq!(std::fs::read_dir(stage.path()).unwrap().count(), 0);
    }
    assert_eq!(std::fs::read_dir(foreign.path()).unwrap().count(), 0);
}

#[test]
fn raw_requests_and_missing_bound_fields_are_not_compatible_protocols() {
    let root = tempfile::tempdir().unwrap();
    let stage = private_stage();
    let config = config(root.path().into());
    let operation = json!({"name":"file.write","arguments":{"path":"a","content":"x"}});
    assert!(matches!(
        execute_request(&config, serde_json::to_vec(&operation).unwrap().as_slice()),
        Err(WorkerError::Request(_))
    ));
    let original = request(&config, stage.path(), operation);
    let mut value = serde_json::to_value(original).unwrap();
    value["binding"]
        .as_object_mut()
        .unwrap()
        .remove("workspace");
    assert!(matches!(
        execute_request(&config, serde_json::to_vec(&value).unwrap().as_slice()),
        Err(WorkerError::Request(_))
    ));
    assert!(!root.path().join("a").exists());
    assert_eq!(std::fs::read_dir(stage.path()).unwrap().count(), 0);
}

#[test]
fn invalid_or_oversized_request_has_no_effect() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path().into());
    let bad = br#"{"name":"file.write","arguments":{"path":"a","content":"x","grant":"fake"}}"#;
    assert!(matches!(
        execute_request(&config, &bad[..]),
        Err(WorkerError::Request(_))
    ));
    config.max_input_bytes = 1;
    assert!(matches!(
        execute_request(&config, &bad[..]),
        Err(WorkerError::InputLimit)
    ));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn host_configuration_cannot_be_replaced_through_stdin() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path().into());
    config.workspace = "relative".into();
    assert!(matches!(
        execute_request(&config, &b"{}"[..]),
        Err(WorkerError::Configuration)
    ));
    config.workspace = root.path().into();
    config.file_limits.max_write_bytes = 0;
    assert!(matches!(
        execute_request(&config, &b"{}"[..]),
        Err(WorkerError::Configuration)
    ));
}
