use super::*;

fn config(root: PathBuf) -> WorkerConfig {
    WorkerConfig {
        workspace: root,
        max_input_bytes: 1024,
        file_limits: FileOperationLimits {
            max_read_bytes: 100,
            max_write_bytes: 100,
        },
    }
}

#[test]
fn strict_request_writes_then_reads_actual_content() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path().into());
    let write = br#"{"name":"file.write","arguments":{"path":"a.txt","content":"hello"}}"#;
    let result = execute_request(&config, &write[..]).unwrap();
    assert_eq!(result.bytes, 5);
    let read = br#"{"name":"file.read","arguments":{"path":"a.txt"}}"#;
    assert_eq!(
        execute_request(&config, &read[..])
            .unwrap()
            .content
            .as_deref(),
        Some("hello")
    );
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
