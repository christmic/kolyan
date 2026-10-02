//! Fixture-owned filesystem changes; adapters never execute a worker here.

use std::path::Path;
use std::time::Duration;

use kolyan_core::ToolExecutor;
use kolyan_model::ToolCall;
use serde_json::{Value, json};

use super::Case;
use crate::{
    FileOperationLimits, IsolatedFileConfig, IsolatedFileError, IsolatedFileTools,
    IsolatedShellConfig, IsolatedToolSet, IsolatedToolSetConfig, IsolatedToolSetError,
};

pub(super) async fn run(case: &Case) -> Value {
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path();
    for name in [
        "workspace",
        "other",
        "staging",
        "other-staging",
        "protected",
    ] {
        std::fs::create_dir(root.join(name)).unwrap();
    }
    let worker = root.join("worker");
    let worker_body = b"#!/bin/sh\necho unexpected-worker-entry > worker-ran\n";
    std::fs::write(&worker, worker_body).unwrap();
    let target = root.join("workspace/note");
    if case.action == "prepare_existing" {
        std::fs::write(&target, "original bytes 🦀").unwrap();
    }
    let config = IsolatedFileConfig {
        workspace: root.join("workspace"),
        staging_root: root.join("staging"),
        worker: worker.clone(),
        protected_roots: vec![],
        file_limits: FileOperationLimits::default(),
        max_output_bytes: 65536,
        timeout: Duration::from_secs(5),
    };
    let input = ToolCall {
        id: case.id.clone(),
        name: "file.write".into(),
        arguments: json!({"path":"note", "content":"replacement 🦀"}),
    };
    let before = observations(root);
    let files = IsolatedFileTools::new(config.clone()).unwrap();
    let baseline_configuration = configuration_of(&files);
    let configuration;
    let revision = files.adapter_revision().unwrap();
    let clone_revision = files.clone().adapter_revision().unwrap();
    let mut changed = config.clone();
    match case.action.as_str() {
        "workspace" => changed.workspace = root.join("other"),
        "staging" => changed.staging_root = root.join("other-staging"),
        "protected" => changed.protected_roots.push(root.join("protected")),
        "read_limit" => changed.file_limits.max_read_bytes += 1,
        "write_limit" => changed.file_limits.max_write_bytes += 1,
        "output_limit" => changed.max_output_bytes += 1,
        "timeout" => changed.timeout += Duration::from_millis(1),
        "worker_bytes" => std::fs::write(&worker, b"changed worker bytes").unwrap(),
        "same_worker_bytes" => std::fs::write(&worker, worker_body).unwrap(),
        "worker_removed" => std::fs::remove_file(&worker).unwrap(),
        _ => {}
    }
    let mut merged_expected = None;
    let (after_revision, error, prepared, prepare_error, revision, clone_revision) = if case
        .action
        .starts_with("merged_")
        || case.action == "shell_only_limits"
    {
        let shell = IsolatedShellConfig {
            workspace: config.workspace.clone(),
            protected_roots: vec![],
            max_command_bytes: 8192,
            max_output_bytes: 4096,
            timeout: Duration::from_secs(5),
        };
        let mut set_config = IsolatedToolSetConfig {
            files: config,
            shell,
        };
        let baseline = IsolatedToolSet::new(set_config.clone()).unwrap();
        let baseline_revision = baseline.file_adapter_revision().unwrap();
        let clone = baseline.clone().file_adapter_revision().unwrap();
        match case.action.as_str() {
            "merged_shell_protected" => set_config
                .shell
                .protected_roots
                .push(root.join("protected")),
            "merged_file_protected" => set_config
                .files
                .protected_roots
                .push(root.join("protected")),
            "merged_duplicate_roots" => {
                set_config.files.protected_roots.push(root.join("staging"));
                set_config.shell.protected_roots.push(root.join("staging"));
            }
            "shell_only_limits" => set_config.shell.max_output_bytes += 1,
            "merged_worker_removed" => {}
            _ => unreachable!(),
        }
        let tools = IsolatedToolSet::new(set_config.clone()).unwrap();
        // Independently assemble the explicit union, not the pre-merge file config.
        let mut expected_config = set_config.files;
        expected_config
            .protected_roots
            .extend(set_config.shell.protected_roots);
        expected_config
            .protected_roots
            .push(expected_config.staging_root.clone());
        expected_config.protected_roots = expected_config
            .protected_roots
            .into_iter()
            .map(|path| path.canonicalize().unwrap())
            .collect();
        expected_config.protected_roots.sort();
        expected_config.protected_roots.dedup();
        let expected = IsolatedFileTools::new(expected_config).unwrap();
        configuration = configuration_of(&expected);
        if case.action == "merged_worker_removed" {
            std::fs::remove_file(&worker).unwrap();
        } else {
            merged_expected = Some(expected.adapter_revision().unwrap());
        }
        let result = tools.file_adapter_revision().map_err(|error| match error {
            IsolatedToolSetError::File(error) => error,
            _ => panic!("unexpected forwarding error"),
        });
        let (value, error) = revision_result(result);
        // Use the real public async ToolSet preparation entrypoint.
        let (prepared, prepare_error) = match tools.prepare(input.clone()).await {
            Ok(prepared) => (serde_json::to_value(prepared).unwrap(), None),
            Err(kolyan_core::ToolError::Failed { message }) if case.read_error => (
                Value::Null,
                Some(json!({"kind":"failed", "message":message})),
            ),
            Err(error) => panic!("unexpected tool preparation error: {error:?}"),
        };
        (
            value,
            error,
            prepared,
            prepare_error,
            baseline_revision,
            clone,
        )
    } else {
        let current = if matches!(
            case.action.as_str(),
            "worker_removed" | "worker_bytes" | "same_worker_bytes"
        ) {
            files.clone()
        } else {
            IsolatedFileTools::new(changed).unwrap()
        };
        configuration = configuration_of(&current);
        let (value, error) = revision_result(current.adapter_revision());
        let (prepared, prepare_error) = match current.prepare(input.clone()) {
            Ok(prepared) => (serde_json::to_value(prepared).unwrap(), None),
            Err(IsolatedFileError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => (
                Value::Null,
                Some(json!({"kind":"io_not_found", "message":error.to_string()})),
            ),
            Err(error) => panic!("unexpected preparation error: {error}"),
        };
        (
            value,
            error,
            prepared,
            prepare_error,
            revision,
            clone_revision,
        )
    };
    json!({"case_id":case.id, "action":case.action,
        "baseline_configuration":baseline_configuration, "configuration":configuration,
        "input":input, "revision":revision, "clone_revision":clone_revision,
        "after_revision":after_revision, "error":error, "prepared":prepared,
        "prepare_error":prepare_error, "merged_expected":merged_expected,
        "worker_exists":worker.exists(), "worker_bytes":std::fs::read(&worker).ok(),
        "before":before, "after":observations(root)})
}

fn revision_result(
    result: Result<String, IsolatedFileError>,
) -> (Option<String>, Option<&'static str>) {
    match result {
        Ok(value) => (Some(value), None),
        Err(IsolatedFileError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            (None, Some("io_not_found"))
        }
        Err(error) => panic!("unexpected revision error: {error}"),
    }
}

fn configuration_of(files: &IsolatedFileTools) -> Value {
    let config = &files.config;
    json!({"workspace":config.workspace, "staging_root":config.staging_root,
        "worker":config.worker, "protected_roots":config.protected_roots,
        "staging_binding":files.staging_binding,
        "max_read_bytes":config.file_limits.max_read_bytes,
        "max_write_bytes":config.file_limits.max_write_bytes,
        "max_output_bytes":config.max_output_bytes, "timeout_ms":config.timeout.as_millis()})
}

fn observations(root: &Path) -> Value {
    let directories = [
        "workspace",
        "other",
        "staging",
        "other-staging",
        "protected",
    ];
    let entries: Vec<_> = directories
        .into_iter()
        .map(|directory| {
            let mut entries: Vec<_> = std::fs::read_dir(root.join(directory))
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            entries.sort();
            json!({"directory":directory, "entries":entries})
        })
        .collect();
    json!({"entries":entries, "target":std::fs::read(root.join("workspace/note")).ok(),
        "worker_ran":root.join("worker-ran").exists()})
}
