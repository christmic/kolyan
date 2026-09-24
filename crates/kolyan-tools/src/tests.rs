use super::*;
use futures_util::FutureExt;
use std::fs;
use std::sync::Arc;

#[cfg(unix)]
#[test]
fn rejects_symlink_escape_for_reads_writes_and_queries() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("protected"), "unchanged").unwrap();
    symlink(outside.path(), root.path().join("escape")).unwrap();
    symlink(outside.path().join("protected"), root.path().join("file")).unwrap();
    let file = RestrictedFileTool::new(root.path());
    for path in ["file", "escape/protected", "escape/new-file"] {
        assert!(
            file.run_file("file.read", &serde_json::json!({"path":path}))
                .is_err()
        );
        assert!(
            file.run_file(
                "file.write",
                &serde_json::json!({"path":path,"content":"bad"})
            )
            .is_err()
        );
    }
    let shell = RestrictedShellTool::new(root.path());
    for command in ["list", "count_entries", "count_lines"] {
        assert!(
            shell
                .run_query(&serde_json::json!({"command":command,"path":"escape"}))
                .is_err()
        );
    }
    assert_eq!(
        fs::read_to_string(outside.path().join("protected")).unwrap(),
        "unchanged"
    );
    assert!(!outside.path().join("new-file").exists());
}

#[cfg(unix)]
#[test]
fn retained_root_handle_does_not_follow_replaced_root_path() {
    use std::os::unix::fs::symlink;
    let parent = tempfile::tempdir().unwrap();
    let original = parent.path().join("root");
    let moved = parent.path().join("moved");
    let outside = tempfile::tempdir().unwrap();
    fs::create_dir(&original).unwrap();
    let tool = RestrictedFileTool::new(&original);
    fs::rename(&original, &moved).unwrap();
    symlink(outside.path(), &original).unwrap();
    tool.run_file(
        "file.write",
        &serde_json::json!({"path":"note","content":"inside"}),
    )
    .unwrap();
    assert_eq!(fs::read_to_string(moved.join("note")).unwrap(), "inside");
    assert!(!outside.path().join("note").exists());
}

#[cfg(unix)]
#[test]
fn relative_symlink_inside_workspace_remains_usable() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("note"), "inside").unwrap();
    std::os::unix::fs::symlink("note", root.path().join("alias")).unwrap();
    assert_eq!(
        RestrictedFileTool::new(root.path())
            .run_file("file.read", &serde_json::json!({"path":"alias"}))
            .unwrap(),
        "inside"
    );
}

#[test]
fn exposes_only_restricted_query_commands() {
    let definition = RestrictedShellTool::tool_definition();
    assert_eq!(definition.name, "shell.query");
    assert!(
        definition.input_schema["properties"]["command"]["enum"]
            .as_array()
            .is_some_and(|commands| commands.len() == 4)
    );
}

#[test]
fn counts_lines_without_executing_a_shell() {
    let root = std::env::temp_dir().join(format!("kolyan-tool-{}", std::process::id()));
    fs::create_dir_all(&root).expect("temporary root should be created");
    fs::write(root.join("input.txt"), "one\ntwo\n").expect("fixture should be written");
    let tool = RestrictedShellTool::new(&root);
    let result = tool
        .execute(ToolCall {
            id: "call-1".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command":"count_lines","path":"input.txt"}),
        })
        .now_or_never()
        .expect("query should complete")
        .expect("query should succeed");

    assert_eq!(result.content, "2");
    assert!(!result.is_error);
    fs::remove_file(root.join("input.txt")).expect("fixture should be removed");
    fs::remove_dir(root).expect("temporary root should be removed");
}

#[test]
fn rejects_parent_paths() {
    let tool = RestrictedShellTool::new(std::env::temp_dir());
    let result = tool
        .execute(ToolCall {
            id: "call-2".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command":"list","path":"../"}),
        })
        .now_or_never()
        .expect("query should complete")
        .expect("tool should return a ToolResult error");

    assert!(result.is_error);
}

#[test]
fn reads_and_writes_workspace_files() {
    let root = std::env::temp_dir().join(format!("kolyan-file-tool-{}", std::process::id()));
    fs::create_dir_all(&root).expect("temporary root should be created");
    let tool = RestrictedFileTool::new(&root);

    let write = tool
        .execute(ToolCall {
            id: "write-1".into(),
            name: "file.write".into(),
            arguments: serde_json::json!({"path":"note.txt","content":"hello"}),
        })
        .now_or_never()
        .expect("write should complete")
        .expect("write should return a result");
    assert_eq!(write.content, "wrote 5 bytes");
    assert!(!write.is_error);

    let read = tool
        .execute(ToolCall {
            id: "read-1".into(),
            name: "file.read".into(),
            arguments: serde_json::json!({"path":"note.txt"}),
        })
        .now_or_never()
        .expect("read should complete")
        .expect("read should return a result");
    assert_eq!(read.content, "hello");
    assert!(!read.is_error);

    fs::remove_file(root.join("note.txt")).expect("fixture should be removed");
    fs::remove_dir(root).expect("temporary root should be removed");
}

#[test]
fn file_tool_rejects_escape_and_unknown_calls() {
    let tool = RestrictedFileTool::new(std::env::temp_dir());
    for (id, name, arguments) in [
        (
            "escape",
            "file.read",
            serde_json::json!({"path":"../outside.txt"}),
        ),
        (
            "unknown",
            "shell.exec",
            serde_json::json!({"command":"pwd"}),
        ),
    ] {
        let result = tool
            .execute(ToolCall {
                id: id.into(),
                name: name.into(),
                arguments,
            })
            .now_or_never()
            .expect("file tool should complete")
            .expect("file tool should return a result");
        assert!(result.is_error);
    }
}

#[test]
fn policy_wrapper_denies_unregistered_calls_before_execution() {
    let mut policy = kolyan_policy::PolicyEngine::default();
    policy.register(RestrictedFileTool::tool_manifests().remove(0));
    let tool = PolicyEnforcingTool::new(
        RestrictedFileTool::new(std::env::temp_dir()),
        Arc::new(policy),
    );
    let result = tool
        .execute(ToolCall {
            id: "denied".into(),
            name: "file.write".into(),
            arguments: serde_json::json!({"path":"a.txt","content":"x"}),
        })
        .now_or_never()
        .expect("policy decision should complete")
        .expect_err("write must be denied");
    assert!(matches!(result, ToolError::PolicyDenied { .. }));
}
