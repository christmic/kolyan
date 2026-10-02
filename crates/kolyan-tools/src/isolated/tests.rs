use super::*;

#[test]
fn advertised_inventory_contains_only_the_four_strict_environment_tools() {
    let definitions = IsolatedToolSet::tool_definitions();
    assert_eq!(
        definitions
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["file.read", "file.write", "file.edit", "shell"]
    );
    for tool in &definitions {
        assert_eq!(tool.input_schema["type"], "object");
        assert_eq!(tool.input_schema["additionalProperties"], false);
    }
    let edit = &definitions[2].input_schema;
    assert_eq!(edit["required"], json!(["path", "old_text", "new_text"]));
    assert!(edit["properties"].get("expected_sha256").is_some());
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use kolyan_core::TurnControl;
    use kolyan_policy::{
        ApprovalEvidence, ExecutionConstraints, PolicyDecision, PolicyDecisionKind, PreparedGrant,
        ToolExecutionScope,
    };
    use std::time::Duration;

    fn config(root: &std::path::Path, staging: &std::path::Path) -> IsolatedToolSetConfig {
        IsolatedToolSetConfig {
            files: IsolatedFileConfig {
                workspace: root.into(),
                staging_root: staging.into(),
                worker: std::env::current_exe().unwrap(),
                protected_roots: vec![],
                file_limits: crate::FileOperationLimits::default(),
                max_output_bytes: 4096,
                timeout: Duration::from_secs(5),
            },
            shell: IsolatedShellConfig {
                workspace: root.into(),
                protected_roots: vec![],
                max_command_bytes: 8192,
                max_output_bytes: 4096,
                timeout: Duration::from_secs(5),
            },
        }
    }

    #[test]
    fn distinct_workspaces_and_workspace_staging_are_refused() {
        let root = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let mut input = config(root.path(), staging.path());
        input.shell.workspace = other.path().into();
        assert!(matches!(
            IsolatedToolSet::new(input),
            Err(IsolatedToolSetError::Invalid(_))
        ));
        std::fs::create_dir(root.path().join("staging")).unwrap();
        assert!(matches!(
            IsolatedToolSet::new(config(root.path(), &root.path().join("staging"))),
            Err(IsolatedToolSetError::File(_))
        ));
    }

    #[tokio::test]
    async fn unknown_and_legacy_tool_names_have_no_execution_fallback() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let tools = IsolatedToolSet::new(config(root.path(), staging.path())).unwrap();
        for name in ["shell.query", "unknown", "agent.invoke"] {
            assert!(matches!(
                tools
                    .prepare(ToolCall {
                        id: "call".into(),
                        name: name.into(),
                        arguments: json!({})
                    })
                    .await,
                Err(ToolError::Unavailable { .. })
            ));
        }
    }

    #[tokio::test]
    async fn shell_cannot_read_either_adapters_control_paths_or_private_staging() {
        let root = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir().unwrap();
        let file_control = root.path().join("file-control");
        let shell_control = root.path().join("shell-control");
        for directory in [&file_control, &shell_control] {
            std::fs::create_dir(directory).unwrap();
            std::fs::write(directory.join("secret"), "NEVER_DISCLOSE").unwrap();
        }
        std::fs::write(staging.path().join("secret"), "NEVER_DISCLOSE").unwrap();
        let mut input = config(root.path(), staging.path());
        input.files.protected_roots.push(file_control.clone());
        input.shell.protected_roots.push(shell_control.clone());
        let tools = IsolatedToolSet::new(input).unwrap();
        for directory in [&file_control, &shell_control, &staging.path().to_path_buf()] {
            let command = format!(
                "/bin/cat '{}'",
                directory.join("secret").canonicalize().unwrap().display()
            );
            let prepared = tools
                .prepare(ToolCall {
                    id: "shell-call".into(),
                    name: "shell".into(),
                    arguments: json!({"command":command}),
                })
                .await
                .unwrap();
            let scope = ToolExecutionScope {
                execution: kolyan_types::ExecutionKey {
                    session_id: "s".into(),
                    turn_id: "t".into(),
                    execution_id: "e".into(),
                },
                step_id: "step".into(),
                agent_snapshot_digest: Some("a".repeat(64)),
            };
            let decision = PolicyDecision {
                kind: PolicyDecisionKind::Allow,
                reason: "trusted fixture admission".into(),
                policy_version: "fixture-v1".into(),
                constraints: ExecutionConstraints {
                    timeout_ms: None,
                    max_output_bytes: None,
                },
            };
            let grant = PreparedGrant::issue(
                &prepared,
                decision,
                ApprovalEvidence::NotConfirmed,
                scope.clone(),
            )
            .unwrap();
            let result = tools
                .execute_invocation(ToolInvocation {
                    prepared,
                    grant,
                    scope,
                    policy_revision: "fixture-v1".into(),
                    control: TurnControl::default(),
                    window: kolyan_core::ToolExecutionWindow::at_deadline(
                        std::time::Instant::now() + std::time::Duration::from_secs(30),
                    ),
                })
                .await
                .unwrap();
            let kolyan_core::ToolOutcome::Completed(result) = result else {
                panic!("isolated shell must complete without external waiting");
            };
            assert!(result.is_error);
            let content: serde_json::Value = serde_json::from_str(&result.content).unwrap();
            assert_eq!(content["stdout"]["data"], "");
            assert!(!result.content.contains("NEVER_DISCLOSE"));
        }
    }
}
