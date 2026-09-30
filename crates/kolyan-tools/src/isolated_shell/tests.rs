use super::*;

#[cfg(not(target_os = "macos"))]
#[test]
fn unsupported_platform_has_no_execution_fallback() {
    let root = tempfile::tempdir().unwrap();
    assert!(matches!(
        IsolatedShellTool::new(config(root.path())),
        Err(IsolatedShellError::Sandbox(SandboxError::Unsupported))
    ));
}

fn config(root: &Path) -> IsolatedShellConfig {
    IsolatedShellConfig {
        workspace: root.into(),
        protected_roots: vec![],
        max_command_bytes: 8192,
        max_output_bytes: 8192,
        timeout: Duration::from_secs(5),
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;

    use kolyan_policy::{
        ApprovalEvidence, ExecutionConstraints, PolicyDecision, PolicyDecisionKind,
    };
    use serde_json::json;

    fn setup() -> (tempfile::TempDir, IsolatedShellTool) {
        let root = tempfile::tempdir().unwrap();
        let tool = IsolatedShellTool::new(config(root.path())).unwrap();
        (root, tool)
    }

    fn call(command: &str) -> ToolCall {
        ToolCall {
            id: "shell-call-1".into(),
            name: "shell".into(),
            arguments: json!({"command":command}),
        }
    }

    fn grant(prepared: &PreparedCall) -> PreparedGrant {
        issue(prepared, None, None)
    }

    fn issue(prepared: &PreparedCall, output: Option<u64>, timeout: Option<u64>) -> PreparedGrant {
        PreparedGrant::issue(
            prepared,
            PolicyDecision {
                kind: PolicyDecisionKind::AllowWithConstraints,
                reason: "test host admission".into(),
                policy_version: "policy-v1".into(),
                constraints: ExecutionConstraints {
                    max_output_bytes: output,
                    timeout_ms: timeout,
                },
            },
            ApprovalEvidence::NotConfirmed,
        )
        .unwrap()
    }

    #[test]
    fn declares_conservative_claims_even_for_printf() {
        let (root, tool) = setup();
        let prepared = tool.prepare(call("printf harmless")).unwrap();
        assert_eq!(
            prepared.claim().capabilities,
            [
                Capability::FilesystemRead,
                Capability::FilesystemWrite,
                Capability::ProcessExecute
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(
            prepared.claim().effects,
            [
                Effect::Read,
                Effect::Create,
                Effect::Update,
                Effect::Delete,
                Effect::Execute
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(prepared.claim().idempotency, Idempotency::NonIdempotent);
        assert_eq!(
            prepared.claim().resource.path.as_deref(),
            root.path().canonicalize().unwrap().to_str()
        );
        assert!(prepared.requirements().process_sandbox);
        assert!(prepared.tool_revision().starts_with("isolated-shell-v1/"));
        assert_eq!(
            prepared.digest(),
            tool.prepare(call("printf harmless")).unwrap().digest()
        );
        assert_eq!(IsolatedShellTool::tool_definition().name, "shell");
    }

    #[test]
    fn rejects_unknown_fields_names_invalid_commands_and_cwd() {
        let (_root, tool) = setup();
        for arguments in [
            json!({"command":"true", "environment":{"SECRET":"x"}}),
            json!({"command":"true", "extra":1}),
            json!({"command":7}),
            json!({"command":""}),
            json!({"command":"  "}),
            json!({"command":"a\0b"}),
            json!({"command":"x".repeat(8193)}),
            json!({"command":"true", "path":"/tmp"}),
            json!({"command":"true", "path":"../outside"}),
            json!({"command":"true", "path":"sub/../a"}),
            json!({"command":"true", "path":"missing"}),
            json!({"command":"true", "path":""}),
            json!({"command":"true", "path":"a\0b"}),
        ] {
            let mut input = call("unused");
            input.arguments = arguments.clone();
            assert!(tool.prepare(input).is_err(), "accepted {arguments:?}");
        }
        let mut wrong_name = call("true");
        wrong_name.name = "shell.query".into();
        assert!(tool.prepare(wrong_name).is_err());
    }

    #[test]
    fn rejects_invalid_host_limits_and_roots() {
        let root = tempfile::tempdir().unwrap();
        let base = config(root.path());
        for bad in [
            IsolatedShellConfig {
                max_command_bytes: 0,
                ..base.clone()
            },
            IsolatedShellConfig {
                max_output_bytes: 0,
                ..base.clone()
            },
            IsolatedShellConfig {
                max_command_bytes: MAX_COMMAND_BYTES + 1,
                ..base.clone()
            },
            IsolatedShellConfig {
                max_output_bytes: MAX_HOST_BYTES + 1,
                ..base.clone()
            },
            IsolatedShellConfig {
                timeout: Duration::ZERO,
                ..base.clone()
            },
            IsolatedShellConfig {
                timeout: Duration::from_nanos(1),
                ..base.clone()
            },
            IsolatedShellConfig {
                workspace: "/".into(),
                ..base.clone()
            },
            IsolatedShellConfig {
                workspace: root.path().join("missing"),
                ..base
            },
        ] {
            assert!(IsolatedShellTool::new(bad).is_err());
        }
    }

    #[test]
    fn rejects_outside_and_protected_cwd_symlinks() {
        use std::os::unix::fs::symlink;
        let (root, _tool) = setup();
        let outside = tempfile::tempdir().unwrap();
        for name in ["private", ".git", ".kolyan"] {
            std::fs::create_dir(root.path().join(name)).unwrap();
        }
        symlink(outside.path(), root.path().join("escape")).unwrap();
        symlink("private", root.path().join("alias")).unwrap();
        let mut configuration = config(root.path());
        configuration
            .protected_roots
            .push(root.path().join("private"));
        let tool = IsolatedShellTool::new(configuration).unwrap();
        for path in ["escape", "private", "alias", ".git", ".kolyan"] {
            let mut input = call("true");
            input.arguments["path"] = json!(path);
            assert!(tool.prepare(input).is_err(), "accepted cwd {path}");
        }
    }

    #[tokio::test]
    async fn actual_execution_supports_cwd_streams_and_raw_exit() {
        let (root, tool) = setup();
        std::fs::create_dir(root.path().join("sub")).unwrap();
        let cases = [
            (
                "printf content > output; /bin/cat output; printf err >&2",
                0,
                "content",
                "err",
            ),
            ("printf failure >&2; exit 7", 7, "", "failure"),
        ];
        let logs = tempfile::tempdir().unwrap();
        for (index, (command, code, stdout, stderr)) in cases.into_iter().enumerate() {
            let mut input = call(command);
            input.arguments["path"] = json!("sub");
            let prepared = tool.prepare(input).unwrap();
            std::fs::write(
                logs.path().join(format!("{index}-request.json")),
                serde_json::to_vec(&prepared).unwrap(),
            )
            .unwrap();
            let output = tool
                .execute(
                    &prepared,
                    &grant(&prepared),
                    "policy-v1",
                    SandboxCancellation::default(),
                )
                .await
                .unwrap();
            std::fs::write(logs.path().join(format!("{index}-stdout")), &output.stdout).unwrap();
            std::fs::write(logs.path().join(format!("{index}-stderr")), &output.stderr).unwrap();
            assert_eq!(output.exit_code, Some(code));
            assert_eq!(output.stdout, stdout.as_bytes());
            assert_eq!(output.stderr, stderr.as_bytes());
        }
        assert_eq!(
            std::fs::read(root.path().join("sub/output")).unwrap(),
            b"content"
        );
        assert!(!root.path().join("output").exists());
    }

    #[tokio::test]
    async fn stale_policy_input_revision_and_forged_claims_have_no_effects() {
        let (root, tool) = setup();
        let prepared = tool.prepare(call("printf bad > forbidden")).unwrap();
        let authority = grant(&prepared);
        assert!(matches!(
            tool.execute(
                &prepared,
                &authority,
                "policy-v2",
                SandboxCancellation::default()
            )
            .await,
            Err(IsolatedShellError::Prepared(PreparedError::BindingMismatch))
        ));
        let changed = tool.prepare(call("printf changed > forbidden")).unwrap();
        assert!(matches!(
            tool.execute(
                &changed,
                &authority,
                "policy-v1",
                SandboxCancellation::default()
            )
            .await,
            Err(IsolatedShellError::Prepared(PreparedError::BindingMismatch))
        ));
        let mut claim = prepared.claim().clone();
        claim.capabilities = [Capability::ProcessExecute].into_iter().collect();
        let forged = PreparedCall::new(
            prepared.call().clone(),
            prepared.tool_revision().into(),
            claim,
            prepared.requirements().clone(),
        )
        .unwrap();
        assert!(matches!(
            tool.execute(
                &forged,
                &grant(&forged),
                "policy-v1",
                SandboxCancellation::default()
            )
            .await,
            Err(IsolatedShellError::Prepared(PreparedError::BindingMismatch))
        ));
        let changed_revision = PreparedCall::new(
            prepared.call().clone(),
            "wrong-shell-version".into(),
            prepared.claim().clone(),
            prepared.requirements().clone(),
        )
        .unwrap();
        assert!(matches!(
            tool.execute(
                &changed_revision,
                &grant(&changed_revision),
                "policy-v1",
                SandboxCancellation::default()
            )
            .await,
            Err(IsolatedShellError::Prepared(PreparedError::BindingMismatch))
        ));
        assert!(!root.path().join("forbidden").exists());
    }

    #[tokio::test]
    async fn changed_cwd_binding_and_host_protections_invalidate_grants() {
        use std::os::unix::fs::symlink;
        let (root, tool) = setup();
        for name in ["first", "second", "private"] {
            std::fs::create_dir(root.path().join(name)).unwrap();
        }
        symlink("first", root.path().join("cwd")).unwrap();
        let mut input = call("printf bad > forbidden");
        input.arguments["path"] = json!("cwd");
        let prepared = tool.prepare(input).unwrap();
        std::fs::remove_file(root.path().join("cwd")).unwrap();
        symlink("second", root.path().join("cwd")).unwrap();
        assert!(matches!(
            tool.execute(
                &prepared,
                &grant(&prepared),
                "policy-v1",
                SandboxCancellation::default()
            )
            .await,
            Err(IsolatedShellError::Prepared(PreparedError::BindingMismatch))
        ));
        let prepared = tool.prepare(call("printf bad > forbidden")).unwrap();
        let mut configuration = config(root.path());
        configuration
            .protected_roots
            .push(root.path().join("private"));
        let replacement = IsolatedShellTool::new(configuration).unwrap();
        assert!(matches!(
            replacement
                .execute(
                    &prepared,
                    &grant(&prepared),
                    "policy-v1",
                    SandboxCancellation::default()
                )
                .await,
            Err(IsolatedShellError::Prepared(PreparedError::BindingMismatch))
        ));
        assert!(!root.path().join("first/forbidden").exists());
        assert!(!root.path().join("second/forbidden").exists());
        assert!(!root.path().join("forbidden").exists());
    }

    #[tokio::test]
    async fn enforces_granted_output_timeout_and_precancel_limits() {
        let (root, tool) = setup();
        let prepared = tool
            .prepare(call("while :; do printf 1234567890; done"))
            .unwrap();
        assert!(matches!(
            tool.execute(
                &prepared,
                &issue(&prepared, Some(64), None),
                "policy-v1",
                SandboxCancellation::default()
            )
            .await,
            Err(IsolatedShellError::Sandbox(SandboxError::OutputLimit))
        ));
        let prepared = tool.prepare(call("/bin/sleep 10")).unwrap();
        assert!(matches!(
            tool.execute(
                &prepared,
                &issue(&prepared, None, Some(100)),
                "policy-v1",
                SandboxCancellation::default()
            )
            .await,
            Err(IsolatedShellError::Sandbox(SandboxError::Timeout))
        ));
        let prepared = tool.prepare(call("printf bad > forbidden")).unwrap();
        let cancelled = SandboxCancellation::default();
        cancelled.cancel();
        assert!(matches!(
            tool.execute(&prepared, &grant(&prepared), "policy-v1", cancelled)
                .await,
            Err(IsolatedShellError::Sandbox(SandboxError::Cancelled))
        ));
        assert!(!root.path().join("forbidden").exists());
    }

    #[tokio::test]
    async fn actual_sandbox_denies_outside_and_control_paths() {
        let (root, _tool) = setup();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), "secret-not-visible").unwrap();
        std::fs::create_dir(root.path().join("private")).unwrap();
        std::fs::write(root.path().join("private/state"), "untouched").unwrap();
        let mut configuration = config(root.path());
        configuration
            .protected_roots
            .push(root.path().join("private"));
        let tool = IsolatedShellTool::new(configuration).unwrap();
        for command in [
            format!("/bin/cat '{}'", outside.path().join("secret").display()),
            "printf bad > private/state".into(),
            "mkdir .git".into(),
        ] {
            let prepared = tool.prepare(call(&command)).unwrap();
            let output = tool
                .execute(
                    &prepared,
                    &grant(&prepared),
                    "policy-v1",
                    SandboxCancellation::default(),
                )
                .await
                .unwrap();
            assert_ne!(output.exit_code, Some(0));
            assert!(!String::from_utf8_lossy(&output.stdout).contains("secret-not-visible"));
        }
        assert_eq!(
            std::fs::read(root.path().join("private/state")).unwrap(),
            b"untouched"
        );
        assert!(!root.path().join(".git").exists());
    }
}
