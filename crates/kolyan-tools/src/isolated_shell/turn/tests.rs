use super::*;

#[test]
fn exact_bytes_and_worst_framing_bound_complete_results() {
    let call_id = "quotes\" slash\\ controls\n\0\u{0001} Unicode🦀";
    for exit_code in [Some(0), Some(7), Some(i32::MIN), Some(i32::MAX), None] {
        let limit = 2048;
        let cap = raw_cap(call_id, limit).unwrap();
        for byte in (0..=31).chain([b'"', b'\\', b'a', 255]) {
            let stdout = vec![byte; cap / 2];
            let stderr = vec![byte; cap - stdout.len()];
            let output = result(
                call_id,
                SandboxOutput {
                    exit_code,
                    stdout: stdout.clone(),
                    stderr: stderr.clone(),
                },
            )
            .unwrap();
            assert!(envelope_len(&output).unwrap() <= limit);
            assert!(cap * 8 <= limit);
            if byte == 0 {
                let empty = result(
                    call_id,
                    SandboxOutput {
                        exit_code,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    },
                )
                .unwrap();
                assert_eq!(
                    envelope_len(&output).unwrap() - envelope_len(&empty).unwrap(),
                    cap * 7
                );
            }
            assert_eq!(output.is_error, exit_code != Some(0));
            let decoded: ToolResult =
                serde_json::from_slice(&serde_json::to_vec(&output).unwrap()).unwrap();
            assert_eq!(decoded.call_id, call_id);
            let content: serde_json::Value = serde_json::from_str(&decoded.content).unwrap();
            assert_eq!(stream_bytes(&content["stdout"]), stdout);
            assert_eq!(stream_bytes(&content["stderr"]), stderr);
        }
    }
    assert_eq!(hex(&[0, 1, 15, 16, 127, 128, 255]), "00010f107f80ff");
    assert!(raw_cap(call_id, 1).is_err());
}

fn stream_bytes(value: &serde_json::Value) -> Vec<u8> {
    let data = value["data"].as_str().unwrap();
    match value["encoding"].as_str().unwrap() {
        "utf8" => data.as_bytes().to_vec(),
        "hex" => {
            assert_eq!(data.len() % 2, 0);
            (0..data.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(&data[index..index + 2], 16).unwrap())
                .collect()
        }
        other => panic!("unexpected stream encoding {other}"),
    }
}

#[test]
fn stream_tags_preserve_readable_unicode_and_every_binary_byte() {
    let text = "你好🦀\nquotes\" backslash\\ control\0";
    let binary: Vec<_> = (0..=255).collect();
    let output = result(
        "mixed",
        SandboxOutput {
            exit_code: Some(7),
            stdout: text.as_bytes().to_vec(),
            stderr: binary.clone(),
        },
    )
    .unwrap();
    let content: serde_json::Value = serde_json::from_str(&output.content).unwrap();
    assert_eq!(
        content["stdout"],
        serde_json::json!({"encoding":"utf8", "data":text})
    );
    assert_eq!(content["stderr"]["encoding"], "hex");
    assert_eq!(stream_bytes(&content["stderr"]), binary);
    assert!(output.is_error);
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    mod observation;
    use crate::IsolatedShellConfig;
    use kolyan_core::TurnControl;
    use kolyan_policy::{
        ApprovalEvidence, ExecutionConstraints, PolicyDecision, PolicyDecisionKind, PreparedGrant,
        ToolExecutionScope,
    };
    use serde_json::json;
    use std::path::Path;
    use std::time::{Duration, Instant};

    fn setup() -> (tempfile::TempDir, IsolatedShellTool) {
        let root = tempfile::tempdir().unwrap();
        let tool = IsolatedShellTool::new(IsolatedShellConfig {
            workspace: root.path().into(),
            protected_roots: vec![],
            max_command_bytes: 8192,
            max_output_bytes: 8192,
            timeout: Duration::from_secs(5),
        })
        .unwrap();
        (root, tool)
    }

    async fn invocation(
        tool: &IsolatedShellTool,
        command: &str,
        limit: Option<u64>,
    ) -> ToolInvocation {
        let prepared = ToolExecutor::prepare(
            tool,
            ToolCall {
                id: "shell-quoted\"\\\n🦀".into(),
                name: "shell".into(),
                arguments: json!({"command": command}),
            },
        )
        .await
        .unwrap();
        let scope = ToolExecutionScope {
            execution: kolyan_types::ExecutionKey {
                session_id: "session".into(),
                execution_id: "execution".into(),
                turn_id: "turn".into(),
            },
            step_id: "step".into(),
            agent_snapshot_digest: Some("a".repeat(64)),
        };
        // This fixture is the trusted host. Production adapter never issues grants.
        let grant = PreparedGrant::issue(
            &prepared,
            PolicyDecision {
                kind: PolicyDecisionKind::AllowWithConstraints,
                reason: "fixture admission".into(),
                policy_version: "shell-turn-policy".into(),
                constraints: ExecutionConstraints {
                    max_output_bytes: limit,
                    timeout_ms: None,
                },
            },
            ApprovalEvidence::NotConfirmed,
            scope.clone(),
        )
        .unwrap();
        ToolInvocation {
            prepared,
            grant,
            scope,
            policy_revision: "shell-turn-policy".into(),
            control: TurnControl::default(),
            window: kolyan_core::ToolExecutionWindow::at_deadline(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
            ),
        }
    }

    async fn observed(
        tool: &dyn ToolExecutor,
        invocation: ToolInvocation,
    ) -> Result<ToolResult, ToolError> {
        let trace = tempfile::Builder::new()
            .prefix("kolyan-shell-turn-")
            .tempdir()
            .unwrap()
            .keep();
        let admitted = json!({
            "prepared": invocation.prepared, "grant": invocation.grant,
            "scope": invocation.scope, "policy_revision": invocation.policy_revision,
            "already_cancelled": invocation.control.is_cancelled(),
        });
        let result = tool.execute_invocation(invocation).await;
        std::fs::write(
            trace.join("actual.json"),
            serde_json::to_vec_pretty(&json!({
                "admitted": admitted, "result": result,
            }))
            .unwrap(),
        )
        .unwrap();
        eprintln!("shell Turn trace: {}", trace.display());
        result.map(|outcome| match outcome {
            kolyan_core::ToolOutcome::Completed(result) => result,
            kolyan_core::ToolOutcome::AwaitingExternal(wait) => {
                panic!("shell unexpectedly suspended: {wait:?}")
            }
        })
    }

    #[tokio::test]
    async fn real_process_and_binary_nonzero_results_are_lossless() {
        let (root, tool) = setup();
        for (command, stdout_encoding, stdout, stderr_encoding, stderr, exit_code, is_error) in [
            (
                "printf real > effect; /bin/cat effect",
                "utf8",
                "real",
                "utf8",
                "",
                0,
                false,
            ),
            (
                "printf '\\000\\377\\200'; printf '\\376\\012' >&2; exit 7",
                "hex",
                "00ff80",
                "hex",
                "fe0a",
                7,
                true,
            ),
            (
                "printf '你好🦀\\n'; printf '\\377' >&2",
                "utf8",
                "你好🦀\n",
                "hex",
                "ff",
                0,
                false,
            ),
        ] {
            let input = invocation(&tool, command, None).await;
            let call_id = input.prepared.call().id.clone();
            let output = observed(&tool, input).await.unwrap();
            assert_eq!(output.call_id, call_id);
            assert_eq!(output.is_error, is_error);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&output.content).unwrap(),
                json!({
                    "exit_code":exit_code,
                    "stdout":{"encoding":stdout_encoding, "data":stdout},
                    "stderr":{"encoding":stderr_encoding, "data":stderr},
                })
            );
        }
        assert_eq!(std::fs::read(root.path().join("effect")).unwrap(), b"real");
    }

    #[tokio::test]
    async fn already_cancelled_has_no_process_effects() {
        let (root, tool) = setup();
        let input = invocation(&tool, "printf forbidden > effect", None).await;
        input.control.cancel();
        assert_eq!(observed(&tool, input).await, Err(ToolError::Cancelled));
        assert!(!root.path().join("effect").exists());
    }

    async fn await_file(path: &Path) -> String {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Ok(text) = std::fs::read_to_string(path)
                && !text.trim().is_empty()
            {
                return text.trim().to_owned();
            }
            assert!(
                Instant::now() < deadline,
                "process did not publish {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn status(pid: &str) -> String {
        let output = std::process::Command::new("/bin/ps")
            .args(["-o", "stat=", "-p", pid])
            .output()
            .unwrap();
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn running_cancel_waits_for_leader_reap_and_stops_descendants() {
        let (root, tool) = setup();
        let input = invocation(
            &tool,
            "/bin/sleep 30 & printf '%s' $! > child; printf '%s' $$ > leader; wait",
            None,
        )
        .await;
        let control = input.control.clone();
        let execution = tokio::spawn(async move { observed(&tool, input).await });
        let leader = await_file(&root.path().join("leader")).await;
        let child = await_file(&root.path().join("child")).await;
        assert!(!status(&leader).is_empty());
        control.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), execution)
                .await
                .unwrap()
                .unwrap(),
            Err(ToolError::Cancelled)
        );
        assert!(
            status(&leader).is_empty(),
            "cancel returned before leader reap"
        );
        let child_status = status(&child);
        assert!(
            child_status.is_empty() || child_status.starts_with('Z'),
            "descendant still running: {child_status}"
        );
    }

    #[tokio::test]
    async fn foreign_scope_and_policy_are_denied_before_effects() {
        let (root, tool) = setup();
        for mutation in ["scope", "policy", "preparation"] {
            let mut input = invocation(&tool, "printf forbidden > effect", None).await;
            match mutation {
                "scope" => input.scope.execution.session_id = "foreign".into(),
                "policy" => input.policy_revision = "changed-policy".into(),
                "preparation" => {
                    input.prepared = ToolExecutor::prepare(
                        &tool,
                        ToolCall {
                            id: input.prepared.call().id.clone(),
                            name: "shell".into(),
                            arguments: json!({"command":"printf changed > effect"}),
                        },
                    )
                    .await
                    .unwrap()
                }
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    observed(&tool, input).await,
                    Err(ToolError::PolicyDenied { .. })
                ),
                "{mutation}"
            );
            assert!(!root.path().join("effect").exists());
        }
    }

    #[tokio::test]
    async fn result_cap_accounts_for_full_envelope_and_refuses_overflow() {
        let (root, tool) = setup();
        let limit = 512;
        let reference = invocation(&tool, "printf small", Some(limit)).await;
        let cap = raw_cap(&reference.prepared.call().id, limit as usize).unwrap();
        assert!(cap < limit as usize);
        let command = format!("i=0; while [ $i -lt {cap} ]; do printf '\\000'; i=$((i+1)); done");
        let output = observed(&tool, invocation(&tool, &command, Some(limit)).await)
            .await
            .unwrap();
        assert!(serde_json::to_vec(&output).unwrap().len() <= limit as usize);
        let content: serde_json::Value = serde_json::from_str(&output.content).unwrap();
        assert_eq!(
            content["stdout"],
            json!({"encoding":"utf8", "data":"\0".repeat(cap)})
        );
        let overflow = format!("{command}; printf '\\377' >&2");
        assert_eq!(
            observed(&tool, invocation(&tool, &overflow, Some(limit)).await).await,
            Err(ToolError::Failed {
                message: SandboxError::OutputLimit.to_string(),
            })
        );
        assert!(matches!(
            observed(
                &tool,
                invocation(&tool, "printf bad > effect", Some(1)).await
            )
            .await,
            Err(ToolError::Failed { .. })
        ));
        assert!(!root.path().join("effect").exists());
    }

    #[tokio::test]
    async fn dropping_turn_future_delegates_cleanup_to_reaper() {
        let (root, tool) = setup();
        let input = invocation(&tool, "printf '%s' $$ > leader; /bin/sleep 30", None).await;
        let execution = tokio::spawn(async move { observed(&tool, input).await });
        let leader = await_file(&root.path().join("leader")).await;
        execution.abort();
        assert!(execution.await.unwrap_err().is_cancelled());
        let deadline = Instant::now() + Duration::from_secs(3);
        while !status(&leader).is_empty() {
            assert!(Instant::now() < deadline, "drop abandoned shell leader");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
