//! Real sibling shell mutation between file preparation and trusted worker launch.
//! This probes pinned plans, not model behavior or a universal hostile-host guarantee.
#![cfg(target_os = "macos")]

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalEvidence, ExecutionConstraints, PolicyDecision, PolicyDecisionKind, PreparedGrant,
    ToolExecutionScope,
};
use kolyan_sandbox::{
    FileSandboxConfig, MacOsSandbox, SandboxCancellation, SandboxCommand, SandboxRequest,
};
use kolyan_tools::{
    ExactFileBinding, ExactFileStaging, ExactFileWorkerRequest, FileOperation, FileOperationResult,
    IsolatedShellConfig, IsolatedShellTool,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    expected_rows: usize,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    operation: FileOperation,
    mutation_command: Option<String>,
    expected_stage: String,
    expected_target: String,
}

#[tokio::test]
async fn exact_worker_refuses_resources_rebound_by_a_real_sibling_shell() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("fixtures/exact_file_boundary.json")).unwrap();
    let report = tempfile::Builder::new()
        .prefix("kolyan-exact-file-boundary-")
        .tempdir()
        .unwrap()
        .keep();
    let mut trajectory = std::fs::File::create(report.join("actual.jsonl")).unwrap();
    println!(
        "exact file boundary trajectory: {}",
        report.join("actual.jsonl").display()
    );
    assert_eq!(dataset.cases.len(), dataset.expected_rows);
    let mut ids = std::collections::HashSet::new();
    for case in dataset.cases {
        assert!(ids.insert(case.id.clone()), "duplicate case identity");
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().canonicalize().unwrap();
        let private = tempfile::tempdir().unwrap();
        let stage = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir_in(private.path())
            .unwrap();
        std::fs::create_dir(workspace.join("parent")).unwrap();
        std::fs::write(workspace.join("parent/note"), "alpha initial").unwrap();
        std::fs::write(workspace.join("sibling"), "sibling content").unwrap();
        let target = workspace.join("parent/note");
        let binding = ExactFileBinding::prepare(
            &workspace,
            &target,
            &[private.path().canonicalize().unwrap()],
        )
        .unwrap();
        let read = matches!(case.operation, FileOperation::Read(_));
        let staging = if read {
            None
        } else {
            Some(
                ExactFileStaging::prepare(
                    &binding,
                    &stage.path().canonicalize().unwrap().join("stage"),
                )
                .unwrap(),
            )
        };
        let worker_request = ExactFileWorkerRequest {
            operation: case.operation,
            binding,
            staging,
        };
        let mutation = match case.mutation_command {
            None => json!(null),
            Some(command) => {
                let shell = IsolatedShellTool::new(IsolatedShellConfig {
                    workspace: workspace.clone(),
                    protected_roots: vec![private.path().into()],
                    max_command_bytes: 4096,
                    max_output_bytes: 4096,
                    timeout: Duration::from_secs(5),
                })
                .unwrap();
                let prepared = shell
                    .prepare(ToolCall {
                        id: case.id.clone(),
                        name: "shell".into(),
                        arguments: json!({"command":command}),
                    })
                    .unwrap();
                let scope = ToolExecutionScope {
                    execution: kolyan_types::ExecutionKey {
                        session_id: "boundary".into(),
                        turn_id: case.id.clone(),
                        execution_id: case.id.clone(),
                    },
                    step_id: "sibling".into(),
                    agent_snapshot_digest: Some("a".repeat(64)),
                };
                let decision = PolicyDecision {
                    kind: PolicyDecisionKind::Allow,
                    reason: "trusted mutation fixture".into(),
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
                let observed = shell
                    .execute(
                        &prepared,
                        &grant,
                        "fixture-v1",
                        &scope,
                        SandboxCancellation::default(),
                    )
                    .await;
                let mutation = match observed {
                    Ok(output) => json!({"prepared":prepared,"exit_code":output.exit_code,
                        "stdout":output.stdout,"stderr":output.stderr}),
                    Err(error) => json!({"prepared":prepared,"error":error.to_string()}),
                };
                if mutation["exit_code"] != json!(0) {
                    writeln!(
                        trajectory,
                        "{}",
                        json!({"case_id":case.id,"mutation":mutation})
                    )
                    .unwrap();
                    trajectory.flush().unwrap();
                    panic!("fixture sibling shell failed: {}", case.id);
                }
                mutation
            }
        };
        let mut write_files = if read { vec![] } else { vec![target.clone()] };
        if let Some(stage) = &worker_request.staging {
            write_files.push(stage.parent.physical_path.join(&stage.leaf));
        }
        let sandbox = MacOsSandbox::new_files(FileSandboxConfig {
            workspace: workspace.clone(),
            read_files: if matches!(worker_request.operation, FileOperation::Write(_)) {
                vec![]
            } else {
                vec![target.clone()]
            },
            write_files,
            protected_roots: vec![],
        });
        let (actual_stage, output) = match sandbox {
            Err(error) => ("admission_refusal", json!({"error":error.to_string()})),
            Ok(sandbox) => match sandbox
                .execute(
                    SandboxRequest {
                        command: SandboxCommand::Executable {
                            path: std::path::PathBuf::from(env!(
                                "CARGO_BIN_EXE_kolyan-test-tool-worker"
                            )),
                            arguments: vec![
                                workspace.to_string_lossy().into_owned(),
                                "16384".into(),
                                "4096".into(),
                                "4096".into(),
                            ],
                        },
                        cwd: workspace.clone(),
                        stdin: serde_json::to_vec(&worker_request).unwrap(),
                        max_input_bytes: 16384,
                        timeout: Duration::from_secs(5),
                        max_output_bytes: 8192,
                    },
                    SandboxCancellation::default(),
                )
                .await
            {
                Err(error) => ("execution_error", json!({"error":error.to_string()})),
                Ok(output) => (
                    if output.exit_code == Some(0) {
                        "success"
                    } else {
                        "worker_refusal"
                    },
                    json!({"exit_code":output.exit_code,"stdout":output.stdout,"stderr":output.stderr}),
                ),
            },
        };
        let decoded = output.get("stdout").and_then(|stdout| {
            let bytes: Vec<u8> = serde_json::from_value(stdout.clone()).ok()?;
            serde_json::from_slice::<FileOperationResult>(&bytes).ok()
        });
        let content = std::fs::read_to_string(&target).unwrap();
        let saved = [workspace.join("saved/note"), workspace.join("parent/saved")]
            .into_iter()
            .find(|path| path.exists())
            .map(|path| std::fs::read_to_string(path).unwrap());
        let row = json!({"case_id":case.id,"worker_request":worker_request,"sibling":mutation,
            "actual_stage":actual_stage,"worker":output,"decoded_result":decoded,"target_content":content,
            "original_content":saved,"sibling_content":std::fs::read_to_string(workspace.join("sibling")).unwrap(),
            "stage_files":std::fs::read_dir(stage.path()).unwrap().count()});
        writeln!(trajectory, "{row}").unwrap();
        trajectory.flush().unwrap();
        assert_eq!(actual_stage, case.expected_stage, "{}: {row}", case.id);
        assert_eq!(
            row["target_content"], case.expected_target,
            "{}: {row}",
            case.id
        );
        assert_eq!(row["sibling_content"], "sibling content");
        assert_eq!(row["stage_files"], 0);
        if actual_stage == "success" {
            let result = decoded.expect("successful worker must return the strict result schema");
            assert_eq!(result.path, "parent/note");
            assert_eq!(result.bytes, case.expected_target.len());
            assert_eq!(
                result.content.as_deref(),
                read.then_some(case.expected_target.as_str())
            );
            assert!(row["worker"]["stderr"].as_array().unwrap().is_empty());
        }
        if actual_stage != "success" {
            assert_eq!(row["original_content"], "alpha initial");
            if actual_stage == "worker_refusal" {
                assert_eq!(
                    row["worker"]["stdout"],
                    json!([]),
                    "refused worker must not disclose file content"
                );
            } else if actual_stage == "admission_refusal" {
                assert!(
                    row["worker"].get("stdout").is_none(),
                    "no process was launched"
                );
            }
        }
    }
    assert_eq!(ids.len(), dataset.expected_rows);
}
