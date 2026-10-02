//! Only existing retained bytes reach telemetry; queue loss never changes results.

use super::*;
use crate::{MacOsSandbox, SandboxCancellation, SandboxCommand, SandboxConfig, SandboxRequest};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    command: String,
    cap: usize,
    queue: usize,
    cancelled: bool,
    expected_error: Option<String>,
    expected_loss: bool,
    expected_stdout: Option<String>,
    expected_stderr: Option<String>,
}

#[tokio::test]
async fn retained_bytes_queue_loss_and_pre_spawn_cancel_export_before_compare() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("retained/cases.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-sandbox-observation-retained-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    println!("SANDBOX_OBSERVATION_TRACE={}", path.display());
    let mut file = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let root = tempfile::tempdir().unwrap();
        let (sender, mut receiver) = sandbox_process_observation_channel(case.queue).unwrap();
        let sandbox = MacOsSandbox::new(SandboxConfig {
            read_roots: vec![root.path().into()],
            write_roots: vec![root.path().into()],
            protected_roots: vec![],
        })
        .unwrap()
        .with_process_observer(sender);
        let cancellation = SandboxCancellation::default();
        if case.cancelled {
            cancellation.cancel();
        }
        let result = sandbox
            .execute(
                SandboxRequest {
                    command: SandboxCommand::Shell(case.command.clone()),
                    cwd: root.path().into(),
                    stdin: vec![],
                    max_input_bytes: 0,
                    timeout: Duration::from_secs(5),
                    max_output_bytes: case.cap,
                },
                cancellation,
            )
            .await;
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }
        let row = json!({"id":case.id,"loss":receiver.dropped(),"events":events,"result":result.map(|output| json!({"stdout":output.stdout,"stderr":output.stderr,"exit":output.exit_code})).unwrap_or_else(|error| json!({"error":error.to_string()}))});
        writeln!(file, "{row}").unwrap();
        file.sync_all().unwrap();
    }
    let actual = std::fs::read_to_string(path).unwrap();
    assert_eq!(actual.lines().count(), cases.len());
    for (line, case) in actual.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(
            row["loss"].as_u64().unwrap() > 0,
            case.expected_loss,
            "{row}"
        );
        if let Some(error) = case.expected_error {
            assert!(
                row["result"]["error"].as_str().unwrap().contains(&error),
                "{row}"
            );
        }
        if let Some(stdout) = case.expected_stdout {
            assert_eq!(row["result"]["stdout"], json!(stdout.as_bytes()));
        }
        if let Some(stderr) = case.expected_stderr {
            assert_eq!(row["result"]["stderr"], json!(stderr.as_bytes()));
        }
        let events = row["events"].as_array().unwrap();
        if case.cancelled {
            assert!(
                events.is_empty(),
                "cancelled before spawn creates no invented PID"
            );
        }
        let retained = events
            .iter()
            .filter(|event| event["event"]["kind"] == "output_observed")
            .map(|event| event["event"]["bytes"].as_array().unwrap().len())
            .sum::<usize>();
        assert!(retained <= case.cap, "{row}");
        if !case.expected_loss && row["result"]["error"].is_null() {
            for stream in ["stdout", "stderr"] {
                let bytes = events
                    .iter()
                    .filter(|event| {
                        event["event"]["kind"] == "output_observed"
                            && event["event"]["stream"] == stream
                    })
                    .flat_map(|event| event["event"]["bytes"].as_array().unwrap().clone())
                    .collect::<Vec<_>>();
                assert_eq!(json!(bytes), row["result"][stream]);
            }
        }
    }
}
