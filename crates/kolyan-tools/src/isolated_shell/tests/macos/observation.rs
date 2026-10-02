//! Real shell execution and cleanup observations, not file-worker inflight proof.

use super::*;
use kolyan_sandbox::{
    SandboxOutputStream, SandboxProcessEvent, sandbox_process_observation_channel,
};
use serde::Deserialize;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    command: String,
    marker: Option<String>,
    control: Control,
    expected_cause: Option<String>,
    expected_exit: Option<i32>,
    stdout: Option<String>,
    stderr: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Control {
    None,
    Explicit,
    Drop,
}

#[tokio::test]
async fn real_shell_native_lifecycle_exports_before_causal_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("observation/cases.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-shell-native-observation-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    println!("SHELL_NATIVE_OBSERVATION_TRACE={}", path.display());
    let mut file = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let (_root, tool) = setup();
        let (sender, mut receiver) = sandbox_process_observation_channel(128).unwrap();
        let tool = tool.with_process_observer(sender);
        let prepared = tool.prepare(call(&case.command)).unwrap();
        let scope = scope();
        let authority = grant(&prepared, &scope);
        let cancel = SandboxCancellation::default();
        let mut execution =
            Box::pin(tool.execute(&prepared, &authority, "policy-v1", &scope, cancel.clone()));
        let mut events = Vec::new();
        let mut output_bytes = Vec::new();
        let mut witnessed = false;
        let mut result = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut timed_out = false;
        loop {
            tokio::select! {
                event = receiver.recv() => {
                    let Some(event) = event else { break };
                    if let SandboxProcessEvent::OutputObserved { stream: SandboxOutputStream::Stdout, bytes } = &event.event { output_bytes.extend_from_slice(bytes); }
                    events.push(event);
                    if !witnessed && case.marker.as_ref().is_some_and(|marker| output_bytes.windows(marker.len()).any(|window| window == marker.as_bytes())) {
                        witnessed = true;
                        break;
                    }
                }
                stopped = &mut execution => { result = Some(stopped); break; }
                () = tokio::time::sleep_until(deadline) => { timed_out = true; cancel.cancel(); break; }
            }
        }
        if result.is_none() && !matches!(case.control, Control::Drop) {
            if witnessed {
                cancel.cancel();
            }
            result = Some(execution.await);
        } else {
            drop(execution);
        }
        if matches!(case.control, Control::Drop) && witnessed {
            while !(events
                .iter()
                .any(|event| matches!(event.event, SandboxProcessEvent::Reaped { .. }))
                && events
                    .iter()
                    .filter(|event| {
                        matches!(event.event, SandboxProcessEvent::CaptureCompleted { .. })
                    })
                    .count()
                    == 2)
            {
                match tokio::time::timeout_at(deadline, receiver.recv()).await {
                    Ok(Some(event)) => events.push(event),
                    _ => {
                        timed_out = true;
                        break;
                    }
                }
            }
        }
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }
        let row = json!({"id":case.id,"scope":scope,"prepared_digest":prepared.digest(),"witnessed":witnessed,"timed_out":timed_out,"loss":receiver.dropped(),"events":events,"result":result.as_ref().map(|result| match result { Ok(output) => json!({"exit":output.exit_code,"stdout":output.stdout,"stderr":output.stderr}), Err(error) => json!({"error":error.to_string()}) })});
        writeln!(file, "{row}").unwrap();
        file.sync_all().unwrap();
    }
    let actual = std::fs::read_to_string(path).unwrap();
    assert_eq!(actual.lines().count(), cases.len());
    for (line, case) in actual.lines().zip(cases) {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["timed_out"], false, "{row}");
        assert_eq!(row["loss"], 0, "{row}");
        let events = row["events"].as_array().unwrap();
        let spawn = events
            .iter()
            .position(|event| event["event"]["kind"] == "spawned")
            .unwrap();
        let reap = events
            .iter()
            .position(|event| event["event"]["kind"] == "reaped")
            .unwrap();
        assert!(spawn < reap);
        for event in events {
            assert_eq!(event["launch_id"], events[spawn]["launch_id"]);
            assert_eq!(event["pid"], events[spawn]["pid"]);
            let context: crate::ToolProcessObservationContext =
                serde_json::from_str(event["context"].as_str().unwrap()).unwrap();
            assert_eq!(serde_json::to_value(context.scope).unwrap(), row["scope"]);
            assert_eq!(context.prepared_digest, row["prepared_digest"]);
        }
        assert!(
            events
                .iter()
                .any(|event| event["event"]["kind"] == "group_cleanup"
                    && event["event"]["error"].is_null())
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event"]["kind"] == "capture_completed")
                .count(),
            2
        );
        if let Some(cause) = case.expected_cause {
            assert_eq!(row["witnessed"], true);
            let phase = events
                .iter()
                .position(|event| event["event"]["kind"] == "output_observed")
                .unwrap();
            let cancelled = events
                .iter()
                .position(|event| event["event"]["kind"] == "cancellation_observed")
                .unwrap();
            assert!(
                spawn < phase && phase < cancelled && cancelled < reap,
                "{row}"
            );
            assert_eq!(events[cancelled]["event"]["cause"], cause);
            assert_eq!(events[reap]["event"]["signal"], 9);
            if matches!(case.control, Control::Explicit) {
                assert!(
                    row["result"]["error"]
                        .as_str()
                        .unwrap()
                        .contains("cancelled"),
                    "{row}"
                );
            } else {
                assert!(
                    row["result"].is_null(),
                    "dropped future has no fabricated result"
                );
            }
        }
        if let Some(exit) = case.expected_exit {
            assert_eq!(row["result"]["exit"], exit);
        }
        if let Some(stdout) = case.stdout {
            assert_eq!(row["result"]["stdout"], json!(stdout.as_bytes()));
        }
        if let Some(stderr) = case.stderr {
            assert_eq!(row["result"]["stderr"], json!(stderr.as_bytes()));
        }
    }
}
