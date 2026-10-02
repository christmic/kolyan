//! Regression for the actual Turn trait port, independent of inherent execute.

use super::*;
use kolyan_sandbox::{
    SandboxOutputStream, SandboxProcessEvent, sandbox_process_observation_channel,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    command: String,
    marker: String,
    content: String,
    phase_timeout_ms: u64,
    cleanup_timeout_ms: u64,
    expected_cause: String,
}

#[tokio::test]
async fn trait_port_observes_exact_native_phase_before_original_control() {
    let case: Case = serde_json::from_str(include_str!("observation/case.json")).unwrap();
    let (root, tool) = setup();
    let (sender, mut receiver) = sandbox_process_observation_channel(128).unwrap();
    let tool = tool.with_process_observer(sender);
    let input = invocation(&tool, &case.command, None).await;
    let scope = input.scope.clone();
    let digest = input.prepared.digest().to_string();
    let control = input.control.clone();
    // Explicit dyn dispatch prevents accidentally testing inherent execute.
    let executor: &dyn ToolExecutor = &tool;
    let mut running = Box::pin(executor.execute_invocation(input));
    let phase_deadline = tokio::time::Instant::now() + Duration::from_millis(case.phase_timeout_ms);
    let mut events = Vec::new();
    let mut stdout = Vec::new();
    let mut phase = false;
    let mut result = None;
    loop {
        tokio::select! {
            event = receiver.recv() => {
                let Some(event) = event else { break };
                if let SandboxProcessEvent::OutputObserved { stream: SandboxOutputStream::Stdout, bytes } = &event.event {
                    stdout.extend_from_slice(bytes);
                }
                events.push(event);
                if stdout.windows(case.marker.len()).any(|bytes| bytes == case.marker.as_bytes()) {
                    phase = true;
                    break;
                }
            }
            stopped = &mut running => { result = Some(stopped); break; }
            () = tokio::time::sleep_until(phase_deadline) => { break; }
        }
    }
    // Even a regression with no phase cleans up the original execution first.
    control.cancel();
    let cleanup_deadline =
        tokio::time::Instant::now() + Duration::from_millis(case.cleanup_timeout_ms);
    let mut cleanup_timeout = false;
    if result.is_none() {
        match tokio::time::timeout_at(cleanup_deadline, running.as_mut()).await {
            Ok(stopped) => result = Some(stopped),
            Err(_) => cleanup_timeout = true,
        }
    }
    drop(running);
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
    }
    let physical = std::fs::read_to_string(root.path().join("effect"));
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-shell-trait-observation-")
        .tempdir()
        .unwrap()
        .keep()
        .join("actual.jsonl");
    let row = json!({"id":case.id,"phase":phase,"scope":scope,"digest":digest,
        "events":events,"loss":receiver.dropped(),"cleanup_timeout":cleanup_timeout,
        "result":result,"physical":physical.as_ref().ok(),
        "physical_error":physical.as_ref().err().map(ToString::to_string)});
    std::fs::write(&evidence, format!("{row}\n")).unwrap();
    println!("SHELL_TRAIT_OBSERVATION_TRACE={}", evidence.display());
    let row: serde_json::Value =
        serde_json::from_str(std::fs::read_to_string(&evidence).unwrap().trim_end()).unwrap();
    assert_eq!(row["phase"], true, "{row}");
    assert_eq!(row["cleanup_timeout"], false, "{row}");
    assert_eq!(row["loss"], 0, "{row}");
    assert_eq!(row["physical"], case.content, "{row}");
    assert!(matches!(result, Some(Err(ToolError::Cancelled))), "{row}");
    let events = row["events"].as_array().unwrap();
    let index = |kind| {
        events
            .iter()
            .position(|event| event["event"]["kind"] == kind)
            .unwrap()
    };
    let spawn = index("spawned");
    let phase = index("output_observed");
    let cancel = index("cancellation_observed");
    let reap = index("reaped");
    assert!(spawn < phase && phase < cancel && cancel < reap, "{row}");
    assert_eq!(
        events[cancel]["event"]["cause"], case.expected_cause,
        "{row}"
    );
    assert_eq!(events[reap]["event"]["signal"], 9, "{row}");
    for event in events {
        assert_eq!(event["launch_id"], events[spawn]["launch_id"]);
        assert_eq!(event["pid"], events[spawn]["pid"]);
        let context: crate::ToolProcessObservationContext =
            serde_json::from_str(event["context"].as_str().unwrap()).unwrap();
        assert_eq!(context.scope, scope);
        assert_eq!(context.prepared_digest, digest);
        assert_eq!(context.tool_name, "shell");
    }
    assert!(
        events
            .iter()
            .any(|event| event["event"]["kind"] == "group_cleanup"
                && event["event"]["error"].is_null()),
        "{row}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"]["kind"] == "capture_completed")
            .count(),
        2,
        "{row}"
    );
}
