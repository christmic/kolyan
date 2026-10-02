//! Real process cleanup remains independently observable under output pressure.

use std::{fs, io::Write, time::Duration};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    MacOsSandbox, SandboxCancellation, SandboxCommand, SandboxConfig, SandboxError,
    SandboxProcessEvent, SandboxProcessObservation, SandboxRequest,
    sandbox_process_observation_channel,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    control: Control,
    queue_capacity: usize,
    command: String,
    phase_content: String,
    process_timeout_ms: u64,
    max_output_bytes: usize,
    host_phase_timeout_ms: u64,
    host_cleanup_timeout_ms: u64,
    expected_cause: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Control {
    Cancel,
    Drop,
}

#[tokio::test]
async fn native_lifecycle_survives_output_queue_saturation() {
    let dataset: Dataset = serde_json::from_str(include_str!("lifecycle_stress.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-sandbox-lifecycle-stress-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    println!("SANDBOX_LIFECYCLE_STRESS_TRACE={}", path.display());
    let mut file = fs::File::create(&path).unwrap();
    for case in &dataset.cases {
        let row = observe(case).await;
        writeln!(file, "{row}").unwrap();
        file.sync_all().unwrap();
    }
    let persisted = fs::read_to_string(path).unwrap();
    assert_eq!(dataset.schema_version, 1);
    assert_eq!(persisted.lines().count(), dataset.cases.len());
    for (line, case) in persisted.lines().zip(&dataset.cases) {
        compare(&serde_json::from_str::<Value>(line).unwrap(), case);
    }
}

async fn observe(case: &Case) -> Value {
    let root = tempfile::tempdir().unwrap();
    let (sender, mut receiver) = sandbox_process_observation_channel(case.queue_capacity).unwrap();
    let bound = sender.bind_context(case.id.clone()).unwrap();
    let sandbox = MacOsSandbox::new(SandboxConfig {
        read_roots: vec![],
        write_roots: vec![root.path().into()],
        protected_roots: vec![],
    })
    .unwrap()
    .with_process_observer(bound);
    let token = SandboxCancellation::default();
    let request = SandboxRequest {
        command: SandboxCommand::Shell(case.command.clone()),
        cwd: root.path().into(),
        stdin: vec![],
        max_input_bytes: 0,
        timeout: Duration::from_millis(case.process_timeout_ms),
        max_output_bytes: case.max_output_bytes,
    };
    let mut execution = Box::pin(sandbox.execute(request, token.clone()));
    let mut result = Value::Null;
    let started = std::time::Instant::now();
    // No receiver polling before control: the real output queue must fill.
    let phase = tokio::time::timeout(Duration::from_millis(case.host_phase_timeout_ms), async {
        loop {
            tokio::select! {
                outcome = &mut execution => {
                    result = outcome_json(outcome);
                    return false;
                }
                () = tokio::time::sleep(Duration::from_millis(5)) => {
                    if fs::read_to_string(root.path().join("phase"))
                        .is_ok_and(|content| content == case.phase_content)
                        && sender.output_dropped() > 0 {
                        return true;
                    }
                }
            }
        }
    })
    .await;
    let controlled = matches!(phase, Ok(true));
    let mut errors = Vec::new();
    if controlled {
        match case.control {
            Control::Cancel => {
                token.cancel();
                match tokio::time::timeout(
                    Duration::from_millis(case.host_cleanup_timeout_ms),
                    &mut execution,
                )
                .await
                {
                    Ok(outcome) => result = outcome_json(outcome),
                    Err(_) => errors.push("explicit result deadline"),
                }
            }
            Control::Drop => result = json!({"future_dropped":true}),
        }
    } else {
        errors.push("phase/pressure not reached while execution pending");
    }
    // This drops the actual boxed execution future, not merely a task handle.
    drop(execution);
    let mut observations = Vec::new();
    let cleanup =
        tokio::time::timeout(Duration::from_millis(case.host_cleanup_timeout_ms), async {
            while !complete(&observations) {
                match receiver.recv().await {
                    Some(row) => observations.push(row),
                    None => return false,
                }
            }
            true
        })
        .await;
    if !matches!(cleanup, Ok(true)) {
        errors.push("lifecycle collection deadline or closed receiver");
    }
    while let Ok(row) = receiver.try_recv() {
        observations.push(row);
    }
    json!({"id":case.id,"request":{"command":case.command,"process_timeout_ms":case.process_timeout_ms,"max_output_bytes":case.max_output_bytes},"controlled":controlled,"phase":fs::read_to_string(root.path().join("phase")).ok(),"elapsed_ms":started.elapsed().as_millis(),"result":result,"errors":errors,"output_dropped":receiver.output_dropped(),"lifecycle_dropped":receiver.lifecycle_dropped(),"aggregate_dropped":receiver.dropped(),"observations":observations})
}

fn outcome_json(outcome: Result<crate::SandboxOutput, SandboxError>) -> Value {
    match outcome {
        Ok(output) => {
            json!({"exit_code":output.exit_code,"stdout_bytes":output.stdout.len(),"stderr_bytes":output.stderr.len()})
        }
        Err(error) => {
            json!({"cancelled":matches!(error, SandboxError::Cancelled),"error":error.to_string()})
        }
    }
}

fn complete(rows: &[SandboxProcessObservation]) -> bool {
    rows.iter()
        .any(|row| matches!(row.event, SandboxProcessEvent::Reaped { .. }))
        && rows
            .iter()
            .any(|row| matches!(row.event, SandboxProcessEvent::GroupCleanup { .. }))
        && rows
            .iter()
            .filter(|row| matches!(row.event, SandboxProcessEvent::CaptureCompleted { .. }))
            .count()
            == 2
}

fn compare(row: &Value, case: &Case) {
    assert_eq!(row["id"], case.id);
    assert_eq!(row["errors"], json!([]), "{row}");
    assert_eq!(row["controlled"], true);
    assert_eq!(row["phase"], case.phase_content);
    assert!(row["output_dropped"].as_u64().unwrap() > 0);
    assert_eq!(row["lifecycle_dropped"], 0);
    assert_eq!(row["aggregate_dropped"], row["output_dropped"]);
    match case.control {
        Control::Cancel => assert_eq!(row["result"]["cancelled"], true),
        Control::Drop => assert_eq!(row["result"]["future_dropped"], true),
    }
    let rows = row["observations"].as_array().unwrap();
    let events = |kind: &'static str| {
        rows.iter()
            .filter(move |entry| entry["event"]["kind"] == kind)
    };
    let spawned = events("spawned").collect::<Vec<_>>();
    assert_eq!(spawned.len(), 1);
    let launch = spawned[0];
    assert!(launch["pid"].as_u64().unwrap() > 0);
    assert_eq!(launch["process_group"], launch["pid"]);
    assert!(!launch["launch_id"].as_str().unwrap().is_empty());
    for entry in rows {
        for key in ["pid", "process_group", "launch_id", "context"] {
            assert_eq!(entry[key], launch[key]);
        }
        assert_eq!(entry["context"], case.id);
    }
    let cancellation = events("cancellation_observed").collect::<Vec<_>>();
    assert_eq!(cancellation.len(), 1);
    assert_eq!(cancellation[0]["event"]["cause"], case.expected_cause);
    for kind in ["termination_attempted", "group_cleanup", "reaped"] {
        let found = events(kind).collect::<Vec<_>>();
        assert_eq!(found.len(), 1, "{kind}: {row}");
        assert!(found[0]["event"]["error"].is_null());
    }
    assert_eq!(events("reaped").next().unwrap()["event"]["signal"], 9);
    assert!(events("reaped").next().unwrap()["event"]["exit_code"].is_null());
    assert_eq!(events("cleanup_failed").count(), 0);
    let positions = [
        "spawned",
        "cancellation_observed",
        "termination_attempted",
        "group_cleanup",
        "reaped",
    ]
    .map(|kind| {
        rows.iter()
            .position(|entry| entry["event"]["kind"] == kind)
            .unwrap()
    });
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
    let captures = events("capture_completed").collect::<Vec<_>>();
    assert_eq!(captures.len(), 2);
    for stream in ["stdout", "stderr"] {
        let matching = captures
            .iter()
            .filter(|entry| entry["event"]["stream"] == stream)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert!(matching[0]["event"]["error"].is_null());
        assert!(matching[0]["event"]["retained_bytes"].as_u64().unwrap() > 0);
    }
}
