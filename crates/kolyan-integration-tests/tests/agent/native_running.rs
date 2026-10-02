//! Actual Agent/shell cancellation. File-worker inflight and live models remain gaps.

mod clocks;
mod driver;
mod host;
mod live;
mod model;
mod ports;

use super::{data, evidence::Evidence, tools};
use futures_util::FutureExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{fs, sync::Arc};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    model_timeout_ms: u64,
    gate_timeout_ms: u64,
    cleanup_timeout_ms: u64,
    max_steps: usize,
    cases: Vec<Case>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: Mode,
    input: String,
    marker: String,
    expected_content: String,
    frames: Vec<data::Frame>,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Cancel,
    Drop,
    Loss,
    Natural,
}

#[tokio::test]
async fn agent_shell_native_running_four_causal_rows_export_before_compare() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("native_running/data/cases.json")).unwrap();
    let expected: Vec<Value> = include_str!("native_running/expected/summary.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut actual = Vec::new();
    for case in &dataset.cases {
        let root = tempfile::Builder::new()
            .prefix("kolyan-agent-native-running-")
            .tempdir()
            .unwrap()
            .keep();
        let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
        println!(
            "AGENT_NATIVE_RUNNING_TRACE={} CASE={}",
            root.join("actual.jsonl").display(),
            case.id
        );
        let mut row = json!({"id":case.id,"network_executed":false,"file_native":"not_proven"});
        let result = std::panic::AssertUnwindSafe(driver::run(
            &root,
            &evidence,
            &installation,
            &dataset,
            case,
            &mut row,
        ))
        .catch_unwind()
        .await;
        row["framework_error"] = json!(match result {
            Ok(result) => result.err(),
            Err(panic) => Some(
                panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).into()))
                    .unwrap_or_else(|| "fixture panic".into())
            ),
        });
        if let Ok(ledger) = kolyan_ledger::SqliteLedger::open(root.join("state/ledger.sqlite")) {
            use kolyan_ledger::LedgerStore;
            match ledger.events_after(0) {
                Ok(events) => row["all_ledger"] = json!(events),
                Err(error) => row["export_error"] = json!(error.to_string()),
            }
        }
        row["physical_content"] = match fs::read_to_string(root.join("workspace/safe/running.txt"))
        {
            Ok(content) => json!(content),
            Err(error) => json!({"error":error.to_string()}),
        };
        evidence
            .append(json!({"event":"native_running_summary","value":row}))
            .unwrap();
        let records = fs::read_to_string(root.join("actual.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        actual.push((row, records));
    }
    assert_eq!(actual.len(), expected.len());
    for ((row, records), expected) in actual.into_iter().zip(expected) {
        assert!(row["framework_error"].is_null(), "{row}");
        assert_eq!(row["observation_errors"], json!([]), "{row}");
        for (key, value) in expected.as_object().unwrap() {
            assert_eq!(&row[key], value, "{} {key}: {row}", row["id"]);
        }
        let processes = row["process_observations"].as_array().unwrap();
        if row["phase_verified"] == true {
            assert_eq!(row["loss"], 0, "{row}");
            assert_eq!(row["reaped"], true, "{row}");
            assert_eq!(row["group_cleanup_ok"], true, "{row}");
            assert_eq!(row["capture_count"], 2, "{row}");
            let spawn = processes
                .iter()
                .position(|event| event["event"]["kind"] == "spawned")
                .unwrap();
            let cancel = processes
                .iter()
                .position(|event| event["event"]["kind"] == "cancellation_observed")
                .unwrap();
            let reap = processes
                .iter()
                .position(|event| event["event"]["kind"] == "reaped")
                .unwrap();
            assert!(spawn < cancel && cancel < reap, "{row}");
            assert_eq!(processes[reap]["event"]["signal"], 9, "{row}");
            let phase = records
                .iter()
                .position(|record| record["event"] == "exact_shell_phase")
                .unwrap();
            let cancellation = records
                .iter()
                .position(|record| {
                    record["event"] == "sandbox_process"
                        && record["value"]["event"]["kind"] == "cancellation_observed"
                })
                .unwrap();
            assert!(phase < cancellation, "{row}");
            if row["durable_cancel"] == true {
                let intent = records
                    .iter()
                    .position(|record| record["event"] == "durable_cancel_published")
                    .unwrap();
                let control = records
                    .iter()
                    .position(|record| record["event"] == "original_control_delivered")
                    .unwrap();
                assert!(
                    phase < intent && intent < control && control < cancellation,
                    "{row}"
                );
                // Core cancels by dropping its tool future after the original
                // control is signalled; the process owner truthfully observes
                // that drop, not an invented sandbox-control delivery.
                assert_eq!(processes[cancel]["event"]["cause"], "dropped_future");
            } else {
                assert_eq!(processes[cancel]["event"]["cause"], "dropped_future");
                assert!(
                    row["finalizations"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|value| value["error"].is_string()),
                    "process cleanup is not durable terminal proof: {row}"
                );
                assert_ne!(row["task_state"], "Completed", "{row}");
                assert_ne!(row["task_state"], "Cancelled", "{row}");
            }
        } else if row["scope_failure"] == "observation_loss" {
            assert!(row["loss"].as_u64().unwrap() > 0, "{row}");
            assert!(processes.is_empty(), "{row}");
        } else {
            assert_eq!(row["scope_failure"], "natural_exit_before_control");
            assert_eq!(row["marker_in_actual_pipe"], true, "{row}");
            assert!(
                processes
                    .iter()
                    .all(|event| event["event"]["kind"] != "cancellation_observed")
            );
        }
        assert!(records.iter().any(|record| record["event"] == "request"));
        assert!(
            records
                .iter()
                .any(|record| record["event"] == "native_invocation_forwarded")
        );
        assert!(
            records
                .iter()
                .any(|record| record["event"] == "native_running_summary")
        );
    }
}
