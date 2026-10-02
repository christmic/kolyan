//! Native effects and deterministic host faults, not native-inflight PID coverage.

mod driver;
mod host;
mod ports;
mod tests;

use std::{fs, sync::Arc};

use futures_util::FutureExt;

use kolyan_ledger::LedgerStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{data, evidence::Evidence, matrix, tools};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    gate_timeout_ms: u64,
    drive_timeout_ms: u64,
    max_steps: usize,
    input: String,
    parent_input: String,
    live_drive_timeout_ms: u64,
    inspection_window_assumption_tokens: u32,
    write_arguments: kolyan_tools::WriteArguments,
    frames: Vec<data::Frame>,
    parent_frames: Vec<data::Frame>,
    cases: Vec<Case>,
    expected_content: String,
    unapproved_gap: String,
}

#[derive(Clone)]
struct Source {
    model: kolyan_model::ModelRef,
    live: Option<Arc<dyn kolyan_model::ModelProvider>>,
}
impl Source {
    fn offline() -> Self {
        Self {
            model: kolyan_model::ModelRef::new("fixture", "native-effects"),
            live: None,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    fault: String,
    expected_receipts: usize,
    expected_uncertain: bool,
    expected_task_state: String,
    expected_model_requests: usize,
}

#[tokio::test]
async fn native_effects_cancel_loss_and_late_child_export_before_comparison() {
    run(false).await;
}

#[tokio::test]
#[ignore = "Requires configured actual Providers; one attempt for all 19 deployments times three native faults"]
async fn actual_model_native_effects_matrix() {
    run(true).await;
}

async fn run(live: bool) {
    let dataset: Dataset =
        serde_json::from_str(include_str!("../fixtures/agent/native_effects.json")).unwrap();
    assert_eq!(dataset.schema_version, 1);
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut rows = Vec::new();
    let plans = if live {
        super::deployments()
            .into_iter()
            .map(Arc::new)
            .flat_map(|deployment| {
                dataset
                    .cases
                    .iter()
                    .cloned()
                    .map(move |case| {
                        let label = format!(
                            "agent/native-effects/{}/{}/{}/{}",
                            deployment.family, deployment.surface, deployment.model, case.id
                        );
                        (label, case, Some(deployment.clone()))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    } else {
        dataset
            .cases
            .iter()
            .cloned()
            .map(|case| (case.id.clone(), case, None))
            .collect()
    };
    for (label, case, deployment) in plans {
        let root = tempfile::Builder::new()
            .prefix("kolyan-native-effect-")
            .tempdir()
            .unwrap()
            .keep();
        let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
        println!(
            "NATIVE_EFFECT_TRACE={} CASE={}",
            root.join("actual.jsonl").display(),
            label
        );
        let mut row = json!({"case":case.id,"label":label,"actual_jsonl":root.join("actual.jsonl"),"network_mode":live});
        let result = std::panic::AssertUnwindSafe(async {
            let source = match deployment {
                Some(deployment) => Source {
                    model: kolyan_model::ModelRef::new(deployment.family, &deployment.model),
                    live: Some(deployment.build()),
                },
                None => Source::offline(),
            };
            driver::observe(
                &root,
                &evidence,
                &installation,
                &dataset,
                &case,
                &source,
                &mut row,
            )
            .await
        })
        .catch_unwind()
        .await;
        row["framework_error"] = json!(match result {
            Ok(result) => result.err(),
            Err(panic) => Some(
                panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic
                        .downcast_ref::<&str>()
                        .map(|message| (*message).to_owned()))
                    .unwrap_or_else(|| "non-string native fixture panic".into())
            ),
        });
        // Even an early refusal retains actual store facts and physical state.
        if let Ok(ledger) = kolyan_ledger::SqliteLedger::open(root.join("state/ledger.sqlite")) {
            match ledger.events_after(0) {
                Ok(events) => row["all_ledger"] = json!(events),
                Err(error) => row["export_error"] = json!(error.to_string()),
            }
        }
        row["physical_content"] =
            json!(fs::read_to_string(root.join("workspace/safe/native-proof.txt")).ok());
        row["physical_sha256"] = json!(
            fs::read(root.join("workspace/safe/native-proof.txt"))
                .ok()
                .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
        );
        evidence
            .append(json!({"event":"native_observation","value":row}))
            .unwrap();
        let actual = fs::read_to_string(root.join("actual.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        rows.push((label, case, row, actual));
    }
    // No scenario comparison can prevent a later planned row from exporting.
    let mut report = matrix::Matrix::new(rows.iter().map(|(label, ..)| label.clone()));
    for (index, (_, case, row, actual)) in rows.into_iter().enumerate() {
        report
            .run(index, async {
                assert!(row["framework_error"].is_null(), "{}: {row}", case.id);
                assert_eq!(
                    row["physical_content"], dataset.expected_content,
                    "{}: {row}",
                    case.id
                );
                assert_eq!(
                    row["physical_sha256"],
                    format!("{:x}", Sha256::digest(dataset.expected_content.as_bytes()))
                );
                assert_eq!(row["executions"], 1);
                assert_eq!(row["completions"], 1);
                assert_eq!(row["selection_refusals"], 0);
                assert_eq!(row["receipts"], case.expected_receipts);
                assert_eq!(row["uncertain"], case.expected_uncertain);
                assert_eq!(row["task_state"], case.expected_task_state);
                if !live {
                    assert_eq!(row["model_requests"], case.expected_model_requests);
                }
                assert_eq!(row["counts_before_rebuild"], row["counts_after_rebuild"]);
                assert_eq!(row["result_consumptions"], 0);
                assert_eq!(row["effects_reconciled"], 0);
                if case.fault == "late_child" {
                    assert_eq!(row["parent_turn_cancelled"], 1);
                    assert_eq!(
                        row["parent_receipts"], 0,
                        "unresolved parent wait must not manufacture a receipt"
                    );
                    if !live {
                        assert_eq!(row["parent_requests"], 1);
                    }
                    assert!(row["consume_error"].is_string());
                    assert_eq!(row["finalize_states"], json!(["Cancelled", "Cancelled"]));
                    assert_eq!(row["parent_stopped_before_task_cancel"], true);
                    assert_eq!(row["child_receipt_matches_admitted_scope"], true);
                    assert!(
                        row["parent_stop_observation_position"].as_u64().unwrap()
                            < row["task_cancel_position"].as_u64().unwrap()
                    );
                    assert_eq!(row["task_cancel_causes_parent_stop"], true);
                } else {
                    assert_eq!(row["injected_faults"], 1);
                    assert!(row["recovery_error"].is_string());
                }
                if case.fault == "cancel_after_receipt" {
                    assert!(
                        row["native_receipt_cursor"].as_u64().unwrap()
                            < row["cancel_cursor"].as_u64().unwrap()
                    );
                    assert_eq!(row["stopped_turn_cancelled"], 1);
                    assert!(
                        row["cancel_cursor"].as_u64().unwrap()
                            < row["stopped_cursor"].as_u64().unwrap()
                    );
                    assert_eq!(row["returned_typed_cancelled"], true);
                }
                if case.fault == "lose_receipt" {
                    assert!(row["native_receipt_cursor"].is_null());
                    assert!(row["cancel_cursor"].is_null());
                    assert_eq!(row["effect_started"], 1);
                    assert_eq!(row["effect_uncertain"], 1);
                    assert!(
                        row["started_cursor"].as_u64().unwrap()
                            < row["uncertain_cursor"].as_u64().unwrap()
                    );
                    assert_eq!(row["turn_completed"], 0);
                }
                let mut offset = 0;
                for expected in include_str!("../expected/agent/native_effects.jsonl").lines() {
                    let expected: Value = serde_json::from_str(expected).unwrap();
                    offset += actual[offset..]
                        .iter()
                        .position(|row| row["event"] == expected["event"])
                        .unwrap_or_else(|| panic!("{} missing {expected}", case.id))
                        + 1;
                }
            })
            .await;
    }
    assert!(
        report.complete(),
        "Native effects report: {}",
        report.directory.display()
    );
}
