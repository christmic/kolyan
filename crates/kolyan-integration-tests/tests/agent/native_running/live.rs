//! Explicitly ignored actual-network entry; inventory is planned, not executed.

use super::super::{Deployment, deployments, evidence::Evidence, matrix, tools};
use super::{Case, Dataset, Mode, driver, host, model::Selection};
use futures_util::FutureExt;
use kolyan_ledger::LedgerStore;
use kolyan_model::ModelRef;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{fs, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    configured_combinations: usize,
    planned_rows: usize,
    model_timeout_ms: u64,
    phase_timeout_ms: u64,
    cleanup_timeout_ms: u64,
    case_ids: Vec<String>,
}

fn configured() -> (Plan, Dataset) {
    let plan: Plan = serde_json::from_str(include_str!("data/live.json")).unwrap();
    let mut dataset: Dataset = serde_json::from_str(include_str!("data/cases.json")).unwrap();
    assert_eq!(
        dataset
            .cases
            .iter()
            .map(|case| &case.id)
            .collect::<Vec<_>>(),
        plan.case_ids.iter().collect::<Vec<_>>()
    );
    dataset.model_timeout_ms = plan.model_timeout_ms;
    dataset.gate_timeout_ms = plan.phase_timeout_ms;
    dataset.cleanup_timeout_ms = plan.cleanup_timeout_ms;
    for case in &mut dataset.cases {
        // Real-network selection must never consult scripted model frames.
        case.frames.clear();
    }
    (plan, dataset)
}

fn inventory() -> Vec<Value> {
    let (_, dataset) = configured();
    deployments()
        .into_iter()
        .flat_map(|deployment| {
            dataset.cases.iter().map(move |case| json!({
            "provider":deployment.family,"protocol":deployment.surface,"model":deployment.model,
            "case":case.id,"status":"planned","attempts":0,"network_executed":false,
            "file_native":"not_proven"
        })).collect::<Vec<_>>()
        })
        .collect()
}

fn live_labels(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            format!(
                "agent/native-running/live/{}/{}/{}/{}",
                row["provider"].as_str().unwrap(),
                row["protocol"].as_str().unwrap(),
                row["model"].as_str().unwrap(),
                row["case"].as_str().unwrap()
            )
        })
        .collect()
}

#[test]
fn shared_supplemental_plan_matches_registered_entrypoint_and_actual_axes() {
    let shared: Value = serde_json::from_str(include_str!(
        "../../fixtures/agent/affected_matrix_plan.json"
    ))
    .unwrap();
    let rows = inventory();
    let labels = live_labels(&rows);
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-native-running-shared-plan-")
        .tempdir()
        .unwrap()
        .keep();
    fs::write(
        evidence.join("shared_plan.json"),
        serde_json::to_vec_pretty(&shared).unwrap(),
    )
    .unwrap();
    let exported = rows
        .iter()
        .zip(&labels)
        .map(|(row, label)| format!("{}\n", json!({"row":row,"label":label})))
        .collect::<String>();
    fs::write(evidence.join("inventory.jsonl"), exported).unwrap();
    println!(
        "NATIVE_RUNNING_SHARED_PLAN_TRACE={}",
        evidence.join("inventory.jsonl").display()
    );
    for line in fs::read_to_string(evidence.join("inventory.jsonl"))
        .unwrap()
        .lines()
    {
        serde_json::from_str::<Value>(line).unwrap();
    }
    let entries = shared["supplemental_matrices"].as_array().unwrap();
    let matches = entries
        .iter()
        .filter(|entry| {
            entry["filter"]
                == "native_running::live::actual_provider_native_running_all_configurations"
        })
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1);
    let entry = matches[0];
    // This reference is checked by Rust, not a count or a string-only claim.
    let _registered_entrypoint = actual_provider_native_running_all_configurations;
    assert_eq!(entry["target"], "agent_root");
    assert_eq!(entry["registered"], true);
    assert_eq!(
        entry["status"],
        "registered_not_network_run_pending_fresh_gates"
    );
    assert_eq!(entry["attempts_per_row"], 1);
    assert_eq!(
        entry["entrypoint_source"],
        "tests/agent/native_running/live.rs"
    );
    assert_eq!(
        entry["case_source"],
        "tests/agent/native_running/data/live.json"
    );
    assert_eq!(
        entry["operation_case_source"],
        "tests/agent/native_running/data/cases.json"
    );
    assert_eq!(
        entry["expected_source"],
        "tests/agent/native_running/expected/live.jsonl"
    );
    let read = |key: &str| {
        fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(entry[key].as_str().unwrap()),
        )
        .unwrap()
    };
    let declared: Value = serde_json::from_str(&read("case_source")).unwrap();
    assert_eq!(
        declared,
        serde_json::from_str::<Value>(include_str!("data/live.json")).unwrap()
    );
    assert_eq!(
        serde_json::from_str::<Value>(&read("operation_case_source")).unwrap(),
        serde_json::from_str::<Value>(include_str!("data/cases.json")).unwrap()
    );
    assert_eq!(read("expected_source"), include_str!("expected/live.jsonl"));
    let source = read("entrypoint_source");
    assert!(source.contains("\nasync fn actual_provider_native_running_all_configurations()"));
    assert!(source.contains("#[tokio::test]\n#[ignore = \"76 actual-provider native shell cases; requires explicit network authorization and all configured credentials\"]"));
    let (plan, dataset) = configured();
    assert_eq!(entry["case_ids"], json!(plan.case_ids));
    let modes = dataset
        .cases
        .iter()
        .map(|case| match case.mode {
            Mode::Cancel => "cancel",
            Mode::Drop => "drop",
            Mode::Loss => "loss",
            Mode::Natural => "natural",
        })
        .collect::<Vec<_>>();
    assert_eq!(entry["case_modes"], json!(modes));
    assert_eq!(
        entry["label_axes"],
        json!(["provider", "protocol", "model", "case"])
    );
    assert_eq!(
        entry["label_template"],
        "agent/native-running/live/{provider}/{protocol}/{model}/{case}"
    );
    assert_eq!(labels.len(), rows.len());
    assert_eq!(
        labels
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        labels.len()
    );
    for deployment in deployments() {
        for case in &dataset.cases {
            let label = format!(
                "agent/native-running/live/{}/{}/{}/{}",
                deployment.family, deployment.surface, deployment.model, case.id
            );
            assert!(
                labels.contains(&label),
                "missing actual deployment/case axis {label}"
            );
        }
    }
    assert!(rows.iter().all(|row| row["status"] == "planned"
        && row["attempts"] == 0
        && row["network_executed"] == false));
    assert_eq!(entry["rows_per_deployment"], dataset.cases.len());
    assert_eq!(entry["total_rows"], rows.len());
    assert_eq!(rows.len(), plan.planned_rows);
    assert_eq!(shared["configured_deployments"], deployments().len());
    let base_axes: u64 = shared["matrices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["rows_per_deployment"].as_u64().unwrap())
        .sum();
    let supplemental_axes: u64 = entries
        .iter()
        .map(|entry| entry["rows_per_deployment"].as_u64().unwrap())
        .sum();
    let allocation = &shared["fresh_base_allocation"];
    assert_eq!(base_axes, 25);
    assert_eq!(allocation["complete_existing_inventory_rows"], 418);
    assert_eq!(
        allocation["complete_inventory_rows_with_native_addition"],
        475
    );
    assert_eq!(
        allocation["selected_base_rows"],
        base_axes * deployments().len() as u64
    );
    assert_eq!(allocation["selected_supplemental_rows"], rows.len());
    assert_eq!(
        allocation["selected_rows_per_deployment"],
        base_axes + supplemental_axes
    );
    assert_eq!(
        allocation["selected_total_rows"],
        (base_axes + supplemental_axes) * deployments().len() as u64
    );
    assert_eq!(allocation["selected_total_rows"], 551);
    assert!(
        allocation["selection_status"]
            .as_str()
            .unwrap()
            .starts_with("Main selected the complete 475-row base")
    );
}

#[test]
fn native_running_inventory_is_all_76_planned_not_network_acceptance() {
    let (plan, dataset) = configured();
    let inventory = inventory();
    let root = tempfile::Builder::new()
        .prefix("kolyan-native-running-planned-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("inventory.jsonl");
    let mut bytes = String::new();
    for row in &inventory {
        bytes.push_str(&format!("{row}\n"));
    }
    fs::write(&path, bytes).unwrap();
    println!("NATIVE_RUNNING_PLANNED_INVENTORY={}", path.display());
    assert_eq!(deployments().len(), plan.configured_combinations);
    assert_eq!(inventory.len(), plan.planned_rows);
    assert_eq!(
        inventory.len(),
        plan.configured_combinations * dataset.cases.len()
    );
    let unique = inventory
        .iter()
        .map(Value::to_string)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        unique.len(),
        inventory.len(),
        "duplicate configuration/case in plan"
    );
    for row in inventory {
        assert_eq!(row["status"], "planned");
        assert_eq!(row["attempts"], 0);
        assert_eq!(row["network_executed"], false);
    }
    let expected: Vec<Value> = include_str!("expected/live.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(expected.len(), dataset.cases.len());
    for (case, expected) in dataset.cases.iter().zip(expected) {
        assert_eq!(case.id, expected["id"]);
        assert!(case.frames.is_empty());
    }
}

#[tokio::test]
#[ignore = "76 actual-provider native shell cases; requires explicit network authorization and all configured credentials"]
async fn actual_provider_native_running_all_configurations() {
    let (plan, dataset) = configured();
    let combinations = deployments();
    let rows = inventory();
    let labels = live_labels(&rows);
    let mut report = matrix::Matrix::new(labels);
    fs::write(
        report.directory.join("planned_inventory.json"),
        serde_json::to_vec_pretty(&rows).unwrap(),
    )
    .unwrap();
    assert_eq!(combinations.len(), plan.configured_combinations);
    assert_eq!(rows.len(), plan.planned_rows);
    let installation = tools::worker::WorkerRun::prepare().await;
    let expected: Vec<Value> = include_str!("expected/live.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mut index = 0;
    for deployment in &combinations {
        for (case, expected) in dataset.cases.iter().zip(&expected) {
            report
                .run(index, async {
                    let (row, records) = run_case(deployment, &dataset, case, &installation).await;
                    compare(&row, &records, case, expected);
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "all failures retained: {}",
        report.directory.display()
    );
}

async fn run_case(
    deployment: &Deployment,
    dataset: &Dataset,
    case: &Case,
    installation: &tools::worker::WorkerRun,
) -> (Value, Vec<Value>) {
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-native-running-live-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    println!(
        "AGENT_NATIVE_RUNNING_LIVE_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let mut row = json!({"id":case.id,"provider":deployment.family,"protocol":deployment.surface,
        "model":deployment.model,"file_native":"not_proven","network_executed":false});
    let result = std::panic::AssertUnwindSafe(async {
        let selection = Selection {
            model: ModelRef::new(deployment.family, &deployment.model),
            live: Some(deployment.build()),
        };
        row["actual_provider_selected"] = json!(true);
        driver::run_selected(
            &root,
            &evidence,
            installation,
            dataset,
            case,
            &mut row,
            &selection,
        )
        .await
    })
    .catch_unwind()
    .await;
    row["actual_provider_entries"] = json!(
        evidence
            .rows()
            .iter()
            .filter(|record| record["event"] == "model_wait_started")
            .count()
    );
    // Entering a real Provider can still fail before transport. Do not claim
    // transport execution solely from builder selection or a fixture plan.
    if row["actual_provider_entries"].as_u64().unwrap() > 0 {
        row["network_executed"] = Value::Null;
        row["network_evidence"] = json!("consult_actual_provider_events_not_builder_selection");
    }
    row["framework_error"] = json!(match result {
        Ok(result) => result.err(),
        Err(panic) => Some(
            panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic
                    .downcast_ref::<&str>()
                    .map(|message| (*message).into()))
                .unwrap_or_else(|| "fixture panic".into())
        ),
    });
    if row["framework_error"].is_string()
        && let Err(error) = host::export_stored(&root, &case.id, &evidence)
    {
        row["source_export_error"] = json!(error);
        evidence
            .append(json!({"event":"failed_source_export","error":error}))
            .unwrap();
    }
    match kolyan_ledger::SqliteLedger::open(root.join("state/ledger.sqlite")) {
        Ok(ledger) => match ledger.events_after(0) {
            Ok(events) => row["all_ledger"] = json!(events),
            Err(error) => row["export_error"] = json!(error.to_string()),
        },
        Err(error) => row["export_error"] = json!(error.to_string()),
    }
    row["physical_content"] = match fs::read_to_string(root.join("workspace/safe/running.txt")) {
        Ok(content) => json!(content),
        Err(error) => json!({"error":error.to_string()}),
    };
    evidence
        .append(json!({"event":"native_running_summary","value":row}))
        .unwrap();
    let records = fs::read_to_string(root.join("actual.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (row, records)
}

fn compare(row: &Value, records: &[Value], case: &Case, expected: &Value) {
    assert!(row["framework_error"].is_null(), "{row}");
    assert_eq!(row["observation_errors"], json!([]), "{row}");
    for (key, value) in expected.as_object().unwrap() {
        assert_eq!(&row[key], value, "{key}: {row}");
    }
    assert_eq!(row["physical_content"], case.expected_content, "{row}");
    assert!(row["requests"].as_u64().unwrap() >= 1, "{row}");
    assert!(
        row["actual_provider_entries"].as_u64().unwrap() >= 1,
        "{row}"
    );
    assert!(records.iter().any(|record| record["event"] == "request"));
    assert!(
        records
            .iter()
            .any(|record| record["event"] == "native_invocation_forwarded")
    );
    let events = row["process_observations"].as_array().unwrap();
    if matches!(case.mode, Mode::Cancel | Mode::Drop) {
        assert_eq!(row["loss"], 0, "{row}");
        let position = |event| {
            records
                .iter()
                .position(|record| record["event"] == event)
                .unwrap()
        };
        let phase = position("exact_shell_phase");
        let cancel = records
            .iter()
            .position(|record| {
                record["event"] == "sandbox_process"
                    && record["value"]["event"]["kind"] == "cancellation_observed"
            })
            .unwrap();
        let reap = records
            .iter()
            .position(|record| {
                record["event"] == "sandbox_process" && record["value"]["event"]["kind"] == "reaped"
            })
            .unwrap();
        assert!(phase < cancel && cancel < reap, "{row}");
        assert_eq!(records[reap]["value"]["event"]["signal"], 9, "{row}");
        if matches!(case.mode, Mode::Cancel) {
            let intent = position("durable_cancel_published");
            let control = position("original_control_delivered");
            assert!(
                phase < intent && intent < control && control < cancel,
                "{row}"
            );
        } else {
            assert!(
                row["finalizations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|value| value["error"].is_string())
            );
            assert_ne!(row["task_state"], "Completed");
            assert_ne!(row["task_state"], "Cancelled");
        }
    } else if matches!(case.mode, Mode::Loss) {
        assert!(row["loss"].as_u64().unwrap() > 0);
        assert!(events.is_empty());
    } else {
        assert_eq!(row["marker_in_actual_pipe"], true);
        assert!(
            events
                .iter()
                .all(|event| event["event"]["kind"] != "cancellation_observed")
        );
    }
    if matches!(case.mode, Mode::Loss | Mode::Natural) {
        assert!(
            row["start_error"].is_null(),
            "Provider/Turn failure is not successful natural completion: {row}"
        );
        assert_eq!(row["task_state"], "Completed", "{row}");
        assert!(
            row["finalizations"]
                .as_array()
                .unwrap()
                .iter()
                .all(|value| value["error"].is_null()),
            "{row}"
        );
    }
}
