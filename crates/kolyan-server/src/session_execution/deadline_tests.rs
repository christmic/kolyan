//! Real anchored deadline and cancellation handoff through both Ledger backends.

mod support;

use std::io::Write;

use kolyan_ledger::{InMemoryLedger, SqliteLedger};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    entry: String,
    deadline_ms: Option<u64>,
    load_ms: u64,
    begin_ms: u64,
    ledger_delay: String,
    delay_ms: u64,
    mode: String,
    expected: String,
    models: usize,
    hooks: usize,
    status: Option<String>,
    admission: bool,
}

#[tokio::test]
async fn memory_anchored_deadline_full_export() {
    matrix(false).await;
}

#[tokio::test]
async fn sqlite_anchored_deadline_full_export() {
    matrix(true).await;
}

async fn matrix(sqlite: bool) {
    let cases: Vec<Case> = serde_json::from_str(include_str!("deadline_tests/cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-anchored-deadline-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let directory = root.join(&case.id);
        std::fs::create_dir(&directory).unwrap();
        let mut row = if sqlite {
            support::run(
                case,
                &directory,
                SqliteLedger::open(directory.join("ledger.sqlite")).unwrap(),
                || SqliteLedger::open(directory.join("ledger.sqlite")).unwrap(),
            )
            .await
        } else {
            let ledger = InMemoryLedger::default();
            support::run(case, &directory, ledger.clone(), || ledger.clone()).await
        };
        row["backend"] = json!(if sqlite { "sqlite" } else { "memory" });
        writeln!(file, "{row}").unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    println!(
        "anchored deadline backend={} actual {} rows: {}",
        if sqlite { "sqlite" } else { "memory" },
        rows.len(),
        path.display()
    );
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["id"], case.id);
        assert_eq!(
            row["outcome"], case.expected,
            "{}: {}",
            case.id, row["error"]
        );
        assert_eq!(
            row["models"].as_array().unwrap().len(),
            case.models,
            "{}",
            case.id
        );
        assert_eq!(
            row["hooks"].as_array().unwrap().len(),
            case.hooks,
            "{}",
            case.id
        );
        assert_eq!(row["status"], json!(case.status), "{}", case.id);
        assert_eq!(row["tools"], 0, "{}", case.id);
        let events = row["ledger"].as_array().unwrap();
        let admitted = events
            .iter()
            .find(|event| event["kind"] == "execution_input_admitted");
        assert_eq!(admitted.is_some(), case.admission, "{}", case.id);
        if let Some(admitted) = admitted {
            let deadline = &admitted["payload"]["deadline_at_ms"];
            if let Some(limit) = case.deadline_ms {
                assert!(
                    deadline.as_u64().unwrap() <= row["entry_ms"].as_u64().unwrap() + limit + 2,
                    "{} no renewed window",
                    case.id
                );
                if case.mode == "tighter" {
                    assert!(
                        deadline.as_u64().unwrap() <= row["executor_ceiling"].as_u64().unwrap()
                    );
                }
            } else {
                assert!(deadline.is_null());
            }
            if !row["checkpoint"].is_null() {
                assert_eq!(row["checkpoint"]["budget"]["deadline_at_ms"], *deadline);
                assert_eq!(row["final_checkpoint_deadline"], *deadline);
            }
        }
        for hook in row["hooks"].as_array().unwrap() {
            assert_eq!(
                hook["deadline_ns"],
                case.deadline_ms
                    .map(|value| json!(value as u128 * 1_000_000))
                    .unwrap_or(Value::Null)
            );
        }
        if case.expected == "runtime_timeout"
            && !matches!(case.entry.as_str(), "core" | "core_explicit")
        {
            assert!(
                events
                    .iter()
                    .any(|event| event["kind"] == "execution_started")
            );
            assert!(events.iter().any(|event| event["kind"] == "turn_timed_out"));
        }
        if case.mode.contains("cancel") {
            assert!(!events.iter().any(|event| event["kind"] == "turn_cancelled"));
        }
        if case.mode.contains("commit_failure") {
            assert_eq!(row["before_reconcile_status"], "running");
            assert_eq!(row["reconcile_error"], Value::Null);
        }
    }
}
