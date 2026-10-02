//! Independent new-Turn seam matrix; exported physical artifacts precede assertions.

mod support;

use std::io::Write;

use kolyan_ledger::{InMemoryLedger, SqliteLedger};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    action: String,
    deadline_ms: Option<u64>,
    expected: String,
    hook_calls: usize,
    model_calls: usize,
    tool_calls: usize,
}

#[tokio::test]
async fn memory_preparation_exports_all_rows_then_reloads() {
    matrix(false).await;
}

#[tokio::test]
async fn sqlite_preparation_exports_all_rows_then_reopens_and_reloads() {
    matrix(true).await;
}

async fn matrix(sqlite: bool) {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-turn-preparation-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("TURN_PREPARATION_TRACE={}", path.display());
    let mut file = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let root = directory.join(&case.id);
        std::fs::create_dir(&root).unwrap();
        let row = if sqlite {
            let ledger_path = root.join("ledger.sqlite");
            let ledger = SqliteLedger::open(&ledger_path).unwrap();
            support::run(case, &root, ledger, || {
                SqliteLedger::open(&ledger_path).unwrap()
            })
            .await
        } else {
            let ledger = InMemoryLedger::default();
            support::run(case, &root, ledger.clone(), || ledger.clone()).await
        };
        writeln!(file, "{row}").unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(&rows) {
        support::compare(case, row);
    }
}
