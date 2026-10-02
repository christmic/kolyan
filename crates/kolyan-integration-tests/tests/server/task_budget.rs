//! Actual durable Task/Session/Runtime budget consumer with an explicit model fixture.

mod task_budget_support;
#[path = "task_input_source.rs"]
mod task_input_source;

use serde::Deserialize;
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: String,
    total: u64,
    request_steps: usize,
    child: bool,
    start_ok: bool,
    resume_ok: Option<bool>,
    deny_ok: Option<bool>,
    calls: usize,
    remaining: u64,
    root_reserved: u32,
    child_reserved: u32,
    file: Option<String>,
}

#[tokio::test]
async fn actual_service_budget_caps_rebuilds_and_refuses_expired_work() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/task_budget.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-task-budget-service-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    for case in &cases {
        writeln!(
            output,
            "{}",
            task_budget_support::run(case, &root.join(&case.id)).await
        )
        .unwrap();
    }
    output.flush().unwrap();
    output.sync_all().unwrap();
    drop(output);
    println!("TASK_BUDGET_SERVICE_ACTUAL={}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (row, case) in rows.iter().zip(&cases) {
        assert_eq!(row["id"], case.id);
        assert_eq!(row["start_ok"], case.start_ok, "{}: {row}", case.id);
        assert_eq!(
            row["resume_ok"],
            serde_json::json!(case.resume_ok),
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["deny_ok"],
            serde_json::json!(case.deny_ok),
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["requests"].as_array().unwrap().len(),
            case.calls,
            "{}",
            case.id
        );
        assert_eq!(row["remaining"], case.remaining, "{}", case.id);
        assert_eq!(row["root_reserved"], case.root_reserved, "{}", case.id);
        assert_eq!(row["child_reserved"], case.child_reserved, "{}", case.id);
        assert_eq!(
            row["child_ok"],
            serde_json::json!(case.child.then_some(case.child_reserved > 0)),
            "{}: {row}",
            case.id
        );
        if case.child && case.child_reserved == 0 {
            assert!(
                row["child_error"].as_str().unwrap().contains("budget"),
                "{}: {row}",
                case.id
            );
            assert!(
                row["child_ledger"].as_array().unwrap().is_empty(),
                "{}",
                case.id
            );
        }
        assert_eq!(row["file"], serde_json::json!(case.file), "{}", case.id);
        if case.mode == "expired" {
            assert!(row["root_admission"].is_null(), "{}", case.id);
            let events = row["ledger"].as_array().unwrap();
            assert_eq!(events.len(), 1, "{}: {row}", case.id);
            assert_eq!(events[0]["kind"], "execution_bound", "{}", case.id);
        } else {
            assert_eq!(
                row["root_admission"]["max_steps"], case.root_reserved,
                "{}",
                case.id
            );
            assert_eq!(
                row["root_admission"]["deadline_at_ms"], row["deadline"],
                "{}",
                case.id
            );
        }
        if case.mode.starts_with("approval") {
            assert_eq!(
                row["before_resume"]["execution_budget"], row["after"]["execution_budget"],
                "{}",
                case.id
            );
            assert_eq!(
                row["checkpoint"]["budget"]["deadline_at_ms"], row["deadline"],
                "{}",
                case.id
            );
            assert_eq!(
                row["checkpoint"]["budget"]["max_steps"], case.root_reserved,
                "{}",
                case.id
            );
            assert_eq!(row["file_before_resume"], Value::Null, "{}", case.id);
        }
        if case.mode == "approval_deny_expired" {
            assert_eq!(
                row["after"]["attempts"]["attempt-root"]["state"], "Failed",
                "{row}"
            );
            assert_eq!(row["session"]["turns"][0]["status"], "failed", "{row}");
            assert_eq!(row["requests"].as_array().unwrap().len(), 1);
        }
    }
}
