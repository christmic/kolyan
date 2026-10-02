//! Synthetic report lifecycle probes, not HTTP sends or provider acceptance.

use std::{fs::File, io::Write};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::{RetryDecision, RetryObservation, RetryReport, StopReason};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    sends: Vec<Send>,
    terminal_stop: Option<StopReason>,
    expected: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Send {
    attempt: u32,
    status: Option<u16>,
    error_code: Option<String>,
    request_id: Option<String>,
    decision: Option<RetryDecision>,
}

#[test]
fn opening_reports_export_all_rows_before_exact_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("opening_report_cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-http-opening-report-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut file = File::create(&path).unwrap();
    for case in cases {
        let mut report = RetryReport::default();
        let mut rejected = Vec::new();
        for send in case.sends {
            let result = report.push(RetryObservation::new(
                send.attempt,
                send.status,
                send.error_code.as_deref(),
                send.request_id.as_deref(),
                send.decision,
            ));
            if let Err(error) = result {
                rejected.push(error.to_string());
            }
        }
        let before = serde_json::to_value(report.observations()).unwrap();
        if let Some(reason) = case.terminal_stop {
            report.finish(reason);
        }
        let actual = json!({
            "report": report,
            "attempts": report.attempts(),
            "was_retried": report.was_retried(),
            "terminal_stop": report.terminal_stop(),
            "observations_preserved": before == serde_json::to_value(report.observations()).unwrap(),
            "rejected": rejected,
        });
        writeln!(
            file,
            "{}",
            json!({
                "case": case.id,
                "before_finish": before,
                "actual": actual,
                "expected": case.expected,
            })
        )
        .unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    eprintln!("HTTP opening report evidence: {}", path.display());
    // Compare the physical export only after every observation has been persisted.
    let exported = std::fs::read_to_string(path).unwrap();
    for line in exported.lines() {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["actual"], row["expected"], "{}", row["case"]);
    }
}
