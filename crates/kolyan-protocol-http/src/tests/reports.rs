//! Report storage bounds do not depend on hostile server string sizes.

use std::{fs::File, io::Write};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    attempts: u32,
    offset: u32,
    text_repeats: usize,
    expected: Value,
}

#[test]
fn bounded_reports_export_all_rows_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("report_cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-http-retry-report-")
        .tempdir()
        .unwrap()
        .keep();
    let mut file = File::create(root.join("actual.jsonl")).unwrap();
    let mut results = Vec::new();
    for case in cases {
        let mut report = RetryReport::default();
        let text = "🦀".repeat(case.text_repeats);
        let mut rejected = 0;
        for attempt in 1..=case.attempts {
            rejected += usize::from(
                report
                    .push(RetryObservation::new(
                        attempt + case.offset,
                        Some(503),
                        Some(&text),
                        Some(&text),
                        None,
                    ))
                    .is_err(),
            );
        }
        let actual = json!({"attempts":report.attempts(),"retried":report.was_retried(),"rejected":rejected,
            "truncated":report.observations().iter().any(|row|row.fields_truncated),
            "bounded":report.observations().iter().all(|row|row.error_code.as_ref().unwrap().len()<=128&&row.request_id.as_ref().unwrap().len()<=256)});
        writeln!(
            file,
            "{}",
            json!({"case":case.id,"actual":actual,"expected":case.expected,"report":report})
        )
        .unwrap();
        results.push((case.id, actual, case.expected));
    }
    file.sync_all().unwrap();
    eprintln!("HTTP retry report evidence: {}", root.display());
    for (id, actual, expected) in results {
        assert_eq!(actual, expected, "{id}");
    }
}
