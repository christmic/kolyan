//! Independent data-driven policy probes; publish all actual rows before comparison.

mod opening_report;
mod reports;

use std::{fs::File, io::Write};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    policy: Value,
    input: Input,
    expected: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    status: u16,
    retries_used: u32,
    elapsed_ms: u64,
    now_unix_ms: u64,
    jitter: f64,
    code: Option<String>,
    should_retry: Option<String>,
    retry_after_ms: Option<String>,
    retry_after: Option<String>,
}

#[test]
fn bounded_policy_data_exports_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/policy_cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-http-retry-policy-")
        .tempdir()
        .unwrap()
        .keep();
    let mut export = File::create(root.join("actual.jsonl")).unwrap();
    let mut results = Vec::new();
    for case in cases {
        let policy = serde_json::from_value::<HttpRetryPolicy>(case.policy.clone());
        let actual = match policy {
            Err(_) => json!({"invalid_policy":true}),
            Ok(policy) => serde_json::to_value(policy.decide(RetryInput {
                retries_used: case.input.retries_used,
                elapsed_ms: case.input.elapsed_ms,
                now_unix_ms: case.input.now_unix_ms,
                jitter: case.input.jitter,
                status: case.input.status,
                error_code: case.input.code.as_deref(),
                headers: RetryHeaders {
                    should_retry: case.input.should_retry.as_deref(),
                    retry_after_ms: case.input.retry_after_ms.as_deref(),
                    retry_after: case.input.retry_after.as_deref(),
                },
            }))
            .unwrap(),
        };
        writeln!(
            export,
            "{}",
            json!({"case":case.id,"policy":case.policy,"actual":actual,"expected":case.expected})
        )
        .unwrap();
        results.push((case.id, actual, case.expected));
    }
    export.sync_all().unwrap();
    eprintln!("HTTP retry policy evidence: {}", root.display());
    for (id, actual, expected) in results {
        assert_eq!(actual, expected, "{id}");
    }
}
