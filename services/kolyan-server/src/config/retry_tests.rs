//! Trusted host settings are validated before credentials or network access.

use std::fs;

use serde::Deserialize;
use serde_json::{Value, json};

use super::Config;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    policy: Option<Value>,
    valid: bool,
    retries: Option<u32>,
}

#[test]
fn retry_configuration_exports_every_outcome_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("retry_cases.json")).unwrap();
    let base: Value =
        serde_json::from_str(include_str!("../../../../examples/server.example.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-service-retry-config-")
        .tempdir()
        .unwrap()
        .keep();
    let actual = cases
        .iter()
        .map(|case| {
            let mut input = base.clone();
            if let Some(policy) = &case.policy {
                input["http_retry"] = policy.clone();
            }
            match serde_json::from_value::<Config>(input) {
                Ok(config) => json!({"case":case.id,"valid":true,
                    "retries":config.http_retry.max_retries()}),
                Err(error) => json!({"case":case.id,"valid":false,
                    "error":error.to_string()}),
            }
        })
        .collect::<Vec<_>>();
    fs::write(
        root.join("actual.jsonl"),
        actual
            .iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )
    .unwrap();
    eprintln!("Service retry config evidence: {}", root.display());
    for (case, row) in cases.iter().zip(actual) {
        assert_eq!(row["valid"], case.valid, "{}: {row}", case.id);
        if let Some(retries) = case.retries {
            assert_eq!(row["retries"], retries, "{}: {row}", case.id);
        }
    }
}
