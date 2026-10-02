//! Data-driven accounting checks are not model-network acceptance.

use kolyan_model::TokenUsage;
use serde::Deserialize;
use serde_json::{Value, json};

use super::observe;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    source: Vec<TokenUsage>,
    expected: Option<Value>,
    error: Option<String>,
}

#[test]
fn reported_cache_missing_fields_and_overflow_follow_task_accounting_contract() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../../fixtures/agent/usage.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-usage-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = super::super::evidence::Evidence::new(&directory.join("actual.jsonl"));
    for case in cases {
        let result = observe(&case.source);
        evidence
            .append(json!({"case":case.id,"source":case.source,"actual":result}))
            .unwrap();
        match (case.expected, case.error) {
            (Some(expected), None) => {
                let actual = result.unwrap();
                assert_eq!(
                    actual.source, case.source,
                    "{}: missing source fields must not become zeros",
                    case.id
                );
                let actual = serde_json::to_value(actual).unwrap();
                for (field, value) in expected.as_object().unwrap() {
                    assert_eq!(actual[field], *value, "{}: {field}", case.id);
                }
            }
            (None, Some(error)) => assert_eq!(result.unwrap_err(), error, "{}", case.id),
            _ => panic!("{} requires exactly one outcome", case.id),
        }
    }
    println!(
        "Agent usage evidence: {}",
        directory.join("actual.jsonl").display()
    );
}
