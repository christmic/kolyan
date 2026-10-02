//! Classifier-only variants, including failures not normally emitted by pure preparation.

use serde::Deserialize;

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    cause: Cause,
    expected: String,
}

#[derive(Deserialize)]
enum Cause {
    Invalid(String),
    Prepared(PreparedCause),
    Agent(AgentCause),
}

#[derive(Deserialize)]
enum PreparedCause {
    Invalid(String),
    Denied(String),
    BindingMismatch,
}

#[derive(Deserialize)]
enum AgentCause {
    SnapshotMismatch,
    Capacity,
}

#[test]
fn typed_mapping_and_cause_bounds_export_all_before_comparison() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../preparation_mapping_cases.json")).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        let cause = match &case.cause {
            Cause::Invalid(value) => {
                crate::InvokePrepareError::Invalid(value.replace("long", &"界".repeat(3000)))
            }
            Cause::Prepared(value) => crate::InvokePrepareError::Prepared(match value {
                PreparedCause::Invalid(message) => PreparedError::Invalid(message.clone()),
                PreparedCause::Denied(message) => PreparedError::Denied(message.clone()),
                PreparedCause::BindingMismatch => PreparedError::BindingMismatch,
            }),
            Cause::Agent(value) => crate::InvokePrepareError::Agent(match value {
                AgentCause::SnapshotMismatch => crate::AgentError::SnapshotMismatch,
                AgentCause::Capacity => crate::AgentError::Capacity,
            }),
        };
        let original = cause.to_string();
        let error = super::super::preparation_error(cause);
        let message = match &error {
            ToolError::Failed { message }
            | ToolError::PolicyDenied { message }
            | ToolError::InvalidBatch { message } => message,
            _ => unreachable!("classifier cannot emit another variant"),
        };
        rows.push(
            json!({"fixture_id":case.id,"source":"classifier_only_not_host_admission",
            "original":original,"category":category(&error),"cause":message,"error":error}),
        );
    }
    for (case, row) in cases.iter().zip(export(&rows)) {
        assert_eq!(row["category"], case.expected, "{}", case.id);
        let cause = row["cause"].as_str().unwrap();
        let original = row["original"].as_str().unwrap();
        assert!(cause.len() <= super::super::MAX_PREPARATION_CAUSE_BYTES);
        if original.len() <= super::super::MAX_PREPARATION_CAUSE_BYTES {
            assert_eq!(cause, original, "{}", case.id);
        } else {
            assert!(cause.ends_with(" [truncated]"));
            assert!(original.starts_with(cause.strip_suffix(" [truncated]").unwrap()));
        }
    }
}
