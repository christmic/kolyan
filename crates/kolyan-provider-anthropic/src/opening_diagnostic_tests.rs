//! Pure mapper evidence from typed SDK errors; no HTTP or model execution.

use super::*;
use kolyan_protocol_http::{RetryObservation, RetryReport, StopReason as OpeningStop};

#[test]
fn opening_report_preserves_provider_classification_and_terminal_status() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("opening_diagnostic_cases.json")).unwrap();
    let directory = std::env::temp_dir().join(format!(
        "kolyan-anthropic-provider-diagnostics-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    eprintln!(
        "ANTHROPIC_PROVIDER_DIAGNOSTIC_EVIDENCE={}",
        directory.display()
    );
    for case in &cases {
        let status = case["status"].as_u64().map(|value| value as u16);
        let stop: OpeningStop = serde_json::from_value(case["stop"].clone()).unwrap();
        let mut report = RetryReport::default();
        for attempt in 1..=case["attempts"].as_u64().unwrap() {
            report
                .push(RetryObservation::new(
                    attempt as u32,
                    status,
                    None,
                    None,
                    None,
                ))
                .unwrap();
        }
        report.finish(stop);
        let cause = status.map_or(
            kolyan_protocol_anthropic::AnthropicError::OpeningBudgetExhausted,
            |status| kolyan_protocol_anthropic::AnthropicError::Http {
                status,
                body: "bounded fixture error".into(),
            },
        );
        let original = cause.to_string();
        let sdk_error = kolyan_protocol_anthropic::AnthropicError::RetriedError {
            source: Box::new(cause),
            report: report.clone(),
        };
        let retry_visible = sdk_error.retry_report().is_some();
        let opening = sdk_error.opening_report().cloned();
        let error = anthropic_error(sdk_error);
        let actual = json!({"case_id":case["id"],"kind":format!("{:?}",error.kind),
            "phase":format!("{:?}",error.phase),"status":error.status,
            "message":error.message,"diagnostics":error.diagnostics,
            "original_cause":original,"expected_report":report,
            "retry_visible":retry_visible,"opening_report":opening,
            "meaning":"typed_mapper_unit_no_network"});
        std::fs::write(
            directory.join(format!("{}.jsonl", case["id"].as_str().unwrap())),
            format!("{actual}\n"),
        )
        .unwrap();
    }
    // Capture every case before comparisons so a refusal cannot hide later evidence.
    for case in &cases {
        let actual: Value = serde_json::from_str(
            &std::fs::read_to_string(
                directory.join(format!("{}.jsonl", case["id"].as_str().unwrap())),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(actual["kind"], case["kind"], "{actual}");
        assert_eq!(
            actual["retry_visible"],
            case["attempts"].as_u64().unwrap() > 1,
            "{actual}"
        );
        assert_eq!(
            actual["opening_report"], actual["expected_report"],
            "{actual}"
        );
        assert_eq!(actual["phase"], "Open", "{actual}");
        assert_eq!(actual["status"], case["status"], "{actual}");
        assert!(
            actual["message"]
                .as_str()
                .unwrap()
                .contains(actual["original_cause"].as_str().unwrap()),
            "{actual}"
        );
        assert_eq!(
            actual["diagnostics"]["kind"], case["diagnostic_kind"],
            "{actual}"
        );
        assert_eq!(
            actual["diagnostics"]["report"], actual["expected_report"],
            "{actual}"
        );
    }
}
