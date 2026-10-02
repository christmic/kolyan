//! Pure opening-report observations; no provider or localhost requests.

use std::{fs, io::Write};

use serde_json::{Value, json};

use super::*;

#[test]
fn diagnostic_reports_preserve_send_decisions_and_terminal_causes() {
    let plan: Value = serde_json::from_str(include_str!("diagnostic_cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-openai-opening-diagnostics-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("OPENAI_DIAGNOSTIC_TRACE={}", path.display());
    let mut output = fs::File::create(&path).unwrap();
    for case in plan["cases"].as_array().unwrap() {
        let mut report = RetryReport::default();
        let decision: Option<RetryDecision> =
            serde_json::from_value(case["decision"].clone()).unwrap();
        for (index, status) in case["statuses"].as_array().unwrap().iter().enumerate() {
            report
                .push(RetryObservation::new(
                    u32::try_from(index + 1).unwrap(),
                    status.as_u64().map(|value| u16::try_from(value).unwrap()),
                    None,
                    None,
                    decision,
                ))
                .unwrap();
        }
        if !case["stop"].is_null() {
            report.finish(serde_json::from_value(case["stop"].clone()).unwrap());
        }
        let original = match case["root"].as_str().unwrap() {
            "http" => OpenAiError::Http {
                status: u16::try_from(case["status"].as_u64().unwrap()).unwrap(),
                body: "original bounded cause".into(),
            },
            "decode" => OpenAiError::Decode(serde_json::from_str::<Value>("{").unwrap_err()),
            "budget" => OpenAiError::OpeningBudgetExhausted,
            other => panic!("unknown diagnostic root {other}"),
        };
        let error = original.with_retry_report(&report);
        let (root, status) = match error.root() {
            OpenAiError::Http { status, .. } => ("http", Some(*status)),
            OpenAiError::Decode(_) => ("decode", None),
            OpenAiError::OpeningBudgetExhausted => ("budget", None),
            other => panic!("unexpected root {other}"),
        };
        writeln!(
            output,
            "{}",
            json!({"id":case["id"],"root":root,"status":status,
            "report":error.opening_report(),"retry_report":error.retry_report(),"source_report":report,"message":error.to_string()})
        )
        .unwrap();
        output.sync_all().unwrap();
    }
    for case in plan["budget_stops"].as_array().unwrap() {
        let mut report = RetryReport::default();
        for (index, status) in case["statuses"].as_array().unwrap().iter().enumerate() {
            report
                .push(RetryObservation::new(
                    u32::try_from(index + 1).unwrap(),
                    status.as_u64().map(|value| u16::try_from(value).unwrap()),
                    None,
                    None,
                    Some(RetryDecision::Retry {
                        delay_ms: 0,
                        basis: kolyan_protocol_http::WaitBasis::Backoff,
                    }),
                ))
                .unwrap();
        }
        let next = u32::try_from(report.attempts() + 1).unwrap();
        record_budget_stop(
            &mut report,
            next,
            case["current_status"]
                .as_u64()
                .map(|value| u16::try_from(value).unwrap()),
            None,
        );
        writeln!(output, "{}", json!({"id":case["id"],"report":report})).unwrap();
        output.sync_all().unwrap();
    }
    let actual = fs::read_to_string(&path).unwrap();
    let rows = actual
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let cases = plan["cases"].as_array().unwrap();
    assert_eq!(plan["schema_version"], 1);
    assert_eq!(
        rows.len(),
        cases.len() + plan["budget_stops"].as_array().unwrap().len()
    );
    for (row, case) in rows.iter().zip(cases) {
        assert_eq!(row["root"], case["root"], "{row}");
        assert_eq!(row["status"], case["status"], "{row}");
        assert_eq!(
            !row["retry_report"].is_null(),
            case["statuses"].as_array().unwrap().len() > 1,
            "{row}"
        );
        if !row["retry_report"].is_null() {
            assert_eq!(row["retry_report"], row["report"], "{row}");
        }
        assert_eq!(
            !row["report"].is_null(),
            case["retained"].as_bool().unwrap(),
            "{row}"
        );
        if !row["report"].is_null() {
            assert_eq!(row["report"], row["source_report"], "{row}");
            assert_eq!(row["report"]["terminal_stop"], case["stop"], "{row}");
            let observations = row["report"]["observations"].as_array().unwrap();
            assert_eq!(
                observations.len(),
                case["statuses"].as_array().unwrap().len()
            );
            for observation in observations {
                assert_eq!(observation["decision"], case["decision"], "{row}");
            }
            assert!(
                row["message"]
                    .as_str()
                    .unwrap()
                    .contains("local opening report")
            );
        }
    }
    for (row, case) in rows[cases.len()..]
        .iter()
        .zip(plan["budget_stops"].as_array().unwrap())
    {
        assert_eq!(row["report"]["terminal_stop"], "ElapsedLimit", "{row}");
        let observations = row["report"]["observations"].as_array().unwrap();
        assert_eq!(
            observations.len(),
            case["statuses"].as_array().unwrap().len() + 1
        );
        assert_eq!(
            observations.last().unwrap()["status"],
            case["current_status"]
        );
    }
}
