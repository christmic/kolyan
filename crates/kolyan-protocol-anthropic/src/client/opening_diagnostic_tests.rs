//! Actual localhost opening failures and deterministic diagnostic-only boundaries.

use super::*;
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    statuses: Vec<u16>,
    mode: String,
    attempts: usize,
    status: Option<u16>,
    stop: StopReason,
}

#[tokio::test]
async fn failed_openings_preserve_single_send_and_terminal_reason() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("opening_diagnostic_cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-anthropic-opening-diagnostics-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("ANTHROPIC_DIAGNOSTIC_EVIDENCE={}", directory.display());
    for case in &cases {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let mode = case.mode.clone();
        let statuses = case.statuses.clone();
        let worker = tokio::spawn(async move {
            let mut requests = Vec::new();
            for status in statuses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    bytes.push(socket.read_u8().await.unwrap());
                    assert!(bytes.len() < 8192);
                }
                let headers = std::str::from_utf8(&bytes).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                requests.push(body);
                if mode == "headers" {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                let body = "{\"error\":{\"type\":\"fixture_error\"}}";
                socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\nretry-after-ms: 1\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                if mode == "body" {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                } else {
                    socket.write_all(body.as_bytes()).await.unwrap();
                }
            }
            requests
        });
        let mut config = AnthropicConfig::new("localhost-placeholder");
        config.base_url = url;
        config.timeout = Duration::from_secs(2);
        config.http_retry =
            crate::HttpRetryPolicy::new(2, 150, 100, RetryProfile::Anthropic).unwrap();
        let client = AnthropicClient::new(config).unwrap();
        let result = client.open(b"{\"fixture\":true}".to_vec()).await;
        let requests = worker.await.unwrap();
        let error = result.as_ref().err();
        let report = error.and_then(AnthropicError::opening_report);
        let status = match error.map(AnthropicError::root_cause) {
            Some(AnthropicError::Http { status, .. }) => Some(*status),
            _ => None,
        };
        let actual = json!({"case_id":case.id,"requests":requests,"report":report,
            "cause":error.map(|error|error.root_cause().to_string()),"status":status,
            "retry_visible":error.and_then(AnthropicError::retry_report).is_some(),
            "opening_budget_exhausted":matches!(error.map(AnthropicError::root_cause),Some(AnthropicError::OpeningBudgetExhausted))});
        std::fs::write(
            directory.join(format!("{}.jsonl", case.id)),
            format!("{actual}\n"),
        )
        .unwrap();
    }
    for case in &cases {
        let actual = read_actual(&directory.join(format!("{}.jsonl", case.id)));
        let report = &actual["report"];
        let requests = actual["requests"].as_array().unwrap();
        assert!(!actual["cause"].is_null(), "{actual}");
        assert_eq!(actual["retry_visible"], case.attempts > 1, "{actual}");
        assert_eq!(requests.len(), case.attempts, "{actual}");
        assert_eq!(
            report["observations"].as_array().unwrap().len(),
            case.attempts,
            "{actual}"
        );
        assert_eq!(report["terminal_stop"], json!(case.stop), "{actual}");
        assert_eq!(actual["status"], json!(case.status), "{actual}");
        assert!(
            requests.windows(2).all(|pair| pair[0] == pair[1]),
            "{actual}"
        );
        if case.mode == "headers" {
            assert_eq!(actual["opening_budget_exhausted"], true, "{actual}");
        }
    }
}

#[test]
fn diagnostic_boundaries_do_not_rewrite_actual_send_decisions() {
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("opening_diagnostic_boundaries.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-anthropic-diagnostic-boundaries-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("ANTHROPIC_BOUNDARY_EVIDENCE={}", directory.display());
    for case in &cases {
        let mut report = RetryReport::default();
        if case["attempts"] == 1 {
            record(
                &mut report,
                None,
                None,
                None,
                Some(RetryDecision::Retry {
                    delay_ms: 0,
                    basis: kolyan_protocol_http::WaitBasis::Backoff,
                }),
            );
        }
        report.finish(StopReason::ElapsedLimit);
        let error = retried(AnthropicError::OpeningBudgetExhausted, report.clone());
        let actual = json!({"case_id":case["id"],"report":error.opening_report(),
            "expected_report":report,"retry_visible":error.retry_report().is_some(),
            "cause":error.root_cause().to_string(),"meaning":"diagnostic_only_no_send"});
        std::fs::write(
            directory.join(format!("{}.jsonl", case["id"].as_str().unwrap())),
            format!("{actual}\n"),
        )
        .unwrap();
    }
    for case in &cases {
        let actual =
            read_actual(&directory.join(format!("{}.jsonl", case["id"].as_str().unwrap())));
        let report = &actual["report"];
        let attempts = report["observations"].as_array().unwrap().len();
        assert_eq!(report, &actual["expected_report"], "{actual}");
        assert_eq!(actual["retry_visible"], attempts > 1, "{actual}");
        assert_eq!(json!(attempts), case["attempts"], "{actual}");
        assert_eq!(
            report["terminal_stop"],
            json!(StopReason::ElapsedLimit),
            "{actual}"
        );
        if attempts == 1 {
            assert!(
                report["observations"][0]["decision"]["Retry"].is_object(),
                "{actual}"
            );
        }
    }
}

fn read_actual(path: &std::path::Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}
