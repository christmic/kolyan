//! Loopback protocol fixtures, not actual LLM acceptance. The real Runner,
//! Runtime and isolated worker execute the effect; only HTTP responses are scripted.

mod host;
mod transport;

use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Protocol {
    Openai,
    Anthropic,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    protocol: Protocol,
    path: String,
    content: String,
    call_id: String,
    fault_status: u16,
    fault_code: String,
    fault_type: String,
    retry_after_ms: String,
    should_retry: String,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    requests: usize,
    completed: bool,
    effect_starts: usize,
    receipts: usize,
    executions: usize,
    retry_statuses: Vec<u16>,
}

#[tokio::test]
async fn completion_opening_retries_preserve_real_write_receipt_over_both_protocols() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/agent/opening_retry.json")).unwrap();
    let installation = super::tools::worker::WorkerRun::prepare().await;
    let mut observations = Vec::new();
    // Export every row, including failures, before any scenario comparison.
    for case in &cases {
        observations.push(host::run(case, &installation).await);
    }
    for (case, actual) in cases.iter().zip(observations) {
        compare(case, &actual);
    }
}

fn compare(case: &Case, actual: &Value) {
    let label = format!("{} evidence {}", case.id, actual["evidence"]);
    assert!(actual["server_error"].is_null(), "{label}: {actual}");
    assert_eq!(actual["completed"], case.expected.completed, "{label}");
    let requests = actual["requests"].as_array().unwrap();
    assert_eq!(requests.len(), case.expected.requests, "{label}: {actual}");
    assert_eq!(actual["file"], case.content, "{label}");
    assert_eq!(
        actual["file_bytes"],
        serde_json::json!(case.content.as_bytes()),
        "{label}"
    );
    assert!(actual["file_error"].is_null(), "{label}");
    assert_eq!(
        actual["effect_starts"], case.expected.effect_starts,
        "{label}"
    );
    assert_eq!(actual["receipts"], case.expected.receipts, "{label}");
    assert_eq!(actual["executions"], case.expected.executions, "{label}");
    assert_eq!(actual["model_requests"], 2, "{label}");
    let result = &actual["tool_result"];
    assert_eq!(result["call_id"], case.call_id, "{label}");
    assert_eq!(result["is_error"], false, "{label}");
    for request in &requests[1..] {
        assert_eq!(request["file_before_response"], case.content, "{label}");
        let receipts: Vec<_> = request["ledger_before_response"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "effect_receipt")
            .cloned()
            .collect();
        assert_eq!(
            receipts,
            *actual["receipt_events"].as_array().unwrap(),
            "{label}: receipt must predate completion opening and remain identical"
        );
        assert_eq!(
            receipts[0]["payload"]["input"]["prepared"]["call"]["name"], "file.write",
            "{label}"
        );
        assert_eq!(
            receipts[0]["payload"]["input"]["prepared"]["call"]["id"], case.call_id,
            "{label}"
        );
        assert_eq!(receipts[0]["payload"]["output"], *result, "{label}");
        let body = &request["body"];
        match case.protocol {
            Protocol::Openai => {
                let output = body["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|item| {
                        item["type"] == "function_call_output" && item["call_id"] == case.call_id
                    })
                    .unwrap_or_else(|| panic!("{label}: missing original tool result: {body}"));
                assert_eq!(
                    output["output"].as_str(),
                    result["content"].as_str(),
                    "{label}"
                );
            }
            Protocol::Anthropic => {
                let output = body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .flat_map(|message| message["content"].as_array().into_iter().flatten())
                    .find(|block| {
                        block["type"] == "tool_result" && block["tool_use_id"] == case.call_id
                    })
                    .unwrap_or_else(|| panic!("{label}: missing original tool result: {body}"));
                assert_eq!(
                    output["content"].as_str(),
                    result["content"].as_str(),
                    "{label}"
                );
            }
        }
    }
    if case.expected.completed {
        assert_eq!(requests[1]["raw_body"], requests[2]["raw_body"], "{label}");
    } else {
        assert!(
            actual["error"].as_str().unwrap().contains(&case.fault_code),
            "{label}: {actual}"
        );
    }
    let reports = actual["retry_reports"].as_array().unwrap();
    for metadata in actual["retry_metadata"].as_array().unwrap() {
        if metadata.get("report").is_some() {
            assert_eq!(metadata["kind"], "local_http_opening_retry", "{label}");
        }
    }
    if case.expected.retry_statuses.is_empty() {
        assert!(reports.is_empty(), "{label}");
    } else {
        assert_eq!(reports.len(), 1, "{label}");
        let observations = reports[0]["observations"].as_array().unwrap();
        let statuses: Vec<_> = observations
            .iter()
            .map(|row| row["status"].as_u64().unwrap())
            .collect();
        assert_eq!(
            statuses,
            case.expected
                .retry_statuses
                .iter()
                .map(|v| u64::from(*v))
                .collect::<Vec<_>>(),
            "{label}"
        );
        assert_eq!(observations[0]["attempt"], 1, "{label}");
        assert_eq!(observations[1]["attempt"], 2, "{label}");
        assert_eq!(observations[0]["request_id"], "opening-1", "{label}");
        assert_eq!(observations[1]["request_id"], "opening-2", "{label}");
        assert_eq!(
            observations[0]["decision"]["Retry"]["basis"], "RetryAfterMs",
            "{label}"
        );
        assert_eq!(
            observations[0]["decision"]["Retry"]["delay_ms"],
            case.retry_after_ms.parse::<u64>().unwrap(),
            "{label}"
        );
    }
}
