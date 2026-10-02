//! Local HTTP opening faults, not actual-provider or effect acceptance.

use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

use super::*;
use crate::HttpRetryPolicy;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    schema_version: u32,
    server_idle_timeout_ms: u64,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    transport_retries: u8,
    http_retries: u32,
    window_ms: u64,
    server_delay_ms: u64,
    diagnostics: bool,
    mode: String,
    cancel_after_ms: Option<u64>,
    responses: Vec<Reply>,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    status: Option<u16>,
    headers: BTreeMap<String, String>,
    body: String,
    truncate: bool,
    #[serde(default)]
    body_delay_ms: u64,
    #[serde(default)]
    header_delay_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    attempts: usize,
    kind: String,
    status: Option<u16>,
    #[serde(default)]
    report_statuses: Option<Vec<Option<u16>>>,
    #[serde(default)]
    message_contains: Option<String>,
}

#[tokio::test]
async fn localhost_opening_fault_matrix_exports_all_rows_before_comparison() {
    let plan: Plan = serde_json::from_str(include_str!("opening_cases.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-openai-opening-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    println!("OPENAI_OPENING_TRACE={}", path.display());
    let mut file = fs::File::create(&path).unwrap();
    for case in &plan.cases {
        let observed = observe(case, plan.server_idle_timeout_ms).await;
        writeln!(file, "{observed}").unwrap();
        file.sync_all().unwrap();
    }
    let actual = fs::read_to_string(path).unwrap();
    assert_eq!(plan.schema_version, 1);
    assert_eq!(actual.lines().count(), plan.cases.len());
    for (line, case) in actual.lines().zip(&plan.cases) {
        compare(&serde_json::from_str::<Value>(line).unwrap(), case);
    }
}

async fn observe(case: &Case, idle_ms: u64) -> Value {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = requests.clone();
    let replies = case.responses.iter().map(|reply| json!({"status":reply.status,"headers":reply.headers,"body":reply.body,"truncate":reply.truncate,"body_delay_ms":reply.body_delay_ms,"header_delay_ms":reply.header_delay_ms})).collect::<Vec<_>>();
    let server = tokio::spawn(async move {
        let started = Instant::now();
        for reply in replies {
            let Ok(Ok((mut socket, _))) =
                tokio::time::timeout(Duration::from_millis(idle_ms), listener.accept()).await
            else {
                break;
            };
            let wire = tokio::time::timeout(Duration::from_millis(idle_ms), async {
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut chunk = [0_u8; 4096];
                    let count = socket.read(&mut chunk).await?;
                    if count == 0 || bytes.len() + count > 131072 {
                        return Err(std::io::Error::other("incomplete/oversized local request"));
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..header_end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .ok_or_else(|| std::io::Error::other("missing content length"))?;
                if length > 131072 {
                    return Err(std::io::Error::other("oversized local body"));
                }
                let method_path = headers.lines().next().unwrap().to_owned();
                while bytes.len() - header_end < length {
                    let mut chunk = [0_u8; 4096];
                    let count = socket.read(&mut chunk).await?;
                    if count == 0 {
                        return Err(std::io::Error::other("incomplete local body"));
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                Ok((method_path, bytes[header_end..header_end + length].to_vec()))
            })
            .await;
            let (method_path, body) = match wire {
                Ok(Ok(wire)) => wire,
                other => return Some(format!("local fixture read failed: {other:?}")),
            };
            captured.lock().await.push(json!({"method_path":method_path,"body":body,"elapsed_ms":started.elapsed().as_millis(),"reply_status":reply["status"]}));
            if let Some(status) = reply["status"].as_u64() {
                let body = reply["body"].as_str().unwrap();
                let length = body.len() + usize::from(reply["truncate"] == true) * 100;
                let mut response = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: text/event-stream\r\nContent-Length: {length}\r\nConnection: close\r\n"
                );
                for (key, value) in reply["headers"].as_object().unwrap() {
                    response.push_str(&format!("{key}: {}\r\n", value.as_str().unwrap()));
                }
                response.push_str("\r\n");
                tokio::time::sleep(Duration::from_millis(
                    reply["header_delay_ms"].as_u64().unwrap(),
                ))
                .await;
                if let Err(error) = socket.write_all(response.as_bytes()).await {
                    if reply["header_delay_ms"].as_u64().unwrap() > 0 {
                        continue;
                    }
                    return Some(error.to_string());
                }
                tokio::time::sleep(Duration::from_millis(
                    reply["body_delay_ms"].as_u64().unwrap(),
                ))
                .await;
                if let Err(error) = socket.write_all(body.as_bytes()).await {
                    // Opening cancellation deliberately closes the socket before a delayed error body.
                    if status == 200 {
                        return Some(error.to_string());
                    }
                }
                let _ = socket.shutdown().await;
            }
        }
        None
    });
    let mut config = OpenAiConfig::new("localhost-fixture-not-a-secret");
    config.base_url = url;
    config.transport_retries = case.transport_retries;
    config.http_retry = HttpRetryPolicy::new(
        case.http_retries,
        case.window_ms,
        case.server_delay_ms,
        RetryProfile::OpenAi,
    )
    .unwrap();
    config.diagnostics = case.diagnostics;
    let client = OpenAiClient::new(config).unwrap();
    let request = crate::ResponseCreateRequest {
        model: "fixture-model".into(),
        input: json!([{"role":"user","content":"same frozen input"}]),
        instructions: None,
        max_output_tokens: Some(64),
        tools: vec![],
        tool_choice: None,
        text: None,
        reasoning: None,
        prompt_cache_key: None,
        prompt_cache_retention: None,
        stream: case.mode == "stream",
    };
    let operation = async {
        if case.mode == "json" {
            match client.create_response_with_report(&request).await {
                Ok(opened) => {
                    json!({"kind":"success","report":opened.retry_report,"id":opened.response.id})
                }
                Err(error) => error_json(&error),
            }
        } else {
            match client.stream_response(&request).await {
                Err(error) => error_json(&error),
                Ok(mut stream) => {
                    let report = stream.retry_report().clone();
                    let mut events = Vec::new();
                    while let Some(event) = stream.next().await {
                        match event {
                            Ok(event) => {
                                events.push(json!({"kind":event.kind,"fields":event.fields}))
                            }
                            Err(error) => {
                                return json!({"kind":error_json(&error)["kind"],"error":error_json(&error),"report":report,"events":events});
                            }
                        }
                    }
                    json!({"kind":"success","report":report,"events":events})
                }
            }
        }
    };
    let outcome = if let Some(ms) = case.cancel_after_ms {
        match tokio::time::timeout(Duration::from_millis(ms), operation).await {
            Ok(result) => result,
            Err(_) => json!({"kind":"cancelled","future_dropped":true}),
        }
    } else {
        operation.await
    };
    let server_error = server.await.unwrap();
    json!({"id":case.id,"actual_requests":requests.lock().await.clone(),"server_error":server_error,"outcome":outcome,"expected":json!({"attempts":case.expected.attempts,"kind":case.expected.kind,"status":case.expected.status})})
}

fn error_json(error: &OpenAiError) -> Value {
    let (kind, status) = match error.root() {
        OpenAiError::Http { status, .. } => ("http", Some(*status)),
        OpenAiError::Decode(_) => ("decode", None),
        OpenAiError::Framing(_) => ("framing", None),
        OpenAiError::Transport { .. } => ("transport", None),
        OpenAiError::OpeningBudgetExhausted => ("opening_budget", None),
        _ => ("other", None),
    };
    json!({"kind":kind,"status":status,"report":error.retry_report(),"message":error.to_string()})
}

fn compare(row: &Value, case: &Case) {
    assert_eq!(row["id"], case.id);
    assert!(row["server_error"].is_null(), "{row}");
    let requests = row["actual_requests"].as_array().unwrap();
    assert_eq!(requests.len(), case.expected.attempts, "{row}");
    assert_eq!(row["outcome"]["kind"], case.expected.kind, "{row}");
    if let Some(status) = case.expected.status {
        assert_eq!(row["outcome"]["status"], status, "{row}");
    }
    if let Some(text) = &case.expected.message_contains {
        assert!(
            row["outcome"]["message"].as_str().unwrap().contains(text),
            "{row}"
        );
    }
    for request in requests {
        assert_eq!(request["method_path"], "POST /v1/responses HTTP/1.1");
        assert_eq!(
            request["body"], requests[0]["body"],
            "every actual attempt uses the same bytes"
        );
    }
    if requests.len() > 1 {
        let observations = row["outcome"]["report"]["observations"].as_array().unwrap();
        assert_eq!(observations.len(), requests.len());
        for (index, observation) in observations.iter().enumerate() {
            assert_eq!(observation["attempt"], index + 1);
            if let Some(statuses) = &case.expected.report_statuses {
                assert_eq!(statuses.len(), observations.len());
                assert_eq!(observation["status"], json!(statuses[index]));
            } else {
                assert_eq!(observation["status"], requests[index]["reply_status"]);
            }
            if let Some(delay) = observation["decision"]["Retry"]["delay_ms"].as_u64() {
                let next = requests[index + 1]["elapsed_ms"].as_u64().unwrap();
                let current = requests[index]["elapsed_ms"].as_u64().unwrap();
                assert!(
                    next - current >= delay,
                    "recorded wait actually precedes next send"
                );
                if delay > 0 && observation["decision"]["Retry"]["basis"] == "Backoff" {
                    let base = (500_u64 << index.min(4)).min(8000);
                    assert!((base * 3 / 4..=base).contains(&delay));
                }
            }
        }
    } else if case.expected.kind == "http" {
        assert!(
            row["outcome"]["report"].is_null(),
            "no Retried wrapper for one send"
        );
    }
}
