//! Real loopback requests only; not actual Provider network acceptance.

use super::*;
use crate::HttpRetryPolicy;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    http_retries: u32,
    transport_retries: u8,
    window_ms: u64,
    server_delay_ms: u64,
    mode: Mode,
    responses: Vec<Reply>,
    expected_requests: usize,
    expected_error: Option<String>,
    expected_stop: Option<String>,
    expected_basis: Option<String>,
    #[serde(default)]
    expected_opening_max_ms: Option<u64>,
    #[serde(default)]
    expected_opening_min_ms: Option<u64>,
    #[serde(default)]
    expected_http_status: Option<u16>,
    #[serde(default)]
    expected_http_body: Option<String>,
    #[serde(default)]
    expected_no_http_status: bool,
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Stream,
    Create,
    Cancel,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    status: u16,
    body: String,
    #[serde(default)]
    headers: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    close: bool,
    #[serde(default)]
    stall_ms: u64,
    #[serde(default)]
    body_delay_ms: u64,
    #[serde(default)]
    declared_extra: usize,
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: oneshot::Sender<()>,
    worker: tokio::task::JoinHandle<Result<(), String>>,
}

impl Server {
    async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let (stop, mut stopped) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let started = Instant::now();
            let mut replies = replies.into_iter();
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    accepted = listener.accept() => accepted.map_err(|e| e.to_string())?,
                };
                let mut request =
                    tokio::time::timeout(Duration::from_secs(2), read_request(&mut socket))
                        .await
                        .map_err(|e| e.to_string())??;
                request["received_elapsed_ms"] = json!(elapsed_ms(started));
                captured.lock().unwrap().push(request);
                let reply = replies.next().ok_or("unexpected extra request")?;
                if reply.close {
                    continue;
                }
                if reply.stall_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(reply.stall_ms)).await;
                }
                let mut response = format!(
                    "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\nrequest-id: localhost-request\r\n",
                    reply.status,
                    reply.body.len() + reply.declared_extra
                );
                for (key, value) in reply.headers {
                    response.push_str(&format!("{key}: {value}\r\n"));
                }
                response.push_str("\r\n");
                // A client may cancel during a bounded send; preserve this in the worker result.
                if let Err(error) = socket.write_all(response.as_bytes()).await {
                    return Err(error.to_string());
                }
                if reply.body_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(reply.body_delay_ms)).await;
                }
                socket
                    .write_all(reply.body.as_bytes())
                    .await
                    .map_err(|error| error.to_string())?;
            }
        });
        Self {
            url,
            requests,
            stop,
            worker,
        }
    }
}

async fn read_request(socket: &mut TcpStream) -> Result<Value, String> {
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 8192 {
            return Err("oversized header".into());
        }
        let byte = socket.read_u8().await.map_err(|e| e.to_string())?;
        bytes.push(byte);
    };
    let header = std::str::from_utf8(&bytes).map_err(|e| e.to_string())?;
    let length = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .ok_or("missing Content-Length")?
        .parse::<usize>()
        .map_err(|e| e.to_string())?;
    if length > 65536 {
        return Err("oversized request body".into());
    }
    let mut body = vec![0; length];
    socket
        .read_exact(&mut body)
        .await
        .map_err(|e| e.to_string())?;
    Ok(
        json!({"request_line":header.lines().next(), "body_bytes":body, "body":serde_json::from_slice::<Value>(&body).map_err(|e| e.to_string())?, "header_bytes":header_end}),
    )
}

fn request(stream: bool) -> MessageCreateRequest {
    MessageCreateRequest {
        model: "localhost-model".into(),
        max_tokens: 32,
        messages: vec![json!({"role":"user","content":"Unicode 原始输入"})],
        system: None,
        tools: vec![],
        tool_choice: None,
        thinking: None,
        output_config: None,
        stream,
    }
}

fn classify(error: &AnthropicError) -> &'static str {
    match error.root_cause() {
        AnthropicError::OpeningBudgetExhausted => "transport",
        AnthropicError::Http { .. } => "http",
        AnthropicError::Transport { .. } => "transport",
        AnthropicError::Decode(_) => "decode",
        AnthropicError::Framing(_) => "framing",
        AnthropicError::Api(_) => "api",
        AnthropicError::Configuration(_) => "configuration",
        AnthropicError::Count(_) => "count",
        AnthropicError::RetriedError { .. } => unreachable!(),
    }
}

#[tokio::test]
async fn localhost_opening_matrix_exports_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("opening_cases.json")).unwrap();
    run_cases(cases).await;
}

#[tokio::test]
async fn current_attempt_timeout_classification_exports_before_comparison() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("opening_timeout_cases.json")).unwrap();
    run_cases(cases).await;
}

async fn run_cases(cases: Vec<Case>) {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-anthropic-opening-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("ANTHROPIC_OPENING_EVIDENCE={}", directory.display());
    let mut failures = Vec::new();
    for case in cases {
        let server = Server::start(case.responses).await;
        let mut config = AnthropicConfig::new("localhost-placeholder");
        config.base_url = server.url;
        config.timeout = Duration::from_secs(2);
        config.transport_retries = case.transport_retries;
        config.http_retry = HttpRetryPolicy::new(
            case.http_retries,
            case.window_ms,
            case.server_delay_ms,
            RetryProfile::Anthropic,
        )
        .unwrap();
        let client = AnthropicClient::new(config).unwrap();
        let started = Instant::now();
        let mut report = RetryReport::default();
        let mut events = Vec::new();
        let result = match case.mode {
            Mode::Create => match client.create_message_with_report(&request(false)).await {
                Ok(response) => {
                    report = response.retry_report;
                    events.push(json!({"message_id":response.response.id}));
                    Ok(())
                }
                Err(error) => Err(error),
            },
            Mode::Stream => match client.stream_message(&request(true)).await {
                Ok(mut stream) => {
                    report = stream.retry_report().clone();
                    let mut error = None;
                    while let Some(event) = stream.next().await {
                        match event {
                            Ok(event) => events.push(json!({"event":format!("{event:?}")})),
                            Err(cause) => {
                                error = Some(cause);
                                break;
                            }
                        }
                    }
                    error.map_or(Ok(()), Err)
                }
                Err(error) => Err(error),
            },
            Mode::Cancel => {
                let request = request(true);
                let future = client.stream_message(&request);
                let result = tokio::time::timeout(Duration::from_millis(100), future).await;
                events.push(json!({"cancelled_by_dropping_open_future": result.is_err()}));
                tokio::time::sleep(Duration::from_millis(600)).await;
                match result {
                    Err(_) => Ok(()),
                    Ok(Err(error)) => Err(error),
                    Ok(Ok(stream)) => {
                        report = stream.retry_report().clone();
                        Ok(())
                    }
                }
            }
        };
        let opening_elapsed_ms = elapsed_ms(started);
        if let Err(error) = &result
            && let Some(retries) = error.retry_report()
        {
            report = retries.clone();
        }
        let error_kind = result.as_ref().err().map(classify);
        let error = result.as_ref().err().map(|error| {
            let (status, body) = match error.root_cause() {
                AnthropicError::Http { status, body } => (Some(*status), Some(body.as_str())),
                _ => (None, None),
            };
            json!({"kind":classify(error),"description":error.to_string(),"root":error.root_cause().to_string(),"wrapped":error.retry_report().is_some(),"http_status":status,"http_body":body})
        });
        let _ = server.stop.send(());
        let server_result = server
            .worker
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result);
        let requests = server.requests.lock().unwrap().clone();
        let actual = json!({"case_id":case.id,"requests":requests,"retry_report":report,"events":events,"error":error,"opening_elapsed_ms":opening_elapsed_ms,"elapsed_ms":elapsed_ms(started),"server_error":server_result.as_ref().err()});
        std::fs::write(
            directory.join(format!("{}.jsonl", case.id)),
            format!("{}\n", serde_json::to_string(&actual).unwrap()),
        )
        .unwrap();
        if requests.len() != case.expected_requests
            || error_kind != case.expected_error.as_deref()
            || server_result.is_err()
        {
            failures.push(format!("{}: {actual}", case.id));
        }
        if case
            .expected_http_status
            .is_some_and(|status| actual["error"]["http_status"] != status)
            || case
                .expected_http_body
                .as_ref()
                .is_some_and(|body| actual["error"]["http_body"] != *body)
        {
            failures.push(format!(
                "{}: current HTTP cause not retained: {actual}",
                case.id
            ));
        }
        if let Some(status) = case.expected_http_status
            && report.was_retried()
            && report.observations().last().unwrap().status != Some(status)
        {
            failures.push(format!(
                "{}: current attempt status missing from report: {actual}",
                case.id
            ));
        }
        if case.expected_no_http_status
            && (!actual["error"]["http_status"].is_null()
                || report
                    .observations()
                    .last()
                    .is_some_and(|observation| observation.status.is_some()))
        {
            failures.push(format!(
                "{}: header timeout fabricated HTTP status: {actual}",
                case.id
            ));
        }
        if case
            .expected_opening_max_ms
            .is_some_and(|bound| opening_elapsed_ms > bound)
            || case
                .expected_opening_min_ms
                .is_some_and(|bound| opening_elapsed_ms < bound)
        {
            failures.push(format!(
                "{}: opening/body elapsed outside expected bound: {actual}",
                case.id
            ));
        }
        if requests
            .windows(2)
            .any(|pair| pair[0]["body_bytes"] != pair[1]["body_bytes"])
        {
            failures.push(format!("{}: changed body bytes", case.id));
        }
        if requests
            .iter()
            .any(|request| request["request_line"] != "POST /v1/messages HTTP/1.1")
        {
            failures.push(format!("{}: wrong endpoint", case.id));
        }
        if case.mode != Mode::Cancel && result.is_ok() && report.attempts() != requests.len() {
            failures.push(format!("{}: missing report", case.id));
        }
        if requests.len() > 1 && result.is_err() && report.attempts() != requests.len() {
            failures.push(format!("{}: terminal retry report missing", case.id));
        }
        if let Some(expected) = case.expected_stop
            && actual["retry_report"]["observations"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()["decision"]["Stop"]["reason"]
                != expected
        {
            failures.push(format!("{}: wrong stop: {actual}", case.id));
        }
        if let Some(expected) = case.expected_basis
            && actual["retry_report"]["observations"][0]["decision"]["Retry"]["basis"] != expected
        {
            failures.push(format!("{}: wrong wait basis: {actual}", case.id));
        }
        if case.mode == Mode::Cancel
            && actual["events"][0]["cancelled_by_dropping_open_future"] != true
        {
            failures.push(format!("{}: did not cancel during backoff", case.id));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
