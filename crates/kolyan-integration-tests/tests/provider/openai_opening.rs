//! Loopback Provider acceptance: actual HTTP, no model or tool execution.

#[path = "opening/openai_effect_tests.rs"]
mod effect_tests;

use futures_util::StreamExt;
use kolyan_model::{ModelEvent, ModelProvider, ModelRequest};
use kolyan_protocol_openai::{HttpRetryPolicy, OpenAiClient, OpenAiConfig, RetryProfile};
use kolyan_provider_openai::OpenAiProvider;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    replies: Vec<Reply>,
    expected_requests: usize,
    expected_kind: Option<String>,
    expected_status: Option<u16>,
    expected_retry_metadata: bool,
    #[serde(default = "default_opening_window")]
    window_ms: u64,
    #[serde(default)]
    expected_phase: Option<String>,
}

fn default_opening_window() -> u64 {
    2000
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    status: u16,
    body: String,
    #[serde(default)]
    body_delay_ms: u64,
}

#[tokio::test]
async fn localhost_provider_preserves_root_cause_and_local_retry_metadata() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("opening/openai.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-openai-provider-opening-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("OPENAI_PROVIDER_OPENING_EVIDENCE={}", directory.display());
    let mut failures = Vec::new();
    for case in cases {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let (stop, mut stopped) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let mut replies = case.replies.into_iter();
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stopped => return Ok::<_,String>(()),
                    result = listener.accept() => result.map_err(|e| e.to_string())?,
                };
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    if headers.len() >= 8192 {
                        return Err("oversized header".into());
                    }
                    let byte = tokio::time::timeout(Duration::from_secs(2), socket.read_u8())
                        .await
                        .map_err(|e| e.to_string())?
                        .map_err(|e| e.to_string())?;
                    headers.push(byte);
                }
                let text = std::str::from_utf8(&headers).map_err(|e| e.to_string())?;
                let length = text
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then_some(value.trim())
                    })
                    .ok_or("missing content-length")?
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?;
                if length > 65536 {
                    return Err("oversized body".into());
                }
                let mut body = vec![0; length];
                tokio::time::timeout(Duration::from_secs(2), socket.read_exact(&mut body))
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string())?;
                captured.lock().unwrap().push(json!({"request_line":text.lines().next(),"body_bytes":body,"body":serde_json::from_slice::<Value>(&body).map_err(|e| e.to_string())?}));
                let reply = replies.next().ok_or("unexpected extra send")?;
                let response = format!(
                    "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\nretry-after-ms: 1\r\nrequest-id: provider-loopback\r\n\r\n",
                    reply.status,
                    reply.body.len()
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .map_err(|e| e.to_string())?;
                tokio::time::sleep(Duration::from_millis(reply.body_delay_ms)).await;
                let sent = socket.write_all(reply.body.as_bytes()).await;
                if reply.body_delay_ms == 0 {
                    sent.map_err(|e| e.to_string())?;
                }
            }
        });
        let mut config = OpenAiConfig::new("localhost-placeholder");
        config.base_url = endpoint;
        config.http_retry = HttpRetryPolicy::new(
            1,
            case.window_ms,
            case.window_ms.min(1000),
            RetryProfile::OpenAi,
        )
        .unwrap();
        let provider = OpenAiProvider::new(OpenAiClient::new(config).unwrap());
        let request: ModelRequest = serde_json::from_value(json!({"request_id":case.id,"model":{"provider":"localhost-openai","model":"localhost-model"},"system":[],"messages":[],"tools":[],"tool_choice":"auto","output_format":null,"prompt_cache":null,"reasoning":null,"max_output_tokens":32,"extensions":{}})).unwrap();
        let mut events = Vec::new();
        let mut error = None;
        match provider.stream(request).await {
            Err(cause) => error = Some(cause),
            Ok(mut stream) => {
                while let Some(event) = stream.next().await {
                    match event {
                        Ok(event) => events.push(event),
                        Err(cause) => {
                            error = Some(cause);
                            break;
                        }
                    }
                }
            }
        }
        let _ = stop.send(());
        let server_result = worker.await.unwrap();
        let requests = requests.lock().unwrap().clone();
        let error_json = error.as_ref().map(|error| json!({"kind":error.kind,"phase":error.phase,"status":error.status,"provider":error.provider,"message":error.message,"diagnostics":error.diagnostics}));
        let local_report = match &error {
            Some(error) => error
                .diagnostics
                .as_ref()
                .filter(|metadata| metadata["kind"] == "local_http_opening_retry")
                .and_then(|metadata| metadata.get("report")),
            None => events.iter().find_map(|event| match event {
                ModelEvent::Provider(metadata) => metadata
                    .raw
                    .as_ref()
                    .filter(|raw| raw["kind"] == "local_http_opening_retry")
                    .and_then(|raw| raw.get("report")),
                _ => None,
            }),
        };
        let actual = json!({"case_id":case.id,"requests":requests,"events":events,"error":error_json,"server_error":server_result.as_ref().err()});
        std::fs::write(
            directory.join(format!("{}.jsonl", case.id)),
            format!("{}\n", serde_json::to_string(&actual).unwrap()),
        )
        .unwrap();
        if requests.len() != case.expected_requests
            || actual["error"]["kind"].as_str() != case.expected_kind.as_deref()
            || actual["error"]["status"].as_u64() != case.expected_status.map(u64::from)
            || server_result.is_err()
        {
            failures.push(format!("{}: {actual}", case.id));
        }
        if let Some(phase) = &case.expected_phase
            && actual["error"]["phase"] != *phase
        {
            failures.push(format!("{}: wrong error phase: {actual}", case.id));
        }
        if local_report.is_some() != case.expected_retry_metadata
            || local_report.is_some_and(|report| {
                report["observations"].as_array().unwrap().len() != requests.len()
            })
        {
            failures.push(format!(
                "{}: missing or wrong local report: {actual}",
                case.id
            ));
        }
        if requests
            .windows(2)
            .any(|pair| pair[0]["body_bytes"] != pair[1]["body_bytes"])
        {
            failures.push(format!("{}: changed serialized body", case.id));
        }
        if let Some(error) = error
            && case.expected_retry_metadata
            && (!error.message.contains("caused by:")
                || (case.expected_status.is_some() && !error.message.contains("OpenAI HTTP error")))
        {
            failures.push(format!("{}: terminal cause lost", case.id));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
