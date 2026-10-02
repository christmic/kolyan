//! Actual localhost opening errors; no vendor requests or simulated send reports.

use std::{fs, io::Write, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::*;

#[tokio::test]
async fn single_http_opening_reports_are_retained_without_retry_reports() {
    let plan: Value =
        serde_json::from_str(include_str!("opening_diagnostic_http_cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-openai-opening-http-diagnostics-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("OPENAI_OPENING_HTTP_DIAGNOSTIC_TRACE={}", path.display());
    let mut output = fs::File::create(&path).unwrap();
    for case in plan["cases"].as_array().unwrap() {
        let observed = observe(&plan, case).await;
        writeln!(output, "{observed}").unwrap();
        output.sync_all().unwrap();
    }
    let actual = fs::read_to_string(&path).unwrap();
    assert_eq!(plan["schema_version"], 1);
    assert_eq!(
        actual.lines().count(),
        plan["cases"].as_array().unwrap().len()
    );
    for (line, case) in actual.lines().zip(plan["cases"].as_array().unwrap()) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["id"], case["id"], "{row}");
        assert!(row["server_error"].is_null(), "{row}");
        let requests = row["requests"].as_array().unwrap();
        assert_eq!(requests.len(), 1, "{row}");
        assert_eq!(
            requests[0]["method_path"], "POST /v1/responses HTTP/1.1",
            "{row}"
        );
        assert_eq!(
            serde_json::from_str::<Value>(requests[0]["body"].as_str().unwrap()).unwrap(),
            row["request"],
            "{row}"
        );
        assert_eq!(row["outcome"]["root"], "http", "{row}");
        assert_eq!(row["outcome"]["status"], case["status"], "{row}");
        assert_eq!(row["outcome"]["body"], case["body"], "{row}");
        assert!(row["outcome"]["retry_report"].is_null(), "{row}");
        let report = &row["outcome"]["opening_report"];
        assert_eq!(report["terminal_stop"], case["expected_stop"], "{row}");
        let observations = report["observations"].as_array().unwrap();
        assert_eq!(observations.len(), 1, "{row}");
        assert_eq!(observations[0]["attempt"], 1, "{row}");
        assert_eq!(observations[0]["status"], case["status"], "{row}");
        assert_eq!(
            observations[0]["decision"],
            json!({"Stop":{"reason":case["expected_stop"]}}),
            "{row}"
        );
    }
}

async fn observe(plan: &Value, case: &Value) -> Value {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let reply = case.clone();
    let io_ms = plan["io_timeout_ms"].as_u64().unwrap();
    let idle_ms = plan["idle_timeout_ms"].as_u64().unwrap();
    let max_requests = plan["max_requests"].as_u64().unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        let mut error = None;
        for index in 0..max_requests {
            let wait_ms = if index == 0 { io_ms } else { idle_ms };
            let accepted =
                tokio::time::timeout(Duration::from_millis(wait_ms), listener.accept()).await;
            let mut socket = match accepted {
                Ok(Ok((socket, _))) => socket,
                Err(_) if index > 0 => break,
                other => {
                    error = Some(format!("local accept failed: {other:?}"));
                    break;
                }
            };
            match tokio::time::timeout(Duration::from_millis(io_ms), read_request(&mut socket))
                .await
            {
                Ok(Ok(request)) => requests.push(request),
                other => {
                    error = Some(format!("local read failed: {other:?}"));
                    break;
                }
            }
            let body = reply["body"].as_str().unwrap();
            let mut response = format!(
                "HTTP/1.1 {} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                reply["status"],
                body.len()
            );
            for (name, value) in reply["headers"].as_object().unwrap() {
                response.push_str(&format!("{name}: {}\r\n", value.as_str().unwrap()));
            }
            response.push_str("\r\n");
            response.push_str(body);
            match tokio::time::timeout(
                Duration::from_millis(io_ms),
                socket.write_all(response.as_bytes()),
            )
            .await
            {
                Ok(Ok(())) => {}
                other => {
                    error = Some(format!("local reply failed: {other:?}"));
                    break;
                }
            }
        }
        (requests, error)
    });
    let mut config = OpenAiConfig::new("localhost-not-a-secret");
    config.base_url = url;
    config.timeout = Duration::from_millis(io_ms);
    config.transport_retries = 0;
    config.http_retry = crate::HttpRetryPolicy::new(
        u32::try_from(plan["http_retries"].as_u64().unwrap()).unwrap(),
        plan["opening_window_ms"].as_u64().unwrap(),
        plan["server_delay_ms"].as_u64().unwrap(),
        RetryProfile::OpenAi,
    )
    .unwrap();
    let client = OpenAiClient::new(config).unwrap();
    let request = ResponseCreateRequest {
        model: "fixture-model".into(),
        input: json!([{"role":"user","content":"diagnostic input"}]),
        instructions: None,
        max_output_tokens: Some(64),
        tools: vec![],
        tool_choice: None,
        text: None,
        reasoning: None,
        prompt_cache_key: None,
        prompt_cache_retention: None,
        stream: true,
    };
    let outcome = match client.stream_response(&request).await {
        Ok(_) => json!({"root":"unexpected_success"}),
        Err(error) => {
            let root = match error.root() {
                OpenAiError::Http { status, body } => {
                    json!({"root":"http","status":status,"body":body})
                }
                other => json!({"root":"unexpected","error":other.to_string()}),
            };
            json!({"root":root["root"],"status":root["status"],"body":root["body"],
                "opening_report":error.opening_report(),"retry_report":error.retry_report(),"message":error.to_string()})
        }
    };
    let (requests, server_error) = server.await.unwrap();
    json!({"id":case["id"],"request":request,"requests":requests,"server_error":server_error,"outcome":outcome})
}

async fn read_request(socket: &mut TcpStream) -> std::io::Result<Value> {
    const MAX_BYTES: usize = 32768;
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut buffer = [0_u8; 4096];
        let count = socket.read(&mut buffer).await?;
        if count == 0 || bytes.len() + count > MAX_BYTES {
            return Err(std::io::Error::other("incomplete or oversized request"));
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let method_path = headers.lines().next().unwrap().to_owned();
    let length = headers
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .ok_or_else(|| std::io::Error::other("missing content length"))?;
    if header_end + length > MAX_BYTES {
        return Err(std::io::Error::other("oversized body"));
    }
    while bytes.len() < header_end + length {
        let mut buffer = [0_u8; 4096];
        let count = socket.read(&mut buffer).await?;
        if count == 0 || bytes.len() + count > MAX_BYTES {
            return Err(std::io::Error::other("incomplete or oversized body"));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let body = String::from_utf8(bytes[header_end..header_end + length].to_vec())
        .map_err(std::io::Error::other)?;
    Ok(json!({"method_path":method_path,"body":body}))
}
