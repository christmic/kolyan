mod identity;

use crate::{AnthropicClient, AnthropicConfig, AnthropicError, MessageCountTokensRequest};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io::Write, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    response: String,
    mode: String,
    expected: String,
    tokens: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    request: Value,
    rows: Vec<Row>,
}

#[tokio::test]
async fn count_http_data_matrix_exports_all_before_compare() {
    let data: Dataset = serde_json::from_str(include_str!("cases.json")).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-anthropic-count-")
        .tempdir()
        .unwrap()
        .keep();
    let mut export = std::fs::File::create(dir.join("actual.jsonl")).unwrap();
    for row in &data.rows {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut config = AnthropicConfig::new("localhost-fixture-only");
        config.base_url = base.clone();
        config.transport_retries = 3;
        let client = AnthropicClient::new(config).unwrap();
        let mut dto: MessageCountTokensRequest =
            serde_json::from_value(data.request.clone()).unwrap();
        if row.mode == "request_oversize" {
            dto.messages = vec![json!({"role":"user","content":"x".repeat(16 * 1024 * 1024)})];
            let file = std::fs::File::create(dir.join(format!("{}-request.json", row.id))).unwrap();
            serde_json::to_writer(file, &dto).unwrap();
        }
        let mode = row.mode.clone();
        let mut body = row.response.clone();
        if mode == "exact" {
            body.extend(std::iter::repeat_n(' ', 65536 - body.len()));
        }
        if mode == "oversize" {
            body.extend(std::iter::repeat_n(' ', 65537 - body.len()));
        }
        let sent_response = body.clone();
        let redirect_to = format!("{base}/redirect-target");
        let server = tokio::spawn(async move {
            let accepted =
                tokio::time::timeout(Duration::from_millis(350), listener.accept()).await;
            let Ok(Ok((mut socket, _))) = accepted else {
                return json!({"sends":0});
            };
            let mut bytes = Vec::new();
            let (end, length) = loop {
                let mut buf = [0; 4096];
                let count = socket.read(&mut buf).await.unwrap();
                if count == 0 {
                    return json!({"sends":1,"incomplete_request":true});
                }
                bytes.extend_from_slice(&buf[..count]);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&bytes[..end]);
                    let length = header
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                                .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        break (end, length);
                    }
                }
            };
            let header = String::from_utf8_lossy(&bytes[..end]);
            let path = header
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap()
                .to_owned();
            let request: Value = serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
            if mode == "slow_headers" || mode == "drop_after_send" {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            let status = match mode.as_str() {
                "http_error" => 529,
                "redirect" => 302,
                _ => 200,
            };
            let location = if mode == "redirect" {
                format!("Location: {redirect_to}\r\n")
            } else {
                String::new()
            };
            let headers = format!(
                "HTTP/1.1 {status} Fixture\r\n{location}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let write_headers = socket
                .write_all(headers.as_bytes())
                .await
                .map_err(|e| e.to_string());
            if mode == "slow_body" {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            let write_body = socket
                .write_all(body.as_bytes())
                .await
                .map_err(|e| e.to_string());
            drop(socket);
            let extra = tokio::time::timeout(Duration::from_millis(60), listener.accept())
                .await
                .is_ok();
            json!({"sends":1 + usize::from(extra),"path":path,"request":request,"write_headers":write_headers,"write_body":write_body})
        });
        let timeout = match row.mode.as_str() {
            "zero_timeout" => Duration::ZERO,
            "overflow_timeout" => Duration::MAX,
            "slow_body" | "slow_headers" => Duration::from_millis(50),
            _ => Duration::from_secs(2),
        };
        let result = match row.mode.as_str() {
            "drop_before_send" => {
                drop(client.count_tokens(&dto, timeout));
                None
            }
            "drop_after_send" => tokio::select! {
                result = client.count_tokens(&dto, timeout) => Some(result),
                _ = tokio::time::sleep(Duration::from_millis(50)) => None,
            },
            _ => Some(client.count_tokens(&dto, timeout).await),
        };
        let (class, tokens, error) = match result {
            None => ("cancelled", None, None),
            Some(Ok(value)) => ("success", Some(value.input_tokens), None),
            Some(Err(error)) => {
                let class = match &error {
                    AnthropicError::Decode(_) => "decode",
                    AnthropicError::Configuration(_) => "configuration",
                    AnthropicError::Http { .. } => "http",
                    AnthropicError::Count(crate::CountFailure::ResponseLimit) => "limit",
                    AnthropicError::Count(crate::CountFailure::Timeout) => "timeout",
                    AnthropicError::Transport { source, .. } if source.is_timeout() => "timeout",
                    _ => "other",
                };
                (class, None, Some(error.to_string()))
            }
        };
        let physical = server.await.unwrap();
        let actual = json!({"case":row.id,"class":class,"tokens":tokens,"error":error,
            "request":dto,"mode":row.mode,"transport_retry_cap":3,
            "response":sent_response,"physical":physical});
        writeln!(export, "{}", actual).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    let observations: Vec<serde_json::Value> = std::fs::read_to_string(dir.join("actual.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(observations.len(), data.rows.len());
    eprintln!("count evidence: {}", dir.display());
    for (row, actual) in data.rows.iter().zip(&observations) {
        assert_eq!(actual["case"], row.id);
        assert_eq!(
            actual["class"],
            row.expected,
            "{} {}",
            row.id,
            dir.display()
        );
        if let Some(tokens) = row.tokens {
            assert_eq!(actual["tokens"], tokens, "{}", row.id);
        }
        let sends = if matches!(
            row.mode.as_str(),
            "zero_timeout" | "overflow_timeout" | "drop_before_send" | "request_oversize"
        ) {
            0
        } else {
            1
        };
        assert_eq!(actual["physical"]["sends"], sends, "{}", row.id);
        if sends == 1 {
            assert_eq!(actual["physical"]["request"], data.request, "{}", row.id);
            assert_eq!(
                actual["physical"]["path"], "/v1/messages/count_tokens",
                "{}",
                row.id
            );
        }
    }
}
