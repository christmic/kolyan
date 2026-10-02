//! Capture actual localhost request bytes; never participate in production I/O.

use std::time::Duration;

use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub async fn serve(
    listener: TcpListener,
    generation_path: &str,
    count_path: &str,
    stream_response: String,
    count_response: String,
) -> Vec<Value> {
    let mut physical = Vec::new();
    while let Ok(Ok((mut socket, _))) =
        tokio::time::timeout(Duration::from_millis(400), listener.accept()).await
    {
        let mut bytes = Vec::new();
        let (end, length) = loop {
            let mut buffer = [0; 4096];
            let n = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(n, 0, "incomplete fixture request");
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                let head = std::str::from_utf8(&bytes[..end]).unwrap();
                let length = head
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    break (end, length);
                }
            }
        };
        let head = std::str::from_utf8(&bytes[..end]).unwrap();
        let path = head
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        assert!(
            path == generation_path || path == count_path,
            "unexpected endpoint {path}"
        );
        let raw = std::str::from_utf8(&bytes[end + 4..end + 4 + length]).unwrap();
        let body: Value = serde_json::from_str(raw).unwrap();
        let count = path == count_path;
        let response = if count {
            count_response.clone()
        } else {
            stream_response.clone()
        };
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            if count {
                "application/json"
            } else {
                "text/event-stream"
            },
            response.len()
        );
        socket.write_all(header.as_bytes()).await.unwrap();
        socket.write_all(response.as_bytes()).await.unwrap();
        physical.push(json!({"path":path,"raw_body":raw,"body_digest":kolyan_model::digest_json(&body).unwrap(),
            "body":body,"response":response}));
    }
    physical
}
