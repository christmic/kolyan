//! Bounded localhost capture; only method/path and body are retained, never headers.

use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Debug, Serialize)]
pub(super) struct Capture {
    pub request_line: String,
    pub body: String,
}

#[derive(Debug, Serialize)]
pub(super) struct Outcome {
    pub capture: Option<Capture>,
    pub error: Option<String>,
}

pub(super) async fn serve(
    listener: TcpListener,
    response: String,
    timeout: Duration,
    max_bytes: usize,
) -> Outcome {
    let mut capture = None;
    let result = tokio::time::timeout(timeout, async {
        let (mut socket, _) = listener.accept().await.map_err(|error| error.to_string())?;
        capture = Some(read(&mut socket, max_bytes).await?);
        let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len());
        socket.write_all(reply.as_bytes()).await.map_err(|error| error.to_string())?;
        Ok::<(), String>(())
    }).await;
    Outcome {
        capture,
        error: match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(error) => Some(format!("capture deadline: {error}")),
        },
    }
}

async fn read(socket: &mut TcpStream, max_bytes: usize) -> Result<Capture, String> {
    let mut bytes = Vec::new();
    let end = loop {
        read_more(socket, &mut bytes, max_bytes).await?;
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).map_err(|error| error.to_string())?;
    let request_line = headers
        .lines()
        .next()
        .ok_or("missing request line")?
        .to_owned();
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .ok_or("missing content length")?
        .map_err(|error| error.to_string())?;
    let required = end
        .checked_add(length)
        .filter(|total| *total <= max_bytes)
        .ok_or("request exceeds capture bound")?;
    while bytes.len() < required {
        read_more(socket, &mut bytes, max_bytes).await?;
    }
    let body =
        String::from_utf8(bytes[end..required].to_vec()).map_err(|error| error.to_string())?;
    // A malformed body is evidence too; parsing/classification happens after export.
    Ok(Capture { request_line, body })
}

async fn read_more(
    socket: &mut TcpStream,
    bytes: &mut Vec<u8>,
    max_bytes: usize,
) -> Result<(), String> {
    let mut buffer = [0_u8; 4096];
    let count = socket
        .read(&mut buffer)
        .await
        .map_err(|error| error.to_string())?;
    if count == 0
        || bytes
            .len()
            .checked_add(count)
            .is_none_or(|size| size > max_bytes)
    {
        return Err("incomplete or oversized capture".into());
    }
    bytes.extend_from_slice(&buffer[..count]);
    Ok(())
}

pub(super) fn parsed(outcome: &Outcome) -> Result<Value, String> {
    serde_json::from_str(&outcome.capture.as_ref().ok_or("no captured request")?.body)
        .map_err(|error| error.to_string())
}
