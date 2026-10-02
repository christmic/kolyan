//! Bounded loopback response script. Capture only raw bodies, never auth headers.

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use serde_json::{Value, json};

use kolyan_ledger::{LedgerStore, SqliteLedger};

use super::super::evidence::Evidence;
use super::{Case, Protocol};

pub(super) struct Server {
    pub url: String,
    stop: Arc<AtomicBool>,
    worker: JoinHandle<Result<(), String>>,
}

impl Server {
    pub fn start(case: &Case, root: PathBuf, evidence: Arc<Evidence>) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let url = format!(
            "http://{}",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let replies = replies(case);
        let path = root.join("workspace").join(&case.path);
        let worker = thread::spawn(move || {
            let mut index = 0;
            while !stopping.load(Ordering::Acquire) {
                let (mut socket, _) = match listener.accept() {
                    Ok(accepted) => accepted,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => return Err(error.to_string()),
                };
                // Accepted sockets are explicitly blocking regardless of listener mode.
                socket.set_nonblocking(false).map_err(|e| e.to_string())?;
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .map_err(|e| e.to_string())?;
                socket
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .map_err(|e| e.to_string())?;
                let (request_line, raw) = read_body(&socket)?;
                let file = fs::read(&path);
                let events = SqliteLedger::open(root.join("state/ledger.sqlite"))
                    .map_err(|e| e.to_string())?
                    .execution_events_after("opening-execution", 0)
                    .map_err(|e| e.to_string())?;
                // Export the actual request even when it exceeds the reply plan.
                evidence.append(json!({"event":"http_request","index":index,"request_line":request_line,"raw_body":raw,"body":serde_json::from_slice::<Value>(&raw).map_err(|e|e.to_string())?,"ledger_before_response":events,"file_before_response":file.as_ref().ok().and_then(|b|std::str::from_utf8(b).ok()),"file_bytes_before_response":file.as_ref().ok(),"file_error":file.as_ref().err().map(ToString::to_string)}))?;
                fs::write(root.join(format!("request-{index}.body")), &raw)
                    .map_err(|e| e.to_string())?;
                let (status, body, headers) = replies
                    .get(index)
                    .ok_or("unexpected additional HTTP send")?;
                let response = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\nrequest-id: opening-{index}\r\nx-request-id: opening-{index}\r\n{headers}\r\n{body}",
                    body.len()
                );
                socket
                    .write_all(response.as_bytes())
                    .map_err(|e| e.to_string())?;
                evidence.append(
                    json!({"event":"http_response","index":index,"status":status,"body":body}),
                )?;
                index += 1;
            }
            Ok(())
        });
        Ok(Self { url, stop, worker })
    }

    pub async fn finish(self) -> Option<String> {
        self.stop.store(true, Ordering::Release);
        match tokio::task::spawn_blocking(move || self.worker.join()).await {
            Ok(Ok(Ok(()))) => None,
            Ok(Ok(Err(error))) => Some(error),
            Ok(Err(_)) => Some("loopback worker panicked".into()),
            Err(error) => Some(error.to_string()),
        }
    }
}

fn read_body(socket: &TcpStream) -> Result<(String, Vec<u8>), String> {
    let mut reader = BufReader::new(socket.try_clone().map_err(|e| e.to_string())?);
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .map_err(|e| e.to_string())?;
    let mut length = None;
    let mut header_bytes = request_line.len();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
            return Err("header EOF".into());
        }
        header_bytes += line.len();
        if header_bytes > 16_384 {
            return Err("oversized request headers".into());
        }
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            if length.is_some() {
                return Err("duplicate content length".into());
            }
            length = Some(value.trim().parse::<usize>().map_err(|e| e.to_string())?);
        }
    }
    let length = length.ok_or("missing content length")?;
    if length > 4 * 1024 * 1024 {
        return Err("oversized body".into());
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).map_err(|e| e.to_string())?;
    Ok((request_line.trim_end().into(), body))
}

fn replies(case: &Case) -> Vec<(u16, String, String)> {
    let fault = json!({"type":"error","error":{"type":case.fault_type,"code":case.fault_code,"message":"Synthetic loopback failure, not an actual LLM"}}).to_string();
    let mut replies = vec![
        (
            200,
            success(case, true),
            "Content-Type: text/event-stream\r\n".into(),
        ),
        (
            case.fault_status,
            fault,
            format!(
                "Content-Type: application/json\r\nretry-after-ms: {}\r\nx-should-retry: {}\r\n",
                case.retry_after_ms, case.should_retry
            ),
        ),
    ];
    // Keep a recovery reply even for quota, so an illicit retry is observable.
    replies.push((
        200,
        success(case, false),
        "Content-Type: text/event-stream\r\n".into(),
    ));
    replies
}

fn event(kind: &str, body: Value) -> String {
    format!("event: {kind}\ndata: {body}\n\n")
}

fn success(case: &Case, tool: bool) -> String {
    let arguments = json!({"path":case.path,"content":case.content});
    match case.protocol {
        Protocol::Openai => {
            let output = if tool {
                json!([{"type":"function_call","id":"item-write","call_id":case.call_id,"name":"file.write","arguments":arguments.to_string()}])
            } else {
                json!([{"type":"message","id":"message-final","role":"assistant","content":[{"type":"output_text","text":"Verified write completed once."}]}])
            };
            event(
                "response.completed",
                json!({"type":"response.completed","response":{"id":if tool{"r-write"}else{"r-final"},"status":"completed","model":"opening-retry","output":output,"usage":{"input_tokens":11,"output_tokens":7}}}),
            )
        }
        Protocol::Anthropic => {
            let mut body = event(
                "message_start",
                json!({"type":"message_start","message":{"id":if tool{"m-write"}else{"m-final"},"type":"message","role":"assistant","model":"opening-retry","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":11,"output_tokens":0}}}),
            );
            let block = if tool {
                json!({"type":"tool_use","id":case.call_id,"name":"file.write","input":{}})
            } else {
                json!({"type":"text","text":""})
            };
            body.push_str(&event(
                "content_block_start",
                json!({"type":"content_block_start","index":0,"content_block":block}),
            ));
            let delta = if tool {
                json!({"type":"input_json_delta","partial_json":arguments.to_string()})
            } else {
                json!({"type":"text_delta","text":"Verified write completed once."})
            };
            body.push_str(&event(
                "content_block_delta",
                json!({"type":"content_block_delta","index":0,"delta":delta}),
            ));
            body.push_str(&event(
                "content_block_stop",
                json!({"type":"content_block_stop","index":0}),
            ));
            body.push_str(&event("message_delta", json!({"type":"message_delta","delta":{"stop_reason":if tool{"tool_use"}else{"end_turn"},"stop_sequence":null},"usage":{"output_tokens":7}})));
            body.push_str(&event("message_stop", json!({"type":"message_stop"})));
            body
        }
    }
}
