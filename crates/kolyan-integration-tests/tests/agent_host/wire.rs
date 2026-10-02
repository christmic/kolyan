//! Bounded localhost SSE fixture, with complete actual requests and sent bodies.

use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use serde_json::{Value, json};

pub(super) struct Backend {
    pub endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<Result<(), String>>>,
}

impl Backend {
    pub fn start(protocol: &str, script: Vec<Value>) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let endpoint = format!(
            "http://{}",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let protocol = protocol.to_owned();
        let thread = thread::spawn(move || {
            let mut replies = VecDeque::from(script);
            while !stopped.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(socket) => socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => return Err(error.to_string()),
                };
                // macOS accepted sockets can inherit O_NONBLOCK; timeouts alone
                // do not turn those reads into blocking bounded reads.
                stream.set_nonblocking(false).map_err(|e| e.to_string())?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .map_err(|e| e.to_string())?;
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .map_err(|e| e.to_string())?;
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    if header.len() >= 16384 {
                        return Err("header bound exceeded".into());
                    }
                    let mut byte = [0];
                    stream.read_exact(&mut byte).map_err(|e| e.to_string())?;
                    header.push(byte[0]);
                }
                let header = String::from_utf8(header).map_err(|e| e.to_string())?;
                let count = header
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then_some(value.trim())
                    })
                    .ok_or("missing body length")?
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?;
                if count > 4 * 1024 * 1024 {
                    return Err("request body bound exceeded".into());
                }
                let mut body = vec![0; count];
                stream.read_exact(&mut body).map_err(|e| e.to_string())?;
                let request: Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
                let Some(reply) = replies.pop_front() else {
                    captured.lock().map_err(|e| e.to_string())?.push(json!({
                        "request_line":header.lines().next(),"body":request,"body_bytes":body,
                        "error":"unexpected extra model request"}));
                    return Err("unexpected extra model request".into());
                };
                let wire = events(&protocol, &reply)?;
                captured.lock().map_err(|e| e.to_string())?.push(json!({
                    "request_line":header.lines().next(),"body":request,"body_bytes":body,
                    "response_body":wire,"reply_fixture":reply}));
                write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",wire.len(),wire)
                    .map_err(|e|e.to_string())?;
                stream.flush().map_err(|e| e.to_string())?;
            }
            Ok(())
        });
        Ok(Self {
            endpoint,
            requests,
            stop,
            thread: Some(thread),
        })
    }
    pub fn finish(&mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Relaxed);
        let worker = self.thread.take().ok_or("backend already finished")?;
        worker
            .join()
            .map_err(|_| "backend thread panicked".to_owned())?
    }
    pub fn requests(&self) -> Vec<Value> {
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for Backend {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.thread.take() {
            let _ = worker.join();
        }
    }
}

fn events(protocol: &str, reply: &Value) -> Result<String, String> {
    let calls = reply["calls"]
        .as_array()
        .ok_or("fixture calls must be an array")?;
    let mut events = Vec::new();
    match protocol {
        "openai_responses" => {
            let output = if calls.is_empty() {
                vec![
                    json!({"type":"message","id":"msg","role":"assistant","status":"completed",
                    "content":[{"type":"output_text","text":reply["text"],"annotations":[]}]}),
                ]
            } else {
                calls.iter().map(|call|json!({"type":"function_call","id":call["id"],
                    "call_id":call["id"],"name":call["name"],"arguments":call["arguments"].to_string(),"status":"completed"})).collect()
            };
            events.push(json!({"type":"response.created","response":{"id":"fixture","model":"fixture-model","status":"in_progress","output":[]}}));
            for (index, item) in output.iter().enumerate() {
                events.push(
                    json!({"type":"response.output_item.done","output_index":index,"item":item}),
                );
            }
            events.push(json!({"type":"response.completed","response":{"id":"fixture","model":"fixture-model",
                "status":"completed","output":output,"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}));
        }
        "anthropic_messages" => {
            events.push(json!({"type":"message_start","message":{"id":"fixture","type":"message","role":"assistant",
                "model":"fixture-model","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}));
            if calls.is_empty() {
                events.push(json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}));
                events.push(json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":reply["text"]}}));
                events.push(json!({"type":"content_block_stop","index":0}));
            } else {
                for (index, call) in calls.iter().enumerate() {
                    events.push(json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use",
                        "id":call["id"],"name":call["name"],"input":{}}}));
                    events.push(json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta",
                        "partial_json":call["arguments"].to_string()}}));
                    events.push(json!({"type":"content_block_stop","index":index}));
                }
            }
            events.push(json!({"type":"message_delta","delta":{"stop_reason":if calls.is_empty(){"end_turn"}else{"tool_use"}},"usage":{"output_tokens":2}}));
            events.push(json!({"type":"message_stop"}));
        }
        _ => return Err("unsupported fixture protocol".into()),
    }
    Ok(events
        .into_iter()
        .map(|event| {
            if protocol == "anthropic_messages" {
                format!(
                    "event: {}\ndata: {event}\n\n",
                    event["type"].as_str().expect("typed fixture event")
                )
            } else {
                format!("data: {event}\n\n")
            }
        })
        .collect())
}
