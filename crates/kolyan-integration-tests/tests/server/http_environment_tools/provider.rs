//! Bounded concurrent fixture connections; only complete valid requests consume scripts.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};

const CONNECTION_LIMIT: usize = 16;
const TOTAL_CONNECTION_LIMIT: usize = 256;
const HEADER_LIMIT: usize = 64 * 1024;
const BODY_LIMIT: usize = 8 * 1024 * 1024;
const READ_DEADLINE: Duration = Duration::from_secs(10);

type Parsed = Result<Option<(TcpStream, Value)>, String>;

#[derive(Clone)]
struct Trace {
    file: Arc<Mutex<File>>,
    started: Instant,
}

impl Trace {
    fn record(&self, connection: usize, phase: &str, detail: Value) -> Result<(), String> {
        let mut file = self.file.lock().map_err(|error| error.to_string())?;
        writeln!(file,"{}",json!({"elapsed_ms":self.started.elapsed().as_millis(),"connection":connection,"phase":phase,"detail":detail})).map_err(|error|error.to_string())?;
        file.flush().map_err(|error| error.to_string())
    }
}

pub(super) struct Provider {
    pub(super) url: String,
    stopped: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<Result<usize, String>>>,
    trace: Trace,
}

impl Provider {
    pub(super) fn start(root: &Path, outputs: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let trace = Trace {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(root.join("connections.jsonl"))
                    .unwrap(),
            )),
            started: Instant::now(),
        };
        let stop = stopped.clone();
        let worker_trace = trace.clone();
        let root = root.to_owned();
        let worker = thread::spawn(move || {
            let outcome = run(listener, &outputs, &root, &stop, &worker_trace);
            let logged = worker_trace.record(0, "worker_terminal", json!({"result":outcome}));
            outcome.and_then(|count| logged.map(|()| count))
        });
        Self {
            url,
            stopped,
            worker: Some(worker),
            trace,
        }
    }

    pub(super) fn finish(mut self) -> Result<usize, String> {
        self.stopped.store(true, Ordering::Release);
        let worker = self.worker.take().ok_or("Provider already finished")?;
        let outcome = worker.join().map_err(panic_message).and_then(|value| value);
        self.trace.record(0, "finish", json!({"result":outcome}))?;
        outcome
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let outcome = worker.join().map_err(panic_message).and_then(|value| value);
            // Cleanup never replaces the original test failure with a destructor panic.
            if let Err(error) = self.trace.record(
                0,
                "drop",
                json!({"result":outcome,"unwinding":thread::panicking()}),
            ) {
                eprintln!("Provider cleanup evidence error: {error}");
            }
            if let Err(error) = outcome {
                eprintln!("Provider worker failed during cleanup: {error}");
            }
        }
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).into()
    } else {
        "Provider worker panicked with non-string payload".into()
    }
}

fn run(
    listener: TcpListener,
    outputs: &[Value],
    root: &Path,
    stop: &Arc<AtomicBool>,
    trace: &Trace,
) -> Result<usize, String> {
    let (sender, receiver) = mpsc::channel::<(usize, Parsed)>();
    let mut readers = Vec::new();
    let mut active = 0;
    let mut serial = 0;
    let mut count = 0;
    let deadline = Instant::now() + Duration::from_secs(120);
    let outcome = (|| {
        loop {
            // Drain every completed read before accepting more connections.
            while let Ok((id, parsed)) = receiver.try_recv() {
                active -= 1;
                if let Some((mut socket, request)) = parsed? {
                    let output = outputs.get(count).ok_or("unexpected extra model request")?;
                    super::append(
                        &root.join("model.jsonl"),
                        &json!({"index":count,"connection":id,"request":request,"scripted_output":output}),
                    );
                    socket.set_nonblocking(false).map_err(|e| e.to_string())?;
                    socket
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .map_err(|e| e.to_string())?;
                    let event = json!({"type":"response.completed","response":{"id":format!("response-{count}"),"status":"completed","output":output}});
                    let body = format!("event: response.completed\ndata: {event}\n\n");
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    if let Err(error) = socket.write_all(response.as_bytes()) {
                        trace.record(id, "write_error", json!({"error":error.to_string()}))?;
                        return Err(error.to_string());
                    }
                    trace.record(id, "response_sent", json!({"script_index":count}))?;
                    count += 1;
                }
            }
            if count == outputs.len() {
                return Ok(count);
            }
            if stop.load(Ordering::Acquire) {
                return Err(format!(
                    "Provider stopped after {count}/{} responses",
                    outputs.len()
                ));
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "Provider deadline after {count}/{} responses",
                    outputs.len()
                ));
            }
            if active < CONNECTION_LIMIT {
                match listener.accept() {
                    Ok((socket, peer)) => {
                        serial += 1;
                        if serial > TOTAL_CONNECTION_LIMIT {
                            return Err("fixture connection budget exhausted".into());
                        }
                        active += 1;
                        trace.record(
                            serial,
                            "accepted",
                            json!({"peer":peer.to_string(),"active":active}),
                        )?;
                        let sender = sender.clone();
                        let trace = trace.clone();
                        let stop = stop.clone();
                        let id = serial;
                        readers.push(thread::spawn(move || {
                            let parsed =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    read_request(socket, id, &trace, &stop)
                                }))
                                .map_err(panic_message)
                                .and_then(|parsed| parsed);
                            if let Err(error) = &parsed {
                                let _ = trace.record(id, "request_error", json!({"error":error}));
                            }
                            let _ = sender.send((id, parsed));
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            thread::sleep(Duration::from_millis(2));
        }
    })();
    stop.store(true, Ordering::Release);
    let mut outcome = outcome;
    for reader in readers {
        if let Err(error) = reader.join() {
            outcome = Err(panic_message(error));
        }
    }
    // A complete request error is never ignored, including during shutdown.
    while let Ok((_, parsed)) = receiver.try_recv() {
        if let Err(error) = parsed {
            outcome = Err(error);
        } else if parsed.is_ok_and(|value| value.is_some()) {
            outcome = Err("unserved complete request during shutdown".into());
        }
    }
    outcome
}

fn read_request(mut socket: TcpStream, id: usize, trace: &Trace, stop: &AtomicBool) -> Parsed {
    socket.set_nonblocking(true).map_err(|e| e.to_string())?;
    let deadline = Instant::now() + READ_DEADLINE;
    let mut bytes = Vec::new();
    let mut length = None;
    let mut last_wait_bytes = None;
    loop {
        let mut chunk = [0; 8192];
        match socket.read(&mut chunk) {
            Ok(0) => {
                trace.record(id, "eof", json!({"partial_bytes":bytes}))?;
                return if bytes.is_empty() {
                    Ok(None)
                } else {
                    Err("truncated request at EOF".into())
                };
            }
            Ok(size) => {
                bytes.extend_from_slice(&chunk[..size]);
                trace.record(id,"read",json!({"received":size,"total":bytes.len(),"partial_header":String::from_utf8_lossy(&bytes[..bytes.len().min(HEADER_LIMIT)])}))?;
                if length.is_none() {
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let end = end + 4;
                        if end > HEADER_LIMIT {
                            return Err("header exceeds limit".into());
                        }
                        let header =
                            std::str::from_utf8(&bytes[..end]).map_err(|e| e.to_string())?;
                        let mut lines = header.split("\r\n");
                        let first = lines.next().ok_or("missing request line")?;
                        if first != "POST /v1/responses HTTP/1.1" {
                            return Err(format!("invalid request line: {first}"));
                        }
                        let mut size = None;
                        for line in lines.filter(|line| !line.is_empty()) {
                            let (name, value) = line.split_once(':').ok_or("invalid header")?;
                            if name.eq_ignore_ascii_case("content-length") {
                                if size.is_some() {
                                    return Err("duplicate content length".into());
                                }
                                size =
                                    Some(value.trim().parse::<usize>().map_err(|e| e.to_string())?);
                            }
                            if name.eq_ignore_ascii_case("transfer-encoding") {
                                return Err("fixture does not admit transfer encoding".into());
                            }
                        }
                        let size = size.ok_or("missing content length")?;
                        if size > BODY_LIMIT {
                            return Err("body exceeds limit".into());
                        }
                        trace.record(
                            id,
                            "headers_parsed",
                            json!({"body_size":size,"header_bytes":end}),
                        )?;
                        length = Some((end, size));
                    } else if bytes.len() > HEADER_LIMIT {
                        return Err("header exceeds limit".into());
                    }
                }
                if let Some((end, size)) = length {
                    if bytes.len() > end + size {
                        return Err("unexpected trailing request bytes".into());
                    }
                    if bytes.len() == end + size {
                        let request: Value =
                            serde_json::from_slice(&bytes[end..]).map_err(|e| e.to_string())?;
                        if !request.is_object() {
                            return Err("request must be a JSON object".into());
                        }
                        trace.record(id, "request_parsed", json!({"request":request}))?;
                        return Ok(Some((socket, request)));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if last_wait_bytes != Some(bytes.len()) {
                    trace.record(id,"would_block",json!({"errno":error.raw_os_error(),"partial_bytes":bytes.len(),"headers_parsed":length.is_some()}))?;
                    last_wait_bytes = Some(bytes.len());
                }
                if stop.load(Ordering::Acquire) || Instant::now() >= deadline {
                    trace.record(id,"read_stopped",json!({"reason":if stop.load(Ordering::Acquire) {"shutdown"} else {"deadline"},"error_kind":"WouldBlock","partial_bytes":bytes,"headers_parsed":length.is_some()}))?;
                    return if bytes.is_empty() {
                        Ok(None)
                    } else {
                        Err("partial request timed out or interrupted".into())
                    };
                }
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => {
                trace.record(
                    id,
                    "read_error",
                    json!({"error":error.to_string(),"partial_bytes":bytes}),
                )?;
                return Err(error.to_string());
            }
        }
    }
}

#[cfg(test)]
#[path = "provider/tests.rs"]
mod tests;
