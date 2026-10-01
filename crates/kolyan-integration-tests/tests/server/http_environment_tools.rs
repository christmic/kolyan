//! Real OS + HTTP subprocess acceptance; local scripted responses are not live-model evidence.

#[path = "../common/parameters.rs"]
#[allow(dead_code)]
mod parameters;

use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use kolyan_ledger::{LedgerEventKind, LedgerStore, SqliteLedger};
use reqwest::{Client, Method};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    files: BTreeMap<String, String>,
    host_files: BTreeMap<String, String>,
    outputs: Vec<Value>,
    actions: Vec<Action>,
    expected_effects: usize,
    expected_approvals: usize,
    request_contains: Vec<Vec<String>>,
    request_excludes: Vec<Vec<String>>,
    ledger_counts: BTreeMap<String, usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultExpectation {
    call_id: String,
    is_error: bool,
    content: Value,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Http {
        method: String,
        path: String,
        body: Option<Value>,
        status: u16,
        expected: Value,
        files: BTreeMap<String, Option<String>>,
        #[serde(default)]
        results: Vec<ResultExpectation>,
    },
    Restart,
    Replace {
        path: String,
        content: String,
    },
}

struct Service {
    child: Child,
    base: String,
}

impl Service {
    fn start(root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kolyan-server"))
            .env("KOLYAN_SERVER_CONFIG", root.join("config.json"))
            .env("KOLYAN_FIXTURE_KEY", "local-fixture-not-a-secret")
            .env("KOLYAN_HTTP_TOKEN", "local-test-token-not-a-secret")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let root = root.to_owned();
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let line = line.unwrap();
                append(&root.join("stderr.jsonl"), &json!({"line":line}));
                if let Some(address) = line.strip_prefix("HTTP listening on ") {
                    let _ = sender.send(address.to_owned());
                }
            }
        });
        let address = receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("service readiness");
        Self {
            child,
            base: format!("http://{address}"),
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Provider {
    url: String,
    stopped: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<usize>>,
}

impl Provider {
    fn start(root: &Path, outputs: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = stopped.clone();
        let root = root.to_owned();
        let worker = thread::spawn(move || {
            let mut count = 0;
            for output in outputs {
                let deadline = Instant::now() + Duration::from_secs(120);
                let mut socket = loop {
                    if stop.load(Ordering::Acquire) {
                        return count;
                    }
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(
                                Instant::now() < deadline,
                                "scripted Provider request deadline"
                            );
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => panic!("Provider accept: {error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut size = 0;
                loop {
                    let mut line = String::new();
                    assert!(reader.read_line(&mut line).unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        size = value.trim().parse().unwrap();
                    }
                }
                assert!(size <= 8 * 1024 * 1024);
                let mut bytes = vec![0; size];
                reader.read_exact(&mut bytes).unwrap();
                let request: Value = serde_json::from_slice(&bytes).unwrap();
                append(
                    &root.join("model.jsonl"),
                    &json!({"index":count,"request":request,"scripted_output":output}),
                );
                let event = json!({"type":"response.completed","response":{"id":format!("response-{count}"),"status":"completed","output":output}});
                let body = format!("event: response.completed\ndata: {event}\n\n");
                write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
                count += 1;
            }
            count
        });
        Self {
            url,
            stopped,
            worker: Some(worker),
        }
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}

fn append(path: &Path, value: &Value) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{value}").unwrap();
    file.flush().unwrap();
}

fn expand(value: &Value, root: &Path, approval: &str) -> Value {
    match value {
        Value::String(text) => json!(
            text.replace("$ROOT", root.to_str().unwrap())
                .replace("$APPROVAL", approval)
        ),
        Value::Array(values) => {
            Value::Array(values.iter().map(|v| expand(v, root, approval)).collect())
        }
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(k, v)| (k.clone(), expand(v, root, approval)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn setup(root: &Path, case: &Case, url: &str) {
    use std::os::unix::fs::DirBuilderExt;

    fs::create_dir(root.join("workspace")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("staging"))
        .unwrap();
    for (path, content) in &case.host_files {
        fs::write(root.join(path), content).unwrap();
    }
    for (path, content) in &case.files {
        let target = root.join("workspace").join(path);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(target, content).unwrap();
    }
    let config = json!({
        "http":{"listen":"127.0.0.1:0","api_token_env":"KOLYAN_HTTP_TOKEN","max_active_turns":4},
        "ledger_path":root.join("state/ledger.sqlite"),"session_root":root.join("state/sessions"),
        "worker_path":env!("CARGO_BIN_EXE_kolyan-server-tool-worker"),"staging_root":root.join("staging"),
        "workspace":root.join("workspace"),"tool_scope":".","allow_shell":true,
        "protocol":"openai_responses","base_url":url,"api_key_env":"KOLYAN_FIXTURE_KEY",
        "timeout_secs":60,"max_steps":20,"max_tool_calls":20,
        "progress":{"repeat_limit":3,"polling_tools":["file.read"]},
        "parameter_table":parameters::parameter_table("fixture","openai_responses","fixture"),
        "request":{"request_id":"template","model":{"provider":"fixture","model":"fixture"},
            "system":[],"messages":[],"tools":[],"tool_choice":"auto","output_format":null,
            "prompt_cache":null,"reasoning":null,"max_output_tokens":4096,"extensions":{}}
    });
    fs::write(
        root.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
}

fn export_ledger(root: &Path) -> Vec<kolyan_ledger::LedgerEvent> {
    let events = SqliteLedger::open(root.join("state/ledger.sqlite"))
        .unwrap()
        .events_after(0)
        .unwrap();
    let mut file = fs::File::create(root.join("ledger.jsonl")).unwrap();
    for event in &events {
        writeln!(file, "{}", serde_json::to_string(event).unwrap()).unwrap();
    }
    file.flush().unwrap();
    events
}

fn compare(actual: &Value, expected: &Value) {
    for (key, value) in expected.as_object().unwrap() {
        if value.is_object() {
            compare(&actual[key], value);
        } else {
            assert_eq!(&actual[key], value, "{key}: {actual}");
        }
    }
}

#[tokio::test]
async fn http_environment_tools_data_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../fixtures/server_http_environment_tools.json"
    ))
    .unwrap();
    let suite = tempfile::Builder::new()
        .prefix("kolyan-http-four-tools-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    eprintln!("Four-tool local OS/HTTP evidence: {}", suite.display());
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    for case in cases {
        let root = suite.join(&case.id);
        fs::create_dir(&root).unwrap();
        let outputs = expand(&json!(case.outputs), &root, "")
            .as_array()
            .unwrap()
            .clone();
        let provider = Provider::start(&root, outputs);
        setup(&root, &case, &provider.url);
        let trusted_config = fs::read_to_string(root.join("config.json")).unwrap();
        let mut service = Some(Service::start(&root));
        let mut approval = String::new();
        for (index, action) in case.actions.iter().enumerate() {
            match action {
                Action::Restart => {
                    drop(service.take());
                    service = Some(Service::start(&root));
                }
                Action::Replace { path, content } => {
                    let target = root.join("workspace").join(path);
                    let replacement = target.with_extension("replacement");
                    fs::write(&replacement, content).unwrap();
                    fs::rename(replacement, target).unwrap();
                    append(
                        &root.join("http.jsonl"),
                        &json!({"case":case.id,"action":index,"mutation":{"path":path,"content":content}}),
                    );
                }
                Action::Http {
                    method,
                    path,
                    body,
                    status,
                    expected,
                    files,
                    results,
                } => {
                    let path = expand(&json!(path), &root, &approval)
                        .as_str()
                        .unwrap()
                        .to_owned();
                    let body = body.as_ref().map(|v| expand(v, &root, &approval));
                    let mut request = client
                        .request(
                            Method::from_bytes(method.as_bytes()).unwrap(),
                            format!("{}{path}", service.as_ref().unwrap().base),
                        )
                        .bearer_auth("local-test-token-not-a-secret");
                    if let Some(value) = &body {
                        request = request.json(value);
                    }
                    append(
                        &root.join("http-raw.jsonl"),
                        &json!({"case":case.id,"action":index,"phase":"request","method":method,"path":path,"body":body}),
                    );
                    let response = request.send().await.unwrap();
                    let code = response.status().as_u16();
                    let bytes = response.bytes().await.unwrap();
                    append(
                        &root.join("http-raw.jsonl"),
                        &json!({"case":case.id,"action":index,"phase":"response","status":code,"body_bytes":bytes.as_ref()}),
                    );
                    export_ledger(&root);
                    let value: Value = serde_json::from_slice(&bytes).unwrap();
                    let current_config = fs::read_to_string(root.join("config.json")).unwrap();
                    let observed: BTreeMap<_, _> = files
                        .keys()
                        .map(|p| (p.clone(), observe_file(&root.join("workspace").join(p))))
                        .collect();
                    append(
                        &root.join("http.jsonl"),
                        &json!({"case":case.id,"action":index,"method":method,"path":path,"request":body,"status":code,"response":value,"files":observed,"loaded_config":current_config}),
                    );
                    export_ledger(&root);
                    assert_eq!(
                        current_config, trusted_config,
                        "loaded host configuration must remain unchanged"
                    );
                    assert_eq!(code, *status, "{} action {index}: {value}", case.id);
                    compare(&value, expected);
                    assert_eq!(&observed, files, "{} action {index}", case.id);
                    for expected in results {
                        let result = value["tool_results"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|r| r["call_id"] == expected.call_id)
                            .expect("paired tool result");
                        assert_eq!(result["is_error"], expected.is_error);
                        let content: Value =
                            serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
                        compare(&content, &expected.content);
                    }
                    if let Some(id) = value["pending_approval"]["approval_id"].as_str() {
                        approval = id.into();
                    }
                }
            }
        }
        let events = export_ledger(&root);
        let count = events
            .iter()
            .filter(|e| e.kind == LedgerEventKind::EffectReceipt)
            .count();
        append(
            &suite.join("report.jsonl"),
            &json!({"case":case.id,"effects":count,"expected_effects":case.expected_effects}),
        );
        assert_eq!(count, case.expected_effects, "{} receipts", case.id);
        let checkpoints: Vec<_> = events
            .iter()
            .filter(|event| {
                event.kind == LedgerEventKind::ApprovalRequested
                    && event.payload.get("continuation").is_some()
            })
            .collect();
        assert_eq!(
            checkpoints.len(),
            case.expected_approvals,
            "{} persisted approvals",
            case.id
        );
        for checkpoint in checkpoints {
            let id = checkpoint.payload["approval_id"].as_str().unwrap();
            let suspensions: Vec<_> = events
                .iter()
                .filter(|event| {
                    event.kind == LedgerEventKind::ExecutionSuspended
                        && event.execution_id == checkpoint.execution_id
                        && event.turn_id == checkpoint.turn_id
                        && event.payload["approval_id"] == id
                })
                .collect();
            assert_eq!(
                suspensions.len(),
                1,
                "one persisted suspension for each saved approval"
            );
            let boundaries: Vec<_> = events
                .iter()
                .filter(|event| {
                    event.kind == LedgerEventKind::ExecutionBoundaryAdmitted
                        && event.execution_id == checkpoint.execution_id
                        && event.turn_id == checkpoint.turn_id
                        && event.payload["approval_id"] == id
                })
                .collect();
            assert_eq!(
                boundaries.len(),
                1,
                "one independent approval boundary admission"
            );
            assert_eq!(
                boundaries[0].event_id,
                format!("{}/approval/{id}/boundary", checkpoint.execution_id)
            );
            assert_eq!(
                suspensions[0].event_id,
                format!("{}/execution-suspended/{id}", checkpoint.execution_id)
            );
            assert!(
                boundaries[0].cursor < checkpoint.cursor
                    && checkpoint.cursor < suspensions[0].cursor
            );
        }
        for (kind, expected) in &case.ledger_counts {
            let actual = events
                .iter()
                .filter(|event| serde_json::to_value(event.kind).unwrap() == *kind)
                .count();
            assert_eq!(actual, *expected, "{} ledger kind {kind}", case.id);
        }
        if count == 0 {
            assert!(
                !events
                    .iter()
                    .any(|e| e.kind == LedgerEventKind::EffectStarted)
            );
        }
        let rows: Vec<Value> = fs::read_to_string(root.join("model.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            rows.len(),
            case.outputs.len(),
            "{} actual model request count",
            case.id
        );
        for (index, row) in rows.iter().enumerate() {
            let text = row["request"].to_string();
            for required in &case.request_contains[index] {
                assert!(
                    text.contains(required),
                    "{} request {index} missing {required}: {text}",
                    case.id
                );
            }
            for excluded in &case.request_excludes[index] {
                assert!(
                    !text.contains(excluded),
                    "{} request {index} leaked {excluded}: {text}",
                    case.id
                );
            }
            let mut names: Vec<_> = row["request"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap())
                .collect();
            names.sort_unstable();
            assert_eq!(names, ["file.edit", "file.read", "file.write", "shell"]);
        }
        drop(service);
        drop(provider);
    }
}

fn observe_file(path: &Path) -> Option<String> {
    match fs::read_to_string(path) {
        Ok(content) => Some(content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("cannot observe {}: {error}", path.display()),
    }
}
