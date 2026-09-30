//! Complete-result HTTP process harness; scenarios and expectations are data.
#[path = "../common/mod.rs"]
mod common;
use kolyan_ledger::{LedgerStore, SqliteLedger};
use reqwest::{Client, Method};
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

struct Process {
    child: Child,
    base: String,
    directory: PathBuf,
    client: Client,
}
impl Process {
    fn start(directory: &Path) -> Self {
        let mut child = Command::new(directory.join("server.bin"))
            .env("KOLYAN_SERVER_CONFIG", directory.join("server.json"))
            .env("KOLYAN_FIXTURE_KEY", "local-fixture-not-a-secret")
            .env("KOLYAN_HTTP_TOKEN", "local-test-token-not-a-secret")
            .env("KOLYAN_DUMP_MODEL_REQUESTS", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let capture = directory.to_owned();
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let mut log = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(capture.join("stderr.log"))
                .unwrap();
            for line in BufReader::new(stderr).lines() {
                let line = line.unwrap();
                writeln!(log, "{line}").unwrap();
                if let Some(address) = line.strip_prefix("HTTP listening on ") {
                    let _ = sender.send(address.to_owned());
                }
            }
        });
        let address = receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("HTTP process readiness");
        Self {
            child,
            base: format!("http://{address}"),
            directory: directory.to_owned(),
            client: Client::builder()
                .timeout(Duration::from_secs(180))
                .build()
                .unwrap(),
        }
    }
    async fn call(&self, method: Method, path: &str, body: Option<Value>, expected: u16) -> Value {
        let mut request = self
            .client
            .request(method.clone(), format!("{}{path}", self.base))
            .bearer_auth("local-test-token-not-a-secret");
        if let Some(value) = &body {
            request = request.json(value);
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let value: Value = response.json().await.unwrap();
        schema::response(&value, status, path);
        let mut trace = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("http.jsonl"))
            .unwrap();
        writeln!(trace,"{}",json!({"method":method.as_str(),"path":path,"request":body,"status":status,"response":value})).unwrap();
        assert_eq!(status, expected, "{path}: {value}");
        value
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let ledger = SqliteLedger::open(self.directory.join("ledger.sqlite")).unwrap();
        let mut file = fs::File::create(self.directory.join("ledger.jsonl")).unwrap();
        for event in ledger.events_after(0).unwrap() {
            writeln!(file, "{}", serde_json::to_string(&event).unwrap()).unwrap();
        }
    }
}
fn setup(directory: &Path, url: &str) {
    fs::create_dir_all(directory.join("workspace/safe")).unwrap();
    let immutable = directory.parent().unwrap().join("server.bin");
    if immutable.is_file() {
        fs::hard_link(immutable, directory.join("server.bin")).unwrap();
    } else {
        fs::copy(
            env!("CARGO_BIN_EXE_kolyan-server"),
            directory.join("server.bin"),
        )
        .unwrap();
    }
    let config = json!({
        "http":{"listen":"127.0.0.1:0","api_token_env":"KOLYAN_HTTP_TOKEN","max_active_turns":4},
        "ledger_path":directory.join("ledger.sqlite"),"session_root":directory.join("sessions"),
        "workspace":directory.join("workspace"),"tool_scope":"safe",
        "protocol":"openai_responses","base_url":url,"api_key_env":"KOLYAN_FIXTURE_KEY",
        "timeout_secs":120,"max_steps":12,"max_tool_calls":12,
        "progress":{"repeat_limit":2,"polling_tools":["file.read"]},
        "parameter_table":common::parameter_table("fixture","openai_responses","fixture"),
        "request":{"request_id":"template","model":{"provider":"fixture","model":"fixture"},
            "system":[],"messages":[],"tools":[],"tool_choice":"auto","output_format":null,
            "prompt_cache":null,"reasoning":null,"max_output_tokens":4096,"extensions":{}}
    });
    fs::write(
        directory.join("server.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
}
fn provider(directory: &Path, outputs: Vec<Value>) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let directory = directory.to_owned();
    let worker = std::thread::spawn(move || {
        for (index, output) in outputs.into_iter().enumerate() {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut size = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    size = value.trim().parse().unwrap();
                }
            }
            let mut bytes = vec![0; size];
            reader.read_exact(&mut bytes).unwrap();
            fs::write(directory.join(format!("model-request-{index}.json")), bytes).unwrap();
            let event = json!({"type":"response.completed","response":{"id":format!("r{index}"),"status":"completed","output":output}});
            let body = format!("event: response.completed\ndata: {event}\n\n");
            write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
        }
    });
    (url, worker)
}
async fn scenarios(directory: &Path, cases: &[Value]) {
    let mut process = Process::start(directory);
    for case in cases {
        let session = case["session_id"].as_str().unwrap();
        let turn = case["turn_id"].as_str().unwrap();
        if case["create"] == true {
            process
                .call(
                    Method::POST,
                    "/v1/sessions",
                    Some(json!({"session_id":session})),
                    201,
                )
                .await;
        }
        let session_path = format!("/v1/sessions/{session}");
        let resource = format!("{session_path}/turns/{turn}");
        let mut result = process
            .call(
                Method::POST,
                &format!("{session_path}/turns"),
                Some(json!({"turn_id":turn,"input":case["input"]})),
                200,
            )
            .await;
        let action = case["action"].as_str().unwrap();
        if action != "none" {
            assert_eq!(result["state"], "suspended");
            assert_eq!(result["execution_stopped"], true);
            assert!(
                !directory
                    .join("workspace")
                    .join(case["path"].as_str().unwrap())
                    .exists()
            );
            let approval = result["pending_approval"]["approval_id"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!(result["pending_approval"].get("continuation").is_none());
            if case["restart"] == true {
                drop(process);
                process = Process::start(directory);
                assert_eq!(
                    process.call(Method::GET, &resource, None, 200).await,
                    result
                );
            }
            let path = if action == "cancel" {
                format!("{resource}/cancel")
            } else {
                format!("{resource}/approvals/{approval}/decision")
            };
            let body = if action == "cancel" {
                json!({})
            } else {
                json!({"decision":action})
            };
            result = process.call(Method::POST, &path, Some(body), 200).await;
            process
                .call(
                    Method::POST,
                    &format!("{resource}/approvals/{approval}/decision"),
                    Some(json!({"decision":"approve"})),
                    409,
                )
                .await;
        }
        assert_eq!(result["state"], case["expected"]["state"]);
        assert_eq!(result["end_reason"], case["expected"]["end_reason"]);
        assert_eq!(
            result["steps"].as_array().unwrap().len() as u64,
            case["expected"]["steps"]
        );
        assert_eq!(result["execution_stopped"], true);
        assert_eq!(result["recovery_required"], false);
        assert_eq!(
            process.call(Method::GET, &resource, None, 200).await,
            result
        );
        process.call(Method::GET, &session_path, None, 200).await;
        process
            .call(
                Method::POST,
                &format!("{session_path}/turns"),
                Some(json!({"turn_id":turn,"input":case["input"]})),
                409,
            )
            .await;
        if let Some(path) = case["path"].as_str() {
            let path = directory.join("workspace").join(path);
            if let Some(content) = case["file_content"].as_str() {
                assert_eq!(fs::read_to_string(path).unwrap(), content);
            } else {
                assert!(!path.exists());
            }
        }
        let execution = format!("http-{}-{session}-{turn}", session.len());
        let events = SqliteLedger::open(directory.join("ledger.sqlite"))
            .unwrap()
            .events_after(0)
            .unwrap()
            .into_iter()
            .filter(|event| event.execution_id == execution)
            .collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == kolyan_ledger::LedgerEventKind::EffectReceipt)
                .count() as u64,
            case["expected"]["effects"]
        );
        if case["expected"]["effects"] == 0 {
            assert!(
                !events
                    .iter()
                    .any(|event| event.kind == kolyan_ledger::LedgerEventKind::EffectStarted)
            );
        }
        if case["verify_context"] == true {
            let first = events
                .iter()
                .find(|event| event.kind == kolyan_ledger::LedgerEventKind::ModelRequested)
                .unwrap();
            let messages = first.payload["request"]["messages"].to_string();
            assert!(
                messages.contains("write1") && messages.contains("http-proof"),
                "{messages}"
            );
        }
    }
    let unauthorized = process
        .client
        .get(format!("{}/v1/sessions/direct", process.base))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), 401);
    process
        .call(Method::GET, "/v1/sessions/absent", None, 404)
        .await;
    process
        .call(Method::GET, "/v1/sessions/direct/turns/absent", None, 404)
        .await;
    process
        .call(Method::GET, "/v1/sessions/direct/turns/t2", None, 404)
        .await;
    process
        .call(
            Method::POST,
            "/v1/sessions",
            Some(json!({"session_id":"bad","extra":1})),
            400,
        )
        .await;
    process
        .call(
            Method::GET,
            "/v1/sessions/direct/turns/t1/events",
            None,
            404,
        )
        .await;
    drop(process);
}

#[tokio::test]
async fn http_process_complete_result_matrix() {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-http-offline-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("HTTP evidence: {}", directory.display());
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/server_http.json")).unwrap();
    let (url, worker) = provider(&directory, fixture["outputs"].as_array().unwrap().clone());
    setup(&directory, &url);
    scenarios(&directory, fixture["cases"].as_array().unwrap()).await;
    worker.join().unwrap();
}

#[tokio::test]
async fn http_process_error_matrix() {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-http-errors-")
        .tempdir()
        .unwrap()
        .keep();
    setup(&directory, "http://127.0.0.1:1");
    let process = Process::start(&directory);
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/server_http_errors.json")).unwrap();
    let mut trace = fs::File::create(directory.join("errors.jsonl")).unwrap();
    for mut case in cases {
        let mut request = process.client.request(
            Method::from_bytes(case["method"].as_str().unwrap().as_bytes()).unwrap(),
            format!("{}{}", process.base, case["path"].as_str().unwrap()),
        );
        if case["auth"] != false {
            request = request.bearer_auth("local-test-token-not-a-secret");
        }
        if let Some(size) = case["text_size"].as_u64() {
            case["body"]["input"] = json!("x".repeat(size as usize));
        }
        if let Some(body) = case.get("body") {
            request = request.json(body);
        }
        if let Some(raw) = case["raw"].as_str() {
            request = request.body(raw.to_owned());
        }
        if let Some(headers) = case["headers"].as_object() {
            for (name, value) in headers {
                request = request.header(name, value.as_str().unwrap());
            }
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        if status >= 400 {
            assert_eq!(
                response.headers()["content-type"],
                "application/problem+json"
            );
        }
        if status == 401 {
            assert_eq!(response.headers()["www-authenticate"], "Bearer");
        }
        if status == 405 {
            assert!(response.headers().contains_key("allow"));
        }
        let value: Value = response.json().await.unwrap();
        schema::response(&value, status, case["path"].as_str().unwrap());
        writeln!(
            trace,
            "{}",
            json!({"case":case["name"],"status":status,"response":value})
        )
        .unwrap();
        assert_eq!(json!(status), case["status"], "{}: {value}", case["name"]);
        if let Some(code) = case.get("code") {
            assert_eq!(&value["code"], code);
        }
    }
    assert!(
        SqliteLedger::open(directory.join("ledger.sqlite"))
            .unwrap()
            .events_after(0)
            .unwrap()
            .is_empty(),
        "rejected requests must not invoke a model"
    );
    eprintln!("HTTP error evidence: {}", directory.display());
}

#[path = "http_process/http_faults.rs"]
mod http_faults;
#[path = "http_process/http_live.rs"]
mod http_live;
#[path = "http_process/schema.rs"]
mod schema;
