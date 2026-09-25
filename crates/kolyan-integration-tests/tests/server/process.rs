//! The test source belongs to integration-tests; the binary target is registered
//! by the service package so Cargo supplies the exact freshly built executable.

#[path = "../common/mod.rs"]
mod common;
#[path = "process_faults.rs"]
mod faults;
#[path = "../common/matrix.rs"]
#[allow(dead_code)]
mod matrix;

use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

struct Process {
    child: Child,
    input: ChildStdin,
    output: mpsc::Receiver<String>,
    trace: fs::File,
    next_id: u64,
}

impl Process {
    fn start(config: &Path, directory: &Path) -> Self {
        let mut child = Command::new(directory.join("server.bin"))
            .env("KOLYAN_SERVER_CONFIG", config)
            .env("KOLYAN_FIXTURE_KEY", "local-fixture-not-a-secret")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(directory.join("stderr.log"))
                    .unwrap(),
            ))
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, output) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if sender.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            input,
            output,
            trace: fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(directory.join("rpc.jsonl"))
                .unwrap(),
            next_id: 0,
        }
    }

    fn rpc(&mut self, method: &str, params: Value) -> Value {
        let response = self.rpc_response(method, params);
        assert!(response.get("error").is_none(), "RPC failed: {response}");
        response["result"].clone()
    }

    fn send(&mut self, method: &str, params: Value) -> u64 {
        self.next_id += 1;
        let request = json!({"jsonrpc":"2.0", "id":self.next_id, "method":method, "params":params});
        writeln!(self.trace, "{}", json!({"request":request})).unwrap();
        writeln!(self.input, "{request}").unwrap();
        self.input.flush().unwrap();
        self.next_id
    }

    fn receive(&mut self) -> Value {
        let line = self
            .output
            .recv_timeout(Duration::from_secs(180))
            .expect("server response timeout or process exit");
        let response: Value = serde_json::from_str(&line).unwrap();
        writeln!(self.trace, "{}", json!({"response":response})).unwrap();
        self.trace.flush().unwrap();
        response
    }

    fn rpc_response(&mut self, method: &str, params: Value) -> Value {
        let id = self.send(method, params);
        let response = self.receive();
        assert_eq!(response["id"], id);
        response
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn config(
    directory: &Path,
    family: &str,
    protocol: &str,
    model: &str,
    url: &str,
    key_env: &str,
    max_tokens: Option<u32>,
) -> PathBuf {
    fs::create_dir_all(directory.join("workspace/safe")).unwrap();
    let matrix_binary = directory.parent().unwrap().join("server.bin");
    if matrix_binary.is_file() {
        fs::hard_link(matrix_binary, directory.join("server.bin")).unwrap();
    } else {
        fs::copy(
            env!("CARGO_BIN_EXE_kolyan-server"),
            directory.join("server.bin"),
        )
        .unwrap();
    }
    let mut table: Value =
        serde_json::from_str(include_str!("../config/request-parameters.json")).unwrap();
    table["provider"] = json!(family);
    table["protocol"] = json!(protocol);
    table["models"] = json!({model:{}});
    let value = json!({
        "ledger_path":directory.join("ledger.sqlite"), "session_root":directory.join("sessions"),
        "workspace":directory.join("workspace"), "tool_scope":"safe", "protocol":protocol,
        "base_url":url, "api_key_env":key_env, "timeout_secs":120, "parameter_table":table,
        "max_steps":12, "max_tool_calls":12, "progress":{"repeat_limit":2,"polling_tools":["file.read"]},
        "request": {"request_id":"template", "model":{"provider":family,"model":model},
            "system":[], "messages":[], "tools":[], "tool_choice":"auto", "output_format":null,
            "prompt_cache":null,"reasoning":null,"max_output_tokens":max_tokens.or(Some(80960)),"extensions":{}}
    });
    let path = directory.join("server.json");
    fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    path
}

fn compare(process: &mut Process, key: Value, expected: &Value, directory: &Path, name: &str) {
    let result = process.rpc("execution.events", key.clone());
    let events = result["events"].as_array().unwrap();
    let mut file = fs::File::create(directory.join(format!("{name}.jsonl"))).unwrap();
    for event in events {
        writeln!(file, "{event}").unwrap();
    }
    assert_fact_contract(events, "normal_contract");
    let mut offset = 0;
    for kind in expected.as_array().unwrap() {
        offset += events[offset..]
            .iter()
            .position(|event| event["kind"] == *kind)
            .unwrap_or_else(|| panic!("missing {kind}, evidence {}", directory.display()))
            + 1;
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "effect_started")
            .count(),
        1,
        "exactly one actual tool execution"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "effect_receipt")
            .count(),
        1
    );
    assert!(!events.iter().any(|event| matches!(
        event["kind"].as_str(),
        Some("effect_uncertain" | "turn_failed")
    )));
    let mut cursor = key;
    cursor["after_cursor"] = result["next_cursor"].clone();
    assert!(
        process.rpc("execution.events", cursor)["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

fn scenario(config: &Path, directory: &Path) {
    let case: Value =
        serde_json::from_str(include_str!("../fixtures/server_process.json")).unwrap();
    let mut process = Process::start(config, directory);
    assert_eq!(process.rpc("server.info", json!({}))["protocol_version"], 1);
    process.rpc("session.create", json!({"session_id":"s"}));
    let first_key = json!({"session_id":"s","turn_id":"t1","execution_id":"e1"});
    let mut start = first_key.clone();
    start["input"] = case["first_input"].clone();
    let paused = process.rpc("execution.start", start);
    assert_eq!(paused["state"], "Suspended");
    assert!(
        !directory
            .join("workspace")
            .join(case["path"].as_str().unwrap())
            .exists()
    );
    let approval_id = paused["approval"]["approval_id"].clone();
    drop(process); // Kill the actual service process while approval is pending.
    let mut process = Process::start(config, directory);
    assert_eq!(
        process.rpc("execution.status", first_key.clone())["state"],
        "Suspended"
    );
    let mut approve = first_key.clone();
    approve["approval_id"] = approval_id;
    let completed = process.rpc("execution.approve", approve);
    assert_eq!(completed["state"], "Completed");
    assert_eq!(completed["end_reason"], "FinalAnswer");
    assert_eq!(
        fs::read_to_string(
            directory
                .join("workspace")
                .join(case["path"].as_str().unwrap())
        )
        .unwrap(),
        case["content"].as_str().unwrap()
    );
    compare(
        &mut process,
        first_key,
        &case["first_expected_kinds"],
        directory,
        "turn1",
    );
    let second_key = json!({"session_id":"s","turn_id":"t2","execution_id":"e2"});
    let mut second = second_key.clone();
    second["input"] = case["second_input"].clone();
    let completed = process.rpc("execution.start", second);
    assert_eq!(completed["state"], "Completed");
    assert_eq!(completed["end_reason"], "FinalAnswer");
    compare(
        &mut process,
        second_key,
        &case["second_expected_kinds"],
        directory,
        "turn2",
    );
    let session = process.rpc("session.load", json!({"session_id":"s"}));
    assert_eq!(session["turns"].as_array().unwrap().len(), 2);
    assert_eq!(session["messages"].as_array().unwrap().len(), 4);
    assert_eq!(session["context_messages"].as_array().unwrap().len(), 8);
}

fn repeat_scenario(config: &Path, directory: &Path) {
    let case: Value =
        serde_json::from_str(include_str!("../fixtures/server_process.json")).unwrap();
    let mut process = Process::start(config, directory);
    process.rpc("session.create", json!({"session_id":"s-repeat"}));
    let key = json!({"session_id":"s-repeat","turn_id":"t3","execution_id":"e3"});
    let mut start = key.clone();
    start["input"] = case["repeat_input"].clone();
    let mut response = process.rpc_response("execution.start", start);
    for _ in 0..2 {
        assert_eq!(response["result"]["state"], "Suspended", "{response}");
        let mut approve = key.clone();
        approve["approval_id"] = response["result"]["approval"]["approval_id"].clone();
        response = process.rpc_response("execution.approve", approve);
    }
    assert!(
        response["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("no progress")),
        "{response}"
    );
    let events = process.rpc("execution.events", key)["events"]
        .as_array()
        .unwrap()
        .clone();
    let mut file = fs::File::create(directory.join("no-progress.jsonl")).unwrap();
    for event in &events {
        writeln!(file, "{event}").unwrap();
    }
    assert_fact_contract(&events, "repeat_contract");
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "effect_started")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "effect_receipt")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "step_completed")
            .count(),
        3
    );
    assert!(
        events.iter().any(
            |event| event["kind"] == "turn_failed" && event.to_string().contains("no progress")
        )
    );
}

fn cancel_scenario(config: &Path, directory: &Path) {
    let case: Value =
        serde_json::from_str(include_str!("../fixtures/server_process.json")).unwrap();
    let mut process = Process::start(config, directory);
    process.rpc("session.create", json!({"session_id":"s-cancel"}));
    let key = json!({"session_id":"s-cancel","turn_id":"t-cancel","execution_id":"e-cancel"});
    let mut start = key.clone();
    start["input"] = case["cancel_input"].clone();
    let suspended = process.rpc("execution.start", start);
    assert_eq!(suspended["state"], "Suspended");
    process.rpc("execution.cancel", key.clone());
    drop(process);
    let mut process = Process::start(config, directory);
    let status = process.rpc("execution.status", key.clone());
    assert_eq!(status["state"], "Cancelled");
    assert_eq!(status["execution_stopped"], true);
    let mut resume = key.clone();
    resume["approval_id"] = suspended["approval"]["approval_id"].clone();
    assert!(
        process
            .rpc_response("execution.approve", resume)
            .get("error")
            .is_some()
    );
    let events = process.rpc("execution.events", key)["events"]
        .as_array()
        .unwrap()
        .clone();
    let mut file = fs::File::create(directory.join("cancelled.jsonl")).unwrap();
    for event in &events {
        writeln!(file, "{event}").unwrap();
    }
    assert_fact_contract(&events, "cancel_contract");
    assert!(!events.iter().any(|event| event["kind"] == "effect_started"));
    assert!(!directory.join("workspace/safe/cancel.txt").exists());
}

fn assert_fact_contract(events: &[Value], name: &str) {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/server_process.json")).unwrap();
    let contract = &fixture[name];
    for (kind, count) in contract["counts"].as_object().unwrap() {
        assert_eq!(
            events.iter().filter(|event| event["kind"] == *kind).count() as u64,
            count.as_u64().unwrap(),
            "{name}: exact {kind} count"
        );
    }
    for kind in contract["forbidden"].as_array().unwrap() {
        assert!(
            !events.iter().any(|event| event["kind"] == *kind),
            "{name}: forbidden {kind}"
        );
    }
    for event in events
        .iter()
        .filter(|event| event["kind"] == "model_requested")
    {
        assert!(
            event["payload"]["request"]["messages"].is_array(),
            "actual model context must be retained"
        );
    }
}

#[test]
fn server_process_approval_restart_and_two_turns() {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-process-offline-")
        .tempdir()
        .unwrap()
        .keep();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let capture = directory.clone();
    let worker = std::thread::spawn(move || {
        for index in 0..8 {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(20)))
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
            fs::write(capture.join(format!("model-request-{index}.json")), &bytes).unwrap();
            let output = match index {
                0 => {
                    json!([{"type":"function_call","id":"item1","call_id":"write1","name":"file.write","arguments":"{\"path\":\"safe/result.txt\",\"content\":\"kolyan-process-proof\"}"}])
                }
                2 => {
                    json!([{"type":"function_call","id":"item2","call_id":"read1","name":"file.read","arguments":"{\"path\":\"safe/result.txt\"}"}])
                }
                4..=6 => {
                    json!([{"type":"function_call","id":format!("item{index}"),"call_id":format!("repeat{index}"),"name":"file.write","arguments":"{\"path\":\"safe/repeat.txt\",\"content\":\"repeat-proof\"}"}])
                }
                7 => {
                    json!([{"type":"function_call","id":"cancel-item","call_id":"cancel-call","name":"file.write","arguments":"{\"path\":\"safe/cancel.txt\",\"content\":\"cancel-proof\"}"}])
                }
                _ => {
                    json!([{"type":"message","id":"msg","role":"assistant","content":[{"type":"output_text","text":"kolyan-process-proof"}]}])
                }
            };
            let mut body = String::new();
            if output[0]["type"] == "function_call" {
                let mut item = output[0].clone();
                let arguments = item["arguments"].clone();
                item["arguments"] = json!("");
                for event in [
                    json!({"type":"response.output_item.added","output_index":0,"item":item}),
                    json!({"type":"response.output_item.done","output_index":0,"item":item}),
                    json!({"type":"response.function_call_arguments.delta","item_id":item["id"],"delta":arguments}),
                ] {
                    body.push_str(&format!(
                        "event: {}\ndata: {event}\n\n",
                        event["type"].as_str().unwrap()
                    ));
                }
            }
            body.push_str(&format!(
                "event: response.completed\ndata: {}\n\n",
                json!({"type":"response.completed","response":{"id":format!("r{index}"),"status":"completed","output":output}})
            ));
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    });
    let path = config(
        &directory,
        "fixture",
        "openai_responses",
        "fixture",
        &url,
        "KOLYAN_FIXTURE_KEY",
        Some(4096),
    );
    eprintln!("process evidence: {}", directory.display());
    scenario(&path, &directory);
    repeat_scenario(&path, &directory);
    cancel_scenario(&path, &directory);
    worker.join().unwrap();
    let next_turn: Value =
        serde_json::from_slice(&fs::read(directory.join("model-request-2.json")).unwrap()).unwrap();
    assert!(
        next_turn.to_string().contains("function_call_output"),
        "previous Turn tool evidence must survive in context"
    );
    let last: Value =
        serde_json::from_slice(&fs::read(directory.join("model-request-3.json")).unwrap()).unwrap();
    assert!(last.to_string().contains("function_call_output"));
    assert!(last.to_string().contains("kolyan-process-proof"));
}

#[tokio::test]
#[ignore = "real server processes: requires all configured provider credentials"]
async fn server_process_live_matrix() {
    let config_set = common::load_config();
    let mut entries = Vec::new();
    for (family, cfg) in [
        ("minimax", config_set.minimax_openai),
        ("qwen", config_set.qwen_openai),
    ] {
        for model in cfg.model_matrix {
            entries.push((
                family,
                "openai_responses",
                cfg.base_url.clone(),
                cfg.api_key_env.clone(),
                model,
            ));
        }
    }
    for (family, cfg) in [
        ("minimax", config_set.minimax_anthropic),
        ("qwen", config_set.qwen_anthropic),
    ] {
        for model in cfg.model_matrix {
            entries.push((
                family,
                "anthropic_messages",
                cfg.base_url.clone(),
                cfg.api_key_env.clone(),
                model,
            ));
        }
    }
    let entries = entries
        .into_iter()
        .flat_map(|entry| {
            ["approval_two_turn", "no_progress", "cancel_approval"]
                .into_iter()
                .map(move |case| (entry.clone(), case))
        })
        .collect::<Vec<_>>();
    let mut report = matrix::Matrix::new(entries.iter().map(
        |((family, protocol, _, _, model), case)| {
            format!("{family}/{protocol}/{}/{case}", model.model)
        },
    ));
    // Keep one immutable executable for the entire matrix, including restarts,
    // even if another Cargo invocation rebuilds target/debug concurrently.
    fs::copy(
        env!("CARGO_BIN_EXE_kolyan-server"),
        report.directory.join("server.bin"),
    )
    .unwrap();
    for (index, ((family, protocol, url, key_env, model), case)) in entries.into_iter().enumerate()
    {
        let directory = report.directory.join(index.to_string());
        fs::create_dir_all(&directory).unwrap();
        report
            .run(index, async {
                assert!(
                    std::env::var(&key_env).is_ok_and(|value| !value.is_empty()),
                    "missing credential variable {key_env}"
                );
                let path = config(
                    &directory,
                    family,
                    protocol,
                    &model.model,
                    &url,
                    &key_env,
                    model.max_output_tokens,
                );
                match case {
                    "approval_two_turn" => scenario(&path, &directory),
                    "no_progress" => repeat_scenario(&path, &directory),
                    "cancel_approval" => cancel_scenario(&path, &directory),
                    _ => unreachable!(),
                }
            })
            .await;
    }
    assert!(
        report.complete(),
        "server process matrix failed: {}",
        report.directory.display()
    );
}

#[test]
fn cancellation_stays_responsive_during_a_model_request() {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-process-cancel-")
        .tempdir()
        .unwrap()
        .keep();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (entered, entered_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
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
        let mut request = vec![0; size];
        reader.read_exact(&mut request).unwrap();
        entered.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let body = format!(
            "event: response.completed\ndata: {}\n\n",
            json!({"type":"response.completed","response":{"id":"cancel-response","status":"completed","output":[{"type":"message","id":"msg","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}})
        );
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let config = config(
        &directory,
        "fixture",
        "openai_responses",
        "fixture",
        &url,
        "KOLYAN_FIXTURE_KEY",
        Some(4096),
    );
    let mut process = Process::start(&config, &directory);
    process.rpc("session.create", json!({"session_id":"s"}));
    let key = json!({"session_id":"s","turn_id":"t","execution_id":"e"});
    let mut start = key.clone();
    start["input"] = json!("answer slowly");
    let start_id = process.send("execution.start", start);
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    process.rpc("execution.cancel", key.clone());
    let pending = process.rpc("execution.status", key.clone());
    assert_eq!(pending["state"], "Cancelling");
    assert_eq!(pending["execution_stopped"], false);
    release.send(()).unwrap();
    let terminal = process.receive();
    assert_eq!(terminal["id"], start_id);
    assert!(
        terminal["error"]["message"]
            .as_str()
            .unwrap()
            .contains("cancelled")
    );
    let stopped = process.rpc("execution.status", key);
    assert_eq!(stopped["state"], "Cancelled");
    assert_eq!(stopped["execution_stopped"], true);
    let session = process.rpc("session.load", json!({"session_id":"s"}));
    assert_eq!(session["turns"][0]["status"], "cancelled");
    worker.join().unwrap();
    eprintln!("active cancellation evidence: {}", directory.display());
}
