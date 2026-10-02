//! Actual SDK localhost transport fixture and the explicitly ignored MiniMax entry.
//! Local replies are scripted data, never evidence of actual model behavior.
use super::{data::*, driver};
use kolyan_agent::{RegisteredSkill, SkillCatalog};
use kolyan_agent_host::{DeploymentProtocol, HostDeployment};
use kolyan_model::{ModelDescriptor, ModelRef};
use serde_json::{Value, json};
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

pub(super) fn protocol(p: Protocol) -> DeploymentProtocol {
    match p {
        Protocol::OpenAI => DeploymentProtocol::OpenaiResponses,
        Protocol::Anthropic => DeploymentProtocol::AnthropicMessages,
    }
}
pub(super) fn protocol_name(p: Protocol) -> &'static str {
    match p {
        Protocol::OpenAI => "openai_responses",
        Protocol::Anthropic => "anthropic_messages",
    }
}
type Script = (VecDeque<Frame>, Option<(SkillCatalog, RegisteredSkill)>);
pub(super) struct Backend {
    pub endpoint: String,
    script: Arc<Mutex<Script>>,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<Result<(), String>>>,
}
impl Backend {
    pub fn start(protocol: Protocol) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let endpoint = format!(
            "http://{}",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let script = Arc::new(Mutex::new((
            VecDeque::<Frame>::new(),
            None::<(SkillCatalog, RegisteredSkill)>,
        )));
        let replies = script.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(s) => s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(e) => return Err(e.to_string()),
                };
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
                        return Err("header bound".into());
                    }
                    let mut byte = [0];
                    stream.read_exact(&mut byte).map_err(|e| e.to_string())?;
                    header.push(byte[0]);
                }
                let header = String::from_utf8(header).map_err(|e| e.to_string())?;
                let size = header
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length").then_some(v.trim())
                    })
                    .ok_or("body size missing")?
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?;
                if size > 4 * 1024 * 1024 {
                    return Err("body bound".into());
                }
                let mut body = vec![0; size];
                stream.read_exact(&mut body).map_err(|e| e.to_string())?;
                let request: Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
                let (frame, revoke) = {
                    let mut script = replies.lock().map_err(|e| e.to_string())?;
                    (script.0.pop_front(), script.1.take())
                };
                let revoked = revoke.is_some();
                if let Some((catalog, selected)) = revoke {
                    driver::revoke(&catalog, &selected)?;
                }
                let Some(frame) = frame else {
                    captured.lock().map_err(|e|e.to_string())?.push(json!({"body":request,"body_bytes":body,"error":"unexpected model opening"}));
                    return Err("unexpected model opening".into());
                };
                let wire = events(protocol, &frame);
                captured.lock().map_err(|e|e.to_string())?.push(json!({"request_line":header.lines().next(),"body":request,"body_bytes":body,"response_body":wire,"script":frame,"revocation_before_reply":revoked}));
                write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",wire.len(),wire).map_err(|e|e.to_string())?;
                stream.flush().map_err(|e| e.to_string())?;
            }
            Ok(())
        });
        Ok(Self {
            endpoint,
            script,
            requests,
            stop,
            thread: Some(thread),
        })
    }
    pub fn set_script(
        &mut self,
        frames: Vec<Frame>,
        revoke: Option<(SkillCatalog, RegisteredSkill)>,
    ) -> Result<(), String> {
        *self.script.lock().map_err(|e| e.to_string())? = (frames.into(), revoke);
        Ok(())
    }
    pub fn finish(&mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        self.thread
            .take()
            .ok_or("already closed")?
            .join()
            .map_err(|_| "backend panic")?
    }
    pub fn requests(&self) -> Vec<Value> {
        self.requests.lock().expect("captured requests").clone()
    }
}
impl Drop for Backend {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
fn events(protocol: Protocol, frame: &Frame) -> String {
    let mut events = Vec::new();
    match protocol {
        Protocol::OpenAI => {
            let output: Vec<Value> = if frame.calls.is_empty() {
                vec![
                    json!({"type":"message","id":"msg","role":"assistant","status":"completed","content":[{"type":"output_text","text":frame.text,"annotations":[]}]}),
                ]
            } else {
                frame.calls.iter().map(|c|json!({"type":"function_call","id":c.id,"call_id":c.id,"name":c.name,"arguments":c.arguments.to_string(),"status":"completed"})).collect()
            };
            events.push(json!({"type":"response.created","response":{"id":"fixture","model":"fixture-model","status":"in_progress","output":[]}}));
            for (i, item) in output.iter().enumerate() {
                events
                    .push(json!({"type":"response.output_item.done","output_index":i,"item":item}));
            }
            events.push(json!({"type":"response.completed","response":{"id":"fixture","model":"fixture-model","status":"completed","output":output,"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}));
        }
        Protocol::Anthropic => {
            events.push(json!({"type":"message_start","message":{"id":"fixture","type":"message","role":"assistant","model":"fixture-model","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}));
            if frame.calls.is_empty() {
                events.push(json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}));
                events.push(json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":frame.text}}));
                events.push(json!({"type":"content_block_stop","index":0}));
            } else {
                for (i, c) in frame.calls.iter().enumerate() {
                    events.push(json!({"type":"content_block_start","index":i,"content_block":{"type":"tool_use","id":c.id,"name":c.name,"input":{}}}));
                    events.push(json!({"type":"content_block_delta","index":i,"delta":{"type":"input_json_delta","partial_json":c.arguments.to_string()}}));
                    events.push(json!({"type":"content_block_stop","index":i}));
                }
            }
            events.push(json!({"type":"message_delta","delta":{"stop_reason":if frame.calls.is_empty(){"end_turn"}else{"tool_use"}},"usage":{"output_tokens":2}}));
            events.push(json!({"type":"message_stop"}));
        }
    }
    events
        .into_iter()
        .map(|e| {
            if protocol == Protocol::Anthropic {
                format!(
                    "event: {}\ndata: {e}\n\n",
                    e["type"].as_str().expect("event type")
                )
            } else {
                format!("data: {e}\n\n")
            }
        })
        .collect()
}

#[tokio::test]
#[ignore = "Actual MiniMax Skills 4 rows: Main alone authorizes network execution"]
async fn actual_minimax_skills_named_inline_both_protocols() {
    let dataset = dataset().expect("strict Skills fixture");
    dataset.validate().unwrap();
    let common = super::super::common::load_config();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-skills-c-minimax-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    driver::save(
        &evidence.join("plan.jsonl"),
        &[
            json!({"dataset":dataset,"planned_rows":dataset.network.planned_rows,
            "actual_llm":true,"attempts_per_row":dataset.network.attempts_per_row}),
        ],
    )
    .unwrap();
    println!("SKILLS_C_MINIMAX_ACTUAL={}", path.display());
    let installation = super::super::tools::worker::WorkerRun::prepare().await;
    let case = dataset
        .cases
        .iter()
        .find(|c| c.id == dataset.network.operation_case)
        .unwrap();
    let mut rows = Vec::new();
    for p in &dataset.network.protocols {
        for selector in &dataset.network.selectors {
            let (base_url, env, timeout) = match p {
                Protocol::OpenAI => (
                    &common.minimax_openai.base_url,
                    &common.minimax_openai.api_key_env,
                    common.minimax_openai.timeout_secs,
                ),
                Protocol::Anthropic => (
                    &common.minimax_anthropic.base_url,
                    &common.minimax_anthropic.api_key_env,
                    common.minimax_anthropic.timeout_secs,
                ),
            };
            if !std::env::var(env).is_ok_and(|s| !s.is_empty()) {
                rows.push(json!({"case_id":case.id,"protocol":p,"selector":selector,"error":"missing credential: NotRun"}));
                driver::save(&path, &rows).unwrap();
                continue;
            }
            let deployment = HostDeployment {
                protocol: protocol(*p),
                descriptor: ModelDescriptor {
                    reference: ModelRef::new("minimax", &dataset.network.model),
                    context_window: Some(65536),
                    max_output_tokens: Some(16384),
                    features: driver::features(),
                },
                base_url: base_url.clone(),
                api_key_env: env.clone(),
                timeout_secs: timeout,
                http_retry: Default::default(),
                parameter_table: serde_json::from_value(super::super::common::parameter_table(
                    "minimax",
                    protocol_name(*p),
                    &dataset.network.model,
                ))
                .unwrap(),
            };
            rows.push(
                driver::scenario(
                    &dataset,
                    case,
                    *p,
                    *selector,
                    &evidence,
                    &installation,
                    Some(deployment),
                )
                .await,
            );
            driver::save(&path, &rows).unwrap();
        }
    }
    let observed = driver::save(&path, &rows).unwrap();
    println!("SKILLS_C_MINIMAX_ACTUAL={}", path.display());
    assert_eq!(observed.len(), dataset.network.planned_rows);
    for row in &observed {
        super::evidence::compare(&dataset, row)
            .unwrap_or_else(|e| panic!("{e}; evidence={}", path.display()));
    }
}
