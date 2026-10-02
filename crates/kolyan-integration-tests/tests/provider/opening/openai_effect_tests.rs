//! Actual loopback Provider -> Core -> Runtime -> confined file effect.
//! HTTP responses are fixture data, not real model outputs or native-worker claims.

use std::{
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{LedgerEventKind, LedgerQuery, LedgerStore, SqliteLedger};
use kolyan_model::ModelRequest;
use kolyan_policy::PolicyEngine;
use kolyan_protocol_openai::{HttpRetryPolicy, OpenAiClient, OpenAiConfig, RetryProfile};
use kolyan_provider_openai::OpenAiProvider;
use kolyan_runtime::{DurableTurnDriver, DurableTurnResult};
use kolyan_tools::RestrictedFileTool;
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    path: String,
    content: String,
    call_id: String,
    completion_statuses: Vec<u16>,
    expected_attempts: usize,
    expected_completed: bool,
    expected_receipts: usize,
}

#[tokio::test]
async fn completion_opening_faults_do_not_replay_receipted_effects() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("openai_effect.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-openai-opening-effects-")
        .tempdir()
        .unwrap()
        .keep();
    println!("OPENAI_OPENING_EFFECT_TRACE={}", root.display());
    let mut rows = Vec::new();
    for case in &cases {
        let directory = root.join(&case.id);
        let workspace = directory.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let ledger = SqliteLedger::open(directory.join("ledger.sqlite")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let tool_output = json!([{"type":"function_call","id":"item-write","call_id":case.call_id,"name":"file.write","arguments":json!({"path":case.path,"content":case.content}).to_string()}]);
        let mut replies = vec![(200, completed(tool_output))];
        replies.extend(case.completion_statuses.iter().map(|status| (*status, match status {
            200 => completed(json!([{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}])),
            429 => json!({"error":{"code":"Throttling.AllocationQuota"}}).to_string(),
            _ => json!({"error":{"code":"server_error"}}).to_string(),
        })));
        let (stop, mut stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut replies = replies.into_iter();
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stopped => return Ok::<_,String>(()),
                    accepted = listener.accept() => accepted.map_err(|e|e.to_string())?,
                };
                let wire = tokio::time::timeout(Duration::from_secs(2), async {
                    let mut headers = Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        if headers.len() >= 8192 {
                            return Err("oversized headers".to_owned());
                        }
                        headers.push(socket.read_u8().await.map_err(|e| e.to_string())?);
                    }
                    let text = std::str::from_utf8(&headers).map_err(|e| e.to_string())?;
                    let length = text
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then_some(value.trim())
                        })
                        .ok_or("missing content length")?
                        .parse::<usize>()
                        .map_err(|e| e.to_string())?;
                    if length > 65536 {
                        return Err("oversized body".to_owned());
                    }
                    let mut body = vec![0; length];
                    socket
                        .read_exact(&mut body)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<_, String>(body)
                })
                .await
                .map_err(|e| e.to_string())??;
                captured.lock().unwrap().push(json!({"body_bytes":wire,"body":serde_json::from_slice::<Value>(&wire).map_err(|e|e.to_string())?}));
                let (status, body) = replies.next().ok_or("unexpected request replay")?;
                socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\nretry-after-ms: 1\r\n\r\n{body}",body.len()).as_bytes()).await.map_err(|e|e.to_string())?;
            }
        });
        let mut config = OpenAiConfig::new("localhost-placeholder");
        config.base_url = endpoint;
        config.transport_retries = 0;
        config.http_retry = HttpRetryPolicy::new(1, 2000, 1000, RetryProfile::OpenAi).unwrap();
        let mut policy = PolicyEngine::default();
        for manifest in RestrictedFileTool::tool_manifests() {
            policy.register(manifest);
        }
        let model_request: ModelRequest = serde_json::from_value(json!({"request_id":case.id,"model":{"provider":"localhost-openai","model":"fixture-model"},"system":[],"messages":[{"role":"user","content":[{"type":"text","text":"fixture write followed by completion"}]}],"tools":RestrictedFileTool::tool_definitions(),"tool_choice":"auto","output_format":null,"prompt_cache":null,"reasoning":null,"max_output_tokens":64,"extensions":{}})).unwrap();
        let result = DurableTurnDriver::new(ledger.clone(), NoopTraceSink)
            .start(
                TurnExecutor::with_tools(
                    OpenAiProvider::new(OpenAiClient::new(config).unwrap()),
                    RestrictedFileTool::new(&workspace),
                )
                .with_policy_engine(Arc::new(policy)),
                TurnRequest {
                    turn_id: case.id.clone(),
                    model_request,
                    config: TurnConfig {
                        max_steps: 4,
                        ..Default::default()
                    },
                },
                "loopback-session",
                case.id.clone(),
            )
            .await;
        let _ = stop.send(());
        let server_result = server.await.unwrap();
        let events = ledger
            .query(&LedgerQuery {
                execution_id: Some(case.id.clone()),
                event_id: None,
                after: 0,
                through: None,
                limit: 1024,
            })
            .unwrap();
        let requests = requests.lock().unwrap().clone();
        let completed = matches!(&result, Ok(DurableTurnResult::Completed(..)));
        let row = json!({"case_id":case.id,"requests":requests,"ledger":events,"completed":completed,"result":format!("{result:?}"),"physical":fs::read_to_string(workspace.join(&case.path)).ok(),"server_error":server_result.err()});
        fs::write(directory.join("actual.jsonl"), format!("{row}\n")).unwrap();
        rows.push(row);
    }
    for (row, case) in rows.iter().zip(&cases) {
        assert!(row["server_error"].is_null(), "{row}");
        assert_eq!(row["completed"], case.expected_completed, "{row}");
        assert_eq!(row["physical"], case.content, "{row}");
        let ledger = row["ledger"].as_array().unwrap();
        for kind in [
            LedgerEventKind::EffectStarted,
            LedgerEventKind::EffectReceipt,
        ] {
            let expected = serde_json::to_value(kind).unwrap();
            assert_eq!(
                ledger
                    .iter()
                    .filter(|event| event["kind"] == expected)
                    .count(),
                case.expected_receipts,
                "{row}"
            );
        }
        let requests = row["requests"].as_array().unwrap();
        assert_eq!(requests.len(), case.expected_attempts, "{row}");
        for request in &requests[1..] {
            assert!(
                request["body"]["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item["type"] == "function_call_output"
                        && item["call_id"] == case.call_id
                        && item["output"]
                            .as_str()
                            .is_some_and(|output| output.contains("wrote"))),
                "successful ToolResult preserved: {row}"
            );
            assert_eq!(
                request["body_bytes"], requests[1]["body_bytes"],
                "completion attempts frozen"
            );
        }
    }
}

fn completed(output: Value) -> String {
    format!(
        "data: {}\n\n",
        json!({"type":"response.completed","response":{"id":"fixture-response","model":"fixture-model","status":"completed","output":output}})
    )
}
