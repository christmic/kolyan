//! Localhost SDK fixtures, not real LLM/count acceptance. All observations are persisted first.
use super::*;
use futures_util::StreamExt;
use kolyan_model::{CountProfile, ModelProvider};
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io::Write, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Unsupported,
    Registered,
    UnchangedClone,
    ForeignOwner,
    ForeignEndpoint,
    ConfigChanged,
    ProfileChanged,
    PlannerChanged,
    CallerChanged,
    WireReplaced,
    PlannedRequestChanged,
    NeutralChanged,
    UnknownExtension,
    Complex,
    Cache,
    StructuredInvalid,
    PlannerOmit,
    OrdinaryStream,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    mode: Mode,
    registered: bool,
    expected_count: String,
    expected_gen: usize,
    expected_error: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    request: Value,
    stream_response: String,
    rows: Vec<Row>,
}

fn host(base: &str) -> OpenAiProvider {
    let mut config = OpenAiConfig::new("local-generation-fixture-only");
    config.base_url = base.into();
    OpenAiProvider::new(OpenAiClient::new(config).unwrap())
}
fn table(extension: bool, omit: bool) -> kolyan_model::ParameterTable {
    let mut defaults = json!({
        "max_output_tokens":{"support":"supported","schema":{"type":"integer"},"default":128},
        "tool_choice":{"support":if omit {"unsupported"} else {"supported"},"schema":{"type":"string"}}
    });
    if extension {
        defaults["extensions.fixture.extra"] = json!({"support":"supported","schema":{},
            "wire_name":"vendor_extra","default":{"preserved":"中文🦀"}});
    }
    serde_json::from_value(json!({"provider":"fixture","protocol":"openai_responses",
        "features":["text_input","text_output","streaming","tool_use"],
        "defaults":defaults,"models":{"fixture-model":{}}}))
    .unwrap()
}

#[tokio::test]
async fn owned_generation_raw_body_matrix_exports_then_reloads() {
    let data: Dataset = serde_json::from_str(include_str!("cases.json")).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-openai-owned-generation-")
        .tempdir()
        .unwrap()
        .keep();
    let mut export = std::fs::File::create(dir.join("actual.jsonl")).unwrap();
    for (index, row) in data.rows.iter().enumerate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut request: ModelRequest = serde_json::from_value(data.request.clone()).unwrap();
        if row.mode == Mode::Cache {
            request.prompt_cache = Some(
                serde_json::from_value(
                    json!({"key":"fixture-cache","retention":null,"breakpoints":[]}),
                )
                .unwrap(),
            );
        }
        if matches!(row.mode, Mode::Complex | Mode::StructuredInvalid) {
            request.output_format = Some(kolyan_model::OutputFormat {
                name: "answer".into(),
                strict: true,
                schema: if row.mode == Mode::StructuredInvalid {
                    json!({"type":"object","required":["missing"]})
                } else {
                    json!({"type":"object"})
                },
            });
            request.reasoning = Some(kolyan_model::ReasoningConfig {
                effort: Some("low".into()),
                budget_tokens: None,
            });
            request.messages.push(serde_json::from_value(json!({"role":"assistant","content":[
                {"type":"tool_call","call":{"id":"call","name":"read","arguments":{"path":"safe/a"}}}]})).unwrap());
            request.messages.push(serde_json::from_value(json!({"role":"user","content":[
                {"type":"tool_result","result":{"call_id":"call","content":"结果🦀\n\\u0000","is_error":false}}]})).unwrap());
        }
        let mut owner = host(&base);
        if matches!(row.mode, Mode::UnknownExtension | Mode::PlannerOmit) {
            owner = owner
                .with_parameter_table(table(
                    row.mode == Mode::UnknownExtension,
                    row.mode == Mode::PlannerOmit,
                ))
                .unwrap();
        }
        let identity = owner.prepare_wire(&request).unwrap().identity().clone();
        if row.registered {
            owner = owner.with_count_profile(
                CountProfile::registered(identity.clone(), "fixture-v1".into()).unwrap(),
            );
        }
        let original_input = request.clone();
        let mut prepared = owner.prepare_generation(&request).unwrap();
        let original_body = prepared.wire().generation_body().clone();
        let original_digest = prepared.wire().generation_wire_digest().to_owned();
        let count_body = prepared.wire().count_body().clone();
        let count_digest = prepared.wire().count_input_digest().to_owned();
        let mut consumer = owner.clone();
        match row.mode {
            Mode::ForeignOwner => consumer = host(&base),
            Mode::ForeignEndpoint => consumer = host("http://127.0.0.1:1"),
            Mode::ConfigChanged => consumer.client = host("http://127.0.0.1:1").client,
            Mode::ProfileChanged => {
                consumer = consumer.with_count_profile(
                    CountProfile::registered(identity.clone(), "fixture-v2".into()).unwrap(),
                )
            }
            Mode::PlannerChanged => {
                consumer = consumer.with_parameter_table(table(false, false)).unwrap()
            }
            Mode::CallerChanged => request.messages.clear(),
            Mode::NeutralChanged => prepared.original.messages.clear(),
            Mode::PlannedRequestChanged => prepared.plan.request.output_format = None,
            Mode::WireReplaced => {
                let mut different = request.clone();
                different.messages.clear();
                prepared.wire = owner.prepare_wire(&different).unwrap();
            }
            _ => {}
        }
        // Ensure the private-plan mutation is an actual change even without output schema.
        if row.mode == Mode::PlannedRequestChanged {
            prepared.plan.request.request_id.push_str("-tampered");
        }
        let prepared_identity = prepared.wire().identity().clone();
        let target_identity = consumer
            .prepare_wire(&prepared.original)
            .unwrap()
            .identity()
            .clone();
        let generation_body = prepared.wire().generation_body().clone();
        let generation_digest = prepared.wire().generation_wire_digest().to_owned();
        let generation_bytes = prepared.wire().generation_wire_bytes();
        let neutral_digest = prepared.wire().neutral_digest().to_owned();
        let owner_same =
            std::sync::Arc::ptr_eq(&owner.accounting_owner, &consumer.accounting_owner);
        let response = data.stream_response.clone();
        let server = tokio::spawn(serve(listener, response));
        let counted = consumer
            .count_prepared(prepared.wire(), Duration::from_secs(2))
            .await;
        let (count_class, count_result, count_error) = match counted {
            Ok(count) => ("success", json!(count), Value::Null),
            Err(error) => (
                "unsupported",
                Value::Null,
                json!({"kind":error.kind,"phase":error.phase,"message":error.to_string()}),
            ),
        };
        let opened = if row.mode == Mode::OrdinaryStream {
            consumer.stream(request.clone()).await
        } else {
            consumer.stream_prepared(prepared).await
        };
        let observed = match opened {
            Ok(stream) => stream.collect::<Vec<_>>().await,
            Err(error) => vec![Err(error)],
        };
        let events:Vec<_> = observed.iter().map(|result|match result {
            Ok(event)=>json!({"event":event}),
            Err(error)=>json!({"error":{"kind":error.kind,"phase":error.phase,"message":error.to_string()}})
        }).collect();
        let physical = server.await.unwrap();
        writeln!(export,"{}",json!({"index":index,"input":request,
            "original_input":original_input,"prepared_identity":prepared_identity,"target_identity":target_identity,
            "profile":owner.count_profile,"target_profile":consumer.count_profile,
            "owner":{"same":owner_same},"neutral_digest":neutral_digest,
            "generation_body":generation_body,"generation_digest":generation_digest,"generation_bytes":generation_bytes,
            "original_body":original_body,"original_digest":original_digest,
            "count_body":count_body,"count_digest":count_digest,
            "count_class":count_class,"count_result":count_result,"count_error":count_error,
            "events":events,"physical":physical,"response":data.stream_response})).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    let actual: Vec<Value> = std::fs::read_to_string(dir.join("actual.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    eprintln!("owned generation evidence: {}", dir.display());
    assert_eq!(actual.len(), data.rows.len());
    for (index, (row, actual)) in data.rows.iter().zip(actual.iter()).enumerate() {
        assert_eq!(actual["index"], index);
        assert_eq!(
            actual["count_class"],
            row.expected_count,
            "row {index}: {}",
            dir.display()
        );
        let physical = actual["physical"].as_array().unwrap();
        let generations: Vec<_> = physical
            .iter()
            .filter(|r| r["path"] == "/v1/responses")
            .collect();
        let counts: Vec<_> = physical
            .iter()
            .filter(|r| r["path"] != "/v1/responses")
            .collect();
        assert_eq!(
            generations.len(),
            row.expected_gen,
            "row {index}: {}",
            dir.display()
        );
        assert_eq!(
            counts.len(),
            usize::from(row.expected_count == "success"),
            "row {index}"
        );
        let errors = actual["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e.get("error").is_some());
        assert_eq!(errors, row.expected_error, "row {index}: {}", dir.display());
        for sent in generations {
            assert_eq!(sent["body"], actual["generation_body"], "row {index}");
            assert_eq!(
                sent["raw_body"],
                serde_json::to_string(&actual["generation_body"]).unwrap(),
                "row {index}"
            );
            assert_eq!(
                sent["body_digest"], actual["generation_digest"],
                "row {index}"
            );
            assert_eq!(
                sent["raw_body"].as_str().unwrap().len() as u64,
                actual["generation_bytes"].as_u64().unwrap()
            );
        }
        for sent in counts {
            assert_eq!(sent["body"], actual["count_body"], "row {index}");
            assert_eq!(sent["body_digest"], actual["count_digest"], "row {index}");
            assert_eq!(actual["count_result"]["input_tokens"], 42);
        }
        if !row.expected_error {
            assert!(
                actual["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["event"].get("Completed").is_some()),
                "row {index}: {}",
                actual["events"]
            );
            assert_eq!(actual["generation_digest"], actual["original_digest"]);
        }
        if row.mode == Mode::UnknownExtension {
            assert_eq!(
                actual["generation_body"]["vendor_extra"],
                json!({"preserved":"中文🦀"})
            );
        }
        if row.mode == Mode::PlannerOmit {
            assert!(actual["generation_body"].get("tool_choice").is_none());
        }
    }
}

async fn serve(listener: TcpListener, stream_response: String) -> Vec<Value> {
    let mut observations = Vec::new();
    loop {
        let Ok(Ok((mut socket, _))) =
            tokio::time::timeout(Duration::from_millis(400), listener.accept()).await
        else {
            break;
        };
        let mut bytes = Vec::new();
        let (end, length) = loop {
            let mut buffer = [0; 4096];
            let n = socket.read(&mut buffer).await.unwrap();
            assert_ne!(n, 0, "incomplete localhost HTTP request");
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                let head = std::str::from_utf8(&bytes[..end]).unwrap();
                let length = head
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
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
        let raw_body = std::str::from_utf8(&bytes[end + 4..end + 4 + length]).unwrap();
        let body: Value = serde_json::from_str(raw_body).unwrap();
        let count = path != "/v1/responses";
        let response = if count {
            json!({"input_tokens":42,"object":"response.input_tokens"}).to_string()
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
        let write = socket
            .write_all(response.as_bytes())
            .await
            .map_err(|error| error.to_string());
        observations.push(json!({"path":path,"raw_body":raw_body,"body":body,
            "body_digest":kolyan_model::digest_json(&body).unwrap(),"response":response,"response_write":write}));
    }
    observations
}
