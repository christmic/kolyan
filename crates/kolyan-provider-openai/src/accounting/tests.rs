//! Local HTTP protocol fixtures, not model/tokenizer precision acceptance.
mod identity;

use crate::OpenAiProvider;
use futures_util::StreamExt;
use kolyan_model::{CountProfile, MappingIdentity, ModelProvider, ModelRequest, ProviderErrorKind};
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io::Write, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Extension {
    wire_name: String,
    value: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    mode: String,
    expected: String,
    tokens: Option<u64>,
    extension: Option<Extension>,
    fault: Option<CountFault>,
    error_kind: Option<String>,
    error_phase: Option<String>,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CountFault {
    Http529,
    Schema,
    Oversize,
    BodyTimeout,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    request: Value,
    stream_response: String,
    count_fields: Vec<String>,
    rows: Vec<Row>,
}

fn provider(base: &str) -> OpenAiProvider {
    let mut config = OpenAiConfig::new("localhost-fixture-only");
    config.base_url = base.into();
    OpenAiProvider::new(OpenAiClient::new(config).unwrap())
}
fn table(row: &Row) -> kolyan_model::ParameterTable {
    let mut defaults = json!({
        "max_output_tokens":{"support":"supported","schema":{"type":"integer"},"default":128},
        "tool_choice":{"support":"supported","schema":{"type":"string","enum":["auto"]}}
    });
    if let Some(extension) = &row.extension {
        defaults["extensions.fixture.extra"] = json!({"support":"supported","schema":{},
            "wire_name":extension.wire_name,"default":extension.value});
    }
    serde_json::from_value(json!({"provider":"fixture","protocol":"openai_responses",
        "features":["text_input","text_output","streaming","tool_use"],
        "defaults":defaults,"models":{"fixture-model":{}}}))
    .unwrap()
}

#[tokio::test]
async fn prepared_count_provider_matrix_exports_before_compare() {
    let data: Dataset = serde_json::from_str(include_str!("cases.json")).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-openai-prepared-count-")
        .tempdir()
        .unwrap()
        .keep();
    let mut export = std::fs::File::create(dir.join("actual.jsonl")).unwrap();
    for row in &data.rows {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut request: ModelRequest = serde_json::from_value(data.request.clone()).unwrap();
        if let Some(extension) = &row.extension {
            request.extensions = json!({"fixture.extra": extension.value});
        }
        if row.mode == "image" {
            request.messages[0]
                .content
                .push(kolyan_model::ContentBlock::Image {
                    source: kolyan_model::ImageSource::Url {
                        url: "https://example.invalid/image".into(),
                    },
                });
        }
        if row.mode == "opaque" {
            request.messages.push(kolyan_model::Message {
                role: kolyan_model::MessageRole::Assistant,
                content: vec![kolyan_model::ContentBlock::Reasoning {
                    text: "opaque".into(),
                    opaque: Some(json!({"type":"reasoning","id":"opaque","summary":[],"vendor_extra":{"unknown":true}})),
                }],
            });
        }
        if row.mode == "complex" {
            request.reasoning = Some(kolyan_model::ReasoningConfig {
                effort: Some("low".into()),
                budget_tokens: None,
            });
            request.output_format = Some(kolyan_model::OutputFormat {
                name: "answer".into(),
                schema: json!({"type":"object"}),
                strict: true,
            });
            request.messages.push(serde_json::from_value(json!({"role":"assistant","content":[
                {"type":"tool_call","call":{"id":"call","name":"read","arguments":{"path":"safe/a"}}}]})).unwrap());
            request.messages.push(serde_json::from_value(json!({"role":"user","content":[
                {"type":"tool_result","result":{"call_id":"call","content":"结果🦀\\u0000","is_error":false}}]})).unwrap());
        }
        if row.mode == "cache" {
            request.prompt_cache = Some(
                serde_json::from_value(
                    json!({"key":"cache-key","retention":null,"breakpoints":[]}),
                )
                .unwrap(),
            );
        }
        let mut host = provider(&base);
        if row.extension.is_some() {
            host = host.with_parameter_table(table(row)).unwrap();
        }
        let identity = host.prepare_wire(&request).unwrap().identity().clone();
        let profile_identity = match row.mode.as_str() {
            "model" => MappingIdentity::new(
                identity.endpoint_id().into(),
                identity.protocol(),
                kolyan_model::ModelRef::new("fixture", "wrong"),
                identity.mapping_revision().into(),
                identity.coverage_revision().into(),
            )
            .unwrap(),
            "revision" => MappingIdentity::new(
                identity.endpoint_id().into(),
                identity.protocol(),
                identity.model().clone(),
                "wrong".into(),
                identity.coverage_revision().into(),
            )
            .unwrap(),
            _ => identity.clone(),
        };
        if row.mode != "unsupported" {
            host = host.with_count_profile(
                CountProfile::registered(profile_identity, "fixture-counter-v1".into()).unwrap(),
            );
        }
        let prepared = host.prepare_wire(&request).unwrap();
        let mut counter = host.clone();
        match row.mode.as_str() {
            "foreign" => {
                counter = provider(&base).with_count_profile(
                    CountProfile::registered(identity.clone(), "fixture-counter-v1".into())
                        .unwrap(),
                )
            }
            "endpoint" => {
                counter = provider("http://127.0.0.1:1").with_count_profile(
                    CountProfile::registered(identity.clone(), "fixture-counter-v1".into())
                        .unwrap(),
                )
            }
            "profile_changed" => {
                counter = counter.with_count_profile(
                    CountProfile::registered(identity.clone(), "fixture-counter-v2".into())
                        .unwrap(),
                )
            }
            "planner_changed" => counter = counter.with_parameter_table(table(row)).unwrap(),
            _ => {}
        }
        let stream_body = data.stream_response.clone();
        let mut count_response =
            json!({"input_tokens":row.tokens.unwrap_or(42),"object":"response.input_tokens"})
                .to_string();
        let fault = row.fault;
        match fault {
            Some(CountFault::Http529) => {
                count_response =
                    json!({"error":{"type":"overloaded","message":"local fixture"}}).to_string()
            }
            Some(CountFault::Schema) => count_response = count_response.replace("42", "-1"),
            Some(CountFault::Oversize) => {
                count_response.extend(std::iter::repeat_n(' ', 65537 - count_response.len()))
            }
            _ => {}
        }
        let response_copy = count_response.clone();
        let server = tokio::spawn(async move {
            let mut observed = Vec::new();
            loop {
                let accepted =
                    tokio::time::timeout(Duration::from_millis(400), listener.accept()).await;
                let Ok(Ok((mut socket, _))) = accepted else {
                    break;
                };
                let mut bytes = Vec::new();
                let (end, length) = loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        panic!("incomplete local fixture request");
                    }
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..end]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                line.split_once(':')
                                    .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                    .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        if bytes.len() >= end + 4 + length {
                            break (end, length);
                        }
                    }
                };
                let head = String::from_utf8_lossy(&bytes[..end]);
                let path = head
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap();
                let value: Value =
                    serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                let count = path.contains("count") || path.ends_with("input_tokens");
                let (body, content_type) = if count {
                    (&count_response, "application/json")
                } else {
                    (&stream_body, "text/event-stream")
                };
                let status = if count && fault == Some(CountFault::Http529) {
                    529
                } else {
                    200
                };
                let header = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                if count && fault == Some(CountFault::BodyTimeout) {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
                let response_write = socket
                    .write_all(body.as_bytes())
                    .await
                    .map_err(|error| error.to_string());
                observed.push(json!({"path":path,"body":value,"status":status,"response_body":body,"response_write":response_write}));
            }
            observed
        });
        let generation = match host.stream(request.clone()).await {
            Ok(stream) => stream.collect::<Vec<_>>().await,
            Err(error) => vec![Err(error)],
        };
        let generation_errors = generation
            .iter()
            .filter_map(|result| result.as_ref().err().map(ToString::to_string))
            .collect::<Vec<_>>();
        let generation_events = generation
            .iter()
            .map(|event| match event {
                Ok(event) => json!({"event":event}),
                Err(error) => json!({"error":error.to_string()}),
            })
            .collect::<Vec<_>>();
        let result = counter
            .count_prepared(
                &prepared,
                if fault == Some(CountFault::BodyTimeout) {
                    Duration::from_millis(50)
                } else {
                    Duration::from_secs(2)
                },
            )
            .await;
        let (class, count, error) = match result {
            Ok(count) => ("success", Some(serde_json::to_value(count).unwrap()), None),
            Err(error) => (
                if error.kind == ProviderErrorKind::Unsupported {
                    "unsupported"
                } else {
                    "other"
                },
                None,
                Some(
                    json!({"message":error.to_string(),"kind":error.kind,"phase":error.phase,"status":error.status}),
                ),
            ),
        };
        let physical = server.await.unwrap();
        let target = counter.prepare_wire(&request).unwrap();
        let actual = json!({"case":row.id,"input":request,"generation_body":prepared.generation_body(),
            "profile":host.count_profile,"target_profile":counter.count_profile,"target_identity":target.identity(),
            "owner":{"same":std::sync::Arc::ptr_eq(&host.accounting_owner,&counter.accounting_owner)},
            "generation_wire_bytes":prepared.generation_wire_bytes(),"generation_wire_digest":prepared.generation_wire_digest(),
            "count_body":prepared.count_body(),"count_input_digest":prepared.count_input_digest(),"coverage":prepared.coverage(),
            "identity":prepared.identity(),"generation_errors":generation_errors,"generation_events":generation_events,"physical":physical,
            "response":response_copy,"class":class,"reported":count,"error":error});
        writeln!(export, "{}", actual).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    let all: Vec<serde_json::Value> = std::fs::read_to_string(dir.join("actual.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(all.len(), data.rows.len());
    eprintln!("prepared count evidence: {}", dir.display());
    for (row, actual) in data.rows.iter().zip(&all) {
        assert_eq!(actual["case"], row.id);
        assert_eq!(
            actual["class"],
            row.expected,
            "{} {}",
            row.id,
            dir.display()
        );
        assert_eq!(actual["generation_errors"], json!([]), "{}", row.id);
        let physical = actual["physical"].as_array().unwrap();
        assert_eq!(
            physical.len(),
            if row.expected == "unsupported" { 1 } else { 2 },
            "{}",
            row.id
        );
        assert_eq!(physical[0]["body"], actual["generation_body"], "{}", row.id);
        assert_eq!(physical[0]["path"], "/v1/responses", "{}", row.id);
        assert_eq!(
            actual["generation_wire_bytes"],
            serde_json::to_vec(&actual["generation_body"])
                .unwrap()
                .len(),
            "{}",
            row.id
        );
        let mut expected = serde_json::Map::new();
        for field in &data.count_fields {
            if let Some(value) = actual["generation_body"].get(field) {
                expected.insert(field.clone(), value.clone());
            }
        }
        assert_eq!(actual["count_body"], Value::Object(expected), "{}", row.id);
        if let Some(tokens) = row.tokens {
            assert_eq!(
                actual["reported"]["source"], "provider_reported",
                "{}",
                row.id
            );
            assert!(actual["reported"].get("assurance").is_none(), "{}", row.id);
            assert_eq!(actual["reported"]["input_tokens"], tokens, "{}", row.id);
            assert_eq!(
                actual["reported"]["count_input_digest"], actual["count_input_digest"],
                "{}",
                row.id
            );
            assert_eq!(physical[1]["body"], actual["count_body"], "{}", row.id);
            assert_eq!(
                physical[1]["path"], "/v1/responses/input_tokens",
                "{}",
                row.id
            );
        }
        if let Some(extension) = &row.extension {
            assert_eq!(
                actual["generation_body"][&extension.wire_name], extension.value,
                "{}",
                row.id
            );
        }
        if let Some(kind) = &row.error_kind {
            assert_eq!(actual["error"]["kind"], *kind, "{}", row.id);
        }
        if let Some(phase) = &row.error_phase {
            assert_eq!(actual["error"]["phase"], *phase, "{}", row.id);
        }
        if row.fault == Some(CountFault::Http529) {
            assert_eq!(actual["error"]["status"], 529, "{}", row.id);
        }
    }
}
