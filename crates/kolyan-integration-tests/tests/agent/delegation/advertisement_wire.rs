//! Production root preparation through real Provider/SDK serialization to localhost.
//! Synthetic responses prove no vendor behavior, model reasoning or tool effects.

mod capture;

use std::{fs, io::Write, os::unix::fs::DirBuilderExt, sync::Arc, time::Duration};

use futures_util::StreamExt;
use kolyan_agent::RootInputPreparationRequest;
use kolyan_model::{ModelEvent, ModelProvider, ModelRef};
use kolyan_protocol_anthropic::{AnthropicClient, AnthropicConfig};
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig};
use kolyan_server::ExecutionRef;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::super::{data, evidence::Evidence, harness, tools};
use super::{dataset, host::Host};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    schema_version: u32,
    expected_rows: usize,
    expected_max_children: usize,
    timeout_ms: u64,
    max_request_bytes: usize,
    max_output_tokens: u32,
    input: String,
    profiles: Vec<Profile>,
    protocols: Vec<Protocol>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    id: String,
    retain_environment: bool,
    retain_named: bool,
    allow_inline: bool,
    allow_self: bool,
    expected_tools: Vec<String>,
    expected_targets: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Protocol {
    id: String,
    path: String,
    schema_field: String,
    response: String,
}

#[tokio::test]
async fn selected_delegation_advertisements_match_both_actual_http_bodies() {
    let plan: Plan =
        serde_json::from_str(include_str!("../../fixtures/agent/advertisement_wire.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-advertisement-wire-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    println!("AGENT_ADVERTISEMENT_WIRE_TRACE={}", path.display());
    let mut output = fs::File::create(&path).unwrap();
    writeln!(output, "{}", json!({"event":"plan","profiles":plan.profiles.iter().map(|p|&p.id).collect::<Vec<_>>(),
        "protocols":plan.protocols.iter().map(|p|&p.id).collect::<Vec<_>>(),"expected_rows":plan.expected_rows,
        "response_source":"synthetic_localhost","vendor_calls":false,"tool_effects":false})).unwrap();
    output.sync_all().unwrap();
    let installation = tools::worker::WorkerRun::prepare().await;
    for profile in &plan.profiles {
        let case_root = root.join(&profile.id);
        fs::create_dir_all(case_root.join("workspace/safe")).unwrap();
        fs::create_dir(case_root.join("state")).unwrap();
        fs::DirBuilder::new()
            .mode(0o700)
            .create(case_root.join("staging"))
            .unwrap();
        let evidence = Arc::new(Evidence::new(&case_root.join("host.jsonl")));
        let prepared = match tools::initialize_worker(&case_root, &evidence, &installation) {
            Err(error) => Err(error),
            Ok(()) => {
                let case = dataset().cases.remove(0);
                let model = ModelRef::new("fixture", "advertisement-wire-model");
                let host = Host::open_plan(&case_root, &case, &model, None, evidence, None);
                let mut permissions = host.permissions.clone();
                if !profile.retain_environment {
                    permissions.tools.clear();
                }
                if !profile.retain_named {
                    permissions.delegation.named_targets.clear();
                }
                permissions.delegation.allow_inline = profile.allow_inline;
                permissions.delegation.allow_self = profile.allow_self;
                let mut source = data::dataset().turns.remove(0);
                source.input.clone_from(&plan.input);
                let request = harness::request(&source, &model, plan.max_output_tokens);
                host.runner
                    .prepare_root_input(RootInputPreparationRequest {
                        task_id: format!("wire-{}", profile.id),
                        invocation_id: "root".into(),
                        execution: ExecutionRef {
                            session_id: "logical-session".into(),
                            turn_id: "wire-turn".into(),
                            execution_id: "wire-execution".into(),
                        },
                        selector: host.selector(),
                        requested_permissions: permissions,
                        model_request: request,
                    })
                    .await
                    .map_err(|error| error.to_string())
            }
        };
        for protocol in &plan.protocols {
            let row = match &prepared {
                Ok(prepared) => observe(&plan, profile, protocol, &prepared.selected_input).await,
                Err(error) => {
                    json!({"event":"case","profile":profile.id,"protocol":protocol.id,"preparation_error":error})
                }
            };
            writeln!(output, "{row}").unwrap();
            output.sync_all().unwrap();
        }
    }
    let actual = fs::read_to_string(&path).unwrap();
    let rows = actual
        .lines()
        .skip(1)
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(plan.schema_version, 1);
    assert_eq!(rows.len(), plan.expected_rows);
    assert_eq!(
        plan.expected_rows,
        plan.profiles.len() * plan.protocols.len()
    );
    for (index, profile) in plan.profiles.iter().enumerate() {
        for (offset, protocol) in plan.protocols.iter().enumerate() {
            compare(
                &rows[index * plan.protocols.len() + offset],
                profile,
                protocol,
                plan.expected_max_children,
            );
        }
    }
}

async fn observe(
    plan: &Plan,
    profile: &Profile,
    protocol: &Protocol,
    request: &kolyan_model::ModelRequest,
) -> Value {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let timeout = Duration::from_millis(plan.timeout_ms);
    let server = tokio::spawn(capture::serve(
        listener,
        protocol.response.clone(),
        timeout,
        plan.max_request_bytes,
    ));
    let provider: Arc<dyn ModelProvider> = match protocol.id.as_str() {
        "openai_responses" => {
            let mut config = OpenAiConfig::new("localhost-not-a-secret");
            config.base_url = base;
            config.timeout = timeout;
            config.transport_retries = 0;
            Arc::new(kolyan_provider_openai::OpenAiProvider::new(
                OpenAiClient::new(config).unwrap(),
            ))
        }
        "anthropic_messages" => {
            let mut config = AnthropicConfig::new("localhost-not-a-secret");
            config.base_url = base;
            config.timeout = timeout;
            config.transport_retries = 0;
            Arc::new(kolyan_provider_anthropic::AnthropicProvider::new(
                AnthropicClient::new(config).unwrap(),
            ))
        }
        other => panic!("unknown fixture protocol {other}"),
    };
    let mut events = Vec::new();
    let sent = tokio::time::timeout(timeout,async {
        let mut stream = provider.stream(request.clone()).await.map_err(|error|error.to_string())?;
        while let Some(event) = stream.next().await {
            events.push(json!({"content":event.as_ref().ok(),"error":event.as_ref().err().map(ToString::to_string)}));
            event.map_err(|error|error.to_string())?;
        }
        Ok::<(),String>(())
    }).await;
    let send_error = match sent {
        Ok(result) => result.err(),
        Err(error) => Some(format!("provider deadline: {error}")),
    };
    let captured = server.await.unwrap();
    let parsed = capture::parsed(&captured);
    json!({"event":"case","profile":profile.id,"protocol":protocol.id,"selected_input":request,
        "capture":captured,"wire_body":parsed.as_ref().ok(),"parse_error":parsed.err(),"send_error":send_error,"events":events})
}

fn compare(row: &Value, profile: &Profile, protocol: &Protocol, expected_max_children: usize) {
    assert_eq!(row["profile"], profile.id, "{row}");
    assert_eq!(row["protocol"], protocol.id, "{row}");
    assert!(row["preparation_error"].is_null(), "{row}");
    assert!(row["capture"]["error"].is_null(), "{row}");
    assert!(row["send_error"].is_null(), "{row}");
    assert!(row["parse_error"].is_null(), "{row}");
    assert_eq!(
        row["capture"]["capture"]["request_line"],
        format!("POST {} HTTP/1.1", protocol.path)
    );
    let selected = &row["selected_input"];
    let tools = selected["tools"].as_array().unwrap();
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, profile.expected_tools);
    let wire = row["wire_body"]["tools"].as_array().unwrap();
    assert_eq!(wire.len(), tools.len());
    for (definition, actual) in tools.iter().zip(wire) {
        assert_eq!(definition["name"], actual["name"]);
        assert_eq!(definition["description"], actual["description"]);
        assert_eq!(definition["input_schema"], actual[&protocol.schema_field]);
        if definition["name"] == "agent.invoke" {
            let schema = &definition["input_schema"];
            assert_eq!(
                schema["properties"]["children"]["maxItems"],
                expected_max_children
            );
            let branches = schema["properties"]["children"]["items"]["oneOf"]
                .as_array()
                .unwrap();
            let kinds = branches
                .iter()
                .map(|branch| {
                    branch["properties"]["target"]["properties"]["kind"]["const"]
                        .as_str()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(kinds, profile.expected_targets);
            for branch in branches {
                let permissions = &branch["properties"]["permissions"]["properties"];
                if !profile.retain_environment {
                    assert_eq!(permissions["tools"]["items"], false);
                    assert_eq!(permissions["tools"]["maxItems"], 0);
                    assert_eq!(permissions["tools"]["examples"], json!([[]]));
                }
                if !profile.retain_named {
                    let delegation = &permissions["delegation"]["properties"];
                    assert_eq!(delegation["named_targets"]["items"], false);
                    assert_eq!(delegation["named_targets"]["examples"], json!([[]]));
                    assert_eq!(delegation["allow_inline"]["const"], false);
                    assert_eq!(delegation["allow_inline"]["type"], "boolean");
                }
            }
        }
    }
    assert!(
        row["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |event| serde_json::from_value::<ModelEvent>(event["content"].clone())
                    .is_ok_and(|event| matches!(event, ModelEvent::Completed(_)))
            ),
        "{row}"
    );
}
