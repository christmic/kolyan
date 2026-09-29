//! Parameter-policy scenarios use real adapters, with wire assertions on loopback.

#[path = "../common/mod.rs"]
mod common;
#[path = "../common/matrix.rs"]
#[allow(dead_code)]
mod matrix;
#[path = "request_planning/wire.rs"]
mod wire;

use std::{fs, io::Write, net::TcpListener, path::Path, time::Duration};

use futures_util::StreamExt;
use serde_json::{Value, json};

use common::*;
use kolyan_model::{
    ModelEvent, ModelProvider, ModelRequest, ModelResponse, ParameterTable, ProviderError,
    ProviderFuture,
};
use kolyan_protocol_anthropic::{AnthropicClient, AnthropicConfig};
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig};

#[derive(Clone)]
enum Provider {
    OpenAi(kolyan_provider_openai::OpenAiProvider),
    Anthropic(kolyan_provider_anthropic::AnthropicProvider),
}

impl Provider {
    fn configure(self, table: ParameterTable) -> Result<Self, ProviderError> {
        match self {
            Self::OpenAi(p) => p.with_parameter_table(table).map(Self::OpenAi),
            Self::Anthropic(p) => p.with_parameter_table(table).map(Self::Anthropic),
        }
    }
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        match self {
            Self::OpenAi(p) => p.stream(request),
            Self::Anthropic(p) => p.stream(request),
        }
    }
}

fn dataset() -> Value {
    serde_json::from_str(include_str!("../fixtures/request_planning.json")).unwrap()
}

#[test]
fn verified_model_profiles_select_exact_tool_choice_rules() {
    for protocol in ["openai_responses", "anthropic_messages"] {
        for model in [
            "qwen3.8-max",
            "qwen3.8-flash",
            "qwen3.7-plus",
            "qwen3.7-max",
            "deepseek-v4.1-flash",
            "deepseek-v4-pro",
            "deepseek-v4-flash-0731",
            "glm-5.3",
            "glm-5.2",
        ] {
            let profile = common::parameter_profile("qwen", protocol, model);
            let value = common::parameter_table("qwen", protocol, model);
            let expected = if matches!(model, "qwen3.8-max" | "qwen3.8-flash" | "qwen3.7-plus") {
                "thinking_auto_only"
            } else if protocol == "openai_responses" {
                "required_single_tool"
            } else {
                "default"
            };
            assert_eq!(profile, expected);
            let table: ParameterTable = serde_json::from_value(value).unwrap();
            let planner = kolyan_model::RequestPlanner::new(table, protocol, &[]).unwrap();
            let mut request = build_request(
                "qwen",
                model,
                &load_fixture("tool_call"),
                "profile".into(),
                Some(4096),
            );
            request.tool_choice = kolyan_model::ToolChoice::Required;
            let plan = planner.plan(&request).unwrap();
            let expected_choice = if expected == "thinking_auto_only" {
                kolyan_model::ToolChoice::Auto
            } else {
                kolyan_model::ToolChoice::Required
            };
            assert_eq!(plan.request.tool_choice, expected_choice);
            if expected == "required_single_tool" {
                let mut second = request.tools[0].clone();
                second.name.push_str("_second");
                request.tools.push(second);
                assert_eq!(
                    planner.plan(&request).unwrap().request.tool_choice,
                    kolyan_model::ToolChoice::Auto
                );
            }
        }
    }
}

fn table(family: &str, protocol: &str, model: &str, case: &Value) -> ParameterTable {
    let mut value = common::parameter_table(family, protocol, model);
    if let Some(features) = case.get("features") {
        value["features"] = features.clone();
    }
    if let Some(overrides) = case["rules"].as_object() {
        value["models"][model]["parameters"]
            .as_object_mut()
            .unwrap()
            .extend(overrides.clone());
    }
    serde_json::from_value(value).unwrap()
}

fn patch(request: ModelRequest, case: &Value) -> ModelRequest {
    let mut value = serde_json::to_value(request).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .extend(case["request"].as_object().unwrap().clone());
    serde_json::from_value(value).unwrap()
}

fn local_provider(protocol: &str, url: String) -> Provider {
    if protocol == "openai_responses" {
        Provider::OpenAi(kolyan_provider_openai::OpenAiProvider::new(
            OpenAiClient::new(OpenAiConfig {
                base_url: url,
                api_key: "fixture".into(),
                timeout: Duration::from_secs(2),
                transport_retries: 0,
                diagnostics: false,
            })
            .unwrap(),
        ))
    } else {
        Provider::Anthropic(kolyan_provider_anthropic::AnthropicProvider::new(
            AnthropicClient::new(AnthropicConfig {
                base_url: url,
                api_key: "fixture".into(),
                version: "2023-06-01".into(),
                timeout: Duration::from_secs(2),
                transport_retries: 0,
                diagnostics: false,
            })
            .unwrap(),
        ))
    }
}

async fn collect(
    provider: &Provider,
    request: ModelRequest,
    directory: &Path,
    case: &Value,
) -> Result<ModelResponse, ProviderError> {
    fs::write(
        directory.join("request.json"),
        serde_json::to_vec_pretty(&request).unwrap(),
    )
    .unwrap();
    let mut stream = provider.stream(request).await?;
    let mut file = fs::File::create(directory.join("events.jsonl")).unwrap();
    let mut decisions = Value::Null;
    let mut completed = None;
    while let Some(event) = stream.next().await {
        writeln!(file, "{}", json!({"event_debug":format!("{event:?}")})).unwrap();
        file.flush().unwrap();
        match event? {
            ModelEvent::Provider(metadata) => {
                if let Some(raw) = metadata.raw
                    && raw["kind"] == "request_planning"
                {
                    decisions = raw["decisions"].clone();
                }
            }
            ModelEvent::Completed(response) => {
                assert!(completed.is_none());
                completed = Some(response);
            }
            _ => {
                assert!(completed.is_none());
            }
        }
    }
    if let Some(expected) = case["decisions"].as_object() {
        for (key, action) in expected {
            assert!(
                decisions
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|d| d["parameter"] == *key && d["action"] == *action),
                "missing decision {key}: {decisions}"
            );
        }
    }
    fs::write(
        directory.join("decisions.json"),
        serde_json::to_vec_pretty(&decisions).unwrap(),
    )
    .unwrap();
    Ok(completed.expect("provider must complete exactly once"))
}

#[tokio::test]
async fn data_driven_parameter_policy_matches_actual_http_requests() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-request-planning-")
        .tempdir()
        .unwrap()
        .keep();
    for protocol in ["openai_responses", "anthropic_messages"] {
        for case in dataset()["offline"].as_array().unwrap() {
            if case.get("protocol").is_some_and(|p| p != protocol) {
                continue;
            }
            let directory = root.join(format!("{protocol}-{}", case["name"].as_str().unwrap()));
            fs::create_dir(&directory).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let request = patch(
                build_request(
                    "fixture",
                    "fixture",
                    &load_fixture("text"),
                    "planning".into(),
                    None,
                ),
                case,
            );
            let provider = local_provider(protocol, url)
                .configure(table("fixture", protocol, "fixture", case));
            if let Some(kind) = case["error"].as_str() {
                let error = match provider {
                    Err(error) => error,
                    Ok(provider) => collect(&provider, request, &directory, case)
                        .await
                        .unwrap_err(),
                };
                assert_eq!(
                    serde_json::to_value(error.kind).unwrap(),
                    kind,
                    "{protocol}/{}: {error}",
                    case["name"]
                );
                listener.set_nonblocking(true).unwrap();
                assert!(
                    matches!(listener.accept(),Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
                    "validation must precede network I/O"
                );
                fs::write(directory.join("error.txt"), error.to_string()).unwrap();
                continue;
            }
            let wire_path = directory.join("wire-request.json");
            let server = wire::serve(listener, protocol, wire_path.clone());
            collect(&provider.unwrap(), request, &directory, case)
                .await
                .unwrap();
            server.join().unwrap();
            let body: Value = serde_json::from_slice(&fs::read(wire_path).unwrap()).unwrap();
            for group in ["all", protocol] {
                if let Some(present) = case["present"][group].as_object() {
                    for (pointer, expected) in present {
                        assert_eq!(
                            body.pointer(pointer),
                            Some(expected),
                            "{} {pointer}",
                            directory.display()
                        );
                    }
                }
            }
            if let Some(absent) = case["absent"].as_array() {
                for pointer in absent {
                    assert!(
                        body.pointer(pointer.as_str().unwrap()).is_none(),
                        "unexpected field {pointer}: {body}"
                    );
                }
            }
        }
    }
    println!("parameter policy wire evidence: {}", root.display());
}

#[tokio::test]
#[ignore = "real models; parameter planning over every configured provider/protocol/model"]
async fn parameter_policy_live_matrix() {
    let config = load_config();
    let cases = dataset()["live"].as_array().unwrap().clone();
    let mut models = vec![];
    for (family, cfg) in [
        ("minimax", config.minimax_openai),
        ("qwen", config.qwen_openai),
    ] {
        let provider = has_api_key(&cfg)
            .then(|| Provider::OpenAi(build_openai_provider(&cfg, &require_api_key(&cfg))));
        for entry in cfg.model_matrix {
            models.push((family, "openai_responses", entry, provider.clone()));
        }
    }
    for (family, cfg) in [
        ("minimax", config.minimax_anthropic),
        ("qwen", config.qwen_anthropic),
    ] {
        let provider = has_api_key_anthropic(&cfg).then(|| {
            Provider::Anthropic(build_anthropic_provider(
                &cfg,
                &require_api_key_anthropic(&cfg),
            ))
        });
        for entry in cfg.model_matrix {
            models.push((family, "anthropic_messages", entry, provider.clone()));
        }
    }
    let labels = models.iter().flat_map(|(family, protocol, entry, _)| {
        cases.iter().map(move |case| {
            format!(
                "{family}/{protocol}/{}/{}",
                entry.model,
                case["name"].as_str().unwrap()
            )
        })
    });
    let mut matrix = matrix::Matrix::new(labels);
    let mut index = 0;
    for (family, protocol, entry, provider) in models {
        for case in &cases {
            let Some(provider) = provider.clone() else {
                matrix.record(index, matrix::Status::NotRun, "missing provider credential");
                index += 1;
                continue;
            };
            let directory = matrix.directory.join(index.to_string());
            fs::create_dir(&directory).unwrap();
            matrix
                .run(index, async {
                    let fixture = load_fixture(case["fixture"].as_str().unwrap());
                    let request = patch(
                        build_request(
                            family,
                            &entry.model,
                            &fixture,
                            "planning-live".into(),
                            entry.max_output_tokens,
                        ),
                        case,
                    );
                    let table = table(family, protocol, &entry.model, case);
                    fs::write(
                        directory.join("parameter-table.json"),
                        serde_json::to_vec_pretty(&table).unwrap(),
                    )
                    .unwrap();
                    let provider = provider.configure(table).unwrap();
                    let mut expectations = case.clone();
                    if let Some(by_profile) = case.get("decisions_by_profile") {
                        expectations["decisions"] = by_profile
                            [common::parameter_profile(family, protocol, &entry.model)]
                        .clone();
                        assert!(
                            expectations["decisions"].is_object(),
                            "missing profile expectations"
                        );
                    }
                    let response = collect(&provider, request, &directory, &expectations)
                        .await
                        .unwrap();
                    assert_expectations(
                        &response,
                        &fixture.expectations,
                        "parameter planning live",
                    );
                    assert_usage(
                        &response.usage,
                        &entry.capabilities,
                        "parameter planning live",
                    );
                })
                .await;
            index += 1;
        }
    }
    assert!(
        matrix.complete(),
        "parameter policy matrix failed: {}",
        matrix.directory.display()
    );
}
