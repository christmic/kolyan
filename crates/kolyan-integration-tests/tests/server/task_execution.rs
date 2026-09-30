//! Data-driven task service acceptance; deterministic and live share the runner.

#[path = "../common/mod.rs"]
mod common;
#[path = "../common/matrix.rs"]
#[allow(dead_code)]
mod matrix;

mod task_execution_support;
use task_execution_support::*;

use std::{fs, sync::Arc};

use kolyan_model::{
    ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse, ProviderFuture,
};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!("../fixtures/task_acceptance.json")).unwrap()
}

#[derive(Clone)]
struct Shared(Arc<dyn ModelProvider>);
impl ModelProvider for Shared {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.0.stream(request)
    }
}

struct Scripted(Value);
impl ModelProvider for Scripted {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let observed = request.messages.iter().any(|message| {
            message.content.iter().any(|block| match block {
                kolyan_model::ContentBlock::ToolResult { result } => self.0["offline_calls"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|call| call["id"] == result.call_id),
                _ => false,
            })
        });
        let content = if observed {
            json!([{"type":"text","text":self.0["offline_answer"]}])
        } else {
            Value::Array(
                self.0["offline_calls"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|call| json!({"type":"tool_call","call":call}))
                    .collect(),
            )
        };
        let response = ModelResponse {
            id: request.request_id,
            model: request.model,
            content: serde_json::from_value(content).unwrap(),
            structured_output: None,
            stop_reason: if observed {
                kolyan_model::StopReason::EndTurn
            } else {
                kolyan_model::StopReason::ToolUse
            },
            usage: kolyan_model::TokenUsage {
                input_tokens: Some(21),
                output_tokens: Some(5),
                ..Default::default()
            },
            metadata: json!({}),
        };
        Box::pin(async move {
            Ok(
                Box::pin(futures_util::stream::iter(vec![Ok(ModelEvent::Completed(
                    response,
                ))])) as ModelEventStream,
            )
        })
    }
}

#[tokio::test]
async fn deterministic_task_self_call_join_and_approval_rebuild() {
    let data = fixture();
    let root = tempfile::Builder::new()
        .prefix("kolyan-task-offline-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("Task evidence: {}", root.display());
    run_task(
        &root,
        &data,
        "fixture",
        "offline",
        |case| Shared(Arc::new(Scripted(case.clone()))),
        Some(1024),
    )
    .await;
}

#[tokio::test]
#[ignore = "actual task service across every configured Provider/protocol/model"]
async fn task_execution_live_matrix() {
    let data = fixture();
    let config = common::load_config();
    let mut rows = Vec::new();
    for (family, cfg) in [
        ("minimax", config.minimax_openai),
        ("qwen", config.qwen_openai),
    ] {
        for model in cfg.model_matrix.clone() {
            rows.push((family, "openai_responses", model, Some(cfg.clone()), None));
        }
    }
    for (family, cfg) in [
        ("minimax", config.minimax_anthropic),
        ("qwen", config.qwen_anthropic),
    ] {
        for model in cfg.model_matrix.clone() {
            rows.push((family, "anthropic_messages", model, None, Some(cfg.clone())));
        }
    }
    assert_eq!(rows.len() as u64, data["configured_combinations"]);
    let mut report = matrix::Matrix::new(rows.iter().map(|(family, protocol, model, _, _)| {
        format!(
            "{family}/{protocol}/{}/{}",
            model.model,
            data["group"].as_str().unwrap()
        )
    }));
    for (index, (family, protocol, model, openai, anthropic)) in rows.into_iter().enumerate() {
        let root = report.directory.join(index.to_string());
        fs::create_dir_all(&root).unwrap();
        report
            .run(index, async {
                let provider = if let Some(cfg) = openai {
                    let key = common::require_api_key(&cfg);
                    Shared(Arc::new(
                        common::build_openai_provider(&cfg, &key)
                            .with_parameter_table(
                                serde_json::from_value(common::parameter_table(
                                    family,
                                    protocol,
                                    &model.model,
                                ))
                                .unwrap(),
                            )
                            .unwrap(),
                    ))
                } else {
                    let cfg = anthropic.unwrap();
                    let key = common::require_api_key_anthropic(&cfg);
                    Shared(Arc::new(
                        common::build_anthropic_provider(&cfg, &key)
                            .with_parameter_table(
                                serde_json::from_value(common::parameter_table(
                                    family,
                                    protocol,
                                    &model.model,
                                ))
                                .unwrap(),
                            )
                            .unwrap(),
                    ))
                };
                run_task(
                    &root,
                    &data,
                    family,
                    &model.model,
                    |_| provider.clone(),
                    model.max_output_tokens.or(Some(80960)),
                )
                .await;
            })
            .await;
    }
    assert!(
        report.complete(),
        "task matrix failed: {}",
        report.directory.display()
    );
}
