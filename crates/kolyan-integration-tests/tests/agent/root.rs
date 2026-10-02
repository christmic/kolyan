//! Agent-root acceptance. Scripted model output and real network output are
//! separate modes; both use the same cases and actual isolated OS tool workers.
#![cfg(target_os = "macos")]

mod data;
mod delegation;
mod diagnostics;
mod evidence;
mod file_byte_fidelity;
mod harness;
mod minimax_live;
mod native_effects;
mod native_running;
mod opening_retry;
mod providers;
mod restart;
mod root_goals;
mod self_iteration;
mod skills;
mod tools;
mod usage;

#[path = "../common/mod.rs"]
mod common;
#[path = "../common/matrix.rs"]
#[allow(dead_code)] // Shared matrix supports retry and sampling gates not used by this target.
mod matrix;

use std::sync::Arc;

use kolyan_model::{ModelProvider, ModelRef};

#[tokio::test]
async fn named_and_inline_approval_restarts_restore_exact_roots_and_session_history() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let dataset = data::dataset();
    let mut report = matrix::Matrix::new(
        dataset
            .selectors
            .iter()
            .map(|selector| format!("agent/root/restart/offline/{selector}")),
    );
    for (index, selector) in dataset.selectors.iter().enumerate() {
        report
            .run(
                index,
                restart::run(
                    selector,
                    ModelRef::new("fixture", "scripted-agent-root"),
                    None,
                    &installation,
                ),
            )
            .await;
    }
    assert!(
        report.complete(),
        "Agent restart evidence: {}",
        report.directory.display()
    );
}

#[tokio::test]
async fn named_and_inline_roots_use_four_real_tools_and_two_independent_turns() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let dataset = data::dataset();
    assert_eq!(dataset.schema_version, 1);
    let mut report = matrix::Matrix::new(
        dataset
            .selectors
            .iter()
            .map(|selector| format!("agent/root/offline/{selector}")),
    );
    for (index, selector) in dataset.selectors.iter().enumerate() {
        report
            .run(
                index,
                harness::run(
                    dataset.clone(),
                    selector,
                    ModelRef::new("fixture", "scripted-agent-root"),
                    None,
                    &installation,
                ),
            )
            .await;
    }
    assert!(
        report.complete(),
        "Agent root offline evidence: {}",
        report.directory.display()
    );
}

#[test]
fn root_matrix_covers_every_configured_model_and_explicit_pending_scope() {
    let dataset = data::dataset();
    let combinations = deployments();
    assert_eq!(combinations.len(), dataset.configured_combinations);
    let pending: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/agent/pending.json")).unwrap();
    assert!(
        pending["cases"]
            .as_array()
            .unwrap()
            .iter()
            .all(|case| case["status"] == "not_run")
    );
    // Planning these cases is not evidence that restart or delegation executed.
    assert_eq!(dataset.selectors, ["named", "inline"]);
}

#[tokio::test]
#[ignore = "actual Agent roots across all configured Providers; requires every configured credential"]
async fn actual_model_root_matrix() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let dataset = data::dataset();
    let combinations = deployments();
    let labels = combinations
        .iter()
        .flat_map(|deployment| {
            dataset
                .selectors
                .iter()
                .map(move |selector| {
                    format!(
                        "agent/root/live/{}/{}/{selector}",
                        deployment.family, deployment.surface
                    )
                })
                .zip(std::iter::repeat(&deployment.model))
                .map(|(label, model)| format!("{label}/{model}"))
        })
        .collect::<Vec<_>>();
    let mut report = matrix::Matrix::new(labels);
    assert_eq!(combinations.len(), dataset.configured_combinations);
    let mut index = 0;
    for deployment in combinations {
        for selector in &dataset.selectors {
            let dataset = dataset.clone();
            report
                .run(index, async {
                    let provider = deployment.build();
                    harness::run(
                        dataset,
                        selector,
                        ModelRef::new(deployment.family, &deployment.model),
                        Some(provider),
                        &installation,
                    )
                    .await;
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "Agent live evidence: {}",
        report.directory.display()
    );
}

struct Deployment {
    family: &'static str,
    surface: &'static str,
    model: String,
    provider: Provider,
}

#[tokio::test]
#[ignore = "actual Agent approval restarts across all configured Providers; requires every configured credential"]
async fn actual_model_root_approval_restart_matrix() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let dataset = data::dataset();
    let combinations = deployments();
    assert_eq!(combinations.len(), dataset.configured_combinations);
    let labels = combinations
        .iter()
        .flat_map(|deployment| {
            dataset.selectors.iter().map(move |selector| {
                format!(
                    "agent/root/restart/live/{}/{}/{}/{selector}",
                    deployment.family, deployment.surface, deployment.model
                )
            })
        })
        .collect::<Vec<_>>();
    let mut report = matrix::Matrix::new(labels);
    let mut index = 0;
    for deployment in combinations {
        for selector in &dataset.selectors {
            report
                .run(index, async {
                    restart::run(
                        selector,
                        ModelRef::new(deployment.family, &deployment.model),
                        Some(deployment.build()),
                        &installation,
                    )
                    .await;
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "Agent approval live evidence: {}",
        report.directory.display()
    );
}

enum Provider {
    OpenAi(common::ProviderConfig),
    Anthropic(common::AnthropicProviderConfig),
}

impl Deployment {
    fn build(&self) -> Arc<dyn ModelProvider> {
        match &self.provider {
            Provider::OpenAi(config) => Arc::new(common::build_openai_provider(
                config,
                &common::require_api_key(config),
            )),
            Provider::Anthropic(config) => Arc::new(common::build_anthropic_provider(
                config,
                &common::require_api_key_anthropic(config),
            )),
        }
    }
}

fn deployments() -> Vec<Deployment> {
    let config = common::load_config();
    let mut combinations = vec![];
    for (family, config) in [
        ("minimax", config.minimax_openai),
        ("qwen", config.qwen_openai),
    ] {
        for entry in &config.model_matrix {
            combinations.push(Deployment {
                family,
                surface: "openai_compat",
                model: entry.model.clone(),
                provider: Provider::OpenAi(config.clone()),
            });
        }
    }
    for (family, config) in [
        ("minimax", config.minimax_anthropic),
        ("qwen", config.qwen_anthropic),
    ] {
        for entry in &config.model_matrix {
            combinations.push(Deployment {
                family,
                surface: "anthropic_compat",
                model: entry.model.clone(),
                provider: Provider::Anthropic(config.clone()),
            });
        }
    }
    combinations
}
