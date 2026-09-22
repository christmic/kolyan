//! Real-network Turn integration tests.
//!
//! These tests exercise the boundary above Step: Turn receives a real model
//! response, executes the restricted V0 shell tool, appends the ToolResult,
//! and asks the model for the next response.

#[path = "../common/mod.rs"]
mod common;

use common::{
    AnthropicProviderConfig, ModelMatrixEntry, ProviderConfig, assert_expectations, assert_usage,
    build_anthropic_provider, build_openai_provider, build_request, has_api_key,
    has_api_key_anthropic, load_config, load_fixture, require_api_key, require_api_key_anthropic,
};
use kolyan_core::{TurnConfig, TurnExecutor, TurnOutcome, TurnRequest, TurnResult};
use kolyan_model::ContentBlock;
use kolyan_tools::RestrictedShellTool;

#[tokio::test]
#[ignore = "real-network test: requires configured provider API keys"]
async fn turn_one_step_completes_across_configured_models() {
    let config = load_config();

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in &config.minimax_openai.model_matrix {
            run_openai_one_step(&provider, &config.minimax_openai, entry, "minimax").await;
        }
    }

    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in &config.minimax_anthropic.model_matrix {
            run_anthropic_one_step(&provider, &config.minimax_anthropic, entry, "minimax").await;
        }
    }

    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in &config.qwen_openai.model_matrix {
            run_openai_one_step(&provider, &config.qwen_openai, entry, "qwen").await;
        }
    }

    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in &config.qwen_anthropic.model_matrix {
            run_anthropic_one_step(&provider, &config.qwen_anthropic, entry, "qwen").await;
        }
    }
}

#[tokio::test]
#[ignore = "real-network test: requires configured provider API keys"]
async fn turn_ten_step_tool_loop_runs_across_all_configured_models() {
    let config = load_config();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in &config.minimax_openai.model_matrix {
            if selected_model(&entry.model) {
                run_openai_multi_step(&provider, entry, "minimax", root).await;
            }
        }
    }

    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in &config.minimax_anthropic.model_matrix {
            if selected_model(&entry.model) {
                run_anthropic_multi_step(&provider, entry, "minimax", root).await;
            }
        }
    }

    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in &config.qwen_openai.model_matrix {
            if selected_model(&entry.model) {
                run_openai_multi_step(&provider, entry, "qwen", root).await;
            }
        }
    }

    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in &config.qwen_anthropic.model_matrix {
            if selected_model(&entry.model) {
                run_anthropic_multi_step(&provider, entry, "qwen", root).await;
            }
        }
    }
}

fn selected_model(model: &str) -> bool {
    std::env::var("KOLYAN_TURN_MODEL_FILTER")
        .ok()
        .map(|filter| filter.split(',').any(|candidate| candidate.trim() == model))
        .unwrap_or(true)
}

async fn run_openai_one_step(
    provider: &kolyan_provider_openai::OpenAiProvider,
    _config: &ProviderConfig,
    entry: &ModelMatrixEntry,
    family: &str,
) {
    let fixture = load_fixture("text");
    let request = TurnRequest {
        turn_id: format!("turn-one-step-{family}-{}", entry.model),
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            format!("turn-one-step-{family}-{}", entry.model),
            entry.max_output_tokens,
        ),
        config: TurnConfig { max_steps: 1 },
    };
    let result = TurnExecutor::new(provider.clone())
        .execute(request)
        .await
        .unwrap_or_else(|error| panic!("[{family}/openai_compat/{}] {error}", entry.model));
    assert_one_step_final(
        &result,
        &fixture.expectations,
        &format!("{family}/openai_compat/{}", entry.model),
    );
    assert_usage(&result.steps[0].response.usage, &entry.capabilities, family);
}

async fn run_anthropic_one_step(
    provider: &kolyan_provider_anthropic::AnthropicProvider,
    _config: &AnthropicProviderConfig,
    entry: &ModelMatrixEntry,
    family: &str,
) {
    let fixture = load_fixture("text");
    let request = TurnRequest {
        turn_id: format!("turn-one-step-{family}-{}", entry.model),
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            format!("turn-one-step-{family}-{}", entry.model),
            entry.max_output_tokens,
        ),
        config: TurnConfig { max_steps: 1 },
    };
    let result = TurnExecutor::new(provider.clone())
        .execute(request)
        .await
        .unwrap_or_else(|error| panic!("[{family}/anthropic_compat/{}] {error}", entry.model));
    assert_one_step_final(
        &result,
        &fixture.expectations,
        &format!("{family}/anthropic_compat/{}", entry.model),
    );
    assert_usage(&result.steps[0].response.usage, &entry.capabilities, family);
}

fn assert_one_step_final(result: &TurnResult, expectations: &common::Expectations, label: &str) {
    assert_eq!(result.steps.len(), 1, "[{label}] expected exactly one Step");
    assert!(matches!(result.outcome, TurnOutcome::FinalAnswer { .. }));
    assert_expectations(&result.steps[0].response, expectations, label);
}

async fn run_openai_multi_step(
    provider: &kolyan_provider_openai::OpenAiProvider,
    entry: &ModelMatrixEntry,
    family: &str,
    root: &std::path::Path,
) {
    let fixture = load_fixture("turn_ten_step");
    let result = TurnExecutor::with_tools(provider.clone(), RestrictedShellTool::new(root))
        .execute(TurnRequest {
            turn_id: format!("turn-ten-step-{family}-{}", entry.model),
            model_request: build_request(
                family,
                &entry.model,
                &fixture,
                format!("turn-ten-step-{family}-{}", entry.model),
                None,
            ),
            config: TurnConfig {
                max_steps: fixture
                    .turn
                    .as_ref()
                    .and_then(|turn| turn.max_steps)
                    .unwrap_or(12),
            },
        })
        .await
        .unwrap_or_else(|error| {
            panic!("[{family}/openai_compat/{}/ten_step] {error}", entry.model)
        });
    assert_multi_step_result(
        result,
        fixture.turn.as_ref(),
        &format!("{family}/openai_compat/{}/ten_step", entry.model),
    );
}

async fn run_anthropic_multi_step(
    provider: &kolyan_provider_anthropic::AnthropicProvider,
    entry: &ModelMatrixEntry,
    family: &str,
    root: &std::path::Path,
) {
    let fixture = load_fixture("turn_ten_step");
    let result = TurnExecutor::with_tools(provider.clone(), RestrictedShellTool::new(root))
        .execute(TurnRequest {
            turn_id: format!("turn-ten-step-{family}-{}", entry.model),
            model_request: build_request(
                family,
                &entry.model,
                &fixture,
                format!("turn-ten-step-{family}-{}", entry.model),
                None,
            ),
            config: TurnConfig {
                max_steps: fixture
                    .turn
                    .as_ref()
                    .and_then(|turn| turn.max_steps)
                    .unwrap_or(12),
            },
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "[{family}/anthropic_compat/{}/ten_step] {error}",
                entry.model
            )
        });
    assert_multi_step_result(
        result,
        fixture.turn.as_ref(),
        &format!("{family}/anthropic_compat/{}/ten_step", entry.model),
    );
}

fn assert_multi_step_result(
    result: TurnResult,
    expectations: Option<&common::TurnExpectations>,
    label: &str,
) {
    let min_steps = expectations.and_then(|turn| turn.min_steps).unwrap_or(11);
    let min_tool_calls = expectations
        .and_then(|turn| turn.min_tool_calls)
        .unwrap_or(10);
    let tool_calls = result
        .steps
        .iter()
        .flat_map(|step| step.response.content.iter())
        .filter(|block| matches!(block, ContentBlock::ToolCall { .. }))
        .count();
    assert!(
        result.steps.len() >= min_steps,
        "[{label}] expected at least {min_steps} Steps, got {}",
        result.steps.len()
    );
    assert!(
        tool_calls >= min_tool_calls,
        "[{label}] expected at least {min_tool_calls} ToolCalls, got {tool_calls}"
    );
    assert!(
        result
            .steps
            .iter()
            .take(min_tool_calls)
            .all(|step| step.outcome == kolyan_core::StepOutcome::ToolCalls)
    );
    assert!(matches!(result.outcome, TurnOutcome::FinalAnswer { .. }));
}
