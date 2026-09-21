//! Real-network Step integration tests.
//!
//! Provider compatibility tests live under `tests/provider/`; these tests
//! deliberately exercise the next boundary up: StepExecutor receives a
//! complete ModelRequest and returns a StepResult.

#[path = "../common/mod.rs"]
mod common;

use common::{
    AnthropicProviderConfig, ProviderConfig, assert_expectations, assert_usage,
    build_anthropic_provider, build_openai_provider, build_request, has_api_key,
    has_api_key_anthropic, load_config, load_fixture, require_api_key, require_api_key_anthropic,
};
use kolyan_core::{StepExecutor, StepRequest};

const FIXTURES: &[&str] = &["text", "tool_call", "structured_output"];

#[tokio::test]
#[ignore = "real-network test: requires KOLYAN_MINIMAX_API_KEY"]
async fn step_executes_minimax_over_both_protocols() {
    let config = load_config();

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for fixture_name in FIXTURES {
            run_openai_step(
                &provider,
                &config.minimax_openai,
                fixture_name,
                &format!("minimax/openai_compat/{fixture_name}"),
            )
            .await;
        }
    } else {
        eprintln!(
            "[SKIP minimax/openai_compat] env var {} not set",
            config.minimax_openai.api_key_env
        );
    }

    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for fixture_name in FIXTURES {
            run_anthropic_step(
                &provider,
                &config.minimax_anthropic,
                fixture_name,
                &format!("minimax/anthropic_compat/{fixture_name}"),
            )
            .await;
        }
    } else {
        eprintln!(
            "[SKIP minimax/anthropic_compat] env var {} not set",
            config.minimax_anthropic.api_key_env
        );
    }
}

async fn run_openai_step(
    provider: &kolyan_provider_openai::OpenAiProvider,
    config: &ProviderConfig,
    fixture_name: &str,
    label: &str,
) {
    let fixture = load_fixture(fixture_name);
    let executor = StepExecutor::new(provider.clone());
    let result = executor
        .execute(StepRequest {
            step_id: format!("step-{fixture_name}"),
            model_request: build_request(
                "minimax",
                &config.model,
                &fixture,
                format!("step-openai-{fixture_name}"),
                None,
            ),
        })
        .await
        .unwrap_or_else(|error| panic!("[{label}] step failed: {error}"));
    assert_expectations(&result.response, &fixture.expectations, label);
    assert_usage(&result.response.usage, &config.capabilities, label);
}

async fn run_anthropic_step(
    provider: &kolyan_provider_anthropic::AnthropicProvider,
    config: &AnthropicProviderConfig,
    fixture_name: &str,
    label: &str,
) {
    let fixture = load_fixture(fixture_name);
    let executor = StepExecutor::new(provider.clone());
    let result = executor
        .execute(StepRequest {
            step_id: format!("step-{fixture_name}"),
            model_request: build_request(
                "minimax",
                &config.model,
                &fixture,
                format!("step-anthropic-{fixture_name}"),
                None,
            ),
        })
        .await
        .unwrap_or_else(|error| panic!("[{label}] step failed: {error}"));
    assert_expectations(&result.response, &fixture.expectations, label);
    assert_usage(&result.response.usage, &config.capabilities, label);
}
