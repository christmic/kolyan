//! Real-network Step integration tests.
//!
//! Provider compatibility tests live under `tests/provider/`; these tests
//! deliberately exercise the next boundary up: StepExecutor receives a
//! complete ModelRequest and exposes both a StepEventStream and a StepResult.

#[path = "../common/mod.rs"]
mod common;

use common::{
    AnthropicProviderConfig, ProviderConfig, assert_expectations, assert_usage,
    build_anthropic_provider, build_openai_provider, build_request, has_api_key,
    has_api_key_anthropic, load_config, load_fixture, require_api_key, require_api_key_anthropic,
};
use futures_util::StreamExt;
use kolyan_core::{StepEvent, StepExecutor, StepRequest, StepResult};
use kolyan_model::{ModelResponse, StopReason};
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

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

#[tokio::test]
#[ignore = "real-network test: requires KOLYAN_MINIMAX_API_KEY"]
async fn step_stream_snapshot_minimax_tool_call_over_both_protocols() {
    let config = load_config();

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        run_openai_stream_snapshot(
            &provider,
            &config.minimax_openai,
            "minimax/openai_compat/tool_call",
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/expected/step/minimax/openai_compat/tool_call.jsonl"),
        )
        .await;
    } else {
        eprintln!(
            "[SKIP minimax/openai_compat/tool_call] env var {} not set",
            config.minimax_openai.api_key_env
        );
    }

    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        run_anthropic_stream_snapshot(
            &provider,
            &config.minimax_anthropic,
            "minimax/anthropic_compat/tool_call",
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/expected/step/minimax/anthropic_compat/tool_call.jsonl"),
        )
        .await;
    } else {
        eprintln!(
            "[SKIP minimax/anthropic_compat/tool_call] env var {} not set",
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

async fn run_openai_stream_snapshot(
    provider: &kolyan_provider_openai::OpenAiProvider,
    config: &ProviderConfig,
    label: &str,
    expected_path: impl AsRef<Path>,
) {
    let fixture = load_fixture("tool_call");
    let executor = StepExecutor::new(provider.clone());
    let request = StepRequest {
        step_id: "step-stream-openai-tool-call".into(),
        model_request: build_request(
            "minimax",
            &config.model,
            &fixture,
            "step-stream-openai-tool-call".into(),
            None,
        ),
    };
    let (snapshot, result) =
        collect_stream_snapshot(&executor, request, &fixture.expectations, label).await;
    assert_snapshot_file(label, &snapshot, expected_path.as_ref());
    assert_usage(&result.response.usage, &config.capabilities, label);
}

async fn run_anthropic_stream_snapshot(
    provider: &kolyan_provider_anthropic::AnthropicProvider,
    config: &AnthropicProviderConfig,
    label: &str,
    expected_path: impl AsRef<Path>,
) {
    let fixture = load_fixture("tool_call");
    let executor = StepExecutor::new(provider.clone());
    let request = StepRequest {
        step_id: "step-stream-anthropic-tool-call".into(),
        model_request: build_request(
            "minimax",
            &config.model,
            &fixture,
            "step-stream-anthropic-tool-call".into(),
            None,
        ),
    };
    let (snapshot, result) =
        collect_stream_snapshot(&executor, request, &fixture.expectations, label).await;
    assert_snapshot_file(label, &snapshot, expected_path.as_ref());
    assert_usage(&result.response.usage, &config.capabilities, label);
}

async fn collect_stream_snapshot<P: kolyan_model::ModelProvider>(
    executor: &StepExecutor<P>,
    request: StepRequest,
    expectations: &common::Expectations,
    label: &str,
) -> (String, StepResult) {
    let mut stream = executor
        .execute_stream(request)
        .await
        .unwrap_or_else(|error| panic!("[{label}] step stream failed to open: {error}"));
    let mut records = Vec::new();
    let mut final_result = None;

    while let Some(event) = stream.next().await {
        let event = event.unwrap_or_else(|error| panic!("[{label}] step stream failed: {error}"));
        if let StepEvent::Completed(result) = &event {
            assert_expectations(&result.response, expectations, label);
            final_result = Some(result.clone());
        }
        if let Some(record) = stable_stream_record(&event)
            && records.last() != Some(&record)
        {
            records.push(record);
        }
    }

    let result = final_result.expect("step stream must emit a Completed event");
    canonicalize_tool_call_order(&mut records);
    let snapshot = records
        .into_iter()
        .map(|record| serde_json::to_string(&record).expect("snapshot record must serialize"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    (snapshot, result)
}

fn canonicalize_tool_call_order(records: &mut Vec<Value>) {
    let Some(arguments_index) = records.iter().position(is_tool_call_arguments) else {
        return;
    };
    let Some(completed_index) = records.iter().position(is_tool_call_completed) else {
        return;
    };
    if arguments_index > completed_index {
        let arguments = records.remove(arguments_index);
        records.insert(completed_index, arguments);
    }
}

fn is_tool_call_arguments(record: &Value) -> bool {
    record.get("event").and_then(Value::as_str) == Some("tool_call_arguments_delta")
}

fn is_tool_call_completed(record: &Value) -> bool {
    record.get("event").and_then(Value::as_str) == Some("tool_call_completed")
}

fn stable_stream_record(event: &StepEvent) -> Option<Value> {
    let record = match event {
        StepEvent::Started { .. } => json!({"event": "started"}),
        StepEvent::TextDelta { .. } => json!({"event": "text_delta"}),
        StepEvent::ReasoningDelta { .. } => json!({"event": "reasoning_delta"}),
        StepEvent::ToolCallStarted { name, .. } => {
            json!({"event": "tool_call_started", "name": name})
        }
        StepEvent::ToolCallArgumentsDelta { .. } => {
            json!({"event": "tool_call_arguments_delta"})
        }
        StepEvent::ToolCallCompleted { call, .. } => {
            json!({"event": "tool_call_completed", "name": call.name})
        }
        StepEvent::Usage { .. } => json!({"event": "usage"}),
        StepEvent::Provider { .. } => return None,
        StepEvent::Completed(result) => {
            json!({"event": "completed", "stop_reason": stop_reason_name(&result.response)})
        }
    };
    Some(record)
}

fn stop_reason_name(response: &ModelResponse) -> &'static str {
    match response.stop_reason {
        StopReason::EndTurn => "end_turn",
        StopReason::ToolUse => "tool_use",
        StopReason::MaxOutputTokens => "max_output_tokens",
        StopReason::Refusal => "refusal",
        StopReason::Other(_) => "other",
    }
}

fn assert_snapshot_file(label: &str, actual: &str, expected_path: &Path) {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after unix epoch")
        .as_nanos();
    let temp_path = std::env::temp_dir().join(format!(
        "kolyan-step-stream-{}-{unique}.jsonl",
        std::process::id()
    ));
    fs::write(&temp_path, actual).expect("stream snapshot must write to temp file");
    let expected = fs::read_to_string(expected_path).unwrap_or_else(|error| {
        panic!(
            "[{label}] expected stream snapshot missing at {}: {error}",
            expected_path.display()
        )
    });
    assert_eq!(
        actual,
        expected,
        "[{label}] stream snapshot mismatch; actual output is at {}",
        temp_path.display()
    );
    fs::remove_file(temp_path).expect("successful stream snapshot should clean up temp file");
}
