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
const MATRIX_FIXTURES: &[&str] = &["text", "tool_call", "structured_output"];

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

#[tokio::test]
#[ignore = "real-network test: requires configured provider API keys"]
async fn step_stream_snapshot_all_configured_models_over_both_protocols() {
    let config = load_config();

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in &config.minimax_openai.model_matrix {
            for fixture_name in MATRIX_FIXTURES {
                run_openai_stream_contract(
                    &provider,
                    entry,
                    "minimax",
                    fixture_name,
                    &format!("minimax/openai_compat/{}/{}", entry.model, fixture_name),
                    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                        "tests/expected/step/matrix/openai_compat/{fixture_name}.jsonl"
                    )),
                )
                .await;
            }
        }
    } else {
        eprintln!(
            "[SKIP minimax/openai_compat/matrix] env var {} not set",
            config.minimax_openai.api_key_env
        );
    }

    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in &config.minimax_anthropic.model_matrix {
            for fixture_name in MATRIX_FIXTURES {
                run_anthropic_stream_contract(
                    &provider,
                    entry,
                    "minimax",
                    fixture_name,
                    &format!("minimax/anthropic_compat/{}/{}", entry.model, fixture_name),
                    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                        "tests/expected/step/matrix/anthropic_compat/{fixture_name}.jsonl"
                    )),
                )
                .await;
            }
        }
    } else {
        eprintln!(
            "[SKIP minimax/anthropic_compat/matrix] env var {} not set",
            config.minimax_anthropic.api_key_env
        );
    }

    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in &config.qwen_openai.model_matrix {
            for fixture_name in MATRIX_FIXTURES {
                run_openai_stream_contract(
                    &provider,
                    entry,
                    "qwen",
                    fixture_name,
                    &format!("qwen/openai_compat/{}/{}", entry.model, fixture_name),
                    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                        "tests/expected/step/matrix/openai_compat/{fixture_name}.jsonl"
                    )),
                )
                .await;
            }
        }
    } else {
        eprintln!(
            "[SKIP qwen/openai_compat/matrix] env var {} not set",
            config.qwen_openai.api_key_env
        );
    }

    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in &config.qwen_anthropic.model_matrix {
            for fixture_name in MATRIX_FIXTURES {
                run_anthropic_stream_contract(
                    &provider,
                    entry,
                    "qwen",
                    fixture_name,
                    &format!("qwen/anthropic_compat/{}/{}", entry.model, fixture_name),
                    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                        "tests/expected/step/matrix/anthropic_compat/{fixture_name}.jsonl"
                    )),
                )
                .await;
            }
        }
    } else {
        eprintln!(
            "[SKIP qwen/anthropic_compat/matrix] env var {} not set",
            config.qwen_anthropic.api_key_env
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

async fn run_openai_stream_contract(
    provider: &kolyan_provider_openai::OpenAiProvider,
    entry: &common::ModelMatrixEntry,
    family: &str,
    fixture_name: &str,
    label: &str,
    expected_path: impl AsRef<Path>,
) {
    let fixture = load_fixture(fixture_name);
    let executor = StepExecutor::new(provider.clone());
    let request = StepRequest {
        step_id: format!("step-stream-{}-{}", family, entry.model),
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            format!("step-stream-{}-{}", family, entry.model),
            entry.max_output_tokens,
        ),
    };
    let (snapshot, result) =
        collect_stream_snapshot(&executor, request, &fixture.expectations, label).await;
    assert_snapshot_contract_file(label, &snapshot, expected_path.as_ref());
    assert_usage(&result.response.usage, &entry.capabilities, label);
}

async fn run_anthropic_stream_contract(
    provider: &kolyan_provider_anthropic::AnthropicProvider,
    entry: &common::ModelMatrixEntry,
    family: &str,
    fixture_name: &str,
    label: &str,
    expected_path: impl AsRef<Path>,
) {
    let fixture = load_fixture(fixture_name);
    let executor = StepExecutor::new(provider.clone());
    let request = StepRequest {
        step_id: format!("step-stream-{}-{}", family, entry.model),
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            format!("step-stream-{}-{}", family, entry.model),
            entry.max_output_tokens,
        ),
    };
    let (snapshot, result) =
        collect_stream_snapshot(&executor, request, &fixture.expectations, label).await;
    assert_snapshot_contract_file(label, &snapshot, expected_path.as_ref());
    assert_usage(&result.response.usage, &entry.capabilities, label);
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
        if let Some(record) = stable_stream_record(&event) {
            append_snapshot_record(&mut records, record);
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

fn append_snapshot_record(records: &mut Vec<Value>, record: Value) {
    let Some(kind) = record.get("event").and_then(Value::as_str) else {
        records.push(record);
        return;
    };
    let Some(previous) = records.last_mut() else {
        records.push(record);
        return;
    };
    if previous.get("event").and_then(Value::as_str) != Some(kind) {
        records.push(record);
        return;
    }
    let Some(previous_object) = previous.as_object_mut() else {
        records.push(record);
        return;
    };
    let Some(mut record_object) = record.as_object().cloned() else {
        records.push(record);
        return;
    };
    match kind {
        "text_delta" | "reasoning_delta" => {
            let previous_text = previous_object
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let next_text = record_object
                .remove("text")
                .and_then(|value| value.as_str().map(String::from))
                .unwrap_or_default();
            previous_object.insert(
                "text".into(),
                Value::String(format!("{previous_text}{next_text}")),
            );
        }
        "tool_call_arguments_delta" => {
            let previous_delta = previous_object
                .get("delta")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let next_delta = record_object
                .remove("delta")
                .and_then(|value| value.as_str().map(String::from))
                .unwrap_or_default();
            previous_object.insert(
                "delta".into(),
                Value::String(format!("{previous_delta}{next_delta}")),
            );
        }
        _ => records.push(record),
    }
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
        StepEvent::TextDelta { text, .. } => json!({"event": "text_delta", "text": text}),
        StepEvent::ReasoningDelta { text, .. } => {
            json!({"event": "reasoning_delta", "text": text})
        }
        StepEvent::ToolCallStarted { name, .. } => {
            json!({"event": "tool_call_started", "name": name})
        }
        StepEvent::ToolCallArgumentsDelta { delta, .. } => {
            json!({"event": "tool_call_arguments_delta", "delta": delta})
        }
        StepEvent::ToolCallCompleted { call, .. } => {
            json!({
                "event": "tool_call_completed",
                "name": call.name,
                "arguments": call.arguments
            })
        }
        StepEvent::Usage { usage, .. } => json!({"event": "usage", "usage": usage}),
        StepEvent::Provider { .. } => return None,
        StepEvent::Completed(result) => {
            json!({
                "event": "completed",
                "stop_reason": stop_reason_name(&result.response),
                "structured_output_present": result.response.structured_output.is_some(),
                "structured_output": result.response.structured_output
            })
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
    assert_snapshot_contract_file(label, actual, expected_path);
}

fn assert_snapshot_contract_file(label: &str, actual: &str, expected_path: &Path) {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after unix epoch")
        .as_nanos();
    let temp_path = std::env::temp_dir().join(format!(
        "kolyan-step-stream-matrix-{}-{unique}.jsonl",
        std::process::id()
    ));
    fs::write(&temp_path, actual).expect("stream snapshot must write to temp file");
    let actual_records = parse_snapshot_records(actual, label);
    let expected = fs::read_to_string(expected_path).unwrap_or_else(|error| {
        panic!(
            "[{label}] expected stream contract missing at {}: {error}",
            expected_path.display()
        )
    });
    let expected_records = parse_snapshot_records(&expected, label);
    let mut actual_index = 0;
    for expected_record in expected_records {
        if expected_record.get("optional") == Some(&Value::Bool(true)) {
            let mut optional_record = expected_record.clone();
            optional_record
                .as_object_mut()
                .expect("optional snapshot record must be an object")
                .remove("optional");
            if actual_records
                .iter()
                .any(|actual_record| record_contains(actual_record, &optional_record))
            {
                continue;
            }
            continue;
        }
        let Some(relative_index) = actual_records[actual_index..]
            .iter()
            .position(|actual_record| record_contains(actual_record, &expected_record))
        else {
            panic!(
                "[{label}] stream contract mismatch; expected {expected_record}, actual output is at {}",
                temp_path.display()
            );
        };
        actual_index += relative_index + 1;
    }
    fs::remove_file(temp_path).expect("successful stream contract should clean up temp file");
}

fn parse_snapshot_records(text: &str, label: &str) -> Vec<Value> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("[{label}] invalid JSONL snapshot record: {error}"))
        })
        .collect()
}

fn record_contains(actual: &Value, expected: &Value) -> bool {
    let (Some(actual), Some(expected)) = (actual.as_object(), expected.as_object()) else {
        return actual == expected;
    };
    expected
        .iter()
        .all(|(key, value)| actual.get(key) == Some(value))
}
