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
use futures_util::StreamExt;
use kolyan_core::{TurnConfig, TurnEvent, TurnExecutor, TurnOutcome, TurnRequest, TurnResult};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ProviderFuture,
    StopReason,
};
use kolyan_tools::RestrictedShellTool;
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
struct RecordingProvider<P> {
    inner: P,
    records: Arc<Mutex<Vec<Value>>>,
}

impl<P> RecordingProvider<P> {
    fn new(inner: P) -> (Self, Arc<Mutex<Vec<Value>>>) {
        let records = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner,
                records: Arc::clone(&records),
            },
            records,
        )
    }
}

impl<P: ModelProvider> ModelProvider for RecordingProvider<P> {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let step_id = request.request_id;
        let records = Arc::clone(&self.records);
        let future = self.inner.stream(ModelRequest {
            request_id: step_id.clone(),
            ..request
        });
        Box::pin(async move {
            let stream = future.await?;
            let stream = stream.map(move |event| {
                if let Ok(event) = &event {
                    let record = trace_record(&step_id, event);
                    records
                        .lock()
                        .expect("turn trace lock must not be poisoned")
                        .push(record);
                }
                event
            });
            Ok(Box::pin(stream) as ModelEventStream)
        })
    }
}

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
async fn turn_event_stream_completes_across_configured_models() {
    let config = load_config();

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in &config.minimax_openai.model_matrix {
            run_openai_event_stream(&provider, entry, "minimax").await;
        }
    }

    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in &config.minimax_anthropic.model_matrix {
            run_anthropic_event_stream(&provider, entry, "minimax").await;
        }
    }

    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in &config.qwen_openai.model_matrix {
            run_openai_event_stream(&provider, entry, "qwen").await;
        }
    }

    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in &config.qwen_anthropic.model_matrix {
            run_anthropic_event_stream(&provider, entry, "qwen").await;
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

async fn run_openai_event_stream(
    provider: &kolyan_provider_openai::OpenAiProvider,
    entry: &ModelMatrixEntry,
    family: &str,
) {
    let fixture = load_fixture("text");
    let executor = TurnExecutor::new(provider.clone());
    let request = TurnRequest {
        turn_id: format!("turn-event-stream-{family}-{}", entry.model),
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            format!("turn-event-stream-{family}-{}", entry.model),
            entry.max_output_tokens,
        ),
        config: TurnConfig { max_steps: 1 },
    };
    let events = collect_event_stream(
        &executor,
        request,
        &format!("{family}/openai_compat/{}", entry.model),
    )
    .await;
    assert_event_stream_final(&events, &format!("{family}/openai_compat/{}", entry.model));
}

async fn run_anthropic_event_stream(
    provider: &kolyan_provider_anthropic::AnthropicProvider,
    entry: &ModelMatrixEntry,
    family: &str,
) {
    let fixture = load_fixture("text");
    let executor = TurnExecutor::new(provider.clone());
    let request = TurnRequest {
        turn_id: format!("turn-event-stream-{family}-{}", entry.model),
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            format!("turn-event-stream-{family}-{}", entry.model),
            entry.max_output_tokens,
        ),
        config: TurnConfig { max_steps: 1 },
    };
    let events = collect_event_stream(
        &executor,
        request,
        &format!("{family}/anthropic_compat/{}", entry.model),
    )
    .await;
    assert_event_stream_final(
        &events,
        &format!("{family}/anthropic_compat/{}", entry.model),
    );
}

async fn collect_event_stream<P, T>(
    executor: &TurnExecutor<P, T>,
    request: TurnRequest,
    label: &str,
) -> Vec<TurnEvent>
where
    P: kolyan_model::ModelProvider,
    T: kolyan_core::ToolExecutor,
{
    let mut stream = executor
        .execute_event_stream(request, Default::default())
        .await
        .unwrap_or_else(|error| panic!("[{label}] turn event stream failed: {error}"));
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap_or_else(|error| panic!("[{label}] turn event failed: {error}")));
    }
    events
}

fn assert_event_stream_final(events: &[TurnEvent], label: &str) {
    assert!(
        matches!(events.first(), Some(TurnEvent::Started { .. })),
        "[{label}] missing Started"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, TurnEvent::StepStarted { .. }))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, TurnEvent::StepCompleted { .. }))
    );
    assert!(
        matches!(
            events.last(),
            Some(TurnEvent::Completed {
                outcome: TurnOutcome::FinalAnswer { .. },
                ..
            })
        ),
        "[{label}] event stream did not end with FinalAnswer"
    );
}

async fn run_openai_multi_step(
    provider: &kolyan_provider_openai::OpenAiProvider,
    entry: &ModelMatrixEntry,
    family: &str,
    root: &std::path::Path,
) {
    let fixture = load_fixture("turn_ten_step");
    let (provider, records) = RecordingProvider::new(provider.clone());
    let result = TurnExecutor::with_tools(provider, RestrictedShellTool::new(root))
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
        &result,
        fixture.turn.as_ref(),
        &format!("{family}/openai_compat/{}/ten_step", entry.model),
    );
    assert_turn_trace(
        &records,
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
    let (provider, records) = RecordingProvider::new(provider.clone());
    let result = TurnExecutor::with_tools(provider, RestrictedShellTool::new(root))
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
        &result,
        fixture.turn.as_ref(),
        &format!("{family}/anthropic_compat/{}/ten_step", entry.model),
    );
    assert_turn_trace(
        &records,
        &format!("{family}/anthropic_compat/{}/ten_step", entry.model),
    );
}

fn assert_multi_step_result(
    result: &TurnResult,
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

fn trace_record(step_id: &str, event: &ModelEvent) -> Value {
    let mut record = match event {
        ModelEvent::Started => json!({"event": "started"}),
        ModelEvent::TextDelta(text) => json!({"event": "text_delta", "text": text}),
        ModelEvent::ReasoningDelta(text) => {
            json!({"event": "reasoning_delta", "text": text})
        }
        ModelEvent::ToolCallStarted { id, name } => {
            json!({"event": "tool_call_started", "id": id, "name": name})
        }
        ModelEvent::ToolCallArgumentsDelta { id, delta } => {
            json!({"event": "tool_call_arguments_delta", "id": id, "delta": delta})
        }
        ModelEvent::ToolCallCompleted(call) => json!({
            "event": "tool_call_completed",
            "id": call.id,
            "name": call.name,
            "arguments": call.arguments,
        }),
        ModelEvent::Usage(usage) => json!({"event": "usage", "usage": usage}),
        ModelEvent::Provider(metadata) => json!({"event": "provider", "metadata": metadata}),
        ModelEvent::Completed(response) => json!({
            "event": "completed",
            "stop_reason": stop_reason_name(&response.stop_reason),
            "response": response,
        }),
    };
    record
        .as_object_mut()
        .expect("turn trace record must be an object")
        .insert("step_id".into(), Value::String(step_id.into()));
    record
}

fn stop_reason_name(stop_reason: &StopReason) -> &'static str {
    match stop_reason {
        StopReason::EndTurn => "end_turn",
        StopReason::ToolUse => "tool_use",
        StopReason::MaxOutputTokens => "max_output_tokens",
        StopReason::Refusal => "refusal",
        StopReason::Other(_) => "other",
    }
}

fn assert_turn_trace(records: &Arc<Mutex<Vec<Value>>>, label: &str) {
    let records = records
        .lock()
        .expect("turn trace lock must not be poisoned")
        .clone();
    let actual = records
        .iter()
        .map(|record| serde_json::to_string(record).expect("turn trace record must serialize"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let temp_path = write_temp_trace(label, &actual);
    let expected_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/expected/turn/ten_step.jsonl");
    let expected = fs::read_to_string(&expected_path).unwrap_or_else(|error| {
        panic!(
            "[{label}] expected turn trace missing at {}: {error}",
            expected_path.display()
        )
    });
    assert_trace_contract(label, &actual, &expected, &temp_path);
    fs::remove_file(&temp_path)
        .expect("successful turn trace comparison should clean up temp file");
}

fn write_temp_trace(label: &str, actual: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after unix epoch")
        .as_nanos();
    let safe_label = label.replace('/', "-");
    let path = std::env::temp_dir().join(format!(
        "kolyan-turn-trace-{safe_label}-{}-{unique}.jsonl",
        std::process::id()
    ));
    fs::write(&path, actual).expect("turn trace must write to temp file");
    path
}

fn assert_trace_contract(label: &str, actual: &str, expected: &str, temp_path: &Path) {
    let actual_records = parse_trace_records(actual, label);
    let expected_records = parse_trace_records(expected, label);
    let mut actual_index = 0;
    for expected_record in expected_records {
        let optional = expected_record
            .get("optional")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut contract = expected_record;
        contract
            .as_object_mut()
            .expect("turn trace contract must be an object")
            .remove("optional");
        let found = actual_records[actual_index..]
            .iter()
            .position(|record| record_contains(record, &contract));
        match (optional, found) {
            (true, None) => continue,
            (_, Some(offset)) => actual_index += offset + 1,
            (false, None) => panic!(
                "[{label}] turn trace contract mismatch; expected {contract}, actual output is at {}",
                temp_path.display()
            ),
        }
    }
}

fn parse_trace_records(text: &str, label: &str) -> Vec<Value> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("[{label}] invalid turn JSONL record: {error}"))
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
