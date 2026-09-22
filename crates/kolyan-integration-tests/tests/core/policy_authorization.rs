//! Real-model authorization scenarios. The model creates the ToolCall; the
//! policy wrapper, not the fixture, decides whether the side effect happens.

#[path = "../common/mod.rs"]
mod common;

use common::{
    ModelMatrixEntry, build_anthropic_provider, build_openai_provider, build_request, has_api_key,
    has_api_key_anthropic, load_config, load_fixture, require_api_key, require_api_key_anthropic,
};
use kolyan_core::{
    ToolDispatchMode, ToolDispatchPolicy, ToolErrorPolicy, TurnConfig, TurnEvent, TurnExecutor,
    TurnOutcome, TurnRequest,
};
use kolyan_model::ModelProvider;
use kolyan_policy::{ApprovalMode, Capability, Effect, PathScope, PolicyEngine, ToolManifest};
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
#[ignore = "real-network test: requires configured provider API keys"]
async fn policy_allows_scoped_write_and_denies_out_of_scope_write() {
    let config = load_config();
    let root = std::env::temp_dir().join(format!("kolyan-policy-live-{}", std::process::id()));
    fs::create_dir_all(root.join("safe")).expect("policy live root should be created");

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in &config.minimax_openai.model_matrix {
            run_cases(&provider, entry, "minimax", &config.minimax_openai, &root).await;
        }
    }
    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in &config.minimax_anthropic.model_matrix {
            run_cases(
                &provider,
                entry,
                "minimax",
                &config.minimax_anthropic,
                &root,
            )
            .await;
        }
    }
    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in &config.qwen_openai.model_matrix {
            run_cases(&provider, entry, "qwen", &config.qwen_openai, &root).await;
        }
    }
    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in &config.qwen_anthropic.model_matrix {
            run_cases(&provider, entry, "qwen", &config.qwen_anthropic, &root).await;
        }
    }

    fs::remove_dir_all(root).expect("policy live root should be removed");
}

async fn run_cases<P, C>(
    provider: &P,
    entry: &ModelMatrixEntry,
    family: &str,
    _config: &C,
    root: &Path,
) where
    P: ModelProvider + Clone,
{
    run_allowed(provider, entry, family, root).await;
    run_denied(provider, entry, family, root).await;
}

async fn run_allowed<P: ModelProvider + Clone>(
    provider: &P,
    entry: &ModelMatrixEntry,
    family: &str,
    root: &Path,
) {
    let fixture = load_fixture("turn_policy_allowed");
    let execution = authorized_executor(provider.clone(), root)
        .execute_with_events(
            TurnRequest {
                turn_id: format!("policy-allowed-{family}-{}", entry.model),
                model_request: build_request(
                    family,
                    &entry.model,
                    &fixture,
                    format!("policy-allowed-{family}-{}", entry.model),
                    entry.max_output_tokens,
                ),
                config: turn_config(&fixture),
            },
            Default::default(),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "[{family}/{}] allowed policy call failed: {error}",
                entry.model
            )
        });

    let label = format!("{family}/{}/policy_allowed", entry.model);
    assert_trace(&execution.events, "turn_policy_allowed", &label);
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert_eq!(
        fs::read_to_string(root.join("safe/allowed.txt")).unwrap(),
        "kolyan-policy-allowed"
    );
    fs::remove_file(root.join("safe/allowed.txt")).expect("allowed fixture should be removed");
}

async fn run_denied<P: ModelProvider + Clone>(
    provider: &P,
    entry: &ModelMatrixEntry,
    family: &str,
    root: &Path,
) {
    let fixture = load_fixture("turn_policy_denied");
    let execution = authorized_executor(provider.clone(), root)
        .with_tool_dispatch_policy(ToolDispatchPolicy {
            mode: ToolDispatchMode::Serial,
            on_error: ToolErrorPolicy::ContinueBatch,
        })
        .execute_with_events(
            TurnRequest {
                turn_id: format!("policy-denied-{family}-{}", entry.model),
                model_request: build_request(
                    family,
                    &entry.model,
                    &fixture,
                    format!("policy-denied-{family}-{}", entry.model),
                    entry.max_output_tokens,
                ),
                config: turn_config(&fixture),
            },
            Default::default(),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "[{family}/{}] denied policy call should recover: {error}",
                entry.model
            )
        });

    let label = format!("{family}/{}/policy_denied", entry.model);
    assert_trace(&execution.events, "turn_policy_denied", &label);
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert!(
        !root.join("blocked/denied.txt").exists(),
        "policy must block the side effect"
    );
}

fn authorized_executor<P>(
    provider: P,
    root: &Path,
) -> TurnExecutor<P, PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>> {
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Update].into_iter().collect(),
        path_scopes: vec![PathScope::new("safe")],
        idempotency: kolyan_policy::Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    policy.restrict_workspace("safe");
    let policy = Arc::new(policy);
    TurnExecutor::with_tools(
        provider,
        PolicyEnforcingTool::new(RestrictedFileTool::new(root), Arc::clone(&policy)),
    )
    .with_policy_engine(policy)
}

fn turn_config(fixture: &common::Fixture) -> TurnConfig {
    TurnConfig {
        max_steps: fixture
            .turn
            .as_ref()
            .and_then(|turn| turn.max_steps)
            .unwrap_or(4),
    }
}

fn assert_trace(events: &[TurnEvent], fixture_name: &str, label: &str) {
    let actual = events.iter().map(event_record).collect::<Vec<_>>();
    let actual_text = actual
        .iter()
        .map(|record| serde_json::to_string(record).expect("policy trace must serialize"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after unix epoch")
        .as_nanos();
    let temp_path = std::env::temp_dir().join(format!(
        "kolyan-policy-trace-{}-{unique}.jsonl",
        label.replace('/', "-")
    ));
    fs::write(&temp_path, &actual_text).expect("policy trace should be written to a temp file");
    let expected_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/expected/turn")
        .join(format!("{fixture_name}.jsonl"));
    let expected = fs::read_to_string(&expected_path).expect("policy trace contract should exist");
    let mut actual_index = 0;
    for (index, line) in expected.lines().enumerate() {
        let expected: Value =
            serde_json::from_str(line).expect("expected policy trace must be JSON");
        let offset = actual[actual_index..]
            .iter()
            .position(|record| record_contains(record, &expected));
        assert!(
            offset.is_some(),
            "[{label}] expected trace record {index} missing; actual={actual:?}"
        );
        actual_index += offset.expect("trace offset was checked") + 1;
    }
    fs::remove_file(temp_path).expect("policy trace temp file should be removed");
}

fn event_record(event: &TurnEvent) -> Value {
    match event {
        TurnEvent::Started { .. } => serde_json::json!({"event":"started"}),
        TurnEvent::StepStarted { .. } => serde_json::json!({"event":"step_started"}),
        TurnEvent::StepCompleted { step, .. } => {
            serde_json::json!({"event":"step_completed","outcome":step_outcome_name(step.outcome)})
        }
        TurnEvent::ToolCallRequested { call, .. } => {
            serde_json::json!({"event":"tool_call_requested","name":call.name})
        }
        TurnEvent::ToolExecutionStarted { name, .. } => {
            serde_json::json!({"event":"tool_execution_started","name":name})
        }
        TurnEvent::ToolExecutionFailed { name, .. } => {
            serde_json::json!({"event":"tool_execution_failed","name":name})
        }
        TurnEvent::ToolResult { result, .. } => {
            serde_json::json!({"event":"tool_result","is_error":result.is_error,"content":result.content})
        }
        TurnEvent::Completed { .. } => {
            serde_json::json!({"event":"completed","outcome":"final_answer"})
        }
    }
}

fn step_outcome_name(outcome: kolyan_core::StepOutcome) -> &'static str {
    match outcome {
        kolyan_core::StepOutcome::FinalAnswer => "final_answer",
        kolyan_core::StepOutcome::ToolCalls => "tool_calls",
        kolyan_core::StepOutcome::Refused => "refused",
        kolyan_core::StepOutcome::Incomplete => "incomplete",
    }
}

fn record_contains(actual: &Value, expected: &Value) -> bool {
    let (Some(actual), Some(expected)) = (actual.as_object(), expected.as_object()) else {
        return actual == expected;
    };
    expected
        .iter()
        .all(|(key, value)| actual.get(key) == Some(value))
}
