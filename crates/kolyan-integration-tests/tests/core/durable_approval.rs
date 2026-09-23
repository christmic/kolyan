//! Real-provider durable approval tests. The checkpoint is persisted before
//! the executor is dropped, then loaded by a newly constructed executor.

#[path = "../common/mod.rs"]
mod common;

use common::{
    ModelMatrixEntry, build_anthropic_provider, build_openai_provider, build_request, has_api_key,
    has_api_key_anthropic, load_config, load_fixture, require_api_key, require_api_key_anthropic,
};
use kolyan_core::{ResumableTurn, TurnConfig, TurnExecutor, TurnOutcome, TurnRequest};
use kolyan_model::ModelProvider;
use kolyan_policy::{ApprovalMode, Capability, Effect, PathScope, PolicyEngine, ToolManifest};
use kolyan_storage::{ApprovalStore, FileApprovalStore};
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
#[ignore = "real-network test: requires configured provider API keys"]
async fn durable_approval_survives_executor_restart_for_every_configured_model() {
    let config = load_config();
    let root = std::env::temp_dir().join(format!("kolyan-durable-live-{}", std::process::id()));
    let safe = root.join("safe");
    fs::create_dir_all(&safe).expect("durable live root should be created");
    let store = FileApprovalStore::new(root.join("approvals")).expect("store should be created");

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in &config.minimax_openai.model_matrix {
            run_case(&provider, entry, "minimax", &root, &store).await;
        }
    }
    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in &config.minimax_anthropic.model_matrix {
            run_case(&provider, entry, "minimax", &root, &store).await;
        }
    }
    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in &config.qwen_openai.model_matrix {
            run_case(&provider, entry, "qwen", &root, &store).await;
        }
    }
    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in &config.qwen_anthropic.model_matrix {
            run_case(&provider, entry, "qwen", &root, &store).await;
        }
    }
    fs::remove_dir_all(root).expect("durable live root should be removed");
}

#[tokio::test]
#[ignore = "real-network test: requires configured provider API keys"]
async fn durable_approval_rejects_and_expires_without_tool_side_effects() {
    let config = load_config();
    let root = std::env::temp_dir().join(format!("kolyan-durable-terminal-{}", std::process::id()));
    fs::create_dir_all(root.join("safe")).expect("terminal test root should be created");

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in &config.minimax_openai.model_matrix {
            run_terminal_cases(&provider, entry, "minimax", &root).await;
        }
    }
    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in &config.minimax_anthropic.model_matrix {
            run_terminal_cases(&provider, entry, "minimax", &root).await;
        }
    }
    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in &config.qwen_openai.model_matrix {
            run_terminal_cases(&provider, entry, "qwen", &root).await;
        }
    }
    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in &config.qwen_anthropic.model_matrix {
            run_terminal_cases(&provider, entry, "qwen", &root).await;
        }
    }
    fs::remove_dir_all(root).expect("terminal test root should be removed");
}

async fn run_case<P>(
    provider: &P,
    entry: &ModelMatrixEntry,
    family: &str,
    root: &Path,
    store: &FileApprovalStore,
) where
    P: ModelProvider + Clone + 'static,
{
    let fixture = load_fixture("turn_policy_allowed");
    let turn_id = format!("durable-{family}-{}", entry.model);
    let request = TurnRequest {
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            turn_id.clone(),
            entry.max_output_tokens,
        ),
        turn_id: turn_id.clone(),
        config: TurnConfig {
            max_steps: fixture.turn.as_ref().and_then(|v| v.max_steps).unwrap_or(4),
        },
    };

    let awaiting = {
        let executor = authorized_executor(provider.clone(), root);
        executor
            .start_resumable(request)
            .await
            .unwrap_or_else(|error| {
                panic!("[{family}/{}] durable start failed: {error}", entry.model)
            })
    };
    let approval = match awaiting {
        ResumableTurn::AwaitingApproval(value) => *value,
        ResumableTurn::Completed(_) => {
            panic!("[{family}/{}] expected approval boundary", entry.model)
        }
    };
    assert_eq!(approval.turn_id, turn_id);
    store
        .save(&approval)
        .expect("approval checkpoint should persist");
    let approval_id = approval.approval_id.clone();
    let trace = vec![
        json!({"state":"awaiting_approval", "approval_id": approval.approval_id}),
        json!({"state":"executor_discarded"}),
    ];
    drop(approval);

    let restored = store.claim(&approval_id).unwrap_or_else(|error| {
        panic!(
            "[{family}/{}] checkpoint should reload: {error}",
            entry.model
        )
    });
    let completed = {
        let executor = authorized_executor(provider.clone(), root);
        executor
            .resume_approval(restored, &approval_id)
            .await
            .unwrap_or_else(|error| {
                panic!("[{family}/{}] durable resume failed: {error}", entry.model)
            })
    };
    let execution = match completed {
        ResumableTurn::Completed(value) => *value,
        ResumableTurn::AwaitingApproval(_) => {
            panic!("[{family}/{}] expected completion", entry.model)
        }
    };
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert_eq!(
        fs::read_to_string(root.join("safe/allowed.txt")).unwrap(),
        "kolyan-policy-allowed"
    );
    fs::remove_file(root.join("safe/allowed.txt")).expect("file should be consumed once");

    let mut trace = trace;
    trace.push(json!({"state":"resumed"}));
    trace.push(json!({"state":"completed"}));
    assert_trace(&trace, &format!("{family}/{}", entry.model));
    store
        .delete(&approval_id)
        .expect("checkpoint should be deleted after completion");
}

async fn run_terminal_cases<P>(provider: &P, entry: &ModelMatrixEntry, family: &str, root: &Path)
where
    P: ModelProvider + Clone + 'static,
{
    let fixture = load_fixture("turn_policy_allowed");
    let make_request = |suffix: &str| TurnRequest {
        turn_id: format!("terminal-{family}-{}-{suffix}", entry.model),
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            format!("terminal-{family}-{}-{suffix}", entry.model),
            entry.max_output_tokens,
        ),
        config: TurnConfig {
            max_steps: fixture.turn.as_ref().and_then(|v| v.max_steps).unwrap_or(4),
        },
    };

    let first = authorized_executor(provider.clone(), root)
        .start_resumable(make_request("reject"))
        .await
        .unwrap_or_else(|error| panic!("[{family}/{}] reject start failed: {error}", entry.model));
    let rejected = match first {
        ResumableTurn::AwaitingApproval(value) => {
            let value = *value;
            authorized_executor(provider.clone(), root)
                .reject_approval(value.clone(), &value.approval_id, "user rejected")
                .expect("reject should be terminal")
        }
        ResumableTurn::Completed(_) => {
            panic!("[{family}/{}] expected rejection approval", entry.model)
        }
    };
    assert!(matches!(
        rejected.result.outcome,
        TurnOutcome::Rejected { .. }
    ));
    assert!(!root.join("safe/allowed.txt").exists());

    let second = authorized_executor(provider.clone(), root)
        .start_resumable(make_request("expire"))
        .await
        .unwrap_or_else(|error| panic!("[{family}/{}] expire start failed: {error}", entry.model));
    let mut expired = match second {
        ResumableTurn::AwaitingApproval(value) => *value,
        ResumableTurn::Completed(_) => {
            panic!("[{family}/{}] expected expiration approval", entry.model)
        }
    };
    let approval_id = expired.approval_id.clone();
    expired.expires_at_ms = Some(0);
    let error = authorized_executor(provider.clone(), root)
        .resume_approval(expired, &approval_id)
        .await
        .expect_err("expired approval must fail closed");
    assert!(error.to_string().contains("expired"));
    assert!(!root.join("safe/allowed.txt").exists());
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
        approval: ApprovalMode::Always,
    });
    policy.restrict_workspace("safe");
    let policy = Arc::new(policy);
    TurnExecutor::with_tools(
        provider,
        PolicyEnforcingTool::new(RestrictedFileTool::new(root), Arc::clone(&policy)),
    )
    .with_policy_engine(policy)
}

fn assert_trace(actual: &[Value], label: &str) {
    let actual_text = actual
        .iter()
        .map(|record| serde_json::to_string(record).expect("trace should serialize"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be valid")
        .as_nanos();
    let temp = std::env::temp_dir().join(format!("kolyan-durable-{unique}.jsonl"));
    fs::write(&temp, actual_text).expect("trace should be written by test code");
    let expected_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/expected/turn/durable_approval.jsonl");
    let expected = fs::read_to_string(expected_path).expect("durable trace contract should exist");
    let mut offset = 0;
    for line in expected.lines() {
        let expected: Value = serde_json::from_str(line).expect("expected trace should be JSON");
        let found = actual[offset..]
            .iter()
            .position(|record| record.get("state") == expected.get("state"));
        assert!(found.is_some(), "[{label}] missing trace state {expected}");
        offset += found.unwrap() + 1;
    }
    fs::remove_file(temp).expect("temporary trace should be removed");
}
