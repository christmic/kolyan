//! Real-provider Server → Runtime approval recovery matrix.
//!
//! This is intentionally ignored in ordinary workspace tests. Run it with
//! `--ignored` after exporting the project-local test credentials.

#[path = "../common/mod.rs"]
mod common;

use common::{
    ModelMatrixEntry, build_anthropic_provider, build_openai_provider, build_request, has_api_key,
    has_api_key_anthropic, load_config, load_fixture, require_api_key, require_api_key_anthropic,
};
use kolyan_core::{TurnConfig, TurnExecutor, TurnOutcome, TurnRequest};
use kolyan_ledger::{FileLedger, LedgerStore};
use kolyan_model::ModelProvider;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyEngine, ToolManifest,
};
use kolyan_runtime::{DurableTurnDriver, DurableTurnResult};
use kolyan_server::{ExecutionRef, ExecutionServer, ExecutionState};
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::VecTraceSink;
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[tokio::test]
#[ignore = "real-network test: requires configured provider API keys"]
async fn server_runtime_approval_live_matrix() {
    let config = load_config();
    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in config.minimax_openai.model_matrix.clone() {
            run_case(&provider, &entry, "minimax", "openai_compat").await;
        }
    }
    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in config.minimax_anthropic.model_matrix.clone() {
            run_case(&provider, &entry, "minimax", "anthropic_compat").await;
        }
    }
    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in config.qwen_openai.model_matrix.clone() {
            run_case(&provider, &entry, "qwen", "openai_compat").await;
        }
    }
    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in config.qwen_anthropic.model_matrix.clone() {
            run_case(&provider, &entry, "qwen", "anthropic_compat").await;
        }
    }
}

async fn run_case<P>(provider: &P, entry: &ModelMatrixEntry, family: &str, surface: &str)
where
    P: ModelProvider + Clone + 'static,
{
    let fixture = load_fixture("turn_policy_allowed");
    let label = format!("server/{family}/{surface}/{}", entry.model);
    let unique = format!(
        "{}-{}-{}-{}",
        family,
        surface,
        sanitize(&entry.model),
        std::process::id()
    );
    let root = std::env::temp_dir().join(format!("kolyan-server-live-{unique}"));
    let safe = root.join("safe");
    fs::create_dir_all(&safe).unwrap_or_else(|error| panic!("[{label}] setup failed: {error}"));
    let ledger_path = root.join("ledger.jsonl");
    let turn_id = format!("server-live-{family}-{surface}-{}", sanitize(&entry.model));
    let execution_id = format!("execution-{turn_id}");
    let session_id = format!("session-{family}");
    let request = TurnRequest {
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            format!("{turn_id}-request"),
            entry.max_output_tokens,
        ),
        turn_id: turn_id.clone(),
        config: TurnConfig {
            max_steps: fixture
                .turn
                .as_ref()
                .and_then(|value| value.max_steps)
                .unwrap_or(4),
            ..TurnConfig::default()
        },
    };
    let policy = live_policy();
    let first_server = ExecutionServer::new(FileLedger::open(&ledger_path).unwrap());
    let execution = ExecutionRef {
        session_id: session_id.clone(),
        turn_id: turn_id.clone(),
        execution_id: execution_id.clone(),
    };
    first_server
        .start(execution.clone())
        .unwrap_or_else(|error| panic!("[{label}] server start failed: {error}"));
    let first_driver = DurableTurnDriver::new(
        FileLedger::open(&ledger_path).unwrap(),
        VecTraceSink::default(),
    );
    let awaiting = first_driver
        .start(
            live_executor(provider.clone(), &root, policy.clone()),
            request,
            &session_id,
            &execution_id,
        )
        .await
        .unwrap_or_else(|error| panic!("[{label}] runtime start failed: {error}"));
    let approval_id = match awaiting {
        DurableTurnResult::AwaitingApproval { approval, .. } => approval.approval_id.clone(),
        DurableTurnResult::Completed(_, _) => panic!("[{label}] expected approval suspension"),
    };
    assert!(
        !safe.join("allowed.txt").exists(),
        "[{label}] approval must precede side effect"
    );
    assert_eq!(
        first_server.state(&execution_id).unwrap(),
        ExecutionState::Suspended,
        "[{label}] server must observe suspension"
    );
    first_server.release(&execution_id);
    drop(first_driver);
    drop(first_server);

    let second_server = ExecutionServer::new(FileLedger::open(&ledger_path).unwrap());
    second_server
        .resume(execution.clone())
        .unwrap_or_else(|error| panic!("[{label}] server resume failed: {error}"));
    let second_driver = DurableTurnDriver::new(
        FileLedger::open(&ledger_path).unwrap(),
        VecTraceSink::default(),
    );
    let completed = second_driver
        .resume(
            live_executor(provider.clone(), &root, policy),
            &session_id,
            &execution_id,
            &approval_id,
        )
        .await
        .unwrap_or_else(|error| panic!("[{label}] runtime resume failed: {error}"));
    let execution_result = match completed {
        DurableTurnResult::Completed(execution, _) => *execution,
        DurableTurnResult::AwaitingApproval { .. } => {
            panic!("[{label}] expected completion after approval")
        }
    };
    assert!(matches!(
        execution_result.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert_eq!(
        fs::read_to_string(safe.join("allowed.txt"))
            .unwrap_or_else(|error| panic!("[{label}] expected file write: {error}")),
        "kolyan-policy-allowed"
    );
    second_server.release(&execution_id);
    assert_eq!(
        second_server.state(&execution_id).unwrap(),
        ExecutionState::Completed,
        "[{label}] server must observe terminal completion"
    );
    compare_live_trajectory(&ledger_path, &execution_id, &label);
    fs::remove_dir_all(&root).unwrap_or_else(|error| panic!("[{label}] cleanup failed: {error}"));
}

fn live_policy() -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Update].into_iter().collect(),
        path_scopes: vec![PathScope::new("safe")],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Always,
    });
    policy.restrict_workspace("safe");
    Arc::new(policy)
}

fn live_executor<P>(
    provider: P,
    root: &Path,
    policy: Arc<PolicyEngine>,
) -> TurnExecutor<P, PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>>
where
    P: ModelProvider,
{
    TurnExecutor::with_tools(
        provider,
        PolicyEnforcingTool::new(RestrictedFileTool::new(root), policy.clone()),
    )
    .with_policy_engine(policy)
}

fn compare_live_trajectory(ledger_path: &Path, execution_id: &str, label: &str) {
    let ledger = FileLedger::open(ledger_path).unwrap();
    let records = ledger
        .events_after(0)
        .unwrap()
        .into_iter()
        .filter(|event| event.execution_id == execution_id)
        .map(|event| json!({"kind": event.kind}))
        .collect::<Vec<_>>();
    let actual = ledger_path.with_extension("actual.jsonl");
    fs::write(
        &actual,
        records
            .iter()
            .map(|record| serde_json::to_string(record).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap_or_else(|error| panic!("[{label}] trajectory write failed: {error}"));
    let expected_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/expected/server/approval_recovery.jsonl");
    let mut offset = 0;
    for line in fs::read_to_string(expected_path).unwrap().lines() {
        let expected: Value = serde_json::from_str(line).unwrap();
        let found = records[offset..]
            .iter()
            .position(|record| record["kind"] == expected["kind"]);
        assert!(
            found.is_some(),
            "[{label}] missing trajectory record {expected}"
        );
        offset += found.unwrap() + 1;
    }
    fs::remove_file(actual).unwrap();
}

fn sanitize(model: &str) -> String {
    model
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect()
}
