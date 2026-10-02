//! Real Session/Runtime execution with local scripted Provider and governed tools.

mod actors;
mod fixtures;
use actors::{Hook, Provider, Tools};
use fixtures::{history, key, request, text, turn_value};

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kolyan_core::{
    ResumeInput, ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture,
    TurnConfig, TurnControl, TurnExecutor, TurnRequest,
};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelEvent, ModelEventStream, ModelProvider, ModelRequest,
    ModelResponse, ProviderFuture, StopReason, TokenUsage, ToolCall, ToolResult,
};
use kolyan_policy::{ApprovalMode, PolicyEngine, PreparedCall, ToolManifest};
use kolyan_runtime::{DurableTurnResult, ExecutionKey, verified_execution_input};
use kolyan_storage::{
    FileSessionStore, SessionContextPolicy, SessionStore, SessionTurn, SessionTurnStatus,
    StorageError,
};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::Case;
use crate::{
    ExecutionRef, ExecutionService, PreparationFailure, ServerError, SessionExecutionService,
    SessionService, TurnPreparationFuture, TurnPreparationHook,
};

pub(super) async fn run<L, F>(case: &Case, root: &Path, ledger: L, reopen: F) -> Value
where
    L: LedgerStore + Clone + 'static,
    F: Fn() -> L,
{
    let store = FileSessionStore::new(root.join("sessions")).unwrap();
    store.create("s").unwrap();
    let history = history();
    store
        .append_turn(
            "s",
            SessionTurn {
                turn_id: "prior".into(),
                execution_id: "prior-e".into(),
                status: SessionTurnStatus::Completed,
            },
            history.clone(),
        )
        .unwrap();
    let before = store.load("s").unwrap();
    let seen = Arc::new(Mutex::new(vec![]));
    let returned = Arc::new(Mutex::new(vec![]));
    let models = Arc::new(Mutex::new(vec![]));
    let opened_at = Arc::new(Mutex::new(vec![]));
    let tools = Arc::new(AtomicUsize::new(0));
    let source_path = root.join("source.json");
    let hook: Arc<dyn TurnPreparationHook> = Arc::new(Hook {
        action: case.action.clone(),
        source_path: source_path.clone(),
        store: store.clone(),
        seen: seen.clone(),
        returned: returned.clone(),
    });
    let prepared = crate::suspension::tests::fixture().checkpoint.calls[0]
        .prepared
        .clone()
        .unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: prepared.call().name.clone(),
        capabilities: prepared.claim().capabilities.clone(),
        effects: prepared.claim().effects.clone(),
        path_scopes: vec![],
        idempotency: prepared.claim().idempotency,
        approval: ApprovalMode::Always,
    });
    let approval = matches!(case.action.as_str(), "approval" | "resume" | "recovery");
    let executor = || {
        TurnExecutor::with_tools(
            Provider {
                seen: models.clone(),
                opened_at: opened_at.clone(),
                approval,
                prepared: prepared.clone(),
            },
            Tools {
                prepared: prepared.clone(),
                calls: tools.clone(),
            },
        )
        .with_policy_engine(Arc::new(policy.clone()))
    };
    let mut service = SessionExecutionService::new(
        ExecutionService::new(ledger.clone(), NoopTraceSink),
        SessionService::new(store.clone()),
    )
    .with_context_policy(SessionContextPolicy::FullTrajectory);
    if case.action != "no_hook" {
        service = service.with_preparation_hook(hook.clone());
    }
    let mut request = request();
    request.config.deadline = case.deadline_ms.map(Duration::from_millis);
    match case.action.as_str() {
        "source_messages_bound" => request.model_request.messages = vec![text("input"); 4097],
        "source_blocks_bound" => {
            request.model_request.messages[0].content =
                vec![ContentBlock::Text { text: "x".into() }; 16385]
        }
        "source_bytes_bound" => {
            request.model_request.extensions = json!("x".repeat(16 * 1024 * 1024))
        }
        _ => {}
    }
    let original = turn_value(&request);
    let started = Instant::now();
    let future = service.start(executor(), request.clone(), "s", "e");
    let mut result = if case.action == "drop" {
        // This case deliberately drops preparation, exported as a distinct outcome.
        tokio::time::timeout(Duration::from_millis(5), future)
            .await
            .ok()
    } else {
        Some(future.await)
    };
    let mut suspended = Value::Null;
    if let Some(Ok(DurableTurnResult::Suspended { suspension, .. })) = &result {
        let saved = suspension.as_ref().clone();
        suspended = serde_json::to_value(&saved).unwrap();
        let reopened = SessionExecutionService::new(
            ExecutionService::new(reopen(), NoopTraceSink),
            SessionService::new(FileSessionStore::new(root.join("sessions")).unwrap()),
        )
        .with_preparation_hook(hook);
        result = Some(match case.action.as_str() {
            "approval" => {
                reopened
                    .resume_approval(
                        executor(),
                        "s",
                        "e",
                        &saved.waiting.approvals[0].approval_id,
                    )
                    .await
            }
            "resume" | "recovery" => {
                let key = key();
                let confirmation = crate::suspension::confirm_approval(
                    &ledger,
                    &key,
                    &saved,
                    &saved.waiting.approvals[0].approval_id,
                )
                .unwrap();
                if case.action == "resume" {
                    reopened
                        .resume(
                            executor(),
                            "s",
                            "e",
                            &saved.checkpoint.checkpoint_id,
                            ResumeInput::ApprovalConfirmed(confirmation),
                        )
                        .await
                } else {
                    let scope = saved.checkpoint.scope.clone();
                    let checkpoint = executor()
                        .with_execution_key(scope.execution.clone())
                        .merge_resume_with_control(
                            saved,
                            ResumeInput::ApprovalConfirmed(confirmation),
                            scope,
                            TurnControl::default(),
                        )
                        .unwrap();
                    let publication = ledger.events_after(0).unwrap().last().unwrap().cursor;
                    let payload = json!({"schema_version":1,"publication_cursor":publication,"checkpoint":checkpoint});
                    let id = format!(
                        "e/checkpoint/merged/{:x}",
                        Sha256::digest(serde_json::to_vec(&payload).unwrap())
                    );
                    ledger
                        .append_unless_cancelled(LedgerEvent {
                            event_id: id.clone(),
                            idempotency_key: id,
                            execution_id: "e".into(),
                            turn_id: "t".into(),
                            cursor: 0,
                            kind: LedgerEventKind::TurnCheckpointMerged,
                            payload,
                        })
                        .unwrap();
                    reopened
                        .resume_committed(executor(), "s", "e", &checkpoint.checkpoint_id)
                        .await
                }
            }
            _ => unreachable!(),
        });
    }
    let elapsed_ms = started.elapsed().as_millis();
    let outcome = match &result {
        None => "dropped".into(),
        Some(Ok(DurableTurnResult::Completed(..))) => "completed".into(),
        Some(Ok(_)) => "suspended".into(),
        Some(Err(error)) => error_kind(error),
    };
    let events = reopen().events_after(0).unwrap();
    let admitted = if events
        .iter()
        .any(|event| event.kind == LedgerEventKind::ExecutionStarted)
    {
        Some(
            verified_execution_input(
                &reopen(),
                &ExecutionKey {
                    session_id: "s".into(),
                    turn_id: "t".into(),
                    execution_id: "e".into(),
                },
                16 * 1024 * 1024,
            )
            .unwrap(),
        )
    } else {
        None
    };
    let artifact: Option<Value> = source_path
        .is_file()
        .then(|| serde_json::from_slice(&std::fs::read(&source_path).unwrap()).unwrap());
    json!({"case":case.id,"original":original,"session_before":before,
        "session_after":store.load("s").unwrap(),"hook_seen":*seen.lock().unwrap(),
        "hook_returned":*returned.lock().unwrap(),"source_artifact":artifact,
        "model_requests":*models.lock().unwrap(),"model_opened_at_ms":*opened_at.lock().unwrap(),"tool_calls":tools.load(Ordering::SeqCst),
        "ledger":events,"admitted_input":admitted,"suspension":suspended,
        "outcome":outcome,"error":result.as_ref().and_then(|value|value.as_ref().err()).map(|error|error.to_string()),
        "elapsed_ms":elapsed_ms})
}

pub(super) fn compare(case: &Case, row: &Value) {
    assert_eq!(row["case"], case.id);
    assert_eq!(
        row["outcome"], case.expected,
        "{}: {}",
        case.id, row["error"]
    );
    assert_eq!(
        row["hook_seen"].as_array().unwrap().len(),
        case.hook_calls,
        "{}",
        case.id
    );
    assert_eq!(
        row["model_requests"].as_array().unwrap().len(),
        case.model_calls,
        "{}",
        case.id
    );
    assert_eq!(row["tool_calls"], case.tool_calls, "{}", case.id);
    let before: &Value = &row["session_before"];
    let after: &Value = &row["session_after"];
    for observation in row["hook_seen"].as_array().unwrap() {
        assert_eq!(observation["execution"], json!(key()));
        assert_eq!(observation["session_version"], before["version"]);
        assert_eq!(observation["request"]["config"], row["original"]["config"]);
        let mut expected_source = row["original"].clone();
        let mut full_messages = before["context_messages"].as_array().unwrap().clone();
        full_messages.extend(
            row["original"]["model_request"]["messages"]
                .as_array()
                .unwrap()
                .iter()
                .cloned(),
        );
        expected_source["model_request"]["messages"] = json!(full_messages);
        assert_eq!(observation["request"], expected_source, "{}", case.id);
    }
    if case.expected == "completed" {
        let current: &Value = &row["original"]["model_request"]["messages"];
        assert_eq!(after["inputs"]["t"]["messages"], *current);
        assert_eq!(after["inputs"]["t"]["base_version"], before["version"]);
        let old = before["context_messages"].as_array().unwrap();
        assert_eq!(
            &after["context_messages"].as_array().unwrap()[..old.len()],
            old
        );
        let selected = if case.action == "no_hook" {
            let mut full: ModelRequest =
                serde_json::from_value(row["original"]["model_request"].clone()).unwrap();
            let mut messages = history();
            messages.extend(full.messages);
            full.messages = messages;
            serde_json::to_value(full).unwrap()
        } else {
            assert_eq!(row["source_artifact"], row["hook_seen"][0]);
            row["hook_returned"][0].clone()
        };
        assert_eq!(
            row["admitted_input"]["model_request"], selected,
            "{}",
            case.id
        );
        let admission = row["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["kind"] == "execution_input_admitted")
            .unwrap();
        assert_eq!(
            admission["payload"]["max_steps"],
            row["original"]["config"]["max_steps"]
        );
        assert_eq!(
            admission["payload"]["max_tool_calls"],
            row["original"]["config"]["max_tool_calls"]
        );
        let mut provider = row["model_requests"][0].clone();
        provider["request_id"] = selected["request_id"].clone();
        assert_eq!(provider, selected, "{}", case.id);
        assert_eq!(
            row["ledger"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|event| event["kind"] == "effect_receipt")
                .count(),
            case.tool_calls
        );
        if case.action == "delay" {
            let admission = row["ledger"]
                .as_array()
                .unwrap()
                .iter()
                .find(|event| event["kind"] == "execution_input_admitted");
            // The physical immutable input includes the adjusted Turn budget.
            assert!(row["elapsed_ms"].as_u64().unwrap() >= 25);
            assert!(admission.is_some());
            let deadline = admission.unwrap()["payload"]["deadline_at_ms"]
                .as_u64()
                .unwrap();
            let opened = row["model_opened_at_ms"][0].as_u64().unwrap();
            assert!(deadline.saturating_sub(opened) <= case.deadline_ms.unwrap() - 25);
        }
    } else {
        assert!(row["ledger"].as_array().unwrap().is_empty(), "{}", case.id);
        if case.action != "conflict" {
            assert_eq!(before, after, "{}", case.id);
        } else {
            assert_eq!(
                after["version"].as_u64().unwrap(),
                before["version"].as_u64().unwrap() + 1
            );
            assert!(after["inputs"].get("t").is_none());
        }
        assert!(row["admitted_input"].is_null());
    }
    if case.id == "pending_hard_ceiling" {
        assert!(row["elapsed_ms"].as_u64().unwrap() >= 30000);
        assert!(row["elapsed_ms"].as_u64().unwrap() < 35000);
    }
}

fn error_kind(error: &ServerError) -> String {
    match error {
        ServerError::Preparation(failure) => serde_json::to_value(failure)
            .unwrap()
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| "Rejected".into()),
        ServerError::Session(StorageError::Conflict(_)) => "session_conflict".into(),
        ServerError::Session(StorageError::Io(_)) => "session_io".into(),
        other => format!("unexpected: {other}"),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
