use futures_util::stream;
use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FileLedger, LedgerStore};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelEvent, ModelEventStream, ModelProvider, ModelRef,
    ModelRequest, ModelResponse, ProviderError, ProviderErrorKind, ProviderErrorPhase,
    ProviderFuture, StopReason, SystemInstruction, TokenUsage, ToolCall, ToolChoice,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyEngine, ToolManifest,
};
use kolyan_server::{ExecutionRef, ExecutionService, SessionExecutionService, SessionService};
use kolyan_storage::{FileSessionStore, SessionStore, SessionTurnStatus};
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::VecTraceSink;
use serde_json::Value;
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct RecordingProvider {
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

#[derive(Clone)]
struct ApprovalProvider {
    calls: Arc<Mutex<usize>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    tool_call: ToolCall,
}

#[derive(Clone)]
struct FailureProvider;

impl ModelProvider for FailureProvider {
    fn stream(&self, _request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async {
            Err(ProviderError::new(
                ProviderErrorKind::Unavailable,
                ProviderErrorPhase::Open,
                "deterministic session failure",
            ))
        })
    }
}

impl ModelProvider for ApprovalProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.requests
            .lock()
            .expect("approval provider request lock")
            .push(request.clone());
        let mut calls = self.calls.lock().expect("approval provider lock");
        let first = *calls == 0;
        *calls += 1;
        let content = if first {
            vec![ContentBlock::ToolCall {
                call: self.tool_call.clone(),
            }]
        } else {
            vec![ContentBlock::Text {
                text: "approved answer".into(),
            }]
        };
        let response = ModelResponse {
            id: request.request_id.clone(),
            model: request.model,
            content,
            structured_output: None,
            stop_reason: if first {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

impl ModelProvider for RecordingProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.requests
            .lock()
            .expect("request recorder lock")
            .push(request.clone());
        let response = ModelResponse {
            id: request.request_id.clone(),
            model: request.model,
            content: vec![ContentBlock::Text {
                text: "persistent answer".into(),
            }],
            structured_output: None,
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

fn request(turn_id: &str, text: &str) -> TurnRequest {
    TurnRequest {
        turn_id: turn_id.into(),
        model_request: ModelRequest {
            request_id: format!("{turn_id}-request"),
            model: ModelRef::new("fixture", "session-model"),
            system: Vec::new(),
            messages: vec![Message {
                role: MessageRole::User,
                content: vec![ContentBlock::Text { text: text.into() }],
            }],
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: Value::Null,
        },
        config: Default::default(),
    }
}

fn approval_policy(root: &std::path::Path) -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Create, Effect::Update].into_iter().collect(),
        path_scopes: vec![PathScope::new(root.join("safe").to_string_lossy())],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Always,
    });
    policy.restrict_workspace(root.join("safe").to_string_lossy());
    Arc::new(policy)
}

fn approval_request(turn_id: &str) -> TurnRequest {
    TurnRequest {
        turn_id: turn_id.into(),
        model_request: ModelRequest {
            request_id: format!("{turn_id}-request"),
            model: ModelRef::new("fixture", "approval-model"),
            system: vec![SystemInstruction {
                text: "write only after approval".into(),
                cache: false,
            }],
            messages: vec![Message {
                role: MessageRole::User,
                content: vec![ContentBlock::Text {
                    text: "write approved file".into(),
                }],
            }],
            tools: RestrictedFileTool::tool_definitions(),
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(128),
            extensions: Value::Null,
        },
        config: TurnConfig {
            max_steps: 3,
            ..TurnConfig::default()
        },
    }
}

#[tokio::test]
async fn session_execution_reopens_and_supplies_prior_turn_context() {
    let root =
        std::env::temp_dir().join(format!("kolyan-session-execution-{}", std::process::id()));
    let ledger_path = root.join("ledger.jsonl");
    let session_path = root.join("sessions");
    let requests = Arc::new(Mutex::new(Vec::new()));

    let first_store = FileSessionStore::new(&session_path).unwrap();
    first_store.create("session-1").unwrap();
    let first = SessionExecutionService::new(
        ExecutionService::new(
            FileLedger::open(&ledger_path).unwrap(),
            VecTraceSink::default(),
        ),
        SessionService::new(first_store),
    );
    first
        .start(
            TurnExecutor::new(RecordingProvider {
                requests: requests.clone(),
            }),
            request("turn-1", "first question"),
            "session-1",
            "execution-1",
        )
        .await
        .unwrap();
    drop(first);

    let reopened = SessionExecutionService::new(
        ExecutionService::new(
            FileLedger::open(&ledger_path).unwrap(),
            VecTraceSink::default(),
        ),
        SessionService::new(FileSessionStore::new(&session_path).unwrap()),
    );
    reopened
        .start(
            TurnExecutor::new(RecordingProvider {
                requests: requests.clone(),
            }),
            request("turn-2", "second question"),
            "session-1",
            "execution-2",
        )
        .await
        .unwrap();

    let captured = requests.lock().expect("request recorder lock");
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].messages.len(), 1);
    assert_eq!(captured[1].messages.len(), 3);
    assert_eq!(captured[1].messages[0].role, MessageRole::User);
    assert_eq!(captured[1].messages[2].role, MessageRole::User);

    let session = FileSessionStore::new(&session_path)
        .unwrap()
        .load("session-1")
        .unwrap();
    assert_eq!(session.version, 4);
    assert_eq!(session.turns.len(), 2);
    assert!(
        session
            .turns
            .iter()
            .all(|turn| turn.status == SessionTurnStatus::Completed)
    );
    assert_eq!(session.messages.len(), 4);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn session_execution_persists_suspension_and_resumes_after_rebuild() {
    let root = std::env::temp_dir().join(format!("kolyan-session-approval-{}", std::process::id()));
    let safe = root.join("safe");
    std::fs::create_dir_all(&safe).unwrap();
    let ledger_path = root.join("ledger.jsonl");
    let session_path = root.join("sessions");
    let store = FileSessionStore::new(&session_path).unwrap();
    store.create("approval-session").unwrap();
    let policy = approval_policy(&root);
    let provider = ApprovalProvider {
        calls: Arc::default(),
        requests: Arc::default(),
        tool_call: ToolCall {
            id: "approval-call".into(),
            name: "file.write".into(),
            arguments: json!({"path":"safe/result.txt","content":"approved"}),
        },
    };
    let service = SessionExecutionService::new(
        ExecutionService::new(
            FileLedger::open(&ledger_path).unwrap(),
            VecTraceSink::default(),
        ),
        SessionService::new(store),
    );
    let executor = || {
        TurnExecutor::with_tools(
            provider.clone(),
            PolicyEnforcingTool::new(RestrictedFileTool::new(&root), policy.clone()),
        )
        .with_policy_engine(policy.clone())
    };
    let awaiting = service
        .start(
            executor(),
            approval_request("approval-turn"),
            "approval-session",
            "approval-execution",
        )
        .await
        .unwrap();
    let approval_id = match awaiting {
        kolyan_runtime::DurableTurnResult::AwaitingApproval { approval, .. } => {
            approval.approval_id
        }
        kolyan_runtime::DurableTurnResult::Completed(_, _) => panic!("approval expected"),
    };
    let suspended = FileSessionStore::new(&session_path)
        .unwrap()
        .load("approval-session")
        .unwrap();
    assert_eq!(suspended.turns[0].status, SessionTurnStatus::Suspended);
    assert!(suspended.messages.is_empty());
    drop(service);

    let reopened = SessionExecutionService::new(
        ExecutionService::new(
            FileLedger::open(&ledger_path).unwrap(),
            VecTraceSink::default(),
        ),
        SessionService::new(FileSessionStore::new(&session_path).unwrap()),
    );
    reopened
        .resume(
            executor(),
            "approval-session",
            "approval-execution",
            &approval_id,
        )
        .await
        .unwrap();
    reopened
        .start(
            executor(),
            approval_request("approval-next-turn"),
            "approval-session",
            "approval-next-execution",
        )
        .await
        .unwrap();
    let completed = FileSessionStore::new(&session_path)
        .unwrap()
        .load("approval-session")
        .unwrap();
    assert_eq!(completed.turns[0].status, SessionTurnStatus::Completed);
    assert_eq!(completed.turns[1].status, SessionTurnStatus::Completed);
    assert_eq!(completed.messages.len(), 4);
    assert!(safe.join("result.txt").exists());
    let requests = provider.requests.lock().unwrap();
    assert!(requests.len() >= 3);
    assert!(requests[2].messages.iter().all(|message| {
        !message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
    }));
    assert!(
        FileLedger::open(&ledger_path)
            .unwrap()
            .events_after(0)
            .unwrap()
            .iter()
            .any(|event| event.kind == kolyan_ledger::LedgerEventKind::ExecutionSuspended)
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn session_execution_marks_failure_and_external_cancel() {
    let root = std::env::temp_dir().join(format!("kolyan-session-terminal-{}", std::process::id()));
    let ledger_path = root.join("ledger.jsonl");
    let session_path = root.join("sessions");
    let store = FileSessionStore::new(&session_path).unwrap();
    store.create("terminal-session").unwrap();
    let service = SessionExecutionService::new(
        ExecutionService::new(
            FileLedger::open(&ledger_path).unwrap(),
            VecTraceSink::default(),
        ),
        SessionService::new(store),
    );
    let failed = service
        .start(
            TurnExecutor::new(FailureProvider),
            request("failed-turn", "fail this turn"),
            "terminal-session",
            "failed-execution",
        )
        .await;
    assert!(failed.is_err());
    let failed_record = FileSessionStore::new(&session_path)
        .unwrap()
        .load("terminal-session")
        .unwrap();
    assert_eq!(failed_record.turns[0].status, SessionTurnStatus::Failed);

    let awaiting = service
        .start(
            TurnExecutor::with_tools(
                ApprovalProvider {
                    calls: Arc::default(),
                    requests: Arc::default(),
                    tool_call: ToolCall {
                        id: "cancel-call".into(),
                        name: "file.write".into(),
                        arguments: json!({"path":"safe/cancel.txt","content":"cancelled"}),
                    },
                },
                PolicyEnforcingTool::new(RestrictedFileTool::new(&root), approval_policy(&root)),
            )
            .with_policy_engine(approval_policy(&root)),
            approval_request("cancel-turn"),
            "terminal-session",
            "cancel-execution",
        )
        .await
        .unwrap();
    assert!(matches!(
        awaiting,
        kolyan_runtime::DurableTurnResult::AwaitingApproval { .. }
    ));
    service
        .cancel(&ExecutionRef {
            session_id: "terminal-session".into(),
            turn_id: "cancel-turn".into(),
            execution_id: "cancel-execution".into(),
        })
        .unwrap();
    let cancelled_record = FileSessionStore::new(&session_path)
        .unwrap()
        .load("terminal-session")
        .unwrap();
    assert_eq!(
        cancelled_record.turns[1].status,
        SessionTurnStatus::Cancelled
    );
    std::fs::remove_dir_all(root).unwrap();
}
