//! Session, standalone and rebuilt approval paths use actual execution services.

mod ports;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kolyan_core::{
    ResumeInput, ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture,
    TurnConfig, TurnControl, TurnDeadline, TurnDeadlineError, TurnExecutor, TurnRequest,
};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{
    ContentBlock, Message, ModelEvent, ModelEventStream, ModelProvider, ModelRequest,
    ModelResponse, ProviderFuture, StopReason, TokenUsage,
};
use kolyan_policy::{ApprovalMode, PolicyEngine, PreparedCall, ToolManifest};
use kolyan_runtime::{DurableTurnDriver, DurableTurnResult, RuntimeError};
use kolyan_storage::{FileSessionStore, SessionStore, SessionTurnStatus, StorageError};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::Case;
use crate::{
    CoordinatorError, ExecutionRef, ExecutionServer, ExecutionService, PreparationFailure,
    ServerError, SessionExecutionService, SessionService, TurnPreparationFuture,
    TurnPreparationHook,
};
use ports::{Hook, Provider, SlowLedger, SlowSessionStore, Timeline, Tools};

pub(super) async fn run<L: LedgerStore + Clone + 'static, F: Fn() -> L>(
    case: &Case,
    root: &Path,
    inner: L,
    reopen: F,
) -> Value {
    let timeline = Timeline::new();
    let ledger = SlowLedger {
        inner,
        delay_kind: match case.ledger_delay.as_str() {
            "execution_started" => Some(LedgerEventKind::ExecutionStarted),
            "execution_input_admitted" => Some(LedgerEventKind::ExecutionInputAdmitted),
            _ => None,
        },
        delay_ms: case.delay_ms,
        delayed: Arc::new(AtomicBool::new(false)),
        timeline: timeline.clone(),
    };
    let base = FileSessionStore::new(root.join("sessions")).unwrap();
    base.create("s").unwrap();
    let models = Arc::new(Mutex::new(vec![]));
    let hooks = Arc::new(Mutex::new(vec![]));
    let tools = Arc::new(AtomicUsize::new(0));
    let key = ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    let cancel_ledger = ledger.clone();
    let mode = case.mode.clone();
    let cancel_key = key.clone();
    let store = SlowSessionStore {
        inner: base.clone(),
        load_ms: case.load_ms,
        begin_ms: case.begin_ms,
        loaded: Arc::new(AtomicBool::new(false)),
        fail_commit: Arc::new(AtomicBool::new(case.mode.contains("commit_failure"))),
        timeline: timeline.clone(),
        after_begin: Arc::new(move || {
            if mode == "cancel" || mode == "cancel_commit_failure" {
                ExecutionServer::new(cancel_ledger.clone())
                    .cancel(&cancel_key)
                    .unwrap();
            } else if mode == "foreign_cancel" || mode == "damaged_cancel" {
                cancel_ledger
                    .append(LedgerEvent {
                        event_id: "e/execution-cancelled".into(),
                        idempotency_key: "e/execution-cancelled".into(),
                        execution_id: "e".into(),
                        turn_id: if mode == "foreign_cancel" {
                            "foreign"
                        } else {
                            "t"
                        }
                        .into(),
                        cursor: 0,
                        kind: LedgerEventKind::ExecutionCancelled,
                        payload: if mode == "damaged_cancel" {
                            json!({"forged":true})
                        } else {
                            Value::Null
                        },
                    })
                    .unwrap();
            }
        }),
    };
    let service = SessionExecutionService::new(
        ExecutionService::new(ledger.clone(), NoopTraceSink),
        SessionService::new(store.clone()),
    )
    .with_preparation_hook(Arc::new(Hook {
        seen: hooks.clone(),
        timeline: timeline.clone(),
    }));
    let prepared = crate::suspension::tests::fixture().checkpoint.calls[0]
        .prepared
        .clone()
        .unwrap();
    let approval = matches!(case.mode.as_str(), "approval" | "resume" | "recovery");
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: prepared.call().name.clone(),
        capabilities: prepared.claim().capabilities.clone(),
        effects: prepared.claim().effects.clone(),
        path_scopes: vec![],
        idempotency: prepared.claim().idempotency,
        approval: ApprovalMode::Always,
    });
    let ceiling = if case.mode == "tighter" {
        Some(now_ms() + 30)
    } else {
        None
    };
    let executor = || {
        let executor = TurnExecutor::with_tools(
            Provider {
                seen: models.clone(),
                approval,
                prepared: prepared.clone(),
                timeline: timeline.clone(),
            },
            Tools {
                prepared: prepared.clone(),
                calls: tools.clone(),
            },
        )
        .with_policy_engine(Arc::new(policy.clone()));
        if let Some(ceiling) = ceiling {
            executor.with_absolute_deadline_at_ms(ceiling)
        } else {
            executor
        }
    };
    let request = TurnRequest {
        turn_id: "t".into(),
        config: TurnConfig {
            max_steps: 3,
            max_tool_calls: Some(2),
            deadline: case.deadline_ms.map(Duration::from_millis),
        },
        model_request: serde_json::from_value(
            json!({"request_id":"original","model":{"provider":"fixture","model":"m"},
            "system":[],"messages":[{"role":"user","content":[{"type":"text","text":"current"}]}],
            "tools":[],"tool_choice":"auto","max_output_tokens":100,"extensions":null}),
        )
        .unwrap(),
    };
    let original = json!({"model_request":request.model_request,"deadline_ns":request.config.deadline.map(|value|value.as_nanos())});
    let entry_ms = now_ms();
    timeline.mark("entry");
    let mut explicit_deadline = Value::Null;
    let mut result = match case.entry.as_str() {
        "session" => service.start(executor(), request.clone(), "s", "e").await,
        "execution" => {
            ExecutionService::new(ledger.clone(), NoopTraceSink)
                .start(executor(), request.clone(), "s", "e")
                .await
        }
        "runtime" => DurableTurnDriver::new(ledger.clone(), NoopTraceSink)
            .start(executor(), request.clone(), "s", "e")
            .await
            .map_err(ServerError::from),
        "core" => executor()
            .start_resumable(request.clone())
            .await
            .map(|value| match value {
                kolyan_core::ResumableTurn::Completed(value) => {
                    DurableTurnResult::Completed(value, Default::default())
                }
                _ => panic!("unexpected Core suspension"),
            })
            .map_err(|error| ServerError::Runtime(RuntimeError::Turn(error))),
        "explicit" | "runtime_explicit" | "core_explicit" => {
            let duration = if case.mode == "mismatch" {
                Some(Duration::from_millis(1))
            } else {
                request.config.deadline
            };
            let deadline = TurnDeadline::capture(duration, None).unwrap();
            explicit_deadline = json!(deadline.deadline_at_ms());
            tokio::time::sleep(Duration::from_millis(case.delay_ms)).await;
            match case.entry.as_str() {
                "runtime_explicit" => DurableTurnDriver::new(ledger.clone(), NoopTraceSink)
                    .start_with_deadline(executor(), request.clone(), "s", "e", deadline)
                    .await
                    .map_err(ServerError::from),
                "core_explicit" => executor()
                    .start_resumable_with_deadline(request.clone(), deadline)
                    .await
                    .map(|value| match value {
                        kolyan_core::ResumableTurn::Completed(value) => {
                            DurableTurnResult::Completed(value, Default::default())
                        }
                        _ => panic!("unexpected suspension"),
                    })
                    .map_err(|error| ServerError::Runtime(RuntimeError::Turn(error))),
                _ => {
                    ExecutionService::new(ledger.clone(), NoopTraceSink)
                        .start_with_deadline(executor(), request.clone(), "s", "e", deadline)
                        .await
                }
            }
        }
        _ => unreachable!(),
    };
    let mut checkpoint = Value::Null;
    let mut final_checkpoint_deadline = Value::Null;
    if let Ok(DurableTurnResult::Suspended { suspension, .. }) = &result {
        let saved = suspension.as_ref().clone();
        checkpoint = serde_json::to_value(&saved.checkpoint).unwrap();
        let id = saved.waiting.approvals[0].approval_id.clone();
        let confirmation = crate::suspension::confirm_approval(&ledger, &key, &saved, &id).unwrap();
        if case.mode == "recovery" {
            let scope = saved.checkpoint.scope.clone();
            let merged = executor()
                .with_execution_key(scope.execution.clone())
                .merge_resume_with_control(
                    saved.clone(),
                    ResumeInput::ApprovalConfirmed(confirmation.clone()),
                    scope,
                    TurnControl::default(),
                )
                .unwrap();
            let publication = ledger.events_after(0).unwrap().last().unwrap().cursor;
            let payload =
                json!({"schema_version":1,"publication_cursor":publication,"checkpoint":merged});
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
        }
        tokio::time::sleep(Duration::from_millis(case.deadline_ms.unwrap() + 80)).await;
        let rebuilt = SessionExecutionService::new(
            ExecutionService::new(
                SlowLedger {
                    inner: reopen(),
                    ..ledger.clone()
                },
                NoopTraceSink,
            ),
            SessionService::new(FileSessionStore::new(root.join("sessions")).unwrap()),
        )
        .with_preparation_hook(Arc::new(Hook {
            seen: hooks.clone(),
            timeline: timeline.clone(),
        }));
        result = match case.mode.as_str() {
            "approval" => rebuilt.resume_approval(executor(), "s", "e", &id).await,
            "resume" => {
                rebuilt
                    .resume(
                        executor(),
                        "s",
                        "e",
                        &saved.checkpoint.checkpoint_id,
                        ResumeInput::ApprovalConfirmed(confirmation),
                    )
                    .await
            }
            "recovery" => {
                rebuilt
                    .resume_committed(
                        executor(),
                        "s",
                        "e",
                        checkpoint["checkpoint_id"].as_str().unwrap(),
                    )
                    .await
            }
            _ => unreachable!(),
        };
        final_checkpoint_deadline = ledger
            .events_after(0)
            .unwrap()
            .iter()
            .rev()
            .find(|event| {
                matches!(
                    event.kind,
                    LedgerEventKind::TurnCheckpointPrepared | LedgerEventKind::TurnCheckpointMerged
                )
            })
            .map(|event| event.payload["checkpoint"]["budget"]["deadline_at_ms"].clone())
            .unwrap_or(Value::Null);
    }
    let before_reconcile = base.load("s").unwrap();
    let mut reconcile_error = None;
    if case.mode.contains("commit_failure") {
        let rebuilt = SessionExecutionService::new(
            ExecutionService::new(
                SlowLedger {
                    inner: reopen(),
                    ..ledger.clone()
                },
                NoopTraceSink,
            ),
            SessionService::new(FileSessionStore::new(root.join("sessions")).unwrap()),
        );
        if let Err(error) = rebuilt.load_reconciled("s") {
            reconcile_error = Some(error.to_string());
        }
    }
    let session = base.load("s").unwrap();
    let outcome = match &result {
        Ok(_) => "completed",
        Err(ServerError::Preparation(PreparationFailure::DeadlineExpired)) => "preparation_expired",
        Err(ServerError::Deadline(TurnDeadlineError::DurationMismatch)) => "duration_mismatch",
        Err(ServerError::Runtime(RuntimeError::Deadline(TurnDeadlineError::DurationMismatch))) => {
            "duration_mismatch"
        }
        Err(ServerError::Runtime(RuntimeError::Turn(kolyan_core::TurnError::Deadline(
            TurnDeadlineError::DurationMismatch,
        )))) => "duration_mismatch",
        Err(ServerError::Runtime(RuntimeError::Turn(kolyan_core::TurnError::TimedOut))) => {
            "runtime_timeout"
        }
        Err(ServerError::Coordinator(CoordinatorError::Terminal {
            state: crate::ExecutionState::Cancelled,
            ..
        })) => "cancelled",
        Err(ServerError::Coordinator(CoordinatorError::Ledger(_))) => "invalid_cancel",
        Err(ServerError::Session(_)) => "storage",
        Err(_) => "other",
    };
    let events = ledger.events_after(0).unwrap();
    timeline.mark("finished");
    json!({"id":case.id,"original":original,"entry_ms":entry_ms,"executor_ceiling":ceiling,
        "timeline":*timeline.events.lock().unwrap(),
        "explicit_deadline":explicit_deadline,"outcome":outcome,"error":result.err().map(|value|value.to_string()),
        "models":*models.lock().unwrap(),"hooks":*hooks.lock().unwrap(),"tools":tools.load(Ordering::SeqCst),
        "ledger":events,"session":session,"checkpoint":checkpoint,"final_checkpoint_deadline":final_checkpoint_deadline,
        "status":session.turns.first().map(|turn|turn.status),"before_reconcile_status":before_reconcile.turns.first().map(|turn|turn.status),
        "reconcile_error":reconcile_error})
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
