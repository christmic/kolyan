use futures_util::stream;
use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FileLedger, LeaseStore, LedgerEventKind, LedgerStore, SqliteLedger};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRef, ModelRequest,
    ModelResponse, ProviderFuture, StopReason, TokenUsage, ToolChoice,
};
use kolyan_runtime::{
    AdmissionDecision, AdmissionPort, DurableTurnDriver, DurableTurnResult, EffectExecutor,
    EffectGrant, EffectOutcome, EffectReceipt, EffectRequest, ExecutionKey, ExecutionRuntime,
    ReceiptStatus, RuntimeExecutionError,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
struct GrantOnFirstUse {
    decisions: Arc<AtomicUsize>,
}

impl AdmissionPort for GrantOnFirstUse {
    fn decide(
        &self,
        _: &ExecutionKey,
        effect: &EffectRequest,
    ) -> Result<AdmissionDecision, RuntimeExecutionError> {
        self.decisions.fetch_add(1, Ordering::SeqCst);
        Ok(AdmissionDecision::Grant(EffectGrant {
            authorization_id: format!("auth-{}", effect.effect_id),
            effect_id: effect.effect_id.clone(),
            input_digest: effect.input_digest.clone(),
            constraints_digest: "constraints-v1".into(),
            authority_revision: effect.policy_revision.clone(),
        }))
    }
}

#[derive(Clone)]
struct CountingExecutor {
    executions: Arc<AtomicUsize>,
}

impl EffectExecutor for CountingExecutor {
    fn execute(
        &self,
        _: &ExecutionKey,
        effect: &EffectRequest,
        grant: &EffectGrant,
    ) -> Result<EffectOutcome, RuntimeExecutionError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Ok(EffectOutcome::Completed {
            receipt: EffectReceipt {
                receipt_id: format!("receipt-{}", effect.effect_id),
                effect_id: effect.effect_id.clone(),
                authorization_id: grant.authorization_id.clone(),
                input_digest: effect.input_digest.clone(),
                executor_id: "test-executor".into(),
                executor_revision: "test-executor-v1".into(),
                result_digest: "result-digest".into(),
                status: ReceiptStatus::Completed,
            },
            output: json!({"ok": true, "value": 42}),
        })
    }
}

fn key() -> ExecutionKey {
    ExecutionKey {
        session_id: "session-real-1".into(),
        turn_id: "turn-real-1".into(),
        execution_id: "execution-real-1".into(),
    }
}

fn effect() -> EffectRequest {
    EffectRequest {
        effect_id: "effect-real-1".into(),
        operation_kind: "filesystem.write".into(),
        input_digest: "input-digest".into(),
        requirements: vec!["filesystem_write".into()],
        policy_revision: "policy-v1".into(),
    }
}

#[test]
fn file_ledger_runtime_reopens_and_does_not_repeat_completed_effect() {
    let root = std::env::temp_dir().join(format!("kolyan-runtime-real-{}", std::process::id()));
    let file = root.join("session-real-1.jsonl");
    let decisions = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let first = ExecutionRuntime::new(
        FileLedger::open(&file).unwrap(),
        GrantOnFirstUse {
            decisions: decisions.clone(),
        },
        CountingExecutor {
            executions: executions.clone(),
        },
    );
    let execution_key = key();
    let requested = effect();
    first.start(&execution_key).unwrap();
    let result = first.apply_effect(&execution_key, &requested).unwrap();
    assert!(matches!(
        result,
        kolyan_runtime::EffectDisposition::Completed { .. }
    ));
    drop(first);

    let reopened = ExecutionRuntime::new(
        FileLedger::open(&file).unwrap(),
        GrantOnFirstUse {
            decisions: decisions.clone(),
        },
        CountingExecutor {
            executions: executions.clone(),
        },
    );
    let replayed = reopened.apply_effect(&execution_key, &requested).unwrap();
    assert!(
        matches!(replayed, kolyan_runtime::EffectDisposition::Completed { output } if output == json!({"ok": true, "value": 42}))
    );
    assert_eq!(decisions.load(Ordering::SeqCst), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(
        reopened.status(&execution_key).unwrap(),
        Some(kolyan_runtime::ExecutionStatus::Completed)
    );
    let events = reopened.ledger().events_after(0).unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == LedgerEventKind::EffectReceipt)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == LedgerEventKind::EffectStarted)
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_is_durable_before_a_new_effect_is_admitted() {
    let root = std::env::temp_dir().join(format!("kolyan-runtime-cancel-{}", std::process::id()));
    let ledger = FileLedger::open(root.join("ledger.jsonl")).unwrap();
    let decisions = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let runtime = ExecutionRuntime::new(
        ledger,
        GrantOnFirstUse {
            decisions: decisions.clone(),
        },
        CountingExecutor {
            executions: executions.clone(),
        },
    );
    let execution_key = key();
    runtime.start(&execution_key).unwrap();
    runtime.cancel(&execution_key).unwrap();
    assert!(matches!(
        runtime.apply_effect(&execution_key, &effect()).unwrap(),
        kolyan_runtime::EffectDisposition::Cancelled
    ));
    assert_eq!(decisions.load(Ordering::SeqCst), 0);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[derive(Clone)]
struct FinalProvider;

impl ModelProvider for FinalProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let response = ModelResponse {
            id: request.request_id.clone(),
            model: request.model,
            content: vec![ContentBlock::Text {
                text: "runtime done".into(),
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

fn turn_request() -> TurnRequest {
    TurnRequest {
        turn_id: "turn-driver-real".into(),
        model_request: ModelRequest {
            request_id: "request-driver-real".into(),
            model: ModelRef::new("fixture", "runtime"),
            system: Vec::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: Value::Null,
        },
        config: TurnConfig::default(),
    }
}

#[tokio::test]
async fn durable_turn_driver_records_real_core_boundaries() {
    let root = std::env::temp_dir().join(format!("kolyan-runtime-driver-{}", std::process::id()));
    let file = root.join("ledger.jsonl");
    let driver = DurableTurnDriver::new(
        FileLedger::open(&file).unwrap(),
        kolyan_trace::VecTraceSink::default(),
    );
    let result = driver
        .start(
            TurnExecutor::new(FinalProvider),
            turn_request(),
            "session-driver-real",
            "execution-driver-real",
        )
        .await
        .unwrap();
    assert!(matches!(result, DurableTurnResult::Completed(_, _)));
    let events = driver.ledger().events_after(0).unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == LedgerEventKind::ExecutionStarted)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == LedgerEventKind::StepStarted)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == LedgerEventKind::TurnCompleted)
    );
    drop(driver);
    let reopened = FileLedger::open(&file).unwrap();
    assert!(reopened.events_after(0).unwrap().len() >= 4);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sqlite_ledger_reopens_and_fences_execution_leases() {
    let root = std::env::temp_dir().join(format!("kolyan-runtime-sqlite-{}", std::process::id()));
    let file = root.join("ledger.sqlite3");
    let ledger = SqliteLedger::open(&file).unwrap();
    let event = kolyan_ledger::LedgerEvent {
        event_id: "sqlite-event-1".into(),
        turn_id: "turn-sqlite".into(),
        execution_id: "execution-sqlite".into(),
        cursor: 0,
        kind: LedgerEventKind::ExecutionStarted,
        idempotency_key: "sqlite-event-1".into(),
        payload: Value::Null,
    };
    assert_eq!(ledger.append(event.clone()).unwrap().cursor, 1);
    assert!(ledger.claim("claim-1").unwrap());
    assert!(!ledger.claim("claim-1").unwrap());
    let lease = ledger
        .acquire_lease("execution-sqlite", "owner-a", 100, 10)
        .unwrap();
    assert_eq!(lease.revision, 1);
    assert!(
        ledger
            .acquire_lease("execution-sqlite", "owner-b", 105, 10)
            .is_err()
    );
    let taken = ledger
        .acquire_lease("execution-sqlite", "owner-b", 111, 10)
        .unwrap();
    assert_eq!(taken.revision, 2);
    assert!(ledger.renew_lease(&lease, 112, 10).is_err());
    drop(ledger);
    let reopened = SqliteLedger::open(&file).unwrap();
    assert_eq!(reopened.events_after(0).unwrap().len(), 1);
    assert!(reopened.release_lease(&taken).is_ok());
    std::fs::remove_dir_all(root).unwrap();
}
