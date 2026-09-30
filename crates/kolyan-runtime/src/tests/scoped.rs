//! Recovery must work when the global audit port is unavailable.

mod approval;

use super::{FinalProvider, request};
use crate::*;
use kolyan_ledger::{InMemoryLedger, LedgerQuery};
use kolyan_trace::NoopTraceSink;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Clone, Default)]
pub(crate) struct ScopedLedger {
    inner: InMemoryLedger,
    queries: Arc<Mutex<Vec<LedgerQuery>>>,
    global_reads: Arc<AtomicUsize>,
    fail_queries: Arc<AtomicBool>,
}

impl ScopedLedger {
    pub(crate) fn assert_scoped_reads(&self) {
        assert_eq!(self.global_reads.load(Ordering::SeqCst), 0);
        let queries = self.queries.lock().unwrap();
        assert!(!queries.is_empty());
        assert!(
            queries
                .iter()
                .all(|query| { query.execution_id.is_some() || query.event_id.is_some() })
        );
    }
}

impl LedgerStore for ScopedLedger {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.inner.append(event)
    }

    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.inner.append_unless_cancelled(event)
    }

    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        assert!(query.execution_id.is_some() || query.event_id.is_some());
        self.queries.lock().unwrap().push(query.clone());
        if self.fail_queries.load(Ordering::SeqCst) {
            return Err(LedgerError::Storage("scoped query unavailable".into()));
        }
        self.inner.query(query)
    }

    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.global_reads.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage("global audit unavailable".into()))
    }

    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.inner.claim(key)
    }
}

#[tokio::test]
async fn durable_start_projection_and_cancel_use_only_scoped_reads() {
    let ledger = ScopedLedger::default();
    // An unrelated execution has both a cancellation and matching trajectory prefix.
    ledger
        .append(LedgerEvent {
            event_id: "target/turn-event/unrelated".into(),
            turn_id: "other-turn".into(),
            execution_id: "other".into(),
            cursor: 0,
            kind: LedgerEventKind::ExecutionCancelled,
            idempotency_key: "unrelated".into(),
            payload: json!({"unrelated": true}),
        })
        .unwrap();
    let driver = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    let result = driver
        .start(
            kolyan_core::TurnExecutor::new(FinalProvider),
            request("turn"),
            "session",
            "target",
        )
        .await
        .unwrap();
    let DurableTurnResult::Completed(_, trajectory) = result else {
        panic!("expected completion");
    };
    assert!(!trajectory.records.is_empty());
    assert!(
        trajectory
            .records
            .windows(2)
            .all(|rows| rows[0].sequence < rows[1].sequence)
    );
    assert!(
        trajectory
            .records
            .iter()
            .all(|row| row.payload.get("unrelated").is_none())
    );
    drop(driver);
    let rebuilt = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    rebuilt.cancel("cancelled", "cancelled-turn").unwrap();
    assert!(matches!(
        rebuilt
            .start(
                kolyan_core::TurnExecutor::new(FinalProvider),
                request("cancelled-turn"),
                "session",
                "cancelled",
            )
            .await,
        Err(RuntimeError::Turn(kolyan_core::TurnError::Cancelled))
    ));
    let events = ledger.execution_events_after("cancelled", 0).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|row| row.kind == LedgerEventKind::TurnCancelled)
            .count(),
        1
    );
    assert!(
        !events
            .iter()
            .any(|row| row.kind == LedgerEventKind::TurnCompleted)
    );
    ledger.assert_scoped_reads();
}

struct Allow;
impl AdmissionPort for Allow {
    fn decide(
        &self,
        _: &ExecutionKey,
        effect: &EffectRequest,
    ) -> Result<AdmissionDecision, RuntimeExecutionError> {
        Ok(AdmissionDecision::Grant(EffectGrant {
            authorization_id: "auth".into(),
            effect_id: effect.effect_id.clone(),
            input_digest: effect.input_digest.clone(),
            constraints_digest: "constraints".into(),
            authority_revision: "1".into(),
        }))
    }
}

struct CountingEffect(Arc<AtomicUsize>);
impl EffectExecutor for CountingEffect {
    fn execute(
        &self,
        _: &ExecutionKey,
        effect: &EffectRequest,
        grant: &EffectGrant,
    ) -> Result<EffectOutcome, RuntimeExecutionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(EffectOutcome::Completed {
            receipt: EffectReceipt {
                receipt_id: "receipt".into(),
                effect_id: effect.effect_id.clone(),
                authorization_id: grant.authorization_id.clone(),
                input_digest: effect.input_digest.clone(),
                executor_id: "fixture".into(),
                executor_revision: "1".into(),
                result_digest: "result".into(),
                status: ReceiptStatus::Completed,
            },
            output: json!({"result": "target"}),
        })
    }
}

#[test]
fn effect_receipt_recovery_and_cancel_are_execution_scoped() {
    let ledger = ScopedLedger::default();
    let count = Arc::new(AtomicUsize::new(0));
    let key = ExecutionKey {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "target".into(),
    };
    let other = ExecutionKey {
        execution_id: "other".into(),
        ..key.clone()
    };
    let effect = EffectRequest {
        effect_id: "effect".into(),
        operation_kind: "write".into(),
        input_digest: "input".into(),
        requirements: vec![],
        policy_revision: "1".into(),
    };
    let runtime = ExecutionRuntime::new(ledger.clone(), Allow, CountingEffect(count.clone()));
    runtime.start(&other).unwrap();
    runtime.cancel(&other).unwrap();
    runtime.start(&key).unwrap();
    // Force status and terminal recovery past the Ledger query page boundary.
    for index in 0..1030 {
        ledger
            .append(LedgerEvent {
                event_id: format!("target/observation/{index}"),
                turn_id: key.turn_id.clone(),
                execution_id: key.execution_id.clone(),
                cursor: 0,
                kind: LedgerEventKind::ModelStreamEvent,
                idempotency_key: format!("observation/{index}"),
                payload: Value::Null,
            })
            .unwrap();
    }
    assert_eq!(
        runtime.status(&key).unwrap(),
        Some(ExecutionStatus::Running)
    );
    let expected = runtime.apply_effect(&key, &effect).unwrap();
    assert_eq!(
        expected,
        EffectDisposition::Completed {
            output: json!({"result": "target"})
        }
    );
    drop(runtime);
    let rebuilt = ExecutionRuntime::new(ledger.clone(), Allow, CountingEffect(count.clone()));
    assert_eq!(
        rebuilt.status(&key).unwrap(),
        Some(ExecutionStatus::Completed)
    );
    assert_eq!(rebuilt.apply_effect(&key, &effect).unwrap(), expected);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    rebuilt.cancel(&key).unwrap();
    assert_eq!(
        rebuilt.status(&key).unwrap(),
        Some(ExecutionStatus::Cancelled)
    );
    assert_eq!(
        rebuilt.apply_effect(&key, &effect).unwrap(),
        EffectDisposition::Cancelled
    );
    let mut conflicting = key.clone();
    conflicting.turn_id = "different".into();
    assert!(matches!(
        rebuilt.start(&conflicting),
        Err(RuntimeExecutionError::Invalid(_))
    ));
    ledger.assert_scoped_reads();
}

#[test]
fn exact_event_lookup_rejects_foreign_identity_collisions() {
    let ledger = ScopedLedger::default();
    let key = ExecutionKey {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "target".into(),
    };
    for (suffix, kind, payload) in [
        (
            "execution-started",
            LedgerEventKind::ExecutionStarted,
            json!(key),
        ),
        (
            "execution-cancelled",
            LedgerEventKind::ExecutionCancelled,
            Value::Null,
        ),
    ] {
        ledger
            .append(LedgerEvent {
                event_id: format!("target/{suffix}"),
                turn_id: key.turn_id.clone(),
                execution_id: "foreign".into(),
                cursor: 0,
                kind,
                idempotency_key: format!("foreign/{suffix}"),
                payload,
            })
            .unwrap();
    }
    let runtime = ExecutionRuntime::new(
        ledger.clone(),
        Allow,
        CountingEffect(Arc::new(AtomicUsize::new(0))),
    );
    assert!(
        matches!(runtime.start(&key), Err(RuntimeExecutionError::Invalid(message)) if message.contains("conflicting event identity"))
    );
    let driver = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    assert!(
        matches!(driver.cancel("target", "turn"), Err(RuntimeError::Driver(message)) if message.contains("conflicting event identity"))
    );
    assert!(
        ledger
            .execution_events_after("target", 0)
            .unwrap()
            .is_empty()
    );
    ledger.assert_scoped_reads();
}

#[tokio::test]
async fn scoped_query_failure_propagates_before_start_or_dispatch() {
    let ledger = ScopedLedger::default();
    ledger.fail_queries.store(true, Ordering::SeqCst);
    let driver = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    assert!(matches!(driver.start(
        kolyan_core::TurnExecutor::new(FinalProvider), request("turn"), "session", "target",
    ).await, Err(RuntimeError::Ledger(LedgerError::Storage(message))) if message == "scoped query unavailable"));
    let count = Arc::new(AtomicUsize::new(0));
    let runtime = ExecutionRuntime::new(ledger.clone(), Allow, CountingEffect(count.clone()));
    let key = ExecutionKey {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "target".into(),
    };
    assert!(matches!(
        runtime.start(&key),
        Err(RuntimeExecutionError::Ledger(LedgerError::Storage(_)))
    ));
    let effect = EffectRequest {
        effect_id: "effect".into(),
        operation_kind: "write".into(),
        input_digest: "input".into(),
        requirements: vec![],
        policy_revision: "1".into(),
    };
    assert!(matches!(
        runtime.apply_effect(&key, &effect),
        Err(RuntimeExecutionError::Ledger(LedgerError::Storage(_)))
    ));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        ledger
            .inner
            .execution_events_after("target", 0)
            .unwrap()
            .is_empty()
    );
    ledger.assert_scoped_reads();
}
