//! Recovery must not depend on an unrelated execution's global history.

use crate::*;
use kolyan_ledger::{InMemoryLedger, LedgerQuery};
use kolyan_storage::{FileSessionStore, SessionStore};
use kolyan_trace::NoopTraceSink;

#[derive(Clone, Default)]
struct ScopedLedger {
    inner: InMemoryLedger,
    reads: Arc<Mutex<Vec<LedgerQuery>>>,
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
        self.reads.lock().unwrap().push(query.clone());
        self.inner.query(query)
    }

    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        Err(LedgerError::Storage("global audit disabled".into()))
    }

    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.inner.claim(key)
    }
}

fn fact(id: &str, execution: &str, kind: LedgerEventKind) -> LedgerEvent {
    LedgerEvent {
        event_id: id.into(),
        turn_id: format!("turn-{execution}"),
        execution_id: execution.into(),
        cursor: 0,
        kind,
        idempotency_key: id.into(),
        payload: Value::Null,
    }
}

#[test]
fn coordinator_recovers_only_requested_execution_without_global_audit() {
    let ledger = ScopedLedger::default();
    ledger
        .append(fact(
            "foreign-done",
            "other",
            LedgerEventKind::TurnCompleted,
        ))
        .unwrap();
    let key = ExecutionRef {
        session_id: "session".into(),
        turn_id: "turn-target".into(),
        execution_id: "target".into(),
    };
    let server = ExecutionServer::new(ledger.clone());
    server.start(key.clone()).unwrap();
    server.release("target");
    ledger
        .append(fact("pause", "target", LedgerEventKind::ExecutionSuspended))
        .unwrap();
    let rebuilt = ExecutionServer::new(ledger.clone());
    assert_eq!(rebuilt.state("target").unwrap(), ExecutionState::Suspended);
    rebuilt.resume(key.clone()).unwrap();
    assert_eq!(rebuilt.state("target").unwrap(), ExecutionState::Running);
    rebuilt.cancel(&key).unwrap();
    assert_eq!(rebuilt.state("target").unwrap(), ExecutionState::Cancelled);
    assert!(ledger.reads.lock().unwrap().iter().all(|query| {
        query.execution_id.as_deref() == Some("target")
            || query
                .event_id
                .as_deref()
                .is_some_and(|id| id.starts_with("target/"))
    }));
}

#[test]
fn session_projection_repair_does_not_read_or_commit_foreign_session() {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    let ledger = ScopedLedger::default();
    for id in ["target", "other"] {
        store.create(id).unwrap();
        store
            .begin_turn_with_input(
                id,
                SessionTurn {
                    turn_id: format!("turn-{id}"),
                    execution_id: id.into(),
                    status: SessionTurnStatus::Running,
                },
                0,
                vec![],
            )
            .unwrap();
        ledger
            .append(fact(
                &format!("{id}-failed"),
                id,
                LedgerEventKind::TurnFailed,
            ))
            .unwrap();
    }
    let foreign_before = store.load("other").unwrap();
    let service = SessionExecutionService::new(
        ExecutionService::new(ledger.clone(), NoopTraceSink),
        SessionService::new(store.clone()),
    );
    assert_eq!(
        service.load_reconciled("target").unwrap().turns[0].status,
        SessionTurnStatus::Failed
    );
    assert_eq!(store.load("other").unwrap(), foreign_before);
    assert!(ledger.reads.lock().unwrap().iter().all(|query| {
        query.execution_id.as_deref() == Some("target")
            || query
                .event_id
                .as_deref()
                .is_some_and(|id| id.starts_with("target/"))
    }));
}
