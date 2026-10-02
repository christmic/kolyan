mod decision_gap;
mod denial;
mod merge_gap;
mod pending_denial;
mod preparation;

use super::*;
use kolyan_ledger::InMemoryLedger;
use kolyan_storage::FileSessionStore;
use kolyan_trace::VecTraceSink;

#[test]
fn admission_rejection_and_cancel_request_do_not_close_active_session() {
    let root = std::env::temp_dir().join(format!("kolyan-admission-{}", std::process::id()));
    let store = FileSessionStore::new(&root).unwrap();
    store.create("s").unwrap();
    store
        .begin_turn_with_input(
            "s",
            SessionTurn {
                turn_id: "t".into(),
                execution_id: "e".into(),
                status: SessionTurnStatus::Running,
            },
            0,
            vec![],
        )
        .unwrap();
    let ledger = InMemoryLedger::default();
    let service = SessionExecutionService::new(
        ExecutionService::new(ledger.clone(), VecTraceSink::default()),
        SessionService::new(store.clone()),
    );
    let before = store.load("s").unwrap();
    let error = ServerError::Coordinator(CoordinatorError::AlreadyActive {
        execution_id: "e".into(),
    });
    assert!(service.commit_result("s", "t", vec![], Err(error)).is_err());
    assert_eq!(store.load("s").unwrap(), before);
    for (id, kind) in [
        ("start", LedgerEventKind::ExecutionStarted),
        ("cancel-request", LedgerEventKind::ExecutionCancelled),
    ] {
        ledger
            .append(LedgerEvent {
                event_id: id.into(),
                turn_id: "t".into(),
                execution_id: "e".into(),
                cursor: 0,
                kind,
                idempotency_key: id.into(),
                payload: Value::Null,
            })
            .unwrap();
    }
    assert!(service.reconcile("s", "e").is_err());
    assert_eq!(store.load("s").unwrap(), before);
    ledger
        .append(LedgerEvent {
            event_id: "stopped".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
            cursor: 0,
            kind: LedgerEventKind::TurnCancelled,
            idempotency_key: "stopped".into(),
            payload: Value::Null,
        })
        .unwrap();
    assert_eq!(
        service.reconcile("s", "e").unwrap().turns[0].status,
        SessionTurnStatus::Cancelled
    );
    std::fs::remove_dir_all(root).unwrap();
}
