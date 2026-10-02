//! Independent driver handles must publish a real stopped state, not a boundary.
use super::*;
use kolyan_ledger::{FileLedger, SqliteLedger};

async fn released_runtime_publication<L: LedgerStore + Clone + 'static>(ledger: L) {
    let server = ExecutionServer::new(ledger.clone());
    let key = ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    server.start(key.clone()).unwrap();
    let saved = crate::suspension::tests::persist_approval(ledger.clone()).await;
    assert_eq!(server.state("e").unwrap(), ExecutionState::Running);
    server.release("e");
    assert_eq!(server.state("e").unwrap(), ExecutionState::Suspended);
    assert_eq!(
        ExecutionServer::new(ledger.clone()).state("e").unwrap(),
        ExecutionState::Suspended
    );
    assert_eq!(
        crate::execution_suspension(&key, &ledger.execution_events_after("e", 0).unwrap()).unwrap(),
        Some(saved.clone())
    );

    // A new pre-effect barrier is recoverable work, never a stopped publication.
    let publication = ledger
        .execution_events_after("e", 0)
        .unwrap()
        .last()
        .unwrap()
        .cursor;
    let payload =
        json!({"schema_version":1,"publication_cursor":publication,"checkpoint":saved.checkpoint});
    use sha2::{Digest, Sha256};
    let id = format!(
        "e/checkpoint/prepared/{:x}",
        Sha256::digest(serde_json::to_vec(&payload).unwrap())
    );
    ledger
        .append(LedgerEvent {
            event_id: id.clone(),
            idempotency_key: id,
            execution_id: "e".into(),
            turn_id: "t".into(),
            cursor: 0,
            kind: LedgerEventKind::TurnCheckpointPrepared,
            payload,
        })
        .unwrap();
    assert_eq!(server.state("e").unwrap(), ExecutionState::Running);
    assert!(
        crate::execution_suspension(&key, &ledger.execution_events_after("e", 0).unwrap())
            .unwrap()
            .is_none()
    );
    assert!(server.resume(key.clone()).is_err());
    server.cancel(&key).unwrap();
    assert_eq!(server.state("e").unwrap(), ExecutionState::Cancelled);
    assert!(server.recover(key).is_err());
}

#[tokio::test]
async fn file_independent_runtime_handle_release_preserves_compound_suspension() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.jsonl");
    // Driver and coordinator deliberately open the file independently.
    let server = ExecutionServer::new(FileLedger::open(&path).unwrap());
    let key = ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    server.start(key).unwrap();
    crate::suspension::tests::persist_approval(FileLedger::open(&path).unwrap()).await;
    server.release("e");
    assert_eq!(server.state("e").unwrap(), ExecutionState::Suspended);
    assert_eq!(
        ExecutionServer::new(FileLedger::open(&path).unwrap())
            .state("e")
            .unwrap(),
        ExecutionState::Suspended
    );
}

#[tokio::test]
async fn memory_and_sqlite_prepared_barrier_is_not_a_stopped_or_cancel_revival() {
    released_runtime_publication(InMemoryLedger::default()).await;
    let directory = tempfile::tempdir().unwrap();
    released_runtime_publication(
        SqliteLedger::open(directory.path().join("ledger.sqlite")).unwrap(),
    )
    .await;
}
