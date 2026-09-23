use kolyan_ledger::{FileLedger, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_server::{AdmissionKind, ExecutionRef, ExecutionServer, ExecutionState};

fn execution() -> ExecutionRef {
    ExecutionRef {
        session_id: "server-session".into(),
        turn_id: "server-turn".into(),
        execution_id: "server-execution".into(),
    }
}

#[test]
fn file_ledger_server_recovers_running_execution_after_restart() {
    let root =
        std::env::temp_dir().join(format!("kolyan-server-coordinator-{}", std::process::id()));
    let path = root.join("ledger.jsonl");
    let first = ExecutionServer::new(FileLedger::open(&path).unwrap());
    let execution = execution();
    assert_eq!(
        first.start(execution.clone()).unwrap().kind,
        AdmissionKind::Start
    );
    assert_eq!(
        first.state(&execution.execution_id).unwrap(),
        ExecutionState::Running
    );
    first.release(&execution.execution_id);
    drop(first);

    let restarted = ExecutionServer::new(FileLedger::open(&path).unwrap());
    assert_eq!(
        restarted.recover(execution.clone()).unwrap().kind,
        AdmissionKind::Recover
    );
    restarted.cancel(&execution).unwrap();
    assert_eq!(
        restarted.state(&execution.execution_id).unwrap(),
        ExecutionState::Cancelled
    );
    let events = restarted.coordinator().ledger().events_after(0).unwrap();
    assert!(events.iter().any(|event| {
        event.kind == LedgerEventKind::ExecutionStarted && event.execution_id == "server-execution"
    }));
    assert!(events.iter().any(|event| {
        event.kind == LedgerEventKind::ExecutionCancelled
            && event.execution_id == "server-execution"
    }));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn terminal_execution_cannot_be_started_again() {
    let ledger = kolyan_ledger::InMemoryLedger::default();
    ledger
        .append(LedgerEvent {
            event_id: "completed".into(),
            turn_id: "turn-terminal".into(),
            execution_id: "execution-terminal".into(),
            cursor: 0,
            kind: LedgerEventKind::TurnCompleted,
            idempotency_key: "completed".into(),
            payload: serde_json::Value::Null,
        })
        .unwrap();
    let server = ExecutionServer::new(ledger);
    let execution = ExecutionRef {
        session_id: "session-terminal".into(),
        turn_id: "turn-terminal".into(),
        execution_id: "execution-terminal".into(),
    };
    assert!(matches!(
        server.start(execution),
        Err(kolyan_server::CoordinatorError::Terminal { .. })
    ));
}
