use super::*;

fn event(id: &str, kind: LedgerEventKind) -> LedgerEvent {
    LedgerEvent {
        event_id: id.into(),
        turn_id: "turn-1".into(),
        execution_id: "execution-1".into(),
        cursor: 0,
        kind,
        idempotency_key: id.into(),
        payload: Value::Null,
    }
}

#[test]
fn assigns_monotonic_cursors_and_replays_after_cursor() {
    let ledger = InMemoryLedger::default();
    assert_eq!(
        ledger
            .append(event("e1", LedgerEventKind::TurnStarted))
            .unwrap()
            .cursor,
        1
    );
    assert_eq!(
        ledger
            .append(event("e2", LedgerEventKind::TurnCompleted))
            .unwrap()
            .cursor,
        2
    );
    assert_eq!(ledger.events_after(1).unwrap().len(), 1);
}

#[test]
fn claims_are_idempotent() {
    let ledger = InMemoryLedger::default();
    assert!(ledger.claim("turn-1/step-1/call-1").unwrap());
    assert!(!ledger.claim("turn-1/step-1/call-1").unwrap());
}

#[test]
fn file_ledger_reopens_and_replays() {
    let root = std::env::temp_dir().join(format!("kolyan-ledger-{}", std::process::id()));
    let file = root.join("ledger.jsonl");
    let first = FileLedger::open(&file).unwrap();
    first
        .append(event("e1", LedgerEventKind::ExecutionStarted))
        .unwrap();
    drop(first);
    let second = FileLedger::open(&file).unwrap();
    let events = second.events_after(0).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].cursor, 1);
    assert!(!second.claim("e1").unwrap());
    fs::remove_dir_all(root).unwrap();
}
