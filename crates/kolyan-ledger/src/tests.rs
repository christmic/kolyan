use super::*;

#[test]
fn cancellation_and_admission_share_one_storage_order() {
    let root = std::env::temp_dir().join(format!("kolyan-ledger-atomic-{}", std::process::id()));
    let stores: Vec<Box<dyn LedgerStore>> = vec![
        Box::new(InMemoryLedger::default()),
        Box::new(FileLedger::open(root.join("events.jsonl")).unwrap()),
        Box::new(SqliteLedger::open(root.join("events.sqlite")).unwrap()),
    ];
    for store in &stores {
        store
            .append_unless_cancelled(event("admitted", LedgerEventKind::StepStarted))
            .unwrap();
        store
            .append(event("cancel", LedgerEventKind::ExecutionCancelled))
            .unwrap();
        assert!(matches!(
            store.append_unless_cancelled(event("blocked", LedgerEventKind::EffectStarted)),
            Err(LedgerError::Cancelled(_))
        ));
        assert_eq!(store.events_after(0).unwrap().len(), 2);
    }
    drop(stores);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn independently_opened_file_ledgers_share_claims_and_unique_append() {
    let root =
        std::env::temp_dir().join(format!("kolyan-ledger-concurrent-{}", std::process::id()));
    let path = root.join("events.jsonl");
    let first = FileLedger::open(&path).unwrap();
    assert!(first.claim("durable-claim").unwrap());
    drop(first);
    assert!(
        !FileLedger::open(&path)
            .unwrap()
            .claim("durable-claim")
            .unwrap()
    );
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let workers = (0..8)
        .map(|_| {
            let ledger = FileLedger::open(&path).unwrap();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                ledger
                    .append(event("same", LedgerEventKind::EffectStarted))
                    .is_ok()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        workers
            .into_iter()
            .map(|w| usize::from(w.join().unwrap()))
            .sum::<usize>(),
        1
    );
    assert_eq!(
        FileLedger::open(&path)
            .unwrap()
            .events_after(0)
            .unwrap()
            .len(),
        1
    );
    fs::remove_dir_all(root).unwrap();
}

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
