use super::*;
use kolyan_ledger::{InMemoryLedger, LedgerEvent};
use kolyan_trace::{TraceError, TraceSink};

fn binding() -> ExecutionBinding {
    ExecutionBinding {
        task_id: "task".into(),
        invocation_id: "invocation".into(),
        attempt_id: "attempt".into(),
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "execution".into(),
    }
}

fn event(id: &str, execution: &str) -> LedgerEvent {
    LedgerEvent {
        event_id: id.into(),
        turn_id: "turn".into(),
        execution_id: execution.into(),
        cursor: 0,
        kind: LedgerEventKind::ModelRequested,
        idempotency_key: id.into(),
        payload: json!({"step_id":"step", "request":{"text":"secret", "reasoning":"thought", "arguments":"private"}}),
    }
}

#[test]
fn bounded_linked_export_isolated_and_redacted_without_mutation() {
    let ledger = InMemoryLedger::default();
    ledger.append(event("foreign", "other")).unwrap();
    ledger.append(event("first", "execution")).unwrap();
    ledger.append(event("second", "execution")).unwrap();
    let original = ledger.events_after(0).unwrap();
    let page = LinkedTrajectory::load(&ledger, &binding(), 0, 1, ContentPolicy::Full).unwrap();
    assert_eq!(page.records.len(), 1);
    let row = &page.records[0];
    assert_eq!(row.event_id, "first");
    assert_eq!(row.cursor, 2);
    assert_eq!(row.step_id.as_deref(), Some("step"));
    let next = LinkedTrajectory::load(
        &ledger,
        &binding(),
        row.cursor,
        1,
        ContentPolicy::MetadataOnly,
    )
    .unwrap();
    assert_eq!(next.records[0].event_id, "second");
    let text = serde_json::to_string(&next).unwrap();
    for secret in ["secret", "thought", "private"] {
        assert!(!text.contains(secret));
    }
    assert_eq!(ledger.events_after(0).unwrap(), original);
    assert!(LinkedTrajectory::load(&ledger, &binding(), 0, 0, ContentPolicy::Full).is_err());
    let mut missing = binding();
    missing.execution_id = "missing".into();
    assert!(
        LinkedTrajectory::load(&ledger, &missing, 0, 1, ContentPolicy::Full)
            .unwrap()
            .records
            .is_empty()
    );
}

struct BrokenLedger(Vec<LedgerEvent>);
impl LedgerStore for BrokenLedger {
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        assert_eq!(query.execution_id.as_deref(), Some("execution"));
        Ok(self.0.clone())
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        panic!("global read prohibited")
    }
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        panic!("write prohibited")
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        panic!("claim prohibited")
    }
}

#[test]
fn invalid_query_results_and_recorded_step_fail_closed() {
    let mut valid = event("first", "execution");
    valid.cursor = 1;
    let mut foreign = valid.clone();
    foreign.execution_id = "other".into();
    let mut wrong_turn = valid.clone();
    wrong_turn.turn_id = "other".into();
    let mut empty = valid.clone();
    empty.event_id.clear();
    let mut zero = valid.clone();
    zero.cursor = 0;
    let mut invalid_step = valid.clone();
    invalid_step.payload["step_id"] = json!(42);
    let mut invalid_binding = valid.clone();
    invalid_binding.payload["binding"] = json!({"task_id": "other"});
    for events in [
        vec![foreign],
        vec![wrong_turn],
        vec![empty],
        vec![zero],
        vec![invalid_step],
        vec![invalid_binding],
        vec![valid.clone(), valid.clone()],
    ] {
        assert!(
            LinkedTrajectory::load(&BrokenLedger(events), &binding(), 0, 1, ContentPolicy::Full)
                .is_err()
        );
    }
    let mut duplicate = valid.clone();
    duplicate.cursor = 2;
    assert!(
        LinkedTrajectory::load(
            &BrokenLedger(vec![valid.clone(), duplicate]),
            &binding(),
            0,
            2,
            ContentPolicy::Full
        )
        .is_err()
    );
    let mut later = valid.clone();
    later.event_id = "later".into();
    later.cursor = 2;
    assert!(
        LinkedTrajectory::load(
            &BrokenLedger(vec![later, valid.clone()]),
            &binding(),
            0,
            2,
            ContentPolicy::Full
        )
        .is_err()
    );
    valid.payload = json!({"text":"no step evidence"});
    let loaded = LinkedTrajectory::load(
        &BrokenLedger(vec![valid]),
        &binding(),
        0,
        1,
        ContentPolicy::Full,
    )
    .unwrap();
    assert_eq!(loaded.records[0].step_id, None);
}

#[test]
fn binding_validation_and_decode_reject_bad_contract() {
    for value in ["".into(), " ".into(), "x".repeat(257), "bad\n".into()] {
        let mut bad = binding();
        bad.attempt_id = value;
        assert!(bad.validate().is_err());
    }
    let mut encoded = serde_json::to_value(binding()).unwrap();
    encoded["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ExecutionBinding>(encoded).is_err());
    assert!(serde_json::from_value::<ExecutionBinding>(json!({"task_id":1})).is_err());
    let mut encoded = serde_json::to_value(binding()).unwrap();
    encoded["execution_id"] = json!("");
    let decoded: ExecutionBinding = serde_json::from_value(encoded).unwrap();
    assert!(
        LinkedTrajectory::load(&BrokenLedger(vec![]), &decoded, 0, 1, ContentPolicy::Full).is_err()
    );
}

#[test]
fn trace_link_failure_does_not_change_ledger() {
    struct Failing;
    impl TraceSink for Failing {
        fn record(&self, record: TraceRecord) -> Result<(), TraceError> {
            assert_eq!(record.payload["event_id"], "first");
            assert_eq!(record.payload["binding"]["task_id"], "task");
            Err(TraceError {
                message: "offline".into(),
            })
        }
    }
    let ledger = InMemoryLedger::default();
    ledger.append(event("first", "execution")).unwrap();
    let rows = LinkedTrajectory::load(&ledger, &binding(), 0, 1, ContentPolicy::Full).unwrap();
    assert!(rows.records[0].trace().emit(&Failing).is_err());
    assert_eq!(ledger.events_after(0).unwrap().len(), 1);
}
