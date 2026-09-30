use super::*;
use kolyan_ledger::InMemoryLedger;

#[derive(Clone)]
struct RacingLedger {
    inner: InMemoryLedger,
    mismatch: bool,
}
impl LedgerStore for RacingLedger {
    fn query(&self, query: &kolyan_ledger::LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.query(query)
    }
    fn append(&self, mut event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        let id = event.event_id.clone();
        if self.mismatch {
            event.payload = json!({"different":true});
        }
        self.inner.append(event)?;
        Err(LedgerError::Conflict(id))
    }
    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.events_after(cursor)
    }
    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.inner.claim(key)
    }
}

#[test]
fn identical_concurrent_commit_is_idempotent_but_changed_fact_is_rejected() {
    for mismatch in [false, true] {
        let ledger = RacingLedger {
            inner: InMemoryLedger::default(),
            mismatch,
        };
        let coordinator = ExecutionCoordinator::new(ledger.clone());
        let key = ExecutionRef {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        };
        let result = coordinator.append_once(
            &key,
            "commit",
            LedgerEventKind::SessionCommitted,
            json!({"status":"failed"}),
        );
        assert_eq!(result.is_err(), mismatch);
        assert_eq!(ledger.events_after(0).unwrap().len(), 1);
    }
}
