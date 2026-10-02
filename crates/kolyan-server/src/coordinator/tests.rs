use super::*;

mod boundary;
use kolyan_ledger::InMemoryLedger;

mod publication;

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

#[test]
fn active_checkpoint_drive_is_running_until_local_worker_releases_it() {
    let ledger = InMemoryLedger::default();
    let coordinator = ExecutionCoordinator::new(ledger.clone());
    let key = ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    coordinator.start(key.clone()).unwrap();
    coordinator.release("e");
    coordinator.append_once(&key,"paused",LedgerEventKind::ExecutionSuspended,
        json!({"schema_version":1,"publication_cursor":0,"suspension":crate::suspension::tests::fixture()})).unwrap();
    assert_eq!(coordinator.state("e").unwrap(), ExecutionState::Suspended);
    coordinator.admit(key, AdmissionKind::Resume).unwrap();
    assert_eq!(coordinator.state("e").unwrap(), ExecutionState::Running);
    coordinator.release("e");
    assert_eq!(coordinator.state("e").unwrap(), ExecutionState::Suspended);
}
