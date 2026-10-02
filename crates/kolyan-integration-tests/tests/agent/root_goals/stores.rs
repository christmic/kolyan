//! Rebuilt Memory and SQLite storage handles, not replacement snapshots.
use kolyan_ledger::*;
use std::{path::Path, sync::Arc};
#[derive(Clone)]
pub(super) struct Ledger(Arc<dyn LedgerStore>);
#[derive(Clone)]
pub(super) struct Journal(Arc<dyn FactJournal>);
pub(super) fn open(backend: &str, root: &Path) -> (Ledger, Journal) {
    match backend {
        "memory" => (
            Ledger(Arc::new(InMemoryLedger::default())),
            Journal(Arc::new(MemoryFactJournal::default())),
        ),
        "sqlite" => (
            Ledger(Arc::new(
                SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap(),
            )),
            Journal(Arc::new(
                SqliteFactJournal::open(root.join("state/facts.sqlite")).unwrap(),
            )),
        ),
        _ => panic!("unknown backend"),
    }
}
impl LedgerStore for Ledger {
    fn query(&self, q: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.0.query(q)
    }
    fn events_after(&self, c: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.0.events_after(c)
    }
    fn claim(&self, k: &str) -> Result<bool, LedgerError> {
        self.0.claim(k)
    }
    fn append(&self, e: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.0.append(e)
    }
    fn append_unless_cancelled(&self, e: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.0.append_unless_cancelled(e)
    }
}
impl FactJournal for Journal {
    fn read(&self, s: &str, a: u64, l: usize) -> Result<Vec<FactRecord>, FactError> {
        self.0.read(s, a, l)
    }
    fn append(&self, s: &str, p: u64, b: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        self.0.append(s, p, b)
    }
}
