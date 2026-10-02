//! Test-only storage adapters; SQLite reconstruction opens fresh connections.
use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, InMemoryLedger, LedgerError, LedgerEvent,
    LedgerQuery, LedgerStore, MemoryFactJournal, SqliteFactJournal, SqliteLedger,
};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Backend {
    Memory,
    Sqlite,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    backends: Vec<Backend>,
}
pub(super) fn inventory() -> Vec<Backend> {
    serde_json::from_str::<Inventory>(include_str!("backends.json"))
        .unwrap()
        .backends
}
#[derive(Clone)]
pub(super) struct BackendLedger(Arc<dyn LedgerStore>);
#[derive(Clone)]
pub(super) struct BackendJournal(Arc<dyn FactJournal>);
impl Backend {
    pub(super) fn stores(self, root: &Path) -> (BackendLedger, BackendJournal) {
        match self {
            Self::Memory => (
                BackendLedger(Arc::new(InMemoryLedger::default())),
                BackendJournal(Arc::new(MemoryFactJournal::default())),
            ),
            Self::Sqlite => (
                BackendLedger(Arc::new(
                    SqliteLedger::open(root.join("ledger.sqlite")).unwrap(),
                )),
                BackendJournal(Arc::new(
                    SqliteFactJournal::open(root.join("facts.sqlite")).unwrap(),
                )),
            ),
        }
    }
    pub(super) fn reopen(
        self,
        root: &Path,
        ledger: &BackendLedger,
        journal: &BackendJournal,
    ) -> (BackendLedger, BackendJournal) {
        match self {
            Self::Memory => (ledger.clone(), journal.clone()),
            Self::Sqlite => self.stores(root),
        }
    }
}
impl LedgerStore for BackendLedger {
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
impl FactJournal for BackendJournal {
    fn read(&self, s: &str, a: u64, l: usize) -> Result<Vec<FactRecord>, FactError> {
        self.0.read(s, a, l)
    }
    fn append(&self, s: &str, p: u64, b: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        self.0.append(s, p, b)
    }
}
