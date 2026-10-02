//! Real stores with explicit publication/acknowledgement fault injection.

use super::*;
use kolyan_ledger::{LedgerError, LedgerQuery, MemoryFactJournal, SqliteFactJournal, SqliteLedger};

#[derive(Clone)]
pub(super) enum Backend {
    Memory(InMemoryLedger),
    Sqlite(SqliteLedger),
}

#[derive(Clone)]
pub(super) struct Store {
    pub(super) backend: Backend,
    pub(super) mode: String,
}

impl Store {
    fn fault(&self, event: &LedgerEvent) -> Result<(), LedgerError> {
        if (self.mode == "receipt_storage" && event.kind == LedgerEventKind::EffectReceipt)
            || (self.mode == "completion_storage" && event.kind == LedgerEventKind::EffectCompleted)
        {
            return Err(LedgerError::Storage(
                "explicit test publication failure".into(),
            ));
        }
        Ok(())
    }

    fn acknowledge(&self, mut event: LedgerEvent) -> LedgerEvent {
        if self.mode == "receipt_ack" && event.kind == LedgerEventKind::EffectReceipt {
            // Corrupted returned projection; the actual committed event stays intact.
            event.cursor += 100;
        }
        event
    }
}

impl LedgerStore for Store {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.fault(&event)?;
        let actual = match &self.backend {
            Backend::Memory(store) => store.append(event),
            Backend::Sqlite(store) => store.append(event),
        }?;
        Ok(self.acknowledge(actual))
    }

    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.fault(&event)?;
        let actual = match &self.backend {
            Backend::Memory(store) => store.append_unless_cancelled(event),
            Backend::Sqlite(store) => store.append_unless_cancelled(event),
        }?;
        Ok(self.acknowledge(actual))
    }

    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        match &self.backend {
            Backend::Memory(store) => store.query(query),
            Backend::Sqlite(store) => store.query(query),
        }
    }

    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        match &self.backend {
            Backend::Memory(store) => store.events_after(cursor),
            Backend::Sqlite(store) => store.events_after(cursor),
        }
    }

    fn claim(&self, id: &str) -> Result<bool, LedgerError> {
        match &self.backend {
            Backend::Memory(store) => store.claim(id),
            Backend::Sqlite(store) => store.claim(id),
        }
    }
}

pub(super) enum JournalBackend {
    Memory(MemoryFactJournal),
    Sqlite(SqliteFactJournal),
}

pub(super) struct Journal {
    pub(super) backend: JournalBackend,
    pub(super) fail_kind: Option<&'static str>,
}

impl FactJournal for Journal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        match &self.backend {
            JournalBackend::Memory(journal) => journal.read(stream, after, limit),
            JournalBackend::Sqlite(journal) => journal.read(stream, after, limit),
        }
    }

    fn append(
        &self,
        stream: &str,
        expected: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        if batch
            .iter()
            .any(|fact| Some(fact.kind.as_str()) == self.fail_kind)
        {
            return Err(FactError::Storage("explicit test Journal failure".into()));
        }
        match &self.backend {
            JournalBackend::Memory(journal) => journal.append(stream, expected, batch),
            JournalBackend::Sqlite(journal) => journal.append(stream, expected, batch),
        }
    }
}
