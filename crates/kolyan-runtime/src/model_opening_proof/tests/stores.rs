//! Observed adapters preserve full returned rows and reject unintended writes.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, LedgerError, LedgerEvent, LedgerQuery,
    LedgerStore,
};
use serde_json::{Value, json};

pub(super) struct ObservedLedger {
    pub inner: Box<dyn LedgerStore>,
    pub mode: String,
    pub reads: Mutex<Vec<Value>>,
    pub writes: AtomicUsize,
    pub unbounded: AtomicUsize,
}

impl ObservedLedger {
    pub fn new(inner: Box<dyn LedgerStore>, mode: &str) -> Self {
        Self {
            inner,
            mode: mode.into(),
            reads: Mutex::new(Vec::new()),
            writes: AtomicUsize::new(0),
            unbounded: AtomicUsize::new(0),
        }
    }
}

impl LedgerStore for ObservedLedger {
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage("inspector attempted write".into()))
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.append(event)
    }
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        let result = if self.mode == "ledger_storage" {
            Err(LedgerError::Storage("injected Ledger read failure".into()))
        } else {
            self.inner.query(query).map(|mut rows| {
                match self.mode.as_str() {
                    "foreign_turn" => {
                        if let Some(row) = rows.first_mut() {
                            row.turn_id = "foreign".into();
                        }
                    }
                    "reversed_page" => rows.reverse(),
                    "too_many_rows" => {
                        if let Some(row) = rows.first().cloned() {
                            rows.resize(query.limit + 1, row);
                        }
                    }
                    _ => {}
                }
                rows
            })
        };
        self.reads.lock().unwrap().push(json!({"query":{"execution_id":query.execution_id,
            "event_id":query.event_id,"after":query.after,"through":query.through,"limit":query.limit},
            "rows":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string)}));
        result
    }
    fn execution_events_after(&self, _: &str, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.unbounded.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage(
            "unbounded execution read forbidden".into(),
        ))
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.unbounded.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage("global audit forbidden".into()))
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage("inspector attempted claim".into()))
    }
}

pub(super) struct ObservedFacts {
    pub inner: Box<dyn FactJournal>,
    pub mode: String,
    pub reads: Mutex<Vec<Value>>,
    pub writes: AtomicUsize,
}

impl ObservedFacts {
    pub fn new(inner: Box<dyn FactJournal>, mode: &str) -> Self {
        Self {
            inner,
            mode: mode.into(),
            reads: Mutex::new(Vec::new()),
            writes: AtomicUsize::new(0),
        }
    }
}

impl FactJournal for ObservedFacts {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        let result = if self.mode == "fact_storage" {
            Err(FactError::Storage("injected Fact read failure".into()))
        } else {
            self.inner.read(stream, after, limit).map(|mut rows| {
                if self.mode == "fact_wrong_position"
                    && let Some(row) = rows.first_mut()
                {
                    row.position += 1;
                }
                rows
            })
        };
        self.reads
            .lock()
            .unwrap()
            .push(json!({"stream":stream,"after":after,"limit":limit,
            "rows":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string)}));
        result
    }
    fn append(&self, _: &str, _: u64, _: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(FactError::Storage("inspector attempted Fact write".into()))
    }
}

pub(super) fn ledger_rows(ledger: &dyn LedgerStore) -> Vec<LedgerEvent> {
    let mut rows = Vec::new();
    let mut after = 0;
    loop {
        let page = ledger
            .query(&LedgerQuery {
                execution_id: None,
                event_id: None,
                after,
                through: None,
                limit: 512,
            })
            .unwrap();
        if page.is_empty() {
            return rows;
        }
        after = page.last().unwrap().cursor;
        rows.extend(page);
    }
}
