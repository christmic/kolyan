//! Faults preserve the physical stores and export exact calls/results separately.

use std::sync::{Arc, Mutex};

use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, LedgerError, LedgerEvent, LedgerQuery,
    LedgerStore,
};
use serde_json::{Value, json};

pub(super) struct Ledger {
    pub inner: Arc<dyn LedgerStore>,
    pub mode: String,
    pub calls: Arc<Mutex<Vec<Value>>>,
}
pub(super) struct Facts {
    pub inner: Arc<dyn FactJournal>,
    pub mode: String,
    pub calls: Arc<Mutex<Vec<Value>>>,
}

impl LedgerStore for Ledger {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.inner.append(event)
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        let input = event.clone();
        let result = if self.mode == "ledger_fail" {
            Err(LedgerError::Storage(
                "injected opening write failure".into(),
            ))
        } else {
            self.inner
                .append_unless_cancelled(event)
                .and_then(|mut row| {
                    if self.mode == "unknown_ack" {
                        return Err(LedgerError::Storage(
                            "injected lost committed acknowledgement".into(),
                        ));
                    }
                    if self.mode == "bad_ack" {
                        row.payload["neutral_digest"] = json!("e".repeat(64));
                    }
                    Ok(row)
                })
        };
        self.calls.lock().unwrap().push(json!({"operation":"append_unless_cancelled","input":input,"result":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string)}));
        result
    }
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        let result = self.inner.query(query);
        self.calls.lock().unwrap().push(json!({"operation":"query","query":{"execution_id":query.execution_id,"event_id":query.event_id,"after":query.after,"through":query.through,"limit":query.limit},"result":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string)}));
        result
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        Err(LedgerError::Storage("unbounded read forbidden".into()))
    }
    fn execution_events_after(&self, _: &str, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        Err(LedgerError::Storage(
            "unbounded execution read forbidden".into(),
        ))
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        Err(LedgerError::Storage("claim forbidden".into()))
    }
}

impl FactJournal for Facts {
    fn append(
        &self,
        stream: &str,
        position: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        let input = batch.clone();
        let result = if self.mode == "fact_fail" {
            Err(FactError::Storage(
                "injected preparation write failure".into(),
            ))
        } else {
            self.inner.append(stream, position, batch).and_then(|rows| {
                if self.mode == "unknown_fact_ack" {
                    Err(FactError::Storage(
                        "injected lost preparation acknowledgement".into(),
                    ))
                } else {
                    Ok(rows)
                }
            })
        };
        self.calls.lock().unwrap().push(json!({"operation":"append","stream":stream,"expected":position,"input":input,"result":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string)}));
        result
    }
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        let result = if self.mode == "fact_read_fail" {
            Err(FactError::Storage(
                "injected preparation read failure".into(),
            ))
        } else {
            self.inner.read(stream, after, limit).map(|mut rows| {
                if self.mode == "read_mismatch"
                    && let Some(row) = rows.first_mut()
                {
                    row.draft.critical = false;
                }
                rows
            })
        };
        self.calls.lock().unwrap().push(json!({"operation":"read","stream":stream,"after":after,"limit":limit,"result":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string)}));
        result
    }
}
