use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerEventKind {
    TurnStarted,
    StepStarted,
    StepCompleted,
    ToolCallRequested,
    ApprovalRequested,
    ApprovalResolved,
    ToolExecutionStarted,
    ToolExecutionCompleted,
    ToolExecutionFailed,
    TurnFailed,
    TurnCancelled,
    TurnTimedOut,
    TurnCompleted,
    ExecutionStarted,
    ExecutionSuspended,
    ExecutionCancelled,
    EffectPrepared,
    EffectAuthorized,
    EffectAwaitingDecision,
    EffectStarted,
    EffectCompleted,
    EffectFailed,
    EffectUncertain,
    EffectDenied,
    EffectReceipt,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub event_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub cursor: u64,
    pub kind: LedgerEventKind,
    pub idempotency_key: String,
    pub payload: Value,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LedgerError {
    #[error("ledger conflict for idempotency key: {0}")]
    Conflict(String),
    #[error("ledger storage failed: {0}")]
    Storage(String),
}

pub trait LedgerStore: Send + Sync {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError>;
    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError>;
    fn claim(&self, idempotency_key: &str) -> Result<bool, LedgerError>;
}

#[derive(Clone, Default)]
pub struct InMemoryLedger {
    state: Arc<Mutex<LedgerState>>,
}

#[derive(Default)]
struct LedgerState {
    next_cursor: u64,
    events: Vec<LedgerEvent>,
    claims: std::collections::BTreeSet<String>,
}

impl LedgerStore for InMemoryLedger {
    fn append(&self, mut event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        let mut state = self.state.lock().expect("ledger lock must not be poisoned");
        if state
            .events
            .iter()
            .any(|item| item.event_id == event.event_id)
        {
            return Err(LedgerError::Conflict(event.event_id));
        }
        state.next_cursor += 1;
        event.cursor = state.next_cursor;
        state.events.push(event.clone());
        Ok(event)
    }

    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        let state = self.state.lock().expect("ledger lock must not be poisoned");
        Ok(state
            .events
            .iter()
            .filter(|event| event.cursor > cursor)
            .cloned()
            .collect())
    }

    fn claim(&self, idempotency_key: &str) -> Result<bool, LedgerError> {
        let mut state = self.state.lock().expect("ledger lock must not be poisoned");
        Ok(state.claims.insert(idempotency_key.to_owned()))
    }
}

/// A small append-only file ledger used by Runtime adapters and integration tests.
/// Each line is one complete JSON event; reopening the file reconstructs the cursor.
#[derive(Clone, Debug)]
pub struct FileLedger {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl FileLedger {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, LedgerError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| LedgerError::Storage(error.to_string()))?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        Ok(Self {
            path,
            lock: Arc::new(Mutex::new(())),
        })
    }

    fn read_events(path: &Path) -> Result<Vec<LedgerEvent>, LedgerError> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        BufReader::new(file)
            .lines()
            .map(|line| {
                let line = line.map_err(|error| LedgerError::Storage(error.to_string()))?;
                serde_json::from_str(&line).map_err(|error| LedgerError::Storage(error.to_string()))
            })
            .collect()
    }
}

impl LedgerStore for FileLedger {
    fn append(&self, mut event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        let _guard = self
            .lock
            .lock()
            .expect("file ledger lock must not be poisoned");
        let events = Self::read_events(&self.path)?;
        if events.iter().any(|item| item.event_id == event.event_id) {
            return Err(LedgerError::Conflict(event.event_id));
        }
        event.cursor = events.last().map_or(0, |item| item.cursor) + 1;
        let line =
            serde_json::to_vec(&event).map_err(|error| LedgerError::Storage(error.to_string()))?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        file.write_all(&line)
            .and_then(|_| file.write_all(b"\n"))
            .and_then(|_| file.sync_data())
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        Ok(event)
    }

    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        let _guard = self
            .lock
            .lock()
            .expect("file ledger lock must not be poisoned");
        Ok(Self::read_events(&self.path)?
            .into_iter()
            .filter(|event| event.cursor > cursor)
            .collect())
    }

    fn claim(&self, idempotency_key: &str) -> Result<bool, LedgerError> {
        let _guard = self
            .lock
            .lock()
            .expect("file ledger lock must not be poisoned");
        let events = Self::read_events(&self.path)?;
        Ok(!events
            .iter()
            .any(|event| event.idempotency_key == idempotency_key))
    }
}

#[cfg(test)]
mod tests {
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
}
