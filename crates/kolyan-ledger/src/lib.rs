//! Durable execution facts, bounded queries, leases and atomic coordination facts.

mod facts;
mod query;
pub use facts::{
    FactDraft, FactError, FactJournal, FactRecord, FactRef, FactSubject, MemoryFactJournal,
    SqliteFactJournal,
};
pub use query::LedgerQuery;

use rusqlite::{Connection, OptionalExtension, params};
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
    ModelRequested,
    ModelStreamEvent,
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
    ExecutionBound,
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
    EffectReconciled,
    SessionCommitPrepared,
    SessionCommitted,
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
    #[error("invalid ledger query: {0}")]
    InvalidQuery(String),
    #[error("ledger conflict for idempotency key: {0}")]
    Conflict(String),
    #[error("execution was cancelled: {0}")]
    Cancelled(String),
    #[error("ledger storage failed: {0}")]
    Storage(String),
}

pub trait LedgerStore: Send + Sync {
    /// Atomically reject an admission if cancellation has already been committed.
    /// Adapters without a transactional implementation must fail closed.
    fn append_unless_cancelled(&self, _event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        Err(LedgerError::Storage(
            "atomic admission is unsupported".into(),
        ))
    }
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError>;
    /// Read matching facts in ascending cursor order, bounded after filtering.
    /// This required port has no global audit fallback.
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError>;

    /// Look up an exact event identity through the required query port.
    fn event_by_id(&self, event_id: &str) -> Result<Option<LedgerEvent>, LedgerError> {
        Ok(query::checked_query(
            self,
            &LedgerQuery {
                execution_id: None,
                event_id: Some(event_id.into()),
                after: 0,
                through: None,
                limit: 1,
            },
        )?
        .into_iter()
        .next())
    }

    /// Read one execution in pages of 512. Concurrent appends are not a snapshot;
    /// callers needing a frozen range must use query with an explicit through.
    fn execution_events_after(
        &self,
        execution_id: &str,
        after: u64,
    ) -> Result<Vec<LedgerEvent>, LedgerError> {
        let mut query = LedgerQuery {
            execution_id: Some(execution_id.into()),
            event_id: None,
            after,
            through: None,
            limit: 512,
        };
        let mut events = Vec::new();
        loop {
            let page = query::checked_query(self, &query)?;
            let count = page.len();
            if let Some(last) = page.last() {
                query.after = last.cursor;
            }
            events.extend(page);
            if count < query.limit {
                return Ok(events);
            }
        }
    }

    /// Explicit unbounded global audit/export read, not an execution recovery API.
    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError>;
    fn claim(&self, idempotency_key: &str) -> Result<bool, LedgerError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLease {
    pub execution_id: String,
    pub owner_id: String,
    pub revision: u64,
    pub expires_at_ms: u64,
}

pub trait LeaseStore: Send + Sync {
    fn acquire_lease(
        &self,
        execution_id: &str,
        owner_id: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<ExecutionLease, LedgerError>;
    fn renew_lease(
        &self,
        lease: &ExecutionLease,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<ExecutionLease, LedgerError>;
    fn release_lease(&self, lease: &ExecutionLease) -> Result<(), LedgerError>;
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

impl InMemoryLedger {
    fn append_checked(
        &self,
        mut event: LedgerEvent,
        check_cancel: bool,
    ) -> Result<LedgerEvent, LedgerError> {
        let mut state = self.state.lock().expect("ledger lock must not be poisoned");
        if check_cancel
            && state.events.iter().any(|item| {
                item.execution_id == event.execution_id
                    && item.kind == LedgerEventKind::ExecutionCancelled
            })
        {
            return Err(LedgerError::Cancelled(event.execution_id));
        }
        if state.events.iter().any(|item| {
            item.event_id == event.event_id || item.idempotency_key == event.idempotency_key
        }) {
            return Err(LedgerError::Conflict(event.event_id));
        }
        state.next_cursor += 1;
        event.cursor = state.next_cursor;
        state.events.push(event.clone());
        Ok(event)
    }
}

impl LedgerStore for InMemoryLedger {
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        query.validate()?;
        let state = self.state.lock().expect("ledger lock must not be poisoned");
        Ok(state
            .events
            .iter()
            .filter(|event| query.matches(event))
            .take(query.limit)
            .cloned()
            .collect())
    }

    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.append_checked(event, false)
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.append_checked(event, true)
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

impl FileLedger {
    fn append_checked(
        &self,
        mut event: LedgerEvent,
        check_cancel: bool,
    ) -> Result<LedgerEvent, LedgerError> {
        let _guard = self
            .lock
            .lock()
            .expect("file ledger lock must not be poisoned");
        let _file_lock = self.exclusive_lock()?;
        let events = Self::read_events(&self.path)?;
        if check_cancel
            && events.iter().any(|item| {
                item.execution_id == event.execution_id
                    && item.kind == LedgerEventKind::ExecutionCancelled
            })
        {
            return Err(LedgerError::Cancelled(event.execution_id));
        }
        if events.iter().any(|item| {
            item.event_id == event.event_id || item.idempotency_key == event.idempotency_key
        }) {
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

    fn exclusive_lock(&self) -> Result<std::fs::File, LedgerError> {
        let mut path = self.path.as_os_str().to_owned();
        path.push(".lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(PathBuf::from(path))
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        file.lock()
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        Ok(file)
    }
}

impl LedgerStore for FileLedger {
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        query::file_query(self, query)
    }

    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.append_checked(event, false)
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.append_checked(event, true)
    }

    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        let _guard = self
            .lock
            .lock()
            .expect("file ledger lock must not be poisoned");
        let _file_lock = self.exclusive_lock()?;
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
        let _file_lock = self.exclusive_lock()?;
        let mut path = self.path.as_os_str().to_owned();
        path.push(".claims");
        let path = PathBuf::from(path);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        let claims: Vec<String> = BufReader::new(&file)
            .lines()
            .map(|line| {
                let line = line.map_err(|error| LedgerError::Storage(error.to_string()))?;
                serde_json::from_str(&line).map_err(|error| LedgerError::Storage(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        if claims.iter().any(|key| key == idempotency_key)
            || Self::read_events(&self.path)?
                .iter()
                .any(|event| event.idempotency_key == idempotency_key)
        {
            return Ok(false);
        }
        let line = serde_json::to_string(idempotency_key)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        writeln!(file, "{line}")
            .and_then(|_| file.sync_all())
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        Ok(true)
    }
}

/// SQLite-backed Ledger and lease store. The connection is shared by clones so
/// a reopened process can use the same durable schema without Core knowing SQL.
#[derive(Clone, Debug)]
pub struct SqliteLedger {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteLedger {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LedgerError> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|error| LedgerError::Storage(error.to_string()))?;
        }
        let connection =
            Connection::open(path).map_err(|error| LedgerError::Storage(error.to_string()))?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS events (
                cursor INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                turn_id TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS events_execution_cursor ON events (execution_id, cursor);
             CREATE TABLE IF NOT EXISTS leases (
                execution_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                expires_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS claims (
                idempotency_key TEXT PRIMARY KEY
             );",
            )
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }
}

impl SqliteLedger {
    fn append_checked(
        &self,
        event: LedgerEvent,
        check_cancel: bool,
    ) -> Result<LedgerEvent, LedgerError> {
        let mut connection = self
            .connection
            .lock()
            .expect("sqlite ledger lock must not be poisoned");
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        if check_cancel {
            let cancelled: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM events WHERE execution_id = ?1 AND kind = ?2)",
                    params![
                        event.execution_id,
                        serde_json::to_string(&LedgerEventKind::ExecutionCancelled)
                            .expect("event kind")
                    ],
                    |row| row.get(0),
                )
                .map_err(|error| LedgerError::Storage(error.to_string()))?;
            if cancelled {
                return Err(LedgerError::Cancelled(event.execution_id));
            }
        }
        let kind = serde_json::to_string(&event.kind)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        let payload = serde_json::to_string(&event.payload)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        let result = transaction.execute(
            "INSERT INTO events (event_id, turn_id, execution_id, kind, idempotency_key, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![event.event_id, event.turn_id, event.execution_id, kind, event.idempotency_key, payload],
        );
        if let Err(error) = result {
            if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
                return Err(LedgerError::Conflict(event.event_id));
            }
            return Err(LedgerError::Storage(error.to_string()));
        }
        let cursor = transaction.last_insert_rowid() as u64;
        transaction
            .commit()
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        Ok(LedgerEvent { cursor, ..event })
    }
}

impl LedgerStore for SqliteLedger {
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        query::sqlite_query(self, query)
    }

    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.append_checked(event, false)
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.append_checked(event, true)
    }

    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        let connection = self
            .connection
            .lock()
            .expect("sqlite ledger lock must not be poisoned");
        let mut statement = connection.prepare("SELECT cursor, event_id, turn_id, execution_id, kind, idempotency_key, payload FROM events WHERE cursor > ?1 ORDER BY cursor").map_err(|error| LedgerError::Storage(error.to_string()))?;
        let rows = statement
            .query_map(params![cursor], |row| {
                let kind: String = row.get(4)?;
                let payload: String = row.get(6)?;
                Ok(LedgerEvent {
                    cursor: row.get::<_, i64>(0)? as u64,
                    event_id: row.get(1)?,
                    turn_id: row.get(2)?,
                    execution_id: row.get(3)?,
                    kind: serde_json::from_str(&kind).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    idempotency_key: row.get(5)?,
                    payload: serde_json::from_str(&payload).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                })
            })
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        rows.map(|row| row.map_err(|error| LedgerError::Storage(error.to_string())))
            .collect()
    }

    fn claim(&self, idempotency_key: &str) -> Result<bool, LedgerError> {
        let connection = self
            .connection
            .lock()
            .expect("sqlite ledger lock must not be poisoned");
        connection
            .execute(
                "INSERT OR IGNORE INTO claims (idempotency_key) VALUES (?1)",
                params![idempotency_key],
            )
            .map_err(|error| LedgerError::Storage(error.to_string()))
            .map(|count| count == 1)
    }
}

impl LeaseStore for SqliteLedger {
    fn acquire_lease(
        &self,
        execution_id: &str,
        owner_id: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<ExecutionLease, LedgerError> {
        let connection = self
            .connection
            .lock()
            .expect("sqlite ledger lock must not be poisoned");
        let transaction = connection
            .unchecked_transaction()
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        let current: Option<ExecutionLease> = transaction.query_row(
            "SELECT execution_id, owner_id, revision, expires_at_ms FROM leases WHERE execution_id = ?1",
            params![execution_id],
            |row| Ok(ExecutionLease { execution_id: row.get(0)?, owner_id: row.get(1)?, revision: row.get::<_, i64>(2)? as u64, expires_at_ms: row.get::<_, i64>(3)? as u64 }),
        ).optional().map_err(|error| LedgerError::Storage(error.to_string()))?;
        let lease = match current {
            Some(current) if current.expires_at_ms > now_ms && current.owner_id != owner_id => {
                return Err(LedgerError::Conflict(format!(
                    "lease owned by {}",
                    current.owner_id
                )));
            }
            Some(current) => ExecutionLease {
                execution_id: execution_id.into(),
                owner_id: owner_id.into(),
                revision: current.revision + 1,
                expires_at_ms: now_ms + ttl_ms,
            },
            None => ExecutionLease {
                execution_id: execution_id.into(),
                owner_id: owner_id.into(),
                revision: 1,
                expires_at_ms: now_ms + ttl_ms,
            },
        };
        transaction.execute("INSERT INTO leases (execution_id, owner_id, revision, expires_at_ms) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(execution_id) DO UPDATE SET owner_id=excluded.owner_id, revision=excluded.revision, expires_at_ms=excluded.expires_at_ms", params![lease.execution_id, lease.owner_id, lease.revision as i64, lease.expires_at_ms as i64]).map_err(|error| LedgerError::Storage(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        Ok(lease)
    }

    fn renew_lease(
        &self,
        lease: &ExecutionLease,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<ExecutionLease, LedgerError> {
        let connection = self
            .connection
            .lock()
            .expect("sqlite ledger lock must not be poisoned");
        let updated = connection.execute("UPDATE leases SET expires_at_ms = ?1 WHERE execution_id = ?2 AND owner_id = ?3 AND revision = ?4 AND expires_at_ms > ?5", params![(now_ms + ttl_ms) as i64, lease.execution_id, lease.owner_id, lease.revision as i64, now_ms as i64]).map_err(|error| LedgerError::Storage(error.to_string()))?;
        if updated != 1 {
            return Err(LedgerError::Conflict("lease fenced or expired".into()));
        }
        Ok(ExecutionLease {
            expires_at_ms: now_ms + ttl_ms,
            ..lease.clone()
        })
    }

    fn release_lease(&self, lease: &ExecutionLease) -> Result<(), LedgerError> {
        let connection = self
            .connection
            .lock()
            .expect("sqlite ledger lock must not be poisoned");
        let deleted = connection
            .execute(
                "DELETE FROM leases WHERE execution_id = ?1 AND owner_id = ?2 AND revision = ?3",
                params![lease.execution_id, lease.owner_id, lease.revision as i64],
            )
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        if deleted != 1 {
            return Err(LedgerError::Conflict("lease fenced".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
