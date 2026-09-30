//! Validated query contract and native file/SQL reads; no audit fallback.

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use rusqlite::{params_from_iter, types::Value};

use crate::{FileLedger, LedgerError, LedgerEvent, LedgerStore, SqliteLedger};

/// AND-combined identities and an exclusive/inclusive cursor range.
/// Limits are 1..=1024; identities must be nonempty and through >= after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerQuery {
    pub execution_id: Option<String>,
    pub event_id: Option<String>,
    pub after: u64,
    pub through: Option<u64>,
    pub limit: usize,
}

impl LedgerQuery {
    /// Validate before touching storage, including valid empty cursor ranges.
    pub fn validate(&self) -> Result<(), LedgerError> {
        if !(1..=1024).contains(&self.limit) {
            return Err(LedgerError::InvalidQuery("limit must be 1..=1024".into()));
        }
        if self.execution_id.as_ref().is_some_and(String::is_empty)
            || self.event_id.as_ref().is_some_and(String::is_empty)
        {
            return Err(LedgerError::InvalidQuery(
                "identities must be nonempty".into(),
            ));
        }
        if self.through.is_some_and(|through| through < self.after) {
            return Err(LedgerError::InvalidQuery("through must be >= after".into()));
        }
        Ok(())
    }

    pub(crate) fn matches(&self, event: &LedgerEvent) -> bool {
        event.cursor > self.after
            && self.through.is_none_or(|through| event.cursor <= through)
            && self
                .execution_id
                .as_ref()
                .is_none_or(|id| id == &event.execution_id)
            && self
                .event_id
                .as_ref()
                .is_none_or(|id| id == &event.event_id)
    }
}

pub(crate) fn checked_query<S: LedgerStore + ?Sized>(
    store: &S,
    query: &LedgerQuery,
) -> Result<Vec<LedgerEvent>, LedgerError> {
    query.validate()?;
    let page = store.query(query)?;
    if page.len() > query.limit {
        return Err(LedgerError::Storage(
            "query returned more than its limit".into(),
        ));
    }
    let mut previous = query.after;
    for event in &page {
        if !query.matches(event) || event.cursor <= previous {
            return Err(LedgerError::Storage(
                "query returned an out-of-scope or nonascending event".into(),
            ));
        }
        previous = event.cursor;
    }
    Ok(page)
}

pub(crate) fn file_query(
    store: &FileLedger,
    query: &LedgerQuery,
) -> Result<Vec<LedgerEvent>, LedgerError> {
    query.validate()?;
    let _guard = store
        .lock
        .lock()
        .expect("file ledger lock must not be poisoned");
    let mut lock_path = store.path.as_os_str().to_owned();
    lock_path.push(".lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(PathBuf::from(lock_path))
        .map_err(storage_error)?;
    lock.lock_shared().map_err(storage_error)?;
    let file = OpenOptions::new()
        .read(true)
        .open(&store.path)
        .map_err(storage_error)?;
    let mut events = Vec::new();
    for line in BufReader::new(file).lines() {
        let event: LedgerEvent =
            serde_json::from_str(&line.map_err(storage_error)?).map_err(storage_error)?;
        if query.matches(&event) {
            events.push(event);
            if events.len() == query.limit {
                break;
            }
        }
    }
    Ok(events)
}

// Build only present predicates so SQLite can seek using the identity indexes.
fn query_sql(query: &LedgerQuery) -> (String, Vec<Value>) {
    let mut sql = String::from(
        "SELECT cursor, event_id, turn_id, execution_id, kind, idempotency_key, payload FROM events WHERE cursor > ?",
    );
    // SQLite durable cursors are signed INTEGERs; larger bounds cannot match.
    let mut values = vec![Value::Integer(query.after.min(i64::MAX as u64) as i64)];
    if let Some(through) = query.through {
        sql.push_str(" AND cursor <= ?");
        values.push(Value::Integer(through.min(i64::MAX as u64) as i64));
    }
    if let Some(id) = &query.execution_id {
        sql.push_str(" AND execution_id = ?");
        values.push(Value::Text(id.clone()));
    }
    if let Some(id) = &query.event_id {
        sql.push_str(" AND event_id = ?");
        values.push(Value::Text(id.clone()));
    }
    sql.push_str(" ORDER BY cursor ASC LIMIT ?");
    values.push(Value::Integer(query.limit as i64));
    (sql, values)
}

pub(crate) fn sqlite_query(
    store: &SqliteLedger,
    query: &LedgerQuery,
) -> Result<Vec<LedgerEvent>, LedgerError> {
    query.validate()?;
    let connection = store
        .connection
        .lock()
        .expect("sqlite ledger lock must not be poisoned");
    let (sql, values) = query_sql(query);
    let mut statement = connection.prepare(&sql).map_err(storage_error)?;
    let rows = statement
        .query_map(params_from_iter(values), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })
        .map_err(storage_error)?;
    rows.map(|row| {
        let (cursor, event_id, turn_id, execution_id, kind, idempotency_key, payload) =
            row.map_err(storage_error)?;
        Ok(LedgerEvent {
            cursor: cursor as u64,
            event_id,
            turn_id,
            execution_id,
            kind: serde_json::from_str(&kind).map_err(storage_error)?,
            idempotency_key,
            payload: serde_json::from_str(&payload).map_err(storage_error)?,
        })
    })
    .collect()
}

fn storage_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Storage(error.to_string())
}

#[cfg(test)]
mod tests;
