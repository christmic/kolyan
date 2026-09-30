//! SQLite coordination records use a transaction independent of execution tables.

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{
    FactDraft, FactError, FactJournal, FactRecord, plan_append, retry, storage, validate_batch,
    validate_read,
};

#[derive(Clone)]
pub struct SqliteFactJournal {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteFactJournal {
    /// Open a WAL journal with FULL synchronous durability. Tables are separate
    /// from execution Ledger tables, so both domains may share this database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FactError> {
        let connection = Connection::open(path).map_err(storage)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS fact_records (
                 stream_id TEXT NOT NULL, position INTEGER NOT NULL CHECK(position > 0),
                 fact_id TEXT NOT NULL UNIQUE, record_json TEXT NOT NULL,
                 PRIMARY KEY(stream_id, position));
             CREATE TABLE IF NOT EXISTS fact_batches (
                 stream_id TEXT NOT NULL, expected_position INTEGER NOT NULL,
                 records_json TEXT NOT NULL, PRIMARY KEY(stream_id, expected_position));",
            )
            .map_err(storage)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }
}

impl FactJournal for SqliteFactJournal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        validate_read(stream, limit)?;
        if after >= i64::MAX as u64 {
            return Ok(Vec::new());
        }
        let connection = self.connection.lock().map_err(storage)?;
        let mut statement = connection.prepare(
            "SELECT record_json FROM fact_records WHERE stream_id=?1 AND position>?2 ORDER BY position LIMIT ?3"
        ).map_err(storage)?;
        statement
            .query_map(params![stream, after as i64, limit as i64], |row| {
                row.get::<_, String>(0)
            })
            .map_err(storage)?
            .map(|row| serde_json::from_str(&row.map_err(storage)?).map_err(storage))
            .collect()
    }

    fn append(
        &self,
        stream: &str,
        expected_position: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        validate_batch(stream, expected_position, &batch)?;
        let mut connection = self.connection.lock().map_err(storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let original: Option<String> = transaction
            .query_row(
                "SELECT records_json FROM fact_batches WHERE stream_id=?1 AND expected_position=?2",
                params![stream, expected_position as i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        if let Some(json) = original {
            let records: Vec<FactRecord> = serde_json::from_str(&json).map_err(storage)?;
            return retry(&records, &batch);
        }
        let head: u64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(position),0) FROM fact_records WHERE stream_id=?1",
                [stream],
                |row| row.get(0),
            )
            .map_err(storage)?;
        let records = plan_append(stream, expected_position, head, batch, |id| {
            transaction
                .query_row(
                    "SELECT record_json FROM fact_records WHERE fact_id=?1",
                    [id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(storage)?
                .map(|json| serde_json::from_str(&json).map_err(storage))
                .transpose()
        })?;
        for record in &records {
            transaction.execute("INSERT INTO fact_records(stream_id,position,fact_id,record_json) VALUES (?1,?2,?3,?4)",
                    params![stream, record.position as i64, record.draft.fact_id, serde_json::to_string(record).map_err(storage)?]).map_err(storage)?;
        }
        transaction.execute("INSERT INTO fact_batches(stream_id,expected_position,records_json) VALUES (?1,?2,?3)",
            params![stream, expected_position as i64, serde_json::to_string(&records).map_err(storage)?]).map_err(storage)?;
        transaction.commit().map_err(storage)?;
        Ok(records)
    }
}
