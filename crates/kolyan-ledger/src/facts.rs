//! Atomic coordination facts. Domain transitions belong to Server validators.

mod sqlite;
pub use sqlite::SqliteFactJournal;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const MAX_FACT_PAYLOAD_BYTES: usize = 128 * 1024;
pub const MAX_FACT_BATCH: usize = 64;
pub const MAX_FACT_CAUSES: usize = 32;
pub const MAX_FACT_ID_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactSubject {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactRef {
    pub stream_id: String,
    pub position: u64,
    pub fact_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactDraft {
    pub fact_id: String,
    pub subject: FactSubject,
    pub kind: String,
    pub schema_version: u32,
    pub critical: bool,
    pub causes: Vec<FactRef>,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactRecord {
    pub stream_id: String,
    pub position: u64,
    pub draft: FactDraft,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FactError {
    #[error("invalid coordination fact: {0}")]
    Invalid(String),
    #[error("coordination fact conflict: {0}")]
    Conflict(String),
    #[error("stale stream position: expected {expected}, actual {actual}")]
    StalePosition { expected: u64, actual: u64 },
    #[error("causal reference does not resolve exactly: {0}")]
    MissingCause(String),
    #[error("coordination storage failed: {0}")]
    Storage(String),
}

pub trait FactJournal: Send + Sync {
    /// Exclusive ascending read, limited to 1..=1024 records. No writes occur.
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError>;
    /// Atomically compare the head and append 1..=64 structurally valid facts.
    /// An identical batch at its original position returns the original records,
    /// even after later appends. Partial retries and changed content fail closed.
    fn append(
        &self,
        stream: &str,
        expected_position: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError>;
}

#[derive(Clone, Default)]
pub struct MemoryFactJournal {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Default)]
struct MemoryState {
    streams: HashMap<String, Vec<FactRecord>>,
    identities: HashMap<String, FactRecord>,
    batches: HashMap<(String, u64), Vec<FactRecord>>,
}

impl FactJournal for MemoryFactJournal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        validate_read(stream, limit)?;
        let state = self.state.lock().map_err(storage)?;
        Ok(state
            .streams
            .get(stream)
            .into_iter()
            .flatten()
            .filter(|record| record.position > after)
            .take(limit)
            .cloned()
            .collect())
    }

    fn append(
        &self,
        stream: &str,
        expected_position: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        validate_batch(stream, expected_position, &batch)?;
        let mut state = self.state.lock().map_err(storage)?;
        if let Some(original) = state.batches.get(&(stream.into(), expected_position)) {
            return retry(original, &batch);
        }
        let head = state
            .streams
            .get(stream)
            .and_then(|rows| rows.last())
            .map_or(0, |r| r.position);
        let records = plan_append(stream, expected_position, head, batch, |id| {
            Ok(state.identities.get(id).cloned())
        })?;
        for record in &records {
            state
                .identities
                .insert(record.draft.fact_id.clone(), record.clone());
        }
        state
            .streams
            .entry(stream.into())
            .or_default()
            .extend(records.clone());
        state
            .batches
            .insert((stream.into(), expected_position), records.clone());
        Ok(records)
    }
}

fn validate_read(stream: &str, limit: usize) -> Result<(), FactError> {
    identity(stream)?;
    if !(1..=1024).contains(&limit) {
        return Err(FactError::Invalid("read limit must be 1..=1024".into()));
    }
    Ok(())
}

fn identity(value: &str) -> Result<(), FactError> {
    if value.trim().is_empty() || value.len() > MAX_FACT_ID_BYTES {
        return Err(FactError::Invalid(
            "identity must be nonempty and at most 256 bytes".into(),
        ));
    }
    Ok(())
}

fn kind(value: &str) -> Result<(), FactError> {
    identity(value)?;
    if !value.contains('.')
        || value.split('.').any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
    {
        return Err(FactError::Invalid(
            "kind must contain nonempty dotted namespace segments".into(),
        ));
    }
    Ok(())
}

fn validate_batch(stream: &str, expected: u64, batch: &[FactDraft]) -> Result<(), FactError> {
    identity(stream)?;
    if batch.is_empty()
        || batch.len() > MAX_FACT_BATCH
        || expected
            .checked_add(batch.len() as u64)
            .is_none_or(|end| end > i64::MAX as u64)
    {
        return Err(FactError::Invalid(
            "batch size or position exceeds bounds".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for draft in batch {
        identity(&draft.fact_id)?;
        identity(&draft.subject.id)?;
        kind(&draft.subject.kind)?;
        kind(&draft.kind)?;
        if draft.schema_version == 0
            || draft.causes.len() > MAX_FACT_CAUSES
            || serde_json::to_vec(&draft.payload).map_err(storage)?.len() > MAX_FACT_PAYLOAD_BYTES
        {
            return Err(FactError::Invalid(
                "version, causes or payload exceeds bounds".into(),
            ));
        }
        if !seen.insert(&draft.fact_id) {
            return Err(FactError::Conflict(draft.fact_id.clone()));
        }
        for cause in &draft.causes {
            identity(&cause.stream_id)?;
            identity(&cause.fact_id)?;
            if cause.position == 0 || cause.position > i64::MAX as u64 {
                return Err(FactError::Invalid("cause position is out of bounds".into()));
            }
        }
    }
    Ok(())
}

fn plan_append(
    stream: &str,
    expected: u64,
    head: u64,
    batch: Vec<FactDraft>,
    mut lookup: impl FnMut(&str) -> Result<Option<FactRecord>, FactError>,
) -> Result<Vec<FactRecord>, FactError> {
    let records: Vec<_> = batch
        .into_iter()
        .enumerate()
        .map(|(offset, draft)| FactRecord {
            stream_id: stream.into(),
            position: expected + offset as u64 + 1,
            draft,
        })
        .collect();
    let existing = records
        .iter()
        .map(|r| lookup(&r.draft.fact_id))
        .collect::<Result<Vec<_>, _>>()?;
    if existing.iter().any(Option::is_some) {
        return Err(FactError::Conflict("partial or changed batch retry".into()));
    }
    if expected != head {
        return Err(FactError::StalePosition {
            expected,
            actual: head,
        });
    }
    for (index, record) in records.iter().enumerate() {
        for cause in &record.draft.causes {
            let resolved = records[..index]
                .iter()
                .find(|r| r.draft.fact_id == cause.fact_id)
                .cloned()
                .map(|r| Ok(Some(r)))
                .unwrap_or_else(|| lookup(&cause.fact_id))?;
            if !resolved.is_some_and(|r| {
                r.stream_id == cause.stream_id
                    && r.position == cause.position
                    && r.draft.fact_id == cause.fact_id
            }) {
                return Err(FactError::MissingCause(cause.fact_id.clone()));
            }
        }
    }
    Ok(records)
}

fn storage(error: impl std::fmt::Display) -> FactError {
    FactError::Storage(error.to_string())
}

fn retry(original: &[FactRecord], batch: &[FactDraft]) -> Result<Vec<FactRecord>, FactError> {
    if original.len() == batch.len() && original.iter().zip(batch).all(|(r, d)| &r.draft == d) {
        Ok(original.to_vec())
    } else {
        Err(FactError::Conflict("partial or changed batch retry".into()))
    }
}

#[cfg(test)]
mod tests;
