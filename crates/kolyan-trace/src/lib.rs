//! Optional observations, durable event links and integrity-checked local
//! content artifacts. None of these diagnostics grants execution authority.

mod artifacts;
pub use artifacts::{ArtifactError, ArtifactRef, ArtifactStore, Retention};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceKind {
    ProviderEvent,
    ModelDelta,
    TurnEvent,
    ToolOutput,
    Debug,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceRecord {
    pub turn_id: String,
    pub execution_id: String,
    pub sequence: u64,
    pub kind: TraceKind,
    pub payload: Value,
}

/// Diagnostic linkage only; it never constitutes execution or recovery authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinkedTraceRecord<B> {
    pub binding: B,
    pub event_id: String,
    pub cursor: u64,
    pub record: TraceRecord,
}

impl<B: Serialize> LinkedTraceRecord<B> {
    /// Emit a linked diagnostic envelope through the existing sink port.
    /// Failure is returned to the observer, not converted to execution failure.
    pub fn emit(&self, sink: &impl TraceSink) -> Result<(), TraceError> {
        let mut record = self.record.clone();
        record.payload = serde_json::to_value(self).map_err(|error| TraceError {
            message: error.to_string(),
        })?;
        sink.record(record)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("trace sink failed: {message}")]
pub struct TraceError {
    pub message: String,
}

pub trait TraceSink: Send + Sync {
    fn record(&self, record: TraceRecord) -> Result<(), TraceError>;
}

/// Explicitly disable optional observations without changing durable facts.
#[derive(Clone, Copy, Default)]
pub struct NoopTraceSink;

impl TraceSink for NoopTraceSink {
    fn record(&self, _: TraceRecord) -> Result<(), TraceError> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct VecTraceSink {
    records: std::sync::Arc<std::sync::Mutex<Vec<TraceRecord>>>,
}

impl VecTraceSink {
    pub fn records(&self) -> Vec<TraceRecord> {
        self.records
            .lock()
            .expect("trace lock must not be poisoned")
            .clone()
    }
}

impl TraceSink for VecTraceSink {
    fn record(&self, record: TraceRecord) -> Result<(), TraceError> {
        self.records
            .lock()
            .expect("trace lock must not be poisoned")
            .push(record);
        Ok(())
    }
}
