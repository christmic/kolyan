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

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("trace sink failed: {message}")]
pub struct TraceError {
    pub message: String,
}

pub trait TraceSink: Send + Sync {
    fn record(&self, record: TraceRecord) -> Result<(), TraceError>;
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
