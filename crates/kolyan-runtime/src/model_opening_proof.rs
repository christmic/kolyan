//! Read-only opening classification for a complete, explicitly bounded Ledger prefix.
//! Fixtures and callers supply coordinates, not authority. No model, grant, retry,
//! reconciliation or write port is exposed; a digest cannot reconstruct a generation.

mod dto;
mod reader;
mod verify;

pub use dto::{
    ModelContextPrepared, ModelOpeningAccounting, ModelOpeningAdmitted,
    ModelOpeningMappingIdentity, ModelOpeningProtocol,
};

use kolyan_ledger::{FactError, FactJournal, FactRef, LedgerError, LedgerStore};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ExecutionKey;

/// An untrusted lookup coordinate, not a verified reference or execution permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOpeningEventRef {
    pub event_id: String,
    pub cursor: u64,
}

/// Explicit work ceilings. Exhaustion is an error, never evidence of absence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelOpeningInspectionLimits {
    pub max_events: usize,
    pub max_total_bytes: usize,
    pub max_payload_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelOpeningInspectionRequest {
    pub execution: ExecutionKey,
    pub through: ModelOpeningEventRef,
    pub limits: ModelOpeningInspectionLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelOpeningState {
    NotAdmitted,
    Completed,
    Uncertain,
}

/// Private verified sources. Uncertain remains uncertain after any physical stop.
#[derive(Debug)]
pub struct VerifiedModelOpeningStep {
    step_id: String,
    state: ModelOpeningState,
    started: ModelOpeningEventRef,
    requested: Option<ModelOpeningEventRef>,
    preparation: Option<FactRef>,
    opening: Option<ModelOpeningEventRef>,
    completed: Option<ModelOpeningEventRef>,
}

impl VerifiedModelOpeningStep {
    pub fn step_id(&self) -> &str {
        &self.step_id
    }
    pub fn state(&self) -> ModelOpeningState {
        self.state
    }
    pub fn started(&self) -> &ModelOpeningEventRef {
        &self.started
    }
    pub fn requested(&self) -> Option<&ModelOpeningEventRef> {
        self.requested.as_ref()
    }
    pub fn preparation(&self) -> Option<&FactRef> {
        self.preparation.as_ref()
    }
    pub fn opening(&self) -> Option<&ModelOpeningEventRef> {
        self.opening.as_ref()
    }
    pub fn completed(&self) -> Option<&ModelOpeningEventRef> {
        self.completed.as_ref()
    }
}

/// No constructor or Deserialize. This is not an idle/stop proof or retry grant.
#[derive(Debug)]
pub struct VerifiedModelOpenings {
    execution: ExecutionKey,
    input_admission: ModelOpeningEventRef,
    inspected_through: ModelOpeningEventRef,
    steps: Vec<VerifiedModelOpeningStep>,
}

impl VerifiedModelOpenings {
    pub fn execution(&self) -> &ExecutionKey {
        &self.execution
    }
    pub fn input_admission(&self) -> &ModelOpeningEventRef {
        &self.input_admission
    }
    pub fn inspected_through(&self) -> &ModelOpeningEventRef {
        &self.inspected_through
    }
    pub fn steps(&self) -> &[VerifiedModelOpeningStep] {
        &self.steps
    }
    pub fn has_uncertain(&self) -> bool {
        self.steps
            .iter()
            .any(|step| step.state == ModelOpeningState::Uncertain)
    }
}

#[derive(Debug, Error)]
pub enum ModelOpeningProofError {
    #[error("invalid opening inspection request: {0}")]
    InvalidRequest(String),
    #[error("unsupported opening protocol/schema at {source_id}")]
    UnsupportedProtocol { source_id: String },
    #[error("missing opening evidence: {source_id}")]
    MissingEvidence { source_id: String },
    #[error("opening binding differs at {source_id}: {reason}")]
    BindingMismatch { source_id: String, reason: String },
    #[error("opening ordering differs: {0}")]
    OrderingMismatch(String),
    #[error("invalid opening payload at {source_id}: {reason}")]
    InvalidPayload { source_id: String, reason: String },
    #[error("opening inspection exceeds {bound}")]
    BoundsExceeded { bound: &'static str },
    #[error("opening Ledger storage failed: {0}")]
    LedgerStorage(#[source] LedgerError),
    #[error("opening Fact storage failed: {0}")]
    FactStorage(#[source] FactError),
}

/// Inspect a frozen prefix including the exact through event. Callers must also
/// prove physical stop/idle, cancellation and tool safety before authorizing retry.
/// Storage returns owned rows; bounds precede helper cloning/decoding, not the
/// adapter's initial allocation. Historical facts never authorize another GEN.
pub fn inspect_model_openings<L: LedgerStore + ?Sized, F: FactJournal + ?Sized>(
    ledger: &L,
    facts: &F,
    request: &ModelOpeningInspectionRequest,
) -> Result<VerifiedModelOpenings, ModelOpeningProofError> {
    let mut budget = reader::Budget::new(request)?;
    let events = reader::prefix(ledger, request, &mut budget)?;
    verify::verify(facts, request, &events, &mut budget)
}

#[cfg(test)]
mod tests;
