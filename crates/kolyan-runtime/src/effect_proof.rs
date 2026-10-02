//! Read-only historical prepared-effect proof, bounded before cloning or decoding.
//! This proves one definitive receipt before an exact physical terminal, not Task
//! membership, a goal verdict, current resource state or authority for new work.

use std::io::{self, Write};

use kolyan_core::ToolError;
use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ToolResult;
use kolyan_policy::{PreparedCall, ToolExecutionScope};
use serde_json::Value;
use thiserror::Error;

use crate::reconciliation::receipt::{PreparedEvidence, check_event};
use crate::{EffectReceipt, ExecutionKey};

/// Per non-receipt payload ceiling, including execution admission and terminal.
pub const MAX_EFFECT_PROOF_INPUT_BYTES: usize = 16 * 1024 * 1024;
/// Ceiling for the receipt's output/error value. The entire receipt is bounded
/// by max_input_bytes + max_result_bytes, including historical authority metadata.
pub const MAX_EFFECT_PROOF_RESULT_BYTES: usize = 16 * 1024 * 1024;

/// A coordinate is a lookup request until independently loaded and verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectProofCoordinate {
    pub event_id: String,
    pub cursor: u64,
}

/// Explicit host ceilings may tighten, but never exceed the public hard ceilings.
/// Effect IDs follow PreparedCall's native call-ID contract: a <=256-byte Step
/// plus slash plus a <=1024-byte call ID; Unicode and embedded slashes are valid.
/// Terminal event IDs have a 2048-byte host ceiling, above Runtime's composed
/// execution/terminal identifiers; they do not inherit the 256-byte key ceiling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectProofRequest {
    pub execution: ExecutionKey,
    pub effect_id: String,
    pub terminal: EffectProofCoordinate,
    pub max_input_bytes: usize,
    pub max_result_bytes: usize,
}

/// Exact historical sources, in required strict cursor order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectProofSources {
    pub admission: EffectProofCoordinate,
    pub prepared: EffectProofCoordinate,
    pub authorized: EffectProofCoordinate,
    pub started: EffectProofCoordinate,
    pub receipt: EffectProofCoordinate,
    pub terminal: EffectProofCoordinate,
}

/// No constructor, Deserialize implementation, grant accessor or execution port.
/// An Ok ToolResult with is_error=true remains Ok and error-marked. A definitive
/// Failed receipt retains its original ToolError; neither implies goal success.
#[derive(Debug)]
pub struct VerifiedEffectProof {
    prepared: PreparedCall,
    scope: ToolExecutionScope,
    result: Result<ToolResult, ToolError>,
    receipt: EffectReceipt,
    sources: EffectProofSources,
    terminal_kind: LedgerEventKind,
}

#[derive(Debug, Error)]
pub enum EffectProofError {
    #[error("invalid effect proof request: {0}")]
    InvalidRequest(String),
    #[error("missing effect proof evidence: {event_id}")]
    MissingEvidence { event_id: String },
    #[error("effect proof binding mismatch at {event_id}: {reason}")]
    BindingMismatch { event_id: String, reason: String },
    #[error("effect proof sources are not in strict admission-to-terminal order")]
    OrderingMismatch,
    #[error("effect proof payload at {event_id} exceeds {limit} bytes")]
    BoundsExceeded { event_id: String, limit: usize },
    #[error("effect proof is indeterminate at {event_id}: {reason}")]
    Indeterminate { event_id: String, reason: String },
    #[error("effect proof storage: {0}")]
    Storage(#[from] LedgerError),
}

impl VerifiedEffectProof {
    pub fn prepared(&self) -> &PreparedCall {
        &self.prepared
    }
    pub fn scope(&self) -> &ToolExecutionScope {
        &self.scope
    }
    pub fn result(&self) -> &Result<ToolResult, ToolError> {
        &self.result
    }
    pub fn receipt(&self) -> &EffectReceipt {
        &self.receipt
    }
    pub fn sources(&self) -> &EffectProofSources {
        &self.sources
    }
    pub fn terminal_kind(&self) -> LedgerEventKind {
        self.terminal_kind
    }
}

/// Load six exact identities without audit/history fallback, writes, execution,
/// reconciliation or policy evaluation. Each payload is bounded before historical
/// authority decoding. LedgerStore materializes each single event before this
/// layer can inspect it; this is not a raw-storage allocation limit.
///
/// The terminal must be a physical stopped Turn fact, not an execution cancellation
/// request, suspension or diagnostic TurnCompleted projection. This does not prove
/// a successful attempt or exclude other conflicting terminal facts: Server owns
/// that independent check. Hosts must move this synchronous I/O off async workers.
pub fn inspect_effect_proof<L: LedgerStore + ?Sized>(
    ledger: &L,
    request: &EffectProofRequest,
) -> Result<VerifiedEffectProof, EffectProofError> {
    validate_request(request)?;
    let key = &request.execution;
    let terminal = load(
        ledger,
        key,
        &request.terminal.event_id,
        request.max_input_bytes,
    )?;
    if terminal.cursor != request.terminal.cursor || !physical_terminal(&terminal) {
        return Err(mismatch(
            &terminal,
            "not the exact physical stopped terminal",
        ));
    }
    let admission = required(
        ledger,
        key,
        &format!("{}/execution-started", key.execution_id),
        LedgerEventKind::ExecutionStarted,
        request.max_input_bytes,
    )?;
    if admission.payload != serde_json::json!(key) {
        return Err(mismatch(
            &admission,
            "execution admission Session/key differs",
        ));
    }
    let prefix = format!("{}/effect/{}", key.execution_id, request.effect_id);
    let prepared = required(
        ledger,
        key,
        &format!("{prefix}/prepared"),
        LedgerEventKind::EffectPrepared,
        request.max_input_bytes,
    )?;
    let authorized = required(
        ledger,
        key,
        &format!("{prefix}/authorized"),
        LedgerEventKind::EffectAuthorized,
        request.max_input_bytes,
    )?;
    let started = required(
        ledger,
        key,
        &format!("{prefix}/started"),
        LedgerEventKind::EffectStarted,
        request.max_input_bytes,
    )?;
    let receipt = required(
        ledger,
        key,
        &format!("{prefix}/receipt"),
        LedgerEventKind::EffectReceipt,
        request.max_input_bytes + request.max_result_bytes,
    )?;
    let sources = [
        &admission,
        &prepared,
        &authorized,
        &started,
        &receipt,
        &terminal,
    ];
    if sources
        .windows(2)
        .any(|pair| pair[0].cursor >= pair[1].cursor)
    {
        return Err(EffectProofError::OrderingMismatch);
    }
    // Bound outcome members before validate_receipt clones or deserializes them.
    for field in ["output", "error"] {
        if let Some(value) = receipt.payload.get(field) {
            bounded(value, &receipt.event_id, request.max_result_bytes)?;
        }
    }
    // Reconciled receipts duplicate the definitive output inside metadata. Apply
    // the result ceiling there as well before validate_receipt decodes resolution.
    if let Some(value) = receipt
        .payload
        .pointer("/reconciliation/resolution/Committed/output")
    {
        bounded(value, &receipt.event_id, request.max_result_bytes)?;
    }
    let bound = PreparedEvidence::from_facts(
        key,
        &request.effect_id,
        &prepared.payload,
        &authorized.payload,
    )
    .map_err(|error| mismatch(&prepared, error.to_string()))?;
    // Same historical entry binding as DurableTools::check_entered. Its private
    // helper reloads facts; compare the already bounded loaded facts here instead.
    if started.payload != bound.started_payload() {
        return Err(mismatch(&started, "started effect binding differs"));
    }
    if receipt.payload["receipt"]["status"] == "Uncertain" {
        return Err(EffectProofError::Indeterminate {
            event_id: receipt.event_id,
            reason: "uncertain receipt requires trusted reconciliation".into(),
        });
    }
    let result = bound
        .validate_receipt(&receipt.payload)
        .map_err(|error| mismatch(&receipt, error.to_string()))?;
    let receipt_value = serde_json::from_value(receipt.payload["receipt"].clone())
        .map_err(|error| mismatch(&receipt, error.to_string()))?;
    Ok(VerifiedEffectProof {
        prepared: bound.prepared,
        scope: bound.scope,
        result,
        receipt: receipt_value,
        sources: EffectProofSources {
            admission: coordinate(&admission),
            prepared: coordinate(&prepared),
            authorized: coordinate(&authorized),
            started: coordinate(&started),
            receipt: coordinate(&receipt),
            terminal: coordinate(&terminal),
        },
        terminal_kind: terminal.kind,
    })
}

fn validate_request(request: &EffectProofRequest) -> Result<(), EffectProofError> {
    // Check borrowed lengths before allocating a temporary scope for the shared
    // key validator; malformed caller input must not cause an unbounded clone.
    if [
        &request.execution.session_id,
        &request.execution.turn_id,
        &request.execution.execution_id,
    ]
    .iter()
    .any(|id| id.len() > 256)
    {
        return Err(EffectProofError::InvalidRequest(
            "oversized execution key".into(),
        ));
    }
    ToolExecutionScope {
        execution: request.execution.clone(),
        step_id: "proof-validation".into(),
        agent_snapshot_digest: None,
    }
    .validate()
    .map_err(|error| EffectProofError::InvalidRequest(error.to_string()))?;
    if request.effect_id.trim().is_empty()
        || request.effect_id.len() > 1281
        || request.terminal.event_id.trim().is_empty()
        || request.terminal.event_id.len() > 2048
        || request.terminal.cursor == 0
        || request.max_input_bytes == 0
        || request.max_input_bytes > MAX_EFFECT_PROOF_INPUT_BYTES
        || request.max_result_bytes == 0
        || request.max_result_bytes > MAX_EFFECT_PROOF_RESULT_BYTES
    {
        return Err(EffectProofError::InvalidRequest(
            "invalid identity, coordinate or ceiling".into(),
        ));
    }
    Ok(())
}

fn required<L: LedgerStore + ?Sized>(
    ledger: &L,
    key: &ExecutionKey,
    id: &str,
    kind: LedgerEventKind,
    limit: usize,
) -> Result<LedgerEvent, EffectProofError> {
    let event = load(ledger, key, id, limit)?;
    check_event(&event, key, id, kind).map_err(|error| mismatch(&event, error.to_string()))?;
    Ok(event)
}

fn load<L: LedgerStore + ?Sized>(
    ledger: &L,
    key: &ExecutionKey,
    id: &str,
    limit: usize,
) -> Result<LedgerEvent, EffectProofError> {
    let event = ledger
        .event_by_id(id)?
        .ok_or_else(|| EffectProofError::MissingEvidence {
            event_id: id.into(),
        })?;
    bounded(&event.payload, id, limit)?;
    check_event(&event, key, id, event.kind)
        .map_err(|error| mismatch(&event, error.to_string()))?;
    if event.cursor == 0 {
        return Err(mismatch(&event, "zero source cursor"));
    }
    Ok(event)
}

fn physical_terminal(event: &LedgerEvent) -> bool {
    match event.kind {
        LedgerEventKind::TurnCompleted => matches!(
            event.payload["reason"].as_str(),
            Some("FinalAnswer" | "Refused" | "Incomplete" | "MaxSteps" | "NoProgress")
        ),
        LedgerEventKind::TurnFailed
        | LedgerEventKind::TurnCancelled
        | LedgerEventKind::TurnTimedOut => true,
        _ => false,
    }
}

fn coordinate(event: &LedgerEvent) -> EffectProofCoordinate {
    EffectProofCoordinate {
        event_id: event.event_id.clone(),
        cursor: event.cursor,
    }
}

fn mismatch(event: &LedgerEvent, reason: impl Into<String>) -> EffectProofError {
    EffectProofError::BindingMismatch {
        event_id: event.event_id.clone(),
        reason: reason.into(),
    }
}

// Count compact serialized bytes without allocating an unbounded buffer. The
// writer aborts at the first chunk beyond the caller's ceiling.
fn bounded(value: &Value, id: &str, limit: usize) -> Result<(), EffectProofError> {
    struct Counter {
        remaining: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.remaining = self
                .remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| io::Error::other("payload ceiling exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter { remaining: limit }, value).map_err(|_| {
        EffectProofError::BoundsExceeded {
            event_id: id.into(),
            limit,
        }
    })
}

#[cfg(test)]
mod tests;
