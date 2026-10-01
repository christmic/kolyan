//! Executor-owned evidence resolves uncertainty without repeating an effect.
//! Saved preparation and scoped authority are validated as historical evidence;
//! reconciliation never consults current policy to grant another operation.

pub(crate) mod receipt;

use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ToolResult;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{EffectGrant, EffectRequest, ExecutionKey, RuntimeError};
use receipt::{PreparedEvidence, check_event};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationRequest {
    pub reconciliation_id: String,
    pub execution: ExecutionKey,
    pub effect_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReconciliationResolution {
    Committed {
        output: ToolResult,
        executor_id: String,
        executor_revision: String,
        evidence: String,
    },
    NotCommitted {
        evidence: String,
    },
    Unknown {
        evidence: String,
    },
}

/// Only a trusted admitted adapter may attest external effect state. Inspection
/// must not execute/retry effects. The request and grants here are historical,
/// already validated journal evidence, not newly issued execution authority.
pub trait ToolEffectReconciler: Send + Sync {
    fn inspect(
        &self,
        request: &ReconciliationRequest,
        prepared: &EffectRequest,
        authorization: &EffectGrant,
    ) -> Result<ReconciliationResolution, RuntimeError>;
}

/// Persist exact evidence for an uncertain prepared tool effect. A committed
/// receipt is recoverable; Unknown and NotCommitted never authorize retries.
/// A decision alone cannot replace a missing/corrupt committed receipt. Repair
/// after the receipt append uses its saved proof without calling the inspector.
pub fn reconcile_tool_effect<L: LedgerStore, R: ToolEffectReconciler>(
    ledger: &L,
    request: &ReconciliationRequest,
    reconciler: &R,
) -> Result<ReconciliationResolution, RuntimeError> {
    validate_request(request)?;
    let prefix = format!(
        "{}/effect/{}",
        request.execution.execution_id, request.effect_id
    );
    let identity = required(
        ledger,
        request,
        &format!("{}/execution-started", request.execution.execution_id),
        LedgerEventKind::ExecutionStarted,
    )?;
    if identity.payload != json!(request.execution) {
        return Err(invalid("execution binding differs"));
    }
    let prepared = required(
        ledger,
        request,
        &format!("{prefix}/prepared"),
        LedgerEventKind::EffectPrepared,
    )?;
    let authorized = required(
        ledger,
        request,
        &format!("{prefix}/authorized"),
        LedgerEventKind::EffectAuthorized,
    )?;
    let bound = PreparedEvidence::from_facts(
        &request.execution,
        &request.effect_id,
        &prepared.payload,
        &authorized.payload,
    )
    .map_err(invalid)?;
    let started = required(
        ledger,
        request,
        &format!("{prefix}/started"),
        LedgerEventKind::EffectStarted,
    )?;
    if started.payload != bound.started_payload() {
        return Err(invalid("started effect evidence binding differs"));
    }
    let receipt_id = format!("{prefix}/receipt");
    let receipt = ledger.event_by_id(&receipt_id)?;
    if let Some(event) = &receipt {
        check_event(
            event,
            &request.execution,
            &receipt_id,
            LedgerEventKind::EffectReceipt,
        )
        .map_err(invalid)?;
        bound
            .validate_receipt(&event.payload)
            .map_err(invalid)?
            .map_err(|_| invalid("failed receipt is not a committed reconciliation"))?;
    }
    let decision_id = format!("{prefix}/reconciliation/{}", request.reconciliation_id);
    if let Some(decision) = ledger.event_by_id(&decision_id)? {
        check_event(
            &decision,
            &request.execution,
            &decision_id,
            LedgerEventKind::EffectReconciled,
        )
        .map_err(invalid)?;
        let resolution: ReconciliationResolution =
            serde_json::from_value(decision.payload["resolution"].clone()).map_err(invalid)?;
        bound
            .validate_resolution(request, &resolution)
            .map_err(invalid)?;
        if decision.payload != decision_payload(request, &resolution, &bound) {
            return Err(invalid("reconciliation decision binding differs"));
        }
        validate_decision_receipt(&bound, request, &resolution, receipt.as_ref())?;
        return Ok(resolution);
    }
    if let Some(event) = receipt {
        let output = bound
            .validate_receipt(&event.payload)
            .map_err(invalid)?
            .map_err(|_| invalid("failed receipt is not a committed reconciliation"))?;
        if let Some(metadata) = event.payload.get("reconciliation") {
            let saved_request: ReconciliationRequest =
                serde_json::from_value(metadata["request"].clone()).map_err(invalid)?;
            let resolution: ReconciliationResolution =
                serde_json::from_value(metadata["resolution"].clone()).map_err(invalid)?;
            // A different inspection coordinate may observe the same committed
            // result, but cannot claim ownership of the original saved proof.
            if saved_request == *request {
                append_identical(
                    ledger,
                    request,
                    &decision_id,
                    LedgerEventKind::EffectReconciled,
                    decision_payload(request, &resolution, &bound),
                )?;
                return Ok(resolution);
            }
        }
        return Ok(ReconciliationResolution::Committed {
            output,
            executor_id: bound.executor_id(),
            executor_revision: bound.prepared.tool_revision().to_owned(),
            evidence: format!("durable receipt {}", event.event_id),
        });
    }
    let resolution = reconciler.inspect(request, &bound.request, &bound.authorization)?;
    bound
        .validate_resolution(request, &resolution)
        .map_err(invalid)?;
    if let ReconciliationResolution::Committed { output, .. } = &resolution {
        let mut payload = bound
            .receipt_payload(&Ok(output.clone()))
            .map_err(invalid)?;
        payload["reconciliation"] = json!({"request": request, "resolution": resolution});
        // Output, exact binding and external proof commit in one receipt append.
        append_identical(
            ledger,
            request,
            &receipt_id,
            LedgerEventKind::EffectReceipt,
            payload,
        )?;
    }
    append_identical(
        ledger,
        request,
        &decision_id,
        LedgerEventKind::EffectReconciled,
        decision_payload(request, &resolution, &bound),
    )?;
    Ok(resolution)
}

fn validate_request(request: &ReconciliationRequest) -> Result<(), RuntimeError> {
    for id in [
        &request.reconciliation_id,
        &request.execution.session_id,
        &request.execution.execution_id,
        &request.execution.turn_id,
    ] {
        if id.is_empty()
            || id.len() > 256
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
        {
            return Err(invalid("invalid reconciliation identity"));
        }
    }
    if request.effect_id.is_empty()
        || request.effect_id.len() > 1281
        || request.effect_id.chars().any(char::is_control)
    {
        return Err(invalid("invalid reconciliation effect identity"));
    }
    Ok(())
}

fn decision_payload(
    request: &ReconciliationRequest,
    resolution: &ReconciliationResolution,
    bound: &PreparedEvidence,
) -> Value {
    json!({"request": request, "resolution": resolution, "input_digest": bound.request.input_digest,
        "authorization_digest": bound.authorization.constraints_digest, "scope": bound.scope})
}

fn validate_decision_receipt(
    bound: &PreparedEvidence,
    request: &ReconciliationRequest,
    resolution: &ReconciliationResolution,
    receipt: Option<&LedgerEvent>,
) -> Result<(), RuntimeError> {
    match resolution {
        ReconciliationResolution::Committed { output, .. } => {
            let event = receipt.ok_or_else(|| invalid("committed decision has no receipt"))?;
            let actual = bound
                .validate_receipt(&event.payload)
                .map_err(invalid)?
                .map_err(|_| invalid("committed decision has a failed receipt"))?;
            if actual != *output
                || event.payload["reconciliation"]
                    != json!({"request": request, "resolution": resolution})
            {
                return Err(invalid("decision and committed receipt proof differ"));
            }
        }
        ReconciliationResolution::NotCommitted { .. }
        | ReconciliationResolution::Unknown { .. }
            if receipt.is_some() =>
        {
            return Err(invalid("noncommitted decision conflicts with a receipt"));
        }
        _ => {}
    }
    Ok(())
}

fn required<L: LedgerStore>(
    ledger: &L,
    request: &ReconciliationRequest,
    id: &str,
    kind: LedgerEventKind,
) -> Result<LedgerEvent, RuntimeError> {
    let event = ledger
        .event_by_id(id)?
        .ok_or_else(|| invalid(format!("missing fact {id}")))?;
    check_event(&event, &request.execution, id, kind).map_err(invalid)?;
    Ok(event)
}

fn append_identical<L: LedgerStore>(
    ledger: &L,
    request: &ReconciliationRequest,
    id: &str,
    kind: LedgerEventKind,
    payload: Value,
) -> Result<(), RuntimeError> {
    let event = LedgerEvent {
        event_id: id.into(),
        turn_id: request.execution.turn_id.clone(),
        execution_id: request.execution.execution_id.clone(),
        cursor: 0,
        kind,
        idempotency_key: id.into(),
        payload,
    };
    match ledger.append(event.clone()) {
        Ok(_) => Ok(()),
        Err(kolyan_ledger::LedgerError::Conflict(_)) => {
            let mut actual = ledger
                .event_by_id(id)?
                .ok_or_else(|| invalid("conflicting fact unavailable"))?;
            actual.cursor = 0;
            if actual == event {
                Ok(())
            } else {
                Err(invalid("conflicting reconciliation evidence"))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn invalid(message: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Driver(message.to_string())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "reconciliation/tests/prepared.rs"]
mod prepared_tests;
