//! Executor-owned evidence resolves uncertainty without repeating an effect.

use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ToolResult;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{EffectGrant, EffectReceipt, EffectRequest, ExecutionKey, ReceiptStatus, RuntimeError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationRequest {
    pub reconciliation_id: String,
    pub execution: ExecutionKey,
    pub effect_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Only an admitted executor adapter may attest external effect state.
/// This port inspects evidence; it must not execute or retry the operation.
pub trait ToolEffectReconciler: Send + Sync {
    fn inspect(
        &self,
        request: &ReconciliationRequest,
        prepared: &EffectRequest,
        authorization: &EffectGrant,
    ) -> Result<ReconciliationResolution, RuntimeError>;
}

/// Persist verified reconciliation evidence. A recovered receipt is replayable,
/// but neither unknown nor proven-not-committed evidence grants a retry.
pub fn reconcile_tool_effect<L: LedgerStore, R: ToolEffectReconciler>(
    ledger: &L,
    request: &ReconciliationRequest,
    reconciler: &R,
) -> Result<ReconciliationResolution, RuntimeError> {
    if [
        &request.reconciliation_id,
        &request.effect_id,
        &request.execution.execution_id,
        &request.execution.turn_id,
        &request.execution.session_id,
    ]
    .iter()
    .any(|id| id.is_empty() || id.len() > 512)
    {
        return Err(invalid("invalid reconciliation identity"));
    }
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
    let prepared: EffectRequest = serde_json::from_value(
        required(
            ledger,
            request,
            &format!("{prefix}/prepared"),
            LedgerEventKind::EffectPrepared,
        )?
        .payload,
    )
    .map_err(|error| invalid(error.to_string()))?;
    let authorization: EffectGrant = serde_json::from_value(
        required(
            ledger,
            request,
            &format!("{prefix}/authorized"),
            LedgerEventKind::EffectAuthorized,
        )?
        .payload,
    )
    .map_err(|error| invalid(error.to_string()))?;
    let started = required(
        ledger,
        request,
        &format!("{prefix}/started"),
        LedgerEventKind::EffectStarted,
    )?;
    if started.payload["effect_id"] != request.effect_id {
        return Err(invalid("started effect identity differs"));
    }
    if prepared.effect_id != request.effect_id
        || authorization.effect_id != request.effect_id
        || prepared.input_digest != authorization.input_digest
        || prepared.policy_revision != authorization.authority_revision
        || authorization.authorization_id != format!("{prefix}/authorized")
    {
        return Err(invalid("prepared input or authorization binding differs"));
    }
    let decision_id = format!("{prefix}/reconciliation/{}", request.reconciliation_id);
    if let Some(decision) = ledger.event_by_id(&decision_id)? {
        check_identity(&decision, request, LedgerEventKind::EffectReconciled)?;
        if decision.payload["request"] != json!(request) {
            return Err(invalid("reconciliation request binding differs"));
        }
        return serde_json::from_value(decision.payload["resolution"].clone())
            .map_err(|error| invalid(error.to_string()));
    }
    if let Some(receipt) = ledger.event_by_id(&format!("{prefix}/receipt"))? {
        check_identity(&receipt, request, LedgerEventKind::EffectReceipt)?;
        let validated = validate_receipt(&receipt, &prepared, &authorization)?;
        if receipt.payload["reconciliation"]["request"] == json!(request) {
            let resolution: ReconciliationResolution =
                serde_json::from_value(receipt.payload["reconciliation"]["resolution"].clone())
                    .map_err(|error| invalid(error.to_string()))?;
            if let (
                ReconciliationResolution::Committed { output: a, .. },
                ReconciliationResolution::Committed { output: b, .. },
            ) = (&validated, &resolution)
            {
                if a != b {
                    return Err(invalid("reconciliation evidence and receipt differ"));
                }
            } else {
                return Err(invalid("receipt reconciliation is not committed"));
            }
            append_identical(
                ledger,
                request,
                &decision_id,
                LedgerEventKind::EffectReconciled,
                json!({"request":request,"resolution":resolution}),
            )?;
            return Ok(resolution);
        }
        return Ok(validated);
    }
    let resolution = reconciler.inspect(request, &prepared, &authorization)?;
    let evidence = match &resolution {
        ReconciliationResolution::Committed { evidence, .. }
        | ReconciliationResolution::NotCommitted { evidence }
        | ReconciliationResolution::Unknown { evidence } => evidence,
    };
    if evidence.is_empty() || evidence.len() > 16_384 {
        return Err(invalid("reconciliation requires bounded executor evidence"));
    }
    if let ReconciliationResolution::Committed {
        output,
        executor_id,
        executor_revision,
        ..
    } = &resolution
    {
        let call_id = request
            .effect_id
            .rsplit('/')
            .next()
            .ok_or_else(|| invalid("missing call identity"))?;
        if output.call_id != call_id || executor_id.is_empty() || executor_revision.is_empty() {
            return Err(invalid("reconciled result identity differs"));
        }
        let input: Value = serde_json::from_str(&prepared.input_digest)
            .map_err(|error| invalid(error.to_string()))?;
        if input["name"] != prepared.operation_kind {
            return Err(invalid("prepared tool identity differs"));
        }
        let receipt = EffectReceipt {
            receipt_id: format!("{prefix}/receipt"),
            effect_id: request.effect_id.clone(),
            authorization_id: authorization.authorization_id.clone(),
            input_digest: prepared.input_digest.clone(),
            executor_id: executor_id.clone(),
            executor_revision: executor_revision.clone(),
            result_digest: json!(output).to_string(),
            status: ReceiptStatus::Completed,
        };
        // The result, receipt and inspection evidence share one atomic append.
        append_identical(
            ledger,
            request,
            &receipt.receipt_id,
            LedgerEventKind::EffectReceipt,
            json!({"effect_id":request.effect_id,"input":input,"authorization":authorization,
                "output":output,"receipt":receipt,"reconciliation":{"request":request,"resolution":resolution}}),
        )?;
    }
    append_identical(
        ledger,
        request,
        &decision_id,
        LedgerEventKind::EffectReconciled,
        json!({"request":request,"resolution":resolution}),
    )?;
    Ok(resolution)
}

fn validate_receipt(
    event: &LedgerEvent,
    prepared: &EffectRequest,
    grant: &EffectGrant,
) -> Result<ReconciliationResolution, RuntimeError> {
    let receipt: EffectReceipt = serde_json::from_value(event.payload["receipt"].clone())
        .map_err(|e| invalid(e.to_string()))?;
    let output: ToolResult = serde_json::from_value(event.payload["output"].clone())
        .map_err(|e| invalid(e.to_string()))?;
    let result_digest =
        serde_json::to_string(&json!(output)).map_err(|error| invalid(error.to_string()))?;
    let input_digest = serde_json::to_string(&event.payload["input"])
        .map_err(|error| invalid(error.to_string()))?;
    if receipt.receipt_id != event.event_id
        || receipt.effect_id != prepared.effect_id
        || receipt.input_digest != prepared.input_digest
        || receipt.authorization_id != grant.authorization_id
        || receipt.status != ReceiptStatus::Completed
        || receipt.result_digest != result_digest
        || output.call_id != prepared.effect_id.rsplit('/').next().unwrap_or("")
        || input_digest != prepared.input_digest
        || event.payload["authorization"] != json!(grant)
        || receipt.executor_id.is_empty()
        || receipt.executor_revision.is_empty()
    {
        return Err(invalid(
            "receipt evidence does not bind the prepared effect",
        ));
    }
    Ok(ReconciliationResolution::Committed {
        output,
        executor_id: receipt.executor_id,
        executor_revision: receipt.executor_revision,
        evidence: format!("durable receipt {}", event.event_id),
    })
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
    check_identity(&event, request, kind)?;
    Ok(event)
}

fn check_identity(
    event: &LedgerEvent,
    request: &ReconciliationRequest,
    kind: LedgerEventKind,
) -> Result<(), RuntimeError> {
    if event.execution_id != request.execution.execution_id
        || event.turn_id != request.execution.turn_id
        || event.kind != kind
    {
        return Err(invalid("foreign reconciliation fact"));
    }
    Ok(())
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

fn invalid(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Driver(message.into())
}

#[cfg(test)]
mod tests;
