//! Read-only coordinates and display projections of compound suspensions.
//! Authority remains in the Runtime checkpoint; display metadata cannot resume it.

use kolyan_core::{ApprovalConfirmation, ResumeInput, TurnSuspension};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_storage::StorageError;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{ExecutionRef, ServerError, TaskSuspension};

pub(crate) fn verify_resume_input<L: LedgerStore>(
    ledger: &L,
    key: &ExecutionRef,
    suspension: &TurnSuspension,
    input: &ResumeInput,
) -> Result<(), ServerError> {
    validate_owner(key, suspension)?;
    if let ResumeInput::ApprovalConfirmed(confirmation) = input {
        let approval = if let Some(saved) =
            suspension.checkpoint.approvals.iter().find(|approval| {
                approval.approval_id == confirmation.approval_id
                    && approval.evidence_id.as_deref() == Some(confirmation.evidence_id.as_str())
            }) {
            // A persisted merge may precede driving. Reuse its historical proof;
            // do not manufacture a new pending approval or reapply expiry.
            saved
        } else {
            pending_approval(suspension, &confirmation.approval_id)?
        };
        if confirmation.scope != approval.scope
            || confirmation.policy_revision != approval.policy_revision
            || confirmation.prepared_digest != approval.prepared.digest()
        {
            return Err(
                StorageError::Conflict("approval confirmation binding differs".into()).into(),
            );
        }
        let event = ledger
            .event_by_id(&confirmation.evidence_id)
            .map_err(crate::CoordinatorError::from)?
            .ok_or_else(|| {
                StorageError::Conflict("approval confirmation has no durable evidence".into())
            })?;
        if event.kind != LedgerEventKind::ApprovalResolved
            || event.event_id != confirmation.evidence_id
            || event.idempotency_key != confirmation.evidence_id
            || event.execution_id != key.execution_id
            || event.turn_id != key.turn_id
            || event.payload != decision_payload(suspension, confirmation, "approve")
        {
            return Err(StorageError::Conflict(
                "approval evidence is not an exact affirmative decision".into(),
            )
            .into());
        }
    }
    // External result authenticity belongs to the explicit Runtime host verifier.
    // The default verifier refuses it; display coordinates confer no authority.
    Ok(())
}

pub(crate) fn confirm_approval<L: LedgerStore>(
    ledger: &L,
    key: &ExecutionRef,
    suspension: &TurnSuspension,
    approval_id: &str,
) -> Result<ApprovalConfirmation, ServerError> {
    record_approval_decision(ledger, key, suspension, approval_id, "approve")
}

pub(crate) fn record_approval_decision<L: LedgerStore>(
    ledger: &L,
    key: &ExecutionRef,
    suspension: &TurnSuspension,
    approval_id: &str,
    decision: &str,
) -> Result<ApprovalConfirmation, ServerError> {
    validate_owner(key, suspension)?;
    let approval = if let Some(saved) = suspension
        .checkpoint
        .approvals
        .iter()
        .find(|approval| approval.approval_id == approval_id && approval.evidence_id.is_some())
    {
        saved
    } else {
        pending_approval(suspension, approval_id)?
    };
    let coordinate = serde_json::to_vec(&(key, &suspension.checkpoint.checkpoint_id, approval_id))
        .map_err(StorageError::from)?;
    let evidence_id = format!("approval-decision-{:x}", Sha256::digest(coordinate));
    if approval
        .evidence_id
        .as_ref()
        .is_some_and(|saved| saved != &evidence_id)
    {
        return Err(
            StorageError::Conflict("saved approval evidence identity differs".into()).into(),
        );
    }
    let confirmation = ApprovalConfirmation {
        approval_id: approval_id.into(),
        prepared_digest: approval.prepared.digest().into(),
        policy_revision: approval.policy_revision.clone(),
        scope: approval.scope.clone(),
        evidence_id: evidence_id.clone(),
    };
    let payload = decision_payload(suspension, &confirmation, decision);
    if let Some(event) = ledger
        .event_by_id(&evidence_id)
        .map_err(crate::CoordinatorError::from)?
    {
        if event.kind != LedgerEventKind::ApprovalResolved
            || event.event_id != evidence_id
            || event.idempotency_key != evidence_id
            || event.execution_id != key.execution_id
            || event.turn_id != key.turn_id
            || event.payload != payload
        {
            return Err(StorageError::Conflict("conflicting approval decision".into()).into());
        }
    } else {
        ledger
            .append_unless_cancelled(LedgerEvent {
                event_id: evidence_id.clone(),
                idempotency_key: evidence_id,
                execution_id: key.execution_id.clone(),
                turn_id: key.turn_id.clone(),
                cursor: 0,
                kind: LedgerEventKind::ApprovalResolved,
                payload,
            })
            .map_err(crate::CoordinatorError::from)?;
    }
    Ok(confirmation)
}

fn validate_owner(key: &ExecutionRef, suspension: &TurnSuspension) -> Result<(), ServerError> {
    let scope = &suspension.checkpoint.scope;
    if scope.execution.session_id != key.session_id
        || scope.execution.execution_id != key.execution_id
        || scope.execution.turn_id != key.turn_id
    {
        return Err(StorageError::Conflict("foreign suspension owner".into()).into());
    }
    suspension
        .validate(scope)
        .map_err(|error| StorageError::Conflict(error.to_string()))?;
    Ok(())
}

fn pending_approval<'a>(
    suspension: &'a TurnSuspension,
    approval_id: &str,
) -> Result<&'a kolyan_core::CheckpointApproval, ServerError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| StorageError::Conflict(error.to_string()))?
        .as_millis();
    suspension
        .checkpoint
        .approvals
        .iter()
        .find(|approval| {
            approval.approval_id == approval_id
                && approval.evidence_id.is_none()
                && approval
                    .expires_at_ms
                    .is_none_or(|expiry| u128::from(expiry) > now)
                && suspension
                    .waiting
                    .approvals
                    .iter()
                    .any(|waiting| waiting.approval_id == approval_id)
        })
        .ok_or_else(|| {
            StorageError::Conflict("approval is expired, resolved or not pending".into()).into()
        })
}

fn decision_payload(
    suspension: &TurnSuspension,
    confirmation: &ApprovalConfirmation,
    decision: &str,
) -> Value {
    kolyan_runtime::approval_decision_payload(
        &suspension.checkpoint.checkpoint_id,
        confirmation,
        decision,
    )
}

/// Select only the latest compound checkpoint, never an approval-only snapshot.
/// A later execution admission invalidates the earlier stopped projection.
pub fn current_suspension(
    key: &ExecutionRef,
    events: &[LedgerEvent],
) -> Result<Option<TurnSuspension>, ServerError> {
    let latest = events.iter().rev().find(|event| {
        matches!(
            event.kind,
            LedgerEventKind::ExecutionStarted
                | LedgerEventKind::ExecutionSuspended
                | LedgerEventKind::TurnCheckpointMerged
                | LedgerEventKind::TurnCheckpointPrepared
                | LedgerEventKind::ExecutionCancelled
                | LedgerEventKind::TurnCancelled
                | LedgerEventKind::TurnCompleted
                | LedgerEventKind::TurnFailed
                | LedgerEventKind::TurnTimedOut
        )
    });
    let Some(event) = latest.filter(|event| event.kind == LedgerEventKind::ExecutionSuspended)
    else {
        return Ok(None);
    };
    if event.payload["schema_version"] != 1
        || !event
            .payload
            .as_object()
            .is_some_and(|object| object.len() == 3)
        || event.payload["publication_cursor"]
            .as_u64()
            .is_none_or(|cursor| cursor >= event.cursor)
    {
        return Err(StorageError::Conflict(
            "unknown or corrupt compound suspension envelope".into(),
        )
        .into());
    }
    let suspension: TurnSuspension =
        serde_json::from_value(event.payload["suspension"].clone()).map_err(StorageError::from)?;
    let scope = &suspension.checkpoint.scope;
    if event.execution_id != key.execution_id
        || event.turn_id != key.turn_id
        || scope.execution.session_id != key.session_id
        || scope.execution.execution_id != key.execution_id
        || scope.execution.turn_id != key.turn_id
    {
        return Err(StorageError::Conflict("foreign suspension identity".into()).into());
    }
    suspension
        .validate(scope)
        .map_err(|error| StorageError::Conflict(error.to_string()))?;
    Ok(Some(suspension))
}

pub(crate) fn task_waiting(suspension: &TurnSuspension) -> TaskSuspension {
    TaskSuspension {
        checkpoint_id: suspension.checkpoint.checkpoint_id.clone(),
        approval_ids: suspension
            .waiting
            .approvals
            .iter()
            .map(|approval| approval.approval_id.clone())
            .collect(),
        external_wait_ids: suspension
            .waiting
            .external_waits
            .iter()
            .map(|pending| pending.wait.wait_id.clone())
            .collect(),
    }
}

/// Public display metadata, excluding preparation, scope, grants and opaque
/// external bindings. This value is never accepted as resume input.
pub fn suspension_view(suspension: &TurnSuspension) -> Result<Value, ServerError> {
    suspension
        .validate(&suspension.checkpoint.scope)
        .map_err(|error| StorageError::Conflict(error.to_string()))?;
    let approvals = suspension
        .waiting
        .approvals
        .iter()
        .map(|approval| {
            let call = suspension
                .checkpoint
                .calls
                .iter()
                .find(|item| item.call.id == approval.call_id)
                .ok_or_else(|| {
                    StorageError::Conflict("approval call missing from checkpoint".into())
                })?;
            Ok(
                json!({"approval_id":approval.approval_id,"turn_id":approval.turn_id,
            "call_id":approval.call_id,"tool_name":approval.tool_name,"reason":approval.reason,
            "expires_at_ms":approval.expires_at_ms,"arguments":call.call.arguments}),
            )
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    let waits = suspension
        .waiting
        .external_waits
        .iter()
        .map(|pending| {
            json!({
                "call_id":pending.call_id,"wait_id":pending.wait.wait_id,
                "kind":pending.wait.kind,"schema_version":pending.wait.schema_version,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"checkpoint_id":suspension.checkpoint.checkpoint_id,
        "pending_approvals":approvals,"external_waits":waits}))
}

#[cfg(test)]
pub(crate) mod tests;
