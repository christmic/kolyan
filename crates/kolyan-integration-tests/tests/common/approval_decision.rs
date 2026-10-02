//! Trusted host approval evidence for direct Runtime fixtures. Runtime never
//! creates this affirmative decision merely because a caller requests resume.

use kolyan_core::{ApprovalConfirmation, TurnSuspension};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_policy::ToolExecutionScope;
use kolyan_types::ExecutionKey;

pub fn confirm<L: LedgerStore>(
    ledger: &L,
    key: ExecutionKey,
    suspension: &TurnSuspension,
) -> ApprovalConfirmation {
    let scope = ToolExecutionScope {
        step_id: format!(
            "{}-step-{}",
            key.turn_id,
            suspension.checkpoint.steps.len() - 1
        ),
        execution: key.clone(),
        agent_snapshot_digest: None,
    };
    suspension.validate(&scope).unwrap();
    assert!(suspension.waiting.external_waits.is_empty());
    let request = suspension.waiting.approvals.first().unwrap();
    let saved = suspension
        .checkpoint
        .approvals
        .iter()
        .find(|approval| approval.approval_id == request.approval_id)
        .unwrap();
    let confirmation = ApprovalConfirmation {
        approval_id: request.approval_id.clone(),
        prepared_digest: saved.prepared.digest().into(),
        policy_revision: saved.policy_revision.clone(),
        scope,
        evidence_id: format!("fixture-confirmed-{}", request.approval_id),
    };
    ledger
        .append(LedgerEvent {
            event_id: confirmation.evidence_id.clone(),
            idempotency_key: confirmation.evidence_id.clone(),
            execution_id: key.execution_id,
            turn_id: key.turn_id,
            cursor: 0,
            kind: LedgerEventKind::ApprovalResolved,
            payload: kolyan_runtime::approval_decision_payload(
                &suspension.checkpoint.checkpoint_id,
                &confirmation,
                "approve",
            ),
        })
        .unwrap();
    confirmation
}
