//! Derived waiting summaries and pure approval/result merging. Summaries are
//! display projections, not duplicated authority or loop progress.

use kolyan_policy::ToolExecutionScope;
use serde::{Deserialize, Serialize};

use super::checkpoint::{CheckpointError, identifier, required_option};
use super::{CheckpointCallState, ExternalResolution, ExternalWait, TurnCheckpoint};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnSuspension {
    pub checkpoint: TurnCheckpoint,
    pub waiting: SuspensionSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuspensionSummary {
    pub approvals: Vec<ApprovalRequest>,
    pub external_waits: Vec<PendingExternalWait>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRequest {
    pub approval_id: String,
    pub turn_id: String,
    pub call_id: String,
    pub tool_name: String,
    pub reason: String,
    #[serde(deserialize_with = "required_option")]
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingExternalWait {
    pub call_id: String,
    pub wait: ExternalWait,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ResumeInput {
    ApprovalConfirmed(ApprovalConfirmation),
    ExternalResolved(Vec<ExternalResolution>),
}

/// Trusted host proof must be verified before supplying this value. Core checks
/// exact binding, not the authenticity of user confirmation or durable evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalConfirmation {
    pub approval_id: String,
    pub prepared_digest: String,
    pub policy_revision: String,
    #[serde(deserialize_with = "super::checkpoint::exact")]
    pub scope: ToolExecutionScope,
    pub evidence_id: String,
}

impl TurnCheckpoint {
    pub fn suspension_summary(
        &self,
        expected_scope: &ToolExecutionScope,
    ) -> Result<SuspensionSummary, CheckpointError> {
        self.validate(expected_scope)?;
        let approvals = self
            .approvals
            .iter()
            .filter(|approval| {
                approval.evidence_id.is_none()
                    && self.calls.iter().any(|item| {
                        item.call.id == approval.prepared.call().id
                            && matches!(item.state, CheckpointCallState::Ready)
                    })
            })
            .map(|approval| ApprovalRequest {
                approval_id: approval.approval_id.clone(),
                turn_id: self.scope.execution.turn_id.clone(),
                call_id: approval.prepared.call().id.clone(),
                tool_name: approval.prepared.call().name.clone(),
                reason: approval.reason.clone(),
                expires_at_ms: approval.expires_at_ms,
            })
            .collect();
        let external_waits = self
            .calls
            .iter()
            .filter_map(|item| {
                if let CheckpointCallState::AwaitingExternal { wait, .. } = &item.state {
                    Some(PendingExternalWait {
                        call_id: item.call.id.clone(),
                        wait: wait.clone(),
                    })
                } else {
                    None
                }
            })
            .collect();
        Ok(SuspensionSummary {
            approvals,
            external_waits,
        })
    }

    pub(super) fn merge_approval(
        &self,
        confirmation: &ApprovalConfirmation,
        expected_scope: &ToolExecutionScope,
        now_ms: u64,
    ) -> Result<Self, CheckpointError> {
        self.validate(expected_scope)?;
        identifier(&confirmation.evidence_id)?;
        let mut merged = self.clone();
        let approval = merged
            .approvals
            .iter_mut()
            .find(|approval| approval.approval_id == confirmation.approval_id)
            .ok_or_else(|| CheckpointError::Invalid("unknown approval".into()))?;
        if approval.scope != confirmation.scope
            || confirmation.scope != *expected_scope
            || approval.prepared.digest() != confirmation.prepared_digest
            || approval.policy_revision != confirmation.policy_revision
            || approval.evidence_id.is_some()
            || approval
                .expires_at_ms
                .is_some_and(|expiry| expiry <= now_ms)
            || !merged.calls.iter().any(|item| {
                item.call.id == approval.prepared.call().id
                    && matches!(item.state, CheckpointCallState::Ready)
            })
        {
            return Err(CheckpointError::Invalid(
                "approval confirmation binding is stale, resolved or foreign".into(),
            ));
        }
        approval.evidence_id = Some(confirmation.evidence_id.clone());
        merged.validate(expected_scope)?;
        Ok(merged)
    }
}

impl TurnSuspension {
    pub fn from_checkpoint(
        checkpoint: TurnCheckpoint,
        expected_scope: &ToolExecutionScope,
    ) -> Result<Self, CheckpointError> {
        let waiting = checkpoint.suspension_summary(expected_scope)?;
        Ok(Self {
            checkpoint,
            waiting,
        })
    }

    pub fn validate(&self, expected_scope: &ToolExecutionScope) -> Result<(), CheckpointError> {
        if self.waiting != self.checkpoint.suspension_summary(expected_scope)? {
            return Err(CheckpointError::Invalid(
                "suspension summary differs from checkpoint".into(),
            ));
        }
        Ok(())
    }
}
