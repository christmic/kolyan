//! One durable decision payload for host approval and Runtime verification.
//! Encoding evidence is not authorization to create an affirmative decision.

use kolyan_core::ApprovalConfirmation;
use serde_json::{Value, json};

/// Encode a host-owned decision. Runtime accepts only an exact affirmative
/// persisted decision for the current checkpoint and historical preparation.
pub fn approval_decision_payload(
    checkpoint_id: &str,
    confirmation: &ApprovalConfirmation,
    decision: &str,
) -> Value {
    json!({"schema_version":1,"checkpoint_id":checkpoint_id,"approval_id":confirmation.approval_id,
        "decision":decision,"prepared_digest":confirmation.prepared_digest,
        "policy_revision":confirmation.policy_revision,"scope":confirmation.scope,"evidence_id":confirmation.evidence_id})
}
