//! Exact tool preparation and grants. Only trusted adapters prepare claims;
//! deserialization and digests establish integrity, not independent authority.

use kolyan_model::ToolCall;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ExecutionConstraints, InvocationClaim, PolicyDecision, PolicyDecisionKind};

/// Mandatory enforcement ceilings determined by the trusted tool adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRequirements {
    pub process_sandbox: bool,
    pub max_output_bytes: u64,
    pub timeout_ms: u64,
}

/// Immutable validated input, exact implementation and adapter-derived claims.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCall {
    call: ToolCall,
    tool_revision: String,
    claim: InvocationClaim,
    requirements: ToolRequirements,
    digest: String,
}

/// Authority issued by a trusted policy host for exactly one preparation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedGrant {
    call_id: String,
    tool_name: String,
    tool_revision: String,
    prepared_digest: String,
    policy_revision: String,
    constraints: ExecutionConstraints,
    approval_evidence_id: Option<String>,
}

/// Trusted host evidence, verified against its durable approval record before
/// issuance. Possession of arbitrary serialized bytes is not approval authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalEvidence {
    NotConfirmed,
    Confirmed {
        prepared_digest: String,
        policy_revision: String,
        evidence_id: String,
    },
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum PreparedError {
    #[error("invalid prepared call: {0}")]
    Invalid(String),
    #[error("prepared grant denied: {0}")]
    Denied(String),
    #[error("prepared grant binding mismatch")]
    BindingMismatch,
}

impl PreparedCall {
    /// Construct after a trusted adapter has validated arguments and resources.
    /// This method does not authorize execution or make model claims trustworthy.
    pub fn new(
        call: ToolCall,
        tool_revision: String,
        claim: InvocationClaim,
        requirements: ToolRequirements,
    ) -> Result<Self, PreparedError> {
        let mut prepared = Self {
            call,
            tool_revision,
            claim,
            requirements,
            digest: String::new(),
        };
        prepared.validate_fields()?;
        prepared.digest = prepared.compute_digest()?;
        Ok(prepared)
    }

    pub fn call(&self) -> &ToolCall {
        &self.call
    }

    pub fn tool_revision(&self) -> &str {
        &self.tool_revision
    }

    pub fn claim(&self) -> &InvocationClaim {
        &self.claim
    }

    pub fn requirements(&self) -> &ToolRequirements {
        &self.requirements
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Validate stored content before policy evaluation or recovery.
    pub fn validate(&self) -> Result<(), PreparedError> {
        self.validate_fields()?;
        if self.compute_digest()? != self.digest {
            return Err(PreparedError::Invalid("digest mismatch".into()));
        }
        Ok(())
    }

    fn validate_fields(&self) -> Result<(), PreparedError> {
        for identity in [&self.call.id, &self.call.name, &self.tool_revision] {
            if identity.trim().is_empty() || identity.len() > 1024 {
                return Err(PreparedError::Invalid("empty or oversized identity".into()));
            }
        }
        if self.call.name != self.claim.tool_name
            || !self.call.arguments.is_object()
            || self.claim.capabilities.is_empty()
            || self.claim.effects.is_empty()
            || self.requirements.max_output_bytes == 0
            || self.requirements.timeout_ms == 0
        {
            return Err(PreparedError::Invalid(
                "inconsistent semantics or limits".into(),
            ));
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<String, PreparedError> {
        // JSON maps use ordered keys in the workspace serde_json configuration.
        let value = serde_json::json!({
            "schema_version": 1,
            "call": self.call,
            "tool_revision": self.tool_revision,
            "claim": self.claim,
            "requirements": self.requirements,
        });
        let bytes = serde_json::to_vec(&value)
            .map_err(|error| PreparedError::Invalid(error.to_string()))?;
        if bytes.len() > 1024 * 1024 {
            return Err(PreparedError::Invalid(
                "prepared input exceeds byte limit".into(),
            ));
        }
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

impl PreparedGrant {
    /// Issue after Allow or exact approval evidence verified by the trusted host.
    pub fn issue(
        prepared: &PreparedCall,
        decision: PolicyDecision,
        approval: ApprovalEvidence,
    ) -> Result<Self, PreparedError> {
        prepared.validate()?;
        let approval_evidence_id = match approval {
            ApprovalEvidence::NotConfirmed => None,
            ApprovalEvidence::Confirmed {
                prepared_digest,
                policy_revision,
                evidence_id,
            } if prepared_digest == prepared.digest
                && policy_revision == decision.policy_version
                && !evidence_id.trim().is_empty()
                && evidence_id.len() <= 1024 =>
            {
                Some(evidence_id)
            }
            _ => return Err(PreparedError::BindingMismatch),
        };
        match decision.kind {
            PolicyDecisionKind::Allow | PolicyDecisionKind::AllowWithConstraints => {}
            PolicyDecisionKind::RequireApproval if approval_evidence_id.is_some() => {}
            _ => return Err(PreparedError::Denied(decision.reason)),
        }
        if decision.policy_version.trim().is_empty() {
            return Err(PreparedError::Invalid("empty policy revision".into()));
        }
        let max_output_bytes = decision
            .constraints
            .max_output_bytes
            .unwrap_or(prepared.requirements.max_output_bytes)
            .min(prepared.requirements.max_output_bytes);
        let timeout_ms = decision
            .constraints
            .timeout_ms
            .unwrap_or(prepared.requirements.timeout_ms)
            .min(prepared.requirements.timeout_ms);
        if max_output_bytes == 0 || timeout_ms == 0 {
            return Err(PreparedError::Invalid("zero execution limit".into()));
        }
        Ok(Self {
            call_id: prepared.call.id.clone(),
            tool_name: prepared.call.name.clone(),
            tool_revision: prepared.tool_revision.clone(),
            prepared_digest: prepared.digest.clone(),
            policy_revision: decision.policy_version,
            constraints: ExecutionConstraints {
                max_output_bytes: Some(max_output_bytes),
                timeout_ms: Some(timeout_ms),
            },
            approval_evidence_id,
        })
    }

    /// Check exact input and current policy revision before any external effect.
    /// The caller must also enforce isolation and verify trusted grant provenance.
    pub fn validate(
        &self,
        prepared: &PreparedCall,
        current_policy_revision: &str,
    ) -> Result<(), PreparedError> {
        prepared.validate()?;
        if self.call_id != prepared.call.id
            || self.tool_name != prepared.call.name
            || self.tool_revision != prepared.tool_revision
            || self.prepared_digest != prepared.digest
            || self.policy_revision != current_policy_revision
            || self
                .constraints
                .max_output_bytes
                .is_none_or(|limit| limit == 0 || limit > prepared.requirements.max_output_bytes)
            || self
                .constraints
                .timeout_ms
                .is_none_or(|limit| limit == 0 || limit > prepared.requirements.timeout_ms)
        {
            return Err(PreparedError::BindingMismatch);
        }
        Ok(())
    }

    pub fn constraints(&self) -> &ExecutionConstraints {
        &self.constraints
    }
}

#[cfg(test)]
mod tests;
