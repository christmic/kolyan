//! Exact tool preparation and grants. Only trusted adapters prepare claims;
//! deserialization and digests establish integrity, not independent authority.

use kolyan_model::ToolCall;
use kolyan_types::ExecutionKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ExecutionConstraints, InvocationClaim, PolicyDecision, PolicyDecisionKind};

/// Trusted execution coordinates, independent of reusable model tool-call IDs.
/// Agent snapshots are optional only for non-Agent executions; scope is mandatory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionScope {
    pub execution: ExecutionKey,
    pub step_id: String,
    pub agent_snapshot_digest: Option<String>,
}

impl ToolExecutionScope {
    /// Coordinates must be 1..256 ASCII identifier bytes; snapshots are exact
    /// 64-hex digests. Nothing is inferred from a tool-call ID or given a default.
    pub fn validate(&self) -> Result<(), PreparedError> {
        for value in [
            &self.execution.session_id,
            &self.execution.execution_id,
            &self.execution.turn_id,
            &self.step_id,
        ] {
            if value.is_empty()
                || value.len() > 256
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
            {
                return Err(PreparedError::Invalid(
                    "execution coordinates must be 1..256 ASCII identifier bytes".into(),
                ));
            }
        }
        if let Some(digest) = &self.agent_snapshot_digest
            && (digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(PreparedError::Invalid(
                "Agent snapshot digest must be exactly 64 hex bytes".into(),
            ));
        }
        Ok(())
    }
}

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
    execution_binding: serde_json::Value,
    digest: String,
}

/// Authority issued by a trusted policy host for exactly one preparation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedGrant {
    scope: ToolExecutionScope,
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
        scope: ToolExecutionScope,
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
            execution_binding: serde_json::Value::Null,
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

    /// Bind an adapter-owned execution plan without changing model arguments or
    /// conflating resource identity with implementation revision. Null explicitly
    /// means no additional plan; otherwise a bounded JSON object is required.
    /// Policy preserves this opaque plan and hashes it, but only the trusted
    /// adapter can interpret and enforce its semantics.
    pub fn with_execution_binding(
        mut self,
        binding: serde_json::Value,
    ) -> Result<Self, PreparedError> {
        self.validate()?;
        self.execution_binding = binding;
        self.validate_fields()?;
        self.digest = self.compute_digest()?;
        Ok(self)
    }

    pub fn execution_binding(&self) -> &serde_json::Value {
        &self.execution_binding
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
            || (!self.execution_binding.is_null() && !self.execution_binding.is_object())
        {
            return Err(PreparedError::Invalid(
                "inconsistent semantics or limits".into(),
            ));
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<String, PreparedError> {
        let value = canonical(serde_json::json!({
            "schema_version": 2,
            "call": self.call,
            "tool_revision": self.tool_revision,
            "claim": self.claim,
            "requirements": self.requirements,
            "execution_binding": self.execution_binding,
        }));
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

fn canonical(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        // Sort explicitly so enabling serde_json/preserve_order in another
        // workspace crate cannot change the approval digest contract.
        Value::Object(entries) => Value::Object(
            entries
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .map(|(key, value)| (key, canonical(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        value => value,
    }
}

impl PreparedGrant {
    /// Issue after Allow or exact approval evidence verified by the trusted host.
    pub fn issue(
        prepared: &PreparedCall,
        decision: PolicyDecision,
        approval: ApprovalEvidence,
        scope: ToolExecutionScope,
    ) -> Result<Self, PreparedError> {
        prepared.validate()?;
        scope.validate()?;
        let approval_evidence_id = match approval {
            ApprovalEvidence::NotConfirmed => None,
            ApprovalEvidence::Confirmed {
                scope: approval_scope,
                prepared_digest,
                policy_revision,
                evidence_id,
            } if approval_scope == scope
                && prepared_digest == prepared.digest
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
            scope,
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

    /// Check exact input, current policy revision and host-owned execution scope
    /// before any external effect. Expected scope must come from trusted admission,
    /// not from the grant being checked or model-generated arguments.
    /// The caller must also enforce isolation and verify trusted grant provenance.
    pub fn validate(
        &self,
        prepared: &PreparedCall,
        current_policy_revision: &str,
        expected_scope: &ToolExecutionScope,
    ) -> Result<(), PreparedError> {
        prepared.validate()?;
        self.scope.validate()?;
        expected_scope.validate()?;
        if self.scope != *expected_scope
            || self.call_id != prepared.call.id
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

    /// The exact scope bound at issuance; callers cannot mutate it in place.
    pub fn scope(&self) -> &ToolExecutionScope {
        &self.scope
    }
}

#[cfg(test)]
mod tests;
