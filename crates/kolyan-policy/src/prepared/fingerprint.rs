//! Grant content identity only. Reuse the private canonical writer, never issue
//! authority or replace independent grant validation with a matching hash.

use sha2::{Digest, Sha256};

use super::{PreparedError, PreparedGrant, ToolExecutionScope, canonical};
use crate::ExecutionConstraints;
use kolyan_types::ExecutionKey;

const DOMAIN: &[u8] = b"kolyan.prepared-grant.fingerprint/v1\0";
const MAX_BYTES: usize = 1_048_576;

pub(super) fn compute(grant: &PreparedGrant, max_bytes: usize) -> Result<String, PreparedError> {
    if !(1..=MAX_BYTES).contains(&max_bytes) {
        return Err(invalid("grant fingerprint bound must be 1..=1048576"));
    }
    let json_limit = max_bytes
        .checked_sub(DOMAIN.len())
        .ok_or_else(|| invalid("grant fingerprint domain exceeds byte limit"))?;
    preflight(grant, json_limit)?;
    let value = serde_json::to_value(grant).map_err(|error| invalid(error.to_string()))?;
    let bytes = canonical::bytes(&value, json_limit)?;
    let mut hash = Sha256::new();
    hash.update(DOMAIN);
    hash.update(bytes);
    Ok(format!("{:x}", hash.finalize()))
}

fn preflight(grant: &PreparedGrant, limit: usize) -> Result<(), PreparedError> {
    // No `..`: every future serialized field needs an explicit budget review.
    let PreparedGrant {
        scope,
        call_id,
        tool_name,
        tool_revision,
        prepared_digest,
        policy_revision,
        constraints,
        approval_evidence_id,
    } = grant;
    let ToolExecutionScope {
        execution,
        step_id,
        agent_snapshot_digest,
    } = scope;
    let ExecutionKey {
        session_id,
        turn_id,
        execution_id,
    } = execution;
    let ExecutionConstraints {
        max_output_bytes: _,
        timeout_ms: _,
    } = constraints;
    let mut total = 0usize;
    for value in [
        session_id,
        turn_id,
        execution_id,
        step_id,
        call_id,
        tool_name,
        tool_revision,
        prepared_digest,
        policy_revision,
    ]
    .into_iter()
    .chain(agent_snapshot_digest.iter())
    .chain(approval_evidence_id.iter())
    {
        total = total
            .checked_add(value.len())
            .filter(|total| *total <= limit)
            .ok_or_else(|| invalid("grant fingerprint string preflight exceeds byte limit"))?;
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> PreparedError {
    PreparedError::Invalid(message.into())
}

#[cfg(test)]
mod tests;
