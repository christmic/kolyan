//! Task validation transitions; shared admission and replay invariants.

use super::*;

pub(super) fn digest(value: &str) -> Result<(), TaskError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("expected a SHA-256 hex digest"));
    }
    Ok(())
}

pub(super) fn agent(value: &AgentIdentity) -> Result<(), TaskError> {
    identity(&value.definition_id)?;
    identity(&value.revision)?;
    identity(&value.instance_id)
}

pub(super) fn reason(value: &str) -> Result<(), TaskError> {
    if value.trim().is_empty() || value.len() > 8192 {
        return Err(invalid("empty or oversized reason/objective"));
    }
    Ok(())
}

pub(super) fn validate_definition(definition: &TaskDefinition) -> Result<(), TaskError> {
    identity(&definition.task_id)?;
    reason(&definition.objective)?;
    agent(&definition.agent)?;
    digest(&definition.constraints_digest)?;
    if definition.criteria.is_empty() || definition.criteria.len() > 128 {
        return Err(invalid("criteria count must be 1..128"));
    }
    let mut ids = BTreeSet::new();
    for criterion in &definition.criteria {
        identity(criterion.id())?;
        identity(criterion.invocation_id())?;
        if !ids.insert(criterion.id()) {
            return Err(invalid("duplicate criterion"));
        }
        if let CompletionCriterion::ArtifactDigest { sha256, .. } = criterion {
            digest(sha256)?;
        }
    }
    let limits = &definition.limits;
    if limits.max_invocations == 0
        || limits.max_invocations > 4096
        || limits.max_attempts == 0
        || limits.max_attempts > 16384
        || limits.max_tokens == Some(0)
        || limits.max_depth > 128
        || limits.max_steps_per_turn == 0
    {
        return Err(invalid("invalid bounded task limits"));
    }
    Ok(())
}

pub(super) fn available_id(state: &TaskSnapshot, id: &str) -> Result<(), TaskError> {
    identity(id)?;
    if id == state.definition.task_id
        || state.invocations.contains_key(id)
        || state.attempts.contains_key(id)
        || state
            .attempts
            .values()
            .any(|attempt| attempt.binding.execution.execution_id == id)
    {
        return Err(invalid(
            "task/invocation/attempt/execution identity collision",
        ));
    }
    Ok(())
}
