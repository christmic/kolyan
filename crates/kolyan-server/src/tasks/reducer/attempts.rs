//! Task attempts transitions; shared admission and replay invariants.

use super::*;

pub(super) fn start(state: &mut TaskSnapshot, binding: AttemptBinding) -> Result<(), TaskError> {
    available_id(state, &binding.attempt_id)?;
    available_id(state, &binding.execution.execution_id)?;
    if binding.attempt_id == binding.execution.execution_id {
        return Err(invalid("attempt and execution must be distinct"));
    }
    identity(&binding.execution.session_id)?;
    identity(&binding.execution.turn_id)?;
    let inv = state
        .invocations
        .get(&binding.invocation_id)
        .ok_or_else(|| invalid("unknown invocation"))?;
    if inv.definition.agent != binding.agent
        || inv.definition.constraints_digest != binding.constraints_digest
        || inv.definition.input_source != binding.input_source
    {
        return Err(transition(
            "changed revision/instance/constraints require new admission",
        ));
    }
    if state.attempts.len() as u64 >= state.definition.limits.max_attempts
        || state.definition.limits.max_tokens.is_some_and(|limit| {
            state.usage.unreported_steps > 0
                || state.usage.total().is_none_or(|total| total >= limit)
        })
    {
        return Err(transition("shared attempt/token budget exhausted"));
    }
    match inv.state {
        InvocationState::Admitted => {}
        InvocationState::Failed => {
            let previous = inv.attempts.last().and_then(|id| state.attempts.get(id));
            if !previous.is_some_and(|attempt| {
                attempt.retry_authorized
                    || attempt.observation.as_ref().is_some_and(|obs| {
                        matches!(
                            obs.outcome,
                            AttemptOutcome::Failed {
                                safe_to_retry: true,
                                ..
                            }
                        )
                    })
            }) {
                return Err(transition("no explicit safe-retry evidence"));
            }
        }
        _ => return Err(transition("invocation is not startable")),
    }
    if inv
        .definition
        .dependencies
        .iter()
        .any(|id| !has_consumed(inv, id))
    {
        return Err(transition(
            "dependencies require explicit result consumption before execution",
        ));
    }
    let inv = state.invocations.get_mut(&binding.invocation_id).unwrap();
    inv.state = InvocationState::Running;
    inv.terminal_fact = None;
    inv.attempts.push(binding.attempt_id.clone());
    state.attempts.insert(
        binding.attempt_id.clone(),
        AttemptSnapshot {
            binding,
            state: InvocationState::Running,
            observation: None,
            waiting: None,
            cancellation_requested: false,
            retry_authorized: false,
        },
    );
    Ok(())
}

pub(super) fn validate_source(
    source: &ExecutionEvidence,
    binding: &AttemptBinding,
) -> Result<(), TaskError> {
    identity(&source.event_id)?;
    if source.cursor == 0 || source.execution != binding.execution {
        return Err(invalid("evidence is not bound to the admitted execution"));
    }
    Ok(())
}

pub(super) fn authorize_retry(
    state: &mut TaskSnapshot,
    attempt_id: &str,
    source: ExecutionEvidence,
    why: &str,
) -> Result<(), TaskError> {
    reason(why)?;
    let attempt = state
        .attempts
        .get(attempt_id)
        .ok_or_else(|| invalid("unknown retry attempt"))?;
    validate_source(&source, &attempt.binding)?;
    let invocation = state
        .invocations
        .get(&attempt.binding.invocation_id)
        .unwrap();
    if !matches!(
        attempt.state,
        InvocationState::Failed | InvocationState::RecoveryRequired
    ) || attempt.cancellation_requested
        || invocation.cancellation_requested
        || attempt.retry_authorized
        || invocation.attempts.last().map(String::as_str) != Some(attempt_id)
        || !matches!(
            invocation.state,
            InvocationState::Failed | InvocationState::RecoveryRequired
        )
        || source.cursor
            <= attempt
                .observation
                .as_ref()
                .map_or(0, |observation| observation.source.cursor)
    {
        return Err(transition(
            "retry requires the latest stopped attempt and advancing reconciliation evidence",
        ));
    }
    let invocation_id = attempt.binding.invocation_id.clone();
    let attempt = state.attempts.get_mut(attempt_id).unwrap();
    attempt.retry_authorized = true;
    attempt.state = InvocationState::Failed;
    attempt.waiting = None;
    state.invocations.get_mut(&invocation_id).unwrap().state = InvocationState::Failed;
    Ok(())
}

pub(super) fn validate_evidence(
    state: &TaskSnapshot,
    evidence: &[CompletionEvidence],
    binding: &AttemptBinding,
) -> Result<(), TaskError> {
    if evidence.len() > 128 {
        return Err(invalid("too much completion evidence"));
    }
    let mut ids = BTreeSet::new();
    for item in evidence {
        if matches!(item, CompletionEvidence::GoalSatisfied { .. }) {
            return Err(invalid("GoalSatisfied is not physical attempt evidence"));
        }
        validate_source(item.source(), binding)?;
        if !ids.insert(item.criterion_id()) {
            return Err(invalid("duplicate criterion evidence"));
        }
        let criterion = state
            .definition
            .criteria
            .iter()
            .find(|criterion| criterion.id() == item.criterion_id())
            .ok_or_else(|| invalid("unknown completion criterion"))?;
        if criterion.invocation_id() != binding.invocation_id {
            return Err(invalid("criterion belongs to a different invocation"));
        }
        match (criterion, item) {
            (
                CompletionCriterion::ExecutionCompleted { .. },
                CompletionEvidence::ExecutionResult { result_digest, .. },
            ) => digest(result_digest)?,
            (
                CompletionCriterion::ArtifactDigest {
                    sha256: expected, ..
                },
                CompletionEvidence::VerifiedArtifact { sha256, .. },
            ) if sha256 == expected => digest(sha256)?,
            _ => return Err(invalid("criterion evidence type/digest mismatch")),
        }
    }
    Ok(())
}

pub(super) fn observe(
    state: &mut TaskSnapshot,
    observation: AttemptObservation,
    record: &FactRecord,
) -> Result<(), TaskError> {
    let attempt = state
        .attempts
        .get(&observation.attempt_id)
        .ok_or_else(|| invalid("unknown attempt"))?;
    if !matches!(
        attempt.state,
        InvocationState::Running | InvocationState::RecoveryRequired | InvocationState::Suspended
    ) {
        return Err(transition(
            "attempt already stopped; retry exact fact identity instead",
        ));
    }
    if observation.execution != attempt.binding.execution {
        return Err(invalid("observation execution mismatch"));
    }
    validate_source(&observation.source, &attempt.binding)?;
    if attempt.state == InvocationState::Suspended
        && !matches!(
            observation.outcome,
            AttemptOutcome::Suspended { .. }
                | AttemptOutcome::Failed { .. }
                | AttemptOutcome::Cancelled { .. }
                | AttemptOutcome::RecoveryRequired { .. }
        )
    {
        return Err(transition(
            "suspension must be explicitly resumed before execution completion",
        ));
    }
    if attempt.state == InvocationState::Suspended
        && let AttemptOutcome::Suspended { waiting } = &observation.outcome
    {
        let Some(WaitingReason::Suspension(previous)) = &attempt.waiting else {
            return Err(invalid("suspended attempt lacks checkpoint coordinates"));
        };
        if waiting.checkpoint_id != previous.checkpoint_id
            || waiting
                .approval_ids
                .iter()
                .any(|id| !previous.approval_ids.contains(id))
            || waiting
                .external_wait_ids
                .iter()
                .any(|id| !previous.external_wait_ids.contains(id))
        {
            return Err(transition(
                "partial merge cannot replace checkpoint or introduce waits",
            ));
        }
    }
    if let Some(previous) = &attempt.observation
        && observation.source.cursor <= previous.source.cursor
    {
        return Err(transition(
            "observation must advance durable execution evidence",
        ));
    }
    let inv_id = attempt.binding.invocation_id.clone();
    let next = match &observation.outcome {
        AttemptOutcome::Completed { evidence } => {
            validate_evidence(state, evidence, &attempt.binding)?;
            if evidence
                .iter()
                .any(|item| item.source().cursor > observation.source.cursor)
            {
                return Err(invalid(
                    "completion evidence is ahead of the observed execution",
                ));
            }
            let inv = &state.invocations[&inv_id];
            if required_results(state, &inv_id)
                .iter()
                .any(|id| !has_consumed(inv, id))
            {
                return Err(transition(
                    "child results must be consumed before parent completion",
                ));
            }
            InvocationState::Completed
        }
        AttemptOutcome::Suspended { waiting } => {
            validate_waiting(waiting)?;
            InvocationState::Suspended
        }
        AttemptOutcome::Failed { reason: why, .. } => {
            reason(why)?;
            InvocationState::Failed
        }
        AttemptOutcome::Cancelled { reason: why } => {
            reason(why)?;
            InvocationState::Cancelled
        }
        AttemptOutcome::RecoveryRequired { reason: why } => {
            reason(why)?;
            InvocationState::RecoveryRequired
        }
    };
    state.usage.input_tokens = state
        .usage
        .input_tokens
        .checked_add(observation.usage.input_tokens)
        .ok_or_else(|| invalid("usage overflow"))?;
    state.usage.output_tokens = state
        .usage
        .output_tokens
        .checked_add(observation.usage.output_tokens)
        .ok_or_else(|| invalid("usage overflow"))?;
    state.usage.unreported_steps = state
        .usage
        .unreported_steps
        .checked_add(observation.usage.unreported_steps)
        .ok_or_else(|| invalid("usage overflow"))?;
    let total = state
        .usage
        .total()
        .ok_or_else(|| invalid("usage overflow"))?;
    let attempt = state.attempts.get_mut(&observation.attempt_id).unwrap();
    attempt.state = next;
    attempt.waiting = match &observation.outcome {
        AttemptOutcome::Suspended { waiting } => Some(WaitingReason::Suspension(waiting.clone())),
        AttemptOutcome::RecoveryRequired { reason } => Some(WaitingReason::Recovery {
            reason: reason.clone(),
        }),
        _ => None,
    };
    attempt.observation = Some(observation);
    let inv = state.invocations.get_mut(&inv_id).unwrap();
    inv.state = next;
    if matches!(
        next,
        InvocationState::Completed | InvocationState::Failed | InvocationState::Cancelled
    ) {
        inv.terminal_fact = Some(reference(record));
    }
    if next == InvocationState::Completed {
        inv.completion_fact = Some(reference(record));
    }
    // Record actual overspend; rejecting the observation would hide consumed tokens.
    if state
        .definition
        .limits
        .max_tokens
        .is_some_and(|limit| total > limit || state.usage.unreported_steps > 0)
        && !state.state.is_terminal()
    {
        state.state = TaskState::Failed;
    }
    Ok(())
}

pub(super) fn resume(
    state: &mut TaskSnapshot,
    binding: AttemptBinding,
    checkpoint_id: &str,
) -> Result<(), TaskError> {
    identity(checkpoint_id)?;
    if state.definition.limits.max_tokens.is_some_and(|limit| {
        state.usage.unreported_steps > 0 || state.usage.total().is_none_or(|total| total >= limit)
    }) {
        return Err(transition(
            "shared token budget cannot admit checkpoint resumption",
        ));
    }
    let attempt = state
        .attempts
        .get_mut(&binding.attempt_id)
        .ok_or_else(|| invalid("unknown attempt"))?;
    if attempt.binding != binding
        || attempt.state != InvocationState::Suspended
        || !matches!(&attempt.waiting, Some(WaitingReason::Suspension(waiting))
            if waiting.checkpoint_id == checkpoint_id)
        || attempt.cancellation_requested
    {
        return Err(transition(
            "checkpoint/binding/revision/constraints mismatch or cancelled",
        ));
    }
    attempt.state = InvocationState::Running;
    attempt.waiting = None;
    state
        .invocations
        .get_mut(&binding.invocation_id)
        .unwrap()
        .state = InvocationState::Running;
    Ok(())
}

fn validate_waiting(waiting: &crate::TaskSuspension) -> Result<(), TaskError> {
    identity(&waiting.checkpoint_id)?;
    if waiting.approval_ids.is_empty() && waiting.external_wait_ids.is_empty() {
        return Err(invalid("suspension has no pending waits"));
    }
    for ids in [&waiting.approval_ids, &waiting.external_wait_ids] {
        let mut seen = std::collections::BTreeSet::new();
        for id in ids {
            identity(id)?;
            if !seen.insert(id) {
                return Err(invalid("duplicate suspension coordinate"));
            }
        }
    }
    Ok(())
}
