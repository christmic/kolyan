//! Task completion transitions; shared admission and replay invariants.

use super::*;

pub(super) fn complete(
    state: &mut TaskSnapshot,
    evidence: Vec<CompletionEvidence>,
) -> Result<(), TaskError> {
    let root = state
        .invocations
        .values()
        .find(|inv| inv.definition.role == InvocationRole::Root)
        .ok_or_else(|| transition("missing root invocation"))?;
    if root.state != InvocationState::Completed
        || state
            .invocations
            .values()
            .any(|inv| inv.state != InvocationState::Completed)
    {
        return Err(transition(
            "all admitted invocations must complete before task success",
        ));
    }
    let mut available = Vec::new();
    for attempt in state.attempts.values() {
        if let Some(AttemptObservation {
            outcome: AttemptOutcome::Completed { evidence },
            ..
        }) = &attempt.observation
        {
            available.extend(evidence.iter());
        }
    }
    let goals: Vec<_> = state
        .goal_assessments
        .iter()
        .filter(|record| record.assessment.verdict == crate::GoalVerdict::Satisfied)
        .map(|record| CompletionEvidence::GoalSatisfied {
            criterion_id: record.assessment.criterion_id.clone(),
            source: record.assessment.source.clone(),
            assessment: record.reference.clone(),
            assessment_digest: record.assessment_digest.clone(),
        })
        .collect();
    available.extend(goals.iter());
    let ids: BTreeSet<_> = evidence
        .iter()
        .map(CompletionEvidence::criterion_id)
        .collect();
    if ids.len() != evidence.len()
        || ids.len() != state.definition.criteria.len()
        || state
            .definition
            .criteria
            .iter()
            .any(|criterion| !ids.contains(criterion.id()))
        || evidence.iter().any(|item| !available.contains(&item))
    {
        return Err(invalid(
            "completion needs previously observed evidence for every criterion",
        ));
    }
    state.success_evidence = evidence;
    state.state = TaskState::Completed;
    Ok(())
}

pub(super) fn cancel(state: &mut TaskSnapshot, why: &str) -> Result<(), TaskError> {
    reason(why)?;
    for inv in state.invocations.values_mut() {
        if state.definition.cancellation_policy == CancellationPolicy::AllInvocations
            || inv.definition.role == InvocationRole::Root
        {
            inv.cancellation_requested = true;
            if inv.state == InvocationState::Admitted {
                inv.state = InvocationState::Cancelled;
            }
            for attempt_id in &inv.attempts {
                state
                    .attempts
                    .get_mut(attempt_id)
                    .unwrap()
                    .cancellation_requested = true;
            }
        }
    }
    state.state = TaskState::Cancelled;
    Ok(())
}
