//! Pure ownership/history rules and pre-apply enforcing hooks shared by replay.

use kolyan_ledger::{FactRecord, FactRef};

use super::*;
use crate::tasks::types::TaskEvent;
use crate::{
    AttemptOutcome, CompletionCriterion, CompletionEvidence, InvocationRole, InvocationState,
    TaskError, TaskSnapshot, WaitingReason,
};

pub(in crate::tasks) fn assessment_owner<'a>(
    prefix: &'a TaskSnapshot,
    assessment: &GoalAssessment,
) -> Result<(&'a GoalCriterion, &'a FactRef), TaskError> {
    let criterion = prefix
        .definition
        .criteria
        .iter()
        .find_map(|criterion| match criterion {
            CompletionCriterion::Goal(goal) if goal.id == assessment.criterion_id => Some(goal),
            _ => None,
        })
        .ok_or_else(|| bad("unknown goal criterion"))?;
    criterion.validate()?;
    let invocation = prefix
        .invocations
        .get(&criterion.invocation_id)
        .ok_or_else(|| bad("missing goal root"))?;
    let attempt = prefix
        .attempts
        .get(&assessment.attempt.attempt_id)
        .ok_or_else(|| bad("unknown goal attempt"))?;
    let observation = attempt
        .observation
        .as_ref()
        .ok_or_else(|| bad("missing stopped goal observation"))?;
    if invocation.definition.role != InvocationRole::Root
        || invocation.definition.parent_invocation_id.is_some()
        || invocation.definition.agent != prefix.definition.agent
        || invocation.definition.constraints_digest != prefix.definition.constraints_digest
        || criterion.invocation_id != assessment.attempt.invocation_id
        || invocation.attempts.last() != Some(&assessment.attempt.attempt_id)
        || attempt.binding != assessment.attempt
        || attempt.binding.agent != invocation.definition.agent
        || attempt.binding.constraints_digest != invocation.definition.constraints_digest
        || attempt.binding.input_source != invocation.definition.input_source
        || invocation.state != InvocationState::Completed
        || attempt.state != InvocationState::Completed
        || !matches!(observation.outcome, AttemptOutcome::Completed { .. })
        || assessment.source != observation.source
        || assessment.source.execution != attempt.binding.execution
        || assessment.predicate_digest != criterion.predicate_digest
        || assessment.checker != criterion.checker
    {
        return Err(bad("goal root/attempt/definition/stopped source differs"));
    }
    let stopped = invocation
        .completion_fact
        .as_ref()
        .ok_or_else(|| bad("missing goal observation fact"))?;
    if stopped.stream_id != prefix.definition.task_id
        || stopped.position == 0
        || stopped.position > prefix.position
    {
        return Err(bad("goal observation coordinate is outside prefix"));
    }
    Ok((criterion, stopped))
}

pub(in crate::tasks) fn apply_assessment(
    prefix: &mut TaskSnapshot,
    assessment: GoalAssessment,
    record: &FactRecord,
) -> Result<(), TaskError> {
    assessment_owner(prefix, &assessment)?;
    if prefix
        .goal_assessments
        .iter()
        .any(|saved| saved.assessment.criterion_id == assessment.criterion_id)
    {
        return Err(bad(
            "goal already assessed; new work requires an admitted correction edge",
        ));
    }
    let assessment_digest = assessment.digest()?;
    prefix.goal_assessments.push(GoalAssessmentRecord {
        assessment,
        assessment_digest,
        reference: crate::tasks::reducer::reference(record),
    });
    Ok(())
}

pub(in crate::tasks) fn refresh_goals(state: &mut TaskSnapshot) {
    for criterion in &state.definition.criteria {
        let CompletionCriterion::Goal(goal) = criterion else {
            continue;
        };
        if !state
            .invocations
            .get(&goal.invocation_id)
            .is_some_and(|invocation| invocation.state == InvocationState::Completed)
        {
            continue;
        }
        match state
            .goal_assessments
            .iter()
            .rev()
            .find(|record| record.assessment.criterion_id == goal.id)
        {
            None => state.waiting.push(WaitingReason::GoalAssessment {
                criterion_id: goal.id.clone(),
                invocation_id: goal.invocation_id.clone(),
            }),
            Some(record) if record.assessment.verdict != GoalVerdict::Satisfied => {
                state.waiting.push(WaitingReason::GoalUnmet {
                    criterion_id: goal.id.clone(),
                    verdict: record.assessment.verdict,
                    reason: record.assessment.reason.clone(),
                })
            }
            Some(_) => {}
        }
    }
}

pub(in crate::tasks) fn goal_causes(
    prefix: Option<&TaskSnapshot>,
    event: &TaskEvent,
) -> Result<Vec<FactRef>, TaskError> {
    match event {
        TaskEvent::GoalAssessed(assessment) => Ok(vec![
            assessment_owner(prefix.ok_or_else(|| bad("goal has no prefix"))?, assessment)?
                .1
                .clone(),
        ]),
        TaskEvent::Completed { evidence } => Ok(evidence
            .iter()
            .filter_map(|proof| match proof {
                CompletionEvidence::GoalSatisfied { assessment, .. } => Some(assessment.clone()),
                _ => None,
            })
            .collect()),
        _ => Ok(Vec::new()),
    }
}

pub(in crate::tasks) fn verify_event(
    verifier: Option<&dyn TaskGoalVerifier>,
    prefix: Option<&TaskSnapshot>,
    event: &TaskEvent,
    record: &FactRecord,
) -> Result<(), TaskError> {
    if let TaskEvent::Registered(definition) = event {
        for criterion in &definition.criteria {
            if let CompletionCriterion::Goal(goal) = criterion {
                goal.validate()?;
                verifier
                    .ok_or_else(|| bad("goal verifier unavailable"))?
                    .validate_criterion(goal)?;
            }
        }
        return Ok(());
    }
    let Some(prefix) = prefix else {
        return Ok(());
    };
    if !prefix
        .definition
        .criteria
        .iter()
        .any(|criterion| matches!(criterion, CompletionCriterion::Goal(_)))
    {
        return Ok(());
    }
    let verifier = verifier.ok_or_else(|| bad("goal verifier unavailable on replay"))?;
    for cause in goal_causes(Some(prefix), event)? {
        if !record.draft.causes.contains(&cause) {
            return Err(bad("goal fact lacks exact source cause"));
        }
    }
    match event {
        TaskEvent::GoalAssessed(assessment) => verifier.verify_assessment(prefix, assessment),
        TaskEvent::Completed { evidence } => verifier.verify_completion(prefix, evidence),
        _ => Ok(()),
    }
}

fn bad(message: &str) -> TaskError {
    TaskError::Invalid(message.into())
}
