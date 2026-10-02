//! Pure, fail-closed task transitions, shared by append admission and replay.

mod attempts;
mod completion;
mod topology;
mod validation;

use std::collections::BTreeSet;

use kolyan_ledger::{FactRecord, FactRef};

use super::InvocationInputSource;
use super::types::*;

use attempts::{authorize_retry, observe, resume, start};
use completion::{cancel, complete};
use topology::{add_dependency, admit, consume, consume_terminal, has_consumed, required_results};
use validation::{agent, available_id, digest, reason, validate_definition};

pub(super) fn invalid(message: impl Into<String>) -> TaskError {
    TaskError::Invalid(message.into())
}

pub(super) fn transition(message: impl Into<String>) -> TaskError {
    TaskError::Transition(message.into())
}

pub(super) fn identity(value: &str) -> Result<(), TaskError> {
    if value.trim().is_empty() || value.len() > 256 {
        return Err(invalid("empty or oversized identity"));
    }
    Ok(())
}

pub(super) fn reference(record: &FactRecord) -> FactRef {
    FactRef {
        stream_id: record.stream_id.clone(),
        position: record.position,
        fact_id: record.draft.fact_id.clone(),
    }
}

pub(super) fn apply(
    snapshot: &mut Option<TaskSnapshot>,
    event: TaskEvent,
    record: &FactRecord,
) -> Result<(), TaskError> {
    if let TaskEvent::Registered(definition) = event {
        if snapshot.is_some() {
            return Err(transition("task already registered"));
        }
        validate_definition(&definition)?;
        *snapshot = Some(TaskSnapshot {
            definition,
            position: record.position,
            state: TaskState::Ready,
            usage: TaskUsage::default(),
            invocations: Default::default(),
            attempts: Default::default(),
            waiting: Vec::new(),
            success_evidence: Vec::new(),
            goal_assessments: Vec::new(),
        });
        return Ok(());
    }
    let state = snapshot
        .as_mut()
        .ok_or_else(|| transition("missing task registration"))?;
    // Terminal tasks still accept stopped observations of work already in flight.
    // They never accept new work or revive because a child finishes afterwards.
    let detached_resume = state.state == TaskState::Cancelled
        && state.definition.cancellation_policy == CancellationPolicy::RootOnly
        && matches!(&event, TaskEvent::AttemptResumed { binding, .. }
            if state.invocations.get(&binding.invocation_id).is_some_and(|inv| inv.definition.role != InvocationRole::Root));
    if state.state.is_terminal()
        && !detached_resume
        && !matches!(
            event,
            TaskEvent::AttemptObserved(_) | TaskEvent::RecoveryNeeded { .. }
        )
    {
        return Err(transition("task is terminal"));
    }
    match event {
        TaskEvent::GoalAssessed(assessment) => {
            super::goals::apply_assessment(state, *assessment, record)?
        }
        TaskEvent::InvocationAdmitted(definition) => admit(state, definition)?,
        TaskEvent::DependencyAdmitted {
            invocation_id,
            dependency_id,
        } => {
            add_dependency(state, &invocation_id, &dependency_id)?;
        }
        TaskEvent::AttemptStarted(binding) => start(state, binding)?,
        TaskEvent::AttemptObserved(observation) => observe(state, observation, record)?,
        TaskEvent::AttemptResumed {
            binding,
            checkpoint_id,
        } => resume(state, binding, &checkpoint_id)?,
        TaskEvent::RecoveryNeeded {
            attempt_id,
            reason: why,
        } => {
            reason(&why)?;
            let attempt = state
                .attempts
                .get_mut(&attempt_id)
                .ok_or_else(|| transition("unknown attempt"))?;
            if !matches!(
                attempt.state,
                InvocationState::Running
                    | InvocationState::Suspended
                    | InvocationState::RecoveryRequired
            ) {
                return Err(transition(
                    "only running or waiting attempts can require recovery",
                ));
            }
            attempt.state = InvocationState::RecoveryRequired;
            attempt.waiting = Some(WaitingReason::Recovery { reason: why });
            state
                .invocations
                .get_mut(&attempt.binding.invocation_id)
                .unwrap()
                .state = InvocationState::RecoveryRequired;
        }
        TaskEvent::RetryAuthorized {
            attempt_id,
            source,
            reason,
        } => {
            authorize_retry(state, &attempt_id, source, &reason)?;
        }
        TaskEvent::ResultConsumed {
            invocation_id,
            result,
        } => consume(state, &invocation_id, result, record)?,
        TaskEvent::TerminalResultConsumed {
            invocation_id,
            result,
        } => consume_terminal(state, &invocation_id, *result, record)?,
        TaskEvent::Completed { evidence } => complete(state, evidence)?,
        TaskEvent::Cancelled { reason: why } => cancel(state, &why)?,
        TaskEvent::Failed { reason: why } => {
            reason(&why)?;
            state.state = TaskState::Failed;
        }
        TaskEvent::Registered(_) => unreachable!(),
    }
    state.position = record.position;
    refresh(state);
    Ok(())
}

fn refresh(state: &mut TaskSnapshot) {
    state.waiting = state
        .attempts
        .values()
        .filter_map(|attempt| attempt.waiting.clone())
        .collect();
    for (id, inv) in &state.invocations {
        let pending: Vec<_> = required_results(state, id)
            .into_iter()
            .filter(|child| !has_consumed(inv, child))
            .collect();
        if !pending.is_empty() {
            state.waiting.push(WaitingReason::ChildResults {
                invocation_ids: pending,
            });
        }
    }
    super::goals::refresh_goals(state);
    if !state.state.is_terminal() {
        state.state = if state
            .invocations
            .values()
            .any(|inv| inv.state == InvocationState::RecoveryRequired)
        {
            TaskState::RecoveryRequired
        } else if state
            .invocations
            .values()
            .any(|inv| inv.state == InvocationState::Running)
        {
            TaskState::Active
        } else if !state.waiting.is_empty() {
            TaskState::Waiting
        } else {
            TaskState::Ready
        };
    }
}
