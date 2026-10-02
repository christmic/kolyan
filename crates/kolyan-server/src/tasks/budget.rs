//! Durable conservative Step reservations, not measured usage or effect authority.

use std::collections::BTreeMap;

use kolyan_ledger::{FactRecord, FactRef};
use serde::{Deserialize, Serialize};

use super::reducer::{identity, invalid, reference, transition};
use super::{AttemptBinding, TaskError, TaskSnapshot};

/// Host-selected immutable ceiling. The absolute cutoff is never renewed on resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskExecutionBudgetPolicy {
    pub id: String,
    pub revision: String,
    pub max_reserved_steps: u64,
    pub deadline_at_ms: u64,
}

/// One charged Turn ceiling; unused slots are not implicitly refunded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStepReservation {
    pub binding: AttemptBinding,
    pub max_steps: u32,
}

/// Reconstructed facts, never an independently accepted serialized admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskExecutionBudgetState {
    pub policy: TaskExecutionBudgetPolicy,
    pub policy_reference: FactRef,
    pub reservations: BTreeMap<String, TaskStepReservation>,
}

impl TaskExecutionBudgetPolicy {
    pub fn validate(&self) -> Result<(), TaskError> {
        identity(&self.id)?;
        identity(&self.revision)?;
        if self.max_reserved_steps == 0 || self.deadline_at_ms == 0 {
            return Err(invalid(
                "execution budget needs positive Steps and absolute cutoff",
            ));
        }
        Ok(())
    }
}

impl TaskExecutionBudgetState {
    /// Checked sum of irrevocably charged slots, not reported model usage.
    pub fn remaining_steps(&self) -> Result<u64, TaskError> {
        self.policy.validate()?;
        let reserved = self.reservations.values().try_fold(0u64, |sum, slot| {
            sum.checked_add(u64::from(slot.max_steps))
                .ok_or_else(|| invalid("Step reservation overflow"))
        })?;
        self.policy
            .max_reserved_steps
            .checked_sub(reserved)
            .ok_or_else(|| invalid("Step reservations exceed configured budget"))
    }
}

pub(super) fn configure(
    state: &mut TaskSnapshot,
    policy: TaskExecutionBudgetPolicy,
    record: &FactRecord,
) -> Result<(), TaskError> {
    policy.validate()?;
    if state.execution_budget.is_some() || !state.attempts.is_empty() {
        return Err(transition(
            "execution budget already configured or work already started",
        ));
    }
    state.execution_budget = Some(TaskExecutionBudgetState {
        policy,
        policy_reference: reference(record),
        reservations: BTreeMap::new(),
    });
    Ok(())
}

pub(super) fn reserve(
    state: &mut TaskSnapshot,
    binding: &AttemptBinding,
    max_steps: u32,
    record: &FactRecord,
) -> Result<(), TaskError> {
    let ceiling = state.definition.limits.max_steps_per_turn;
    let budget = state
        .execution_budget
        .as_mut()
        .ok_or_else(|| transition("budgeted start requires configured execution budget"))?;
    if max_steps == 0
        || max_steps > ceiling
        || u64::from(max_steps) > budget.remaining_steps()?
        || budget.reservations.contains_key(&binding.attempt_id)
        || !record.draft.causes.contains(&budget.policy_reference)
    {
        return Err(transition(
            "invalid, duplicate or exhausted Step reservation",
        ));
    }
    budget.reservations.insert(
        binding.attempt_id.clone(),
        TaskStepReservation {
            binding: binding.clone(),
            max_steps,
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests;
