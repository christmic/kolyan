//! Bounded prefix-aware physical proof without a coordinator or execution port.

mod budget;
mod effects;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use kolyan_core::{StepOutcome, StepResult};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerQuery, LedgerStore};
use kolyan_model::ModelResponse;
use kolyan_runtime::{
    EffectProofCoordinate, EffectProofError, EffectProofRequest, VerifiedEffectProof,
    inspect_effect_proof,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::{AttemptBinding, AttemptOutcome, ExecutionEvidence, InvocationState, TaskSnapshot};
use budget::ReadBudget;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalSourceLimits {
    pub max_rows: usize,
    pub max_total_bytes: usize,
    pub max_event_bytes: usize,
    pub max_response_bytes: usize,
    pub max_effects: usize,
}

impl Default for GoalSourceLimits {
    fn default() -> Self {
        Self {
            max_rows: 4096,
            max_total_bytes: 32 * 1024 * 1024,
            max_event_bytes: 16 * 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
            max_effects: 128,
        }
    }
}

impl GoalSourceLimits {
    pub fn validate(&self) -> Result<(), GoalSourceError> {
        if !(1..=4096).contains(&self.max_rows)
            || !(1..=32 * 1024 * 1024).contains(&self.max_total_bytes)
            || !(1..=16 * 1024 * 1024).contains(&self.max_event_bytes)
            || !(1..=16 * 1024 * 1024).contains(&self.max_response_bytes)
            || !(1..=128).contains(&self.max_effects)
        {
            return Err(bad("invalid source hard ceilings"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GoalSourceCoverage {
    Complete,
    Exhausted,
    Uncertain,
}

#[derive(Debug, Error)]
pub enum GoalSourceError {
    #[error("invalid goal source: {0}")]
    Invalid(String),
    #[error("goal source read ceiling exhausted before binding/terminal validation")]
    BoundsExhausted,
    #[error("goal source storage: {0}")]
    Storage(#[from] kolyan_ledger::LedgerError),
}

/// Only this reader constructs the proof. No Deserialize or authority accessor.
#[derive(Debug)]
pub struct VerifiedGoalSource {
    binding: AttemptBinding,
    terminal: ExecutionEvidence,
    response: Option<ModelResponse>,
    coverage: GoalSourceCoverage,
    effects: Vec<VerifiedEffectProof>,
    through: u64,
}

impl VerifiedGoalSource {
    pub fn binding(&self) -> &AttemptBinding {
        &self.binding
    }
    pub fn terminal(&self) -> &ExecutionEvidence {
        &self.terminal
    }
    pub fn response(&self) -> Option<&ModelResponse> {
        self.response.as_ref()
    }
    pub fn coverage(&self) -> GoalSourceCoverage {
        self.coverage
    }
    pub fn effects(&self) -> &[VerifiedEffectProof] {
        &self.effects
    }
    pub fn through(&self) -> u64 {
        self.through
    }
}

pub struct GoalSourceReader<L> {
    ledger: L,
}

impl<L: LedgerStore> GoalSourceReader<L> {
    pub fn new(ledger: L) -> Self {
        Self { ledger }
    }

    /// Synchronous read-only I/O. Async hosts must run on a blocking worker.
    /// An empty scoped page freezes the observed end. Short pages do not.
    /// Continuous append exhausts the finite cumulative budget; it cannot prove
    /// complete coverage. This is not a snapshot of arbitrary future writes.
    pub fn inspect_stopped(
        &self,
        prefix: &TaskSnapshot,
        binding: &AttemptBinding,
        source: &ExecutionEvidence,
        limits: &GoalSourceLimits,
    ) -> Result<VerifiedGoalSource, GoalSourceError> {
        limits.validate()?;
        let invocation = prefix
            .invocations
            .get(&binding.invocation_id)
            .ok_or_else(|| bad("unknown invocation"))?;
        let attempt = prefix
            .attempts
            .get(&binding.attempt_id)
            .ok_or_else(|| bad("unknown attempt"))?;
        let observation = attempt
            .observation
            .as_ref()
            .ok_or_else(|| bad("missing observation"))?;
        if invocation.state != InvocationState::Completed
            || attempt.state != InvocationState::Completed
            || invocation.attempts.last() != Some(&binding.attempt_id)
            || attempt.binding != *binding
            || binding.agent != invocation.definition.agent
            || binding.constraints_digest != invocation.definition.constraints_digest
            || binding.input_source != invocation.definition.input_source
            || source != &observation.source
            || source.execution != binding.execution
            || !matches!(observation.outcome, AttemptOutcome::Completed { .. })
        {
            return Err(bad("prefix membership/latest stopped attempt differs"));
        }
        let budget = ReadBudget::new(&self.ledger, limits);
        let terminal = budget.exact(&source.event_id)?;
        check(&terminal, binding)?;
        if terminal.cursor != source.cursor
            || terminal.kind != LedgerEventKind::TurnCompleted
            || terminal.payload["reason"] != "FinalAnswer"
        {
            return Err(bad("not the exact successful physical terminal"));
        }
        let bound = budget.exact(&format!("{}/task-binding", binding.execution.execution_id))?;
        check(&bound, binding)?;
        let expected = kolyan_runtime::ExecutionBinding {
            task_id: prefix.definition.task_id.clone(),
            invocation_id: binding.invocation_id.clone(),
            attempt_id: binding.attempt_id.clone(),
            session_id: binding.execution.session_id.clone(),
            turn_id: binding.execution.turn_id.clone(),
            execution_id: binding.execution.execution_id.clone(),
        };
        if bound.kind != LedgerEventKind::ExecutionBound
            || bound.payload != json!({"binding":expected})
        {
            return Err(bad("actual execution Task binding differs"));
        }
        let admitted = budget.exact(&format!(
            "{}/execution-started",
            binding.execution.execution_id
        ))?;
        check(&admitted, binding)?;
        if admitted.kind != LedgerEventKind::ExecutionStarted
            || admitted.payload != json!(binding.execution)
            || bound.cursor >= admitted.cursor
            || admitted.cursor >= terminal.cursor
        {
            return Err(bad("actual execution admission/order differs"));
        }
        let mut verified = VerifiedGoalSource {
            binding: binding.clone(),
            terminal: source.clone(),
            response: None,
            coverage: GoalSourceCoverage::Complete,
            effects: Vec::new(),
            through: terminal.cursor,
        };
        let mut events = Vec::new();
        let mut after = 0;
        loop {
            let remaining = budget.remaining_rows();
            if remaining == 0 {
                verified.coverage = GoalSourceCoverage::Exhausted;
                return Ok(verified);
            }
            let query = LedgerQuery {
                execution_id: Some(binding.execution.execution_id.clone()),
                event_id: None,
                after,
                through: None,
                limit: remaining.min(1024),
            };
            let page = match budget.page(&query) {
                Ok(page) => page,
                Err(GoalSourceError::BoundsExhausted) => {
                    verified.coverage = GoalSourceCoverage::Exhausted;
                    return Ok(verified);
                }
                Err(error) => return Err(error),
            };
            if page.is_empty() {
                break;
            }
            for event in &page {
                check(event, binding)?;
            }
            after = page.last().expect("nonempty page").cursor;
            events.extend(page);
            // Even a short page may be adapter-capped; observe an empty scoped page.
        }
        verified.through = after;
        let actual_terminal = super::evidence::physical_terminal(&events)
            .map_err(|error| bad(&error.to_string()))?
            .ok_or_else(|| bad("physical terminal missing from scoped range"))?;
        if actual_terminal != &terminal {
            return Err(bad("selected physical terminal differs"));
        }
        if events.iter().any(|event| {
            event.cursor > terminal.cursor
                && matches!(
                    event.kind,
                    LedgerEventKind::StepStarted
                        | LedgerEventKind::StepCompleted
                        | LedgerEventKind::ModelRequested
                        | LedgerEventKind::ModelStreamEvent
                        | LedgerEventKind::EffectPrepared
                        | LedgerEventKind::EffectAuthorized
                        | LedgerEventKind::EffectStarted
                        | LedgerEventKind::EffectReceipt
                        | LedgerEventKind::EffectUncertain
                )
        }) {
            return Err(bad("new model/effect facts after stopped terminal"));
        }
        let step_event = events
            .iter()
            .rev()
            .find(|event| {
                event.cursor < terminal.cursor && event.kind == LedgerEventKind::StepCompleted
            })
            .ok_or_else(|| bad("missing actual final Step"))?;
        let step: StepResult = serde_json::from_value(step_event.payload["step"].clone())
            .map_err(|error| bad(&error.to_string()))?;
        if step.outcome != StepOutcome::FinalAnswer
            || step.response.stop_reason != kolyan_model::StopReason::EndTurn
            || step_event.payload["step_id"] != step.step_id
        {
            return Err(bad("actual Step is not an admitted FinalAnswer"));
        }
        let step_started = events
            .iter()
            .find(|event| {
                event.kind == LedgerEventKind::StepStarted
                    && event.payload["step_id"] == step.step_id
                    && event.cursor > admitted.cursor
                    && event.cursor < step_event.cursor
            })
            .ok_or_else(|| bad("final Step has no prior actual start"))?;
        if !events.iter().any(|event| {
            event.kind == LedgerEventKind::ModelRequested
                && event.payload["request"]["request_id"] == step.step_id
                && event.cursor > step_started.cursor
                && event.cursor < step_event.cursor
        }) {
            return Err(bad("final Step has no actual model request"));
        }
        if super::super::tasks::goals::types::bounded_serialized_bytes(
            &step.response,
            limits.max_response_bytes,
        )
        .is_err()
        {
            verified.coverage = GoalSourceCoverage::Exhausted;
            return Ok(verified);
        }
        verified.response = Some(step.response);
        let inventory = effects::inventory(&events)?;
        if inventory.count > limits.max_effects {
            verified.coverage = GoalSourceCoverage::Exhausted;
            return Ok(verified);
        }
        if inventory.unresolved {
            verified.coverage = GoalSourceCoverage::Uncertain;
        }
        let mut effect_ids = BTreeSet::new();
        for event in inventory.started {
            let id = event.payload["effect_id"]
                .as_str()
                .ok_or_else(|| bad("effect start lacks exact identity"))?;
            if !effect_ids.insert(id) {
                return Err(bad("duplicate physical effect start"));
            }
            if effect_ids.len() > limits.max_effects {
                verified.coverage = GoalSourceCoverage::Exhausted;
                return Ok(verified);
            }
            let proof = inspect_effect_proof(
                &budget,
                &EffectProofRequest {
                    execution: serde_json::from_value(json!(binding.execution))
                        .map_err(|error| bad(&error.to_string()))?,
                    effect_id: id.into(),
                    terminal: EffectProofCoordinate {
                        event_id: source.event_id.clone(),
                        cursor: source.cursor,
                    },
                    max_input_bytes: limits.max_event_bytes,
                    max_result_bytes: limits.max_response_bytes,
                },
            );
            match proof {
                Ok(proof) => {
                    effects::verify_model_call(&events, &proof)?;
                    if proof.scope().agent_snapshot_digest.as_deref()
                        != Some(&binding.constraints_digest)
                    {
                        return Err(bad(
                            "effect Agent snapshot differs from admitted constraints",
                        ));
                    }
                    verified.effects.push(proof);
                }
                Err(EffectProofError::Indeterminate { .. }) => {
                    verified.coverage = GoalSourceCoverage::Uncertain;
                }
                Err(EffectProofError::MissingEvidence { event_id })
                    if event_id.ends_with("/receipt") =>
                {
                    verified.coverage = GoalSourceCoverage::Uncertain;
                }
                Err(_) if budget.exhausted() => {
                    verified.coverage = GoalSourceCoverage::Exhausted;
                    return Ok(verified);
                }
                Err(EffectProofError::Storage(error)) => {
                    return Err(GoalSourceError::Storage(error));
                }
                Err(error) => return Err(bad(&error.to_string())),
            }
        }
        Ok(verified)
    }
}

fn check(event: &LedgerEvent, binding: &AttemptBinding) -> Result<(), GoalSourceError> {
    if event.cursor == 0
        || event.execution_id != binding.execution.execution_id
        || event.turn_id != binding.execution.turn_id
        || event.event_id != event.idempotency_key
    {
        return Err(bad("foreign or invalid physical source envelope"));
    }
    Ok(())
}
fn bad(message: &str) -> GoalSourceError {
    GoalSourceError::Invalid(message.into())
}
