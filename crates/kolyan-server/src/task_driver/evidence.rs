//! Verify execution membership and usage from immutable Runtime facts.

use std::collections::BTreeMap;

use kolyan_core::StepResult;
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{ModelResponse, TokenUsage};
use sha2::{Digest, Sha256};

use crate::{AttemptBinding, ExecutionEvidence, TaskError};

pub(super) struct VerifiedExecution {
    pub source: ExecutionEvidence,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub unreported_steps: u64,
    pub response: Option<ModelResponse>,
    pub stopped: StoppedOutcome,
}

pub(super) enum StoppedOutcome {
    Completed,
    Approval(String),
    Failed(String),
    Cancelled,
    RecoveryRequired,
}

pub(super) fn inspect<L: LedgerStore>(
    ledger: &L,
    binding: &AttemptBinding,
    usage_after: u64,
) -> Result<VerifiedExecution, TaskError> {
    let key = &binding.execution;
    let events = ledger
        .execution_events_after(&key.execution_id, 0)
        .map_err(|error| TaskError::Invalid(error.to_string()))?;
    let identity = events
        .iter()
        .find(|event| event.event_id == format!("{}/execution-started", key.execution_id))
        .ok_or_else(|| invalid("missing admitted execution identity"))?;
    if identity.turn_id != key.turn_id
        || identity.kind != LedgerEventKind::ExecutionStarted
        || identity.payload != serde_json::json!(key)
        || events
            .iter()
            .any(|event| event.execution_id != key.execution_id || event.turn_id != key.turn_id)
    {
        return Err(invalid("execution evidence binding differs"));
    }
    let mut response = None;
    let mut usage = BTreeMap::<String, TokenUsage>::new();
    for event in &events {
        if event.kind == LedgerEventKind::StepCompleted {
            let step: StepResult = serde_json::from_value(event.payload["step"].clone())
                .map_err(|error| invalid(error.to_string()))?;
            if event.payload["step_id"] != step.step_id {
                return Err(invalid("Step evidence identity differs"));
            }
            response = Some(step.response.clone());
            if event.cursor > usage_after {
                usage.insert(step.step_id, step.response.usage);
            }
        } else if event.cursor > usage_after && event.kind == LedgerEventKind::ModelRequested {
            let id = event.payload["request"]["request_id"]
                .as_str()
                .ok_or_else(|| invalid("model request lacks an identity"))?;
            usage.entry(id.into()).or_default();
        } else if event.cursor > usage_after
            && event.kind == LedgerEventKind::ModelStreamEvent
            && event.payload["type"] == "usage"
        {
            let step = event.payload["step_id"]
                .as_str()
                .ok_or_else(|| invalid("usage lacks a Step identity"))?;
            let value: TokenUsage = serde_json::from_value(event.payload["usage"].clone())
                .map_err(|error| invalid(error.to_string()))?;
            usage.insert(step.into(), value);
        }
    }
    let terminal = events.iter().find(|event| {
        matches!(
            event.kind,
            LedgerEventKind::TurnCompleted
                | LedgerEventKind::TurnFailed
                | LedgerEventKind::TurnCancelled
                | LedgerEventKind::TurnTimedOut
        )
    });
    let source = if let Some(terminal) = terminal {
        terminal
    } else {
        events
            .iter()
            .rev()
            .find(|event| {
                event.kind == LedgerEventKind::ExecutionSuspended
                    && event.payload["approval_id"].is_string()
            })
            .or_else(|| events.last())
            .ok_or_else(|| invalid("execution has no evidence"))?
    };
    let stopped = if events.iter().any(|event| {
        event.kind == LedgerEventKind::EffectStarted
            && !events.iter().any(|receipt| {
                receipt.kind == LedgerEventKind::EffectReceipt
                    && receipt.payload["effect_id"] == event.payload["effect_id"]
            })
    }) {
        StoppedOutcome::RecoveryRequired
    } else {
        match source.kind {
            LedgerEventKind::TurnCompleted if source.payload["reason"] == "FinalAnswer" => {
                StoppedOutcome::Completed
            }
            LedgerEventKind::TurnCompleted => {
                StoppedOutcome::Failed("Turn did not produce an admitted final answer".into())
            }
            LedgerEventKind::TurnCancelled => StoppedOutcome::Cancelled,
            LedgerEventKind::TurnFailed | LedgerEventKind::TurnTimedOut => {
                StoppedOutcome::Failed(source.payload.to_string())
            }
            LedgerEventKind::ExecutionSuspended => {
                StoppedOutcome::Approval(source.payload["approval_id"].as_str().unwrap().into())
            }
            _ => StoppedOutcome::RecoveryRequired,
        }
    };
    let (mut input_tokens, mut output_tokens, mut unreported_steps) = (0_u64, 0_u64, 0_u64);
    for value in usage.values() {
        let measured_input = value
            .input_tokens
            .unwrap_or(0)
            .checked_add(value.cache_read_tokens.unwrap_or(0))
            .and_then(|total| total.checked_add(value.cache_write_tokens.unwrap_or(0)))
            .ok_or_else(|| invalid("usage overflow"))?;
        input_tokens = input_tokens
            .checked_add(measured_input)
            .ok_or_else(|| invalid("usage overflow"))?;
        output_tokens = output_tokens
            .checked_add(value.output_tokens.unwrap_or(0))
            .ok_or_else(|| invalid("usage overflow"))?;
        if value.input_tokens.is_none() || value.output_tokens.is_none() {
            unreported_steps += 1;
        }
    }
    Ok(VerifiedExecution {
        source: source_ref(binding, source),
        input_tokens,
        output_tokens,
        unreported_steps,
        response,
        stopped,
    })
}

pub(super) fn source_ref(binding: &AttemptBinding, event: &LedgerEvent) -> ExecutionEvidence {
    ExecutionEvidence {
        execution: binding.execution.clone(),
        event_id: event.event_id.clone(),
        cursor: event.cursor,
    }
}

pub(super) fn response_digest(response: &ModelResponse) -> Result<String, TaskError> {
    let bytes = serde_json::to_vec(response).map_err(|error| invalid(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn invalid(message: impl Into<String>) -> TaskError {
    TaskError::Invalid(message.into())
}

#[cfg(test)]
mod tests;
