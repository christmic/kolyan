//! Verify execution membership and usage from immutable Runtime facts.

use std::collections::BTreeMap;

use kolyan_core::{CheckpointCallState, StepResult, TurnSuspension};
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
    Suspended(crate::TaskSuspension),
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
    let terminal = physical_terminal(&events)?;
    if let Some(terminal) = terminal
        && events.iter().any(|event| {
            event.cursor > terminal.cursor
                && matches!(
                    event.kind,
                    LedgerEventKind::StepStarted
                        | LedgerEventKind::StepCompleted
                        | LedgerEventKind::ModelRequested
                        | LedgerEventKind::ModelStreamEvent
                )
        })
    {
        return Err(invalid(
            "model or Step fact published after physical terminal",
        ));
    }
    let mut response = None;
    let mut final_answer_step = false;
    let mut usage = BTreeMap::<String, TokenUsage>::new();
    // Terminal ordering is checked before decoding or projecting response/usage.
    // Later facts cannot supply the missing response of an already stopped Turn.
    for event in events
        .iter()
        .take_while(|event| terminal.is_none_or(|terminal| event.cursor < terminal.cursor))
    {
        if event.kind == LedgerEventKind::StepCompleted {
            let step: StepResult = serde_json::from_value(event.payload["step"].clone())
                .map_err(|error| invalid(error.to_string()))?;
            if event.payload["step_id"] != step.step_id {
                return Err(invalid("Step evidence identity differs"));
            }
            response = Some(step.response.clone());
            final_answer_step = matches!(step.outcome, kolyan_core::StepOutcome::FinalAnswer);
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
    let source = if let Some(terminal) = terminal {
        terminal
    } else {
        events
            .iter()
            .find(|event| event.kind == LedgerEventKind::ExecutionCancelled)
            .or_else(|| {
                events
                    .iter()
                    .rev()
                    .find(|event| {
                        matches!(
                            event.kind,
                            LedgerEventKind::ExecutionSuspended
                                | LedgerEventKind::TurnCheckpointMerged
                                | LedgerEventKind::TurnCheckpointPrepared
                        )
                    })
                    .or_else(|| events.last())
            })
            .ok_or_else(|| invalid("execution has no evidence"))?
    };
    let suspension = crate::suspension::current_suspension(key, &events)
        .map_err(|error| invalid(error.to_string()))?;
    let cancelled_wait = historical_cancelled_wait(key, &events, source)?;
    let stopped = if events.iter().any(|event| {
        event.kind == LedgerEventKind::EffectStarted
            && !events.iter().any(|receipt| {
                receipt.kind == LedgerEventKind::EffectReceipt
                    && receipt.payload["effect_id"] == event.payload["effect_id"]
            })
            && !suspension
                .as_ref()
                .is_some_and(|saved| committed_wait(&events, event, saved))
            && !cancelled_wait
                .as_ref()
                .is_some_and(|(saved, through)| committed_wait(&events[..*through], event, saved))
    }) {
        StoppedOutcome::RecoveryRequired
    } else {
        match source.kind {
            LedgerEventKind::TurnCompleted
                if source.payload["reason"] == "FinalAnswer" && final_answer_step =>
            {
                StoppedOutcome::Completed
            }
            LedgerEventKind::TurnCompleted => {
                StoppedOutcome::Failed("Turn did not produce an admitted final answer".into())
            }
            LedgerEventKind::TurnCancelled => StoppedOutcome::Cancelled,
            LedgerEventKind::TurnFailed | LedgerEventKind::TurnTimedOut => {
                StoppedOutcome::Failed(source.payload.to_string())
            }
            LedgerEventKind::ExecutionSuspended | LedgerEventKind::TurnCheckpointMerged => {
                match &suspension {
                    Some(saved)
                        if !saved.waiting.approvals.is_empty()
                            || !saved.waiting.external_waits.is_empty() =>
                    {
                        StoppedOutcome::Suspended(crate::suspension::task_waiting(saved))
                    }
                    Some(_) => StoppedOutcome::RecoveryRequired,
                    None => StoppedOutcome::RecoveryRequired,
                }
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

/// Diagnostic completion strings carry no physical stop or success authority.
pub(super) fn is_physical_terminal(event: &LedgerEvent) -> Result<bool, TaskError> {
    match event.kind {
        LedgerEventKind::TurnCompleted => match event.payload.get("reason") {
            Some(reason)
                if matches!(
                    reason.as_str(),
                    Some("FinalAnswer" | "Refused" | "Incomplete" | "MaxSteps" | "NoProgress")
                ) =>
            {
                Ok(true)
            }
            Some(_) => Err(invalid("unknown completed physical boundary reason")),
            None if event.payload.get("outcome").is_some() => Ok(false),
            None => Err(invalid(
                "completed fact has neither physical boundary nor diagnostic outcome",
            )),
        },
        LedgerEventKind::TurnFailed
        | LedgerEventKind::TurnCancelled
        | LedgerEventKind::TurnTimedOut => Ok(true),
        _ => Ok(false),
    }
}

pub(super) fn physical_terminal(events: &[LedgerEvent]) -> Result<Option<&LedgerEvent>, TaskError> {
    let mut terminal: Option<&LedgerEvent> = None;
    for event in events {
        if !is_physical_terminal(event)? {
            continue;
        }
        if let Some(first) = terminal {
            if event.kind != first.kind
                || (event.kind == LedgerEventKind::TurnCompleted
                    && event.payload["reason"] != first.payload["reason"])
            {
                return Err(invalid("conflicting physical terminal boundaries"));
            }
        } else {
            terminal = Some(event);
        }
    }
    Ok(terminal)
}

// This projection proves a stopped external handoff, never resume authority.
// Cancellation intentionally invalidates the active suspension projection.
fn historical_cancelled_wait(
    key: &crate::ExecutionRef,
    events: &[LedgerEvent],
    terminal: &LedgerEvent,
) -> Result<Option<(TurnSuspension, usize)>, TaskError> {
    if terminal.kind != LedgerEventKind::TurnCancelled
        || terminal.payload != serde_json::json!({"boundary":"suspension"})
    {
        return Ok(None);
    }
    let Some(intent) = events.iter().position(|event| {
        event.kind == LedgerEventKind::ExecutionCancelled && event.cursor < terminal.cursor
    }) else {
        return Ok(None);
    };
    if events[intent + 1..].iter().any(|event| {
        event.cursor < terminal.cursor
            && matches!(
                event.kind,
                LedgerEventKind::ExecutionStarted
                    | LedgerEventKind::ExecutionSuspended
                    | LedgerEventKind::TurnCheckpointPrepared
                    | LedgerEventKind::TurnCheckpointMerged
                    | LedgerEventKind::EffectStarted
                    | LedgerEventKind::ModelRequested
                    | LedgerEventKind::StepStarted
                    | LedgerEventKind::StepCompleted
            )
    }) {
        return Ok(None);
    }
    crate::suspension::current_suspension(key, &events[..intent])
        .map(|saved| saved.map(|saved| (saved, intent)))
        .map_err(|error| invalid(error.to_string()))
}

fn committed_wait(
    events: &[LedgerEvent],
    started: &LedgerEvent,
    suspension: &TurnSuspension,
) -> bool {
    suspension.checkpoint.calls.iter().any(|item| {
        let CheckpointCallState::AwaitingExternal { wait, issued } = &item.state else {
            return false;
        };
        let effect_id = started.payload.get("effect_id");
        let prepared = events.iter().find(|event| {
            event.kind == LedgerEventKind::EffectPrepared
                && event.payload.get("effect_id") == effect_id
        });
        let authorized = events.iter().find(|event| {
            event.kind == LedgerEventKind::EffectAuthorized
                && event.payload.get("effect_id") == effect_id
        });
        let (Some(prepared), Some(authorized)) = (prepared, authorized) else {
            return false;
        };
        let mut authorization = authorized.payload.clone();
        let Some(object) = authorization.as_object_mut() else {
            return false;
        };
        object.remove("prepared_grant");
        events.iter().any(|event| {
            event.kind == LedgerEventKind::EffectAwaitingExternal
                && event.cursor > started.cursor
                && event.payload["schema_version"] == 1
                && event
                    .payload
                    .as_object()
                    .is_some_and(|object| object.len() == 6)
                && event.payload.get("effect_id") == effect_id
                && event.payload["wait"] == serde_json::json!(wait)
                && event.payload["input"]
                    == serde_json::json!({"prepared":issued.prepared,"scope":issued.scope})
                && event.payload["input"] == prepared.payload["input"]
                && event.payload["prepared_grant"] == serde_json::json!(issued.grant)
                && event.payload["prepared_grant"] == authorized.payload["prepared_grant"]
                && event.payload["authorization"] == authorization
                && prepared.payload["policy_revision"] == issued.policy_revision
                && started.payload["scope"] == serde_json::json!(issued.scope)
                && started.payload["input_digest"] == authorized.payload["input_digest"]
        })
    })
}

pub(super) fn source_ref(binding: &AttemptBinding, event: &LedgerEvent) -> ExecutionEvidence {
    ExecutionEvidence {
        execution: binding.execution.clone(),
        event_id: event.event_id.clone(),
        cursor: event.cursor,
    }
}

pub(crate) fn response_digest(response: &ModelResponse) -> Result<String, TaskError> {
    let bytes = serde_json::to_vec(response).map_err(|error| invalid(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn invalid(message: impl Into<String>) -> TaskError {
    TaskError::Invalid(message.into())
}

#[cfg(test)]
mod tests;
