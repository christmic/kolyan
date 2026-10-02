//! Exact read-only Task/journal/Ledger proof; completion is never inferred from admission.
use super::*;
use crate::{TaskEvent, TerminalResultDisposition};
use kolyan_ledger::FactRef;
use kolyan_model::ModelResponse;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifiedTaskResult {
    pub binding: AttemptBinding,
    pub terminal_fact: FactRef,
    pub outcome: VerifiedTaskOutcome,
}

/// Both journals are independently verified. Possession is not resume authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifiedConsumedResult {
    pub parent: AttemptBinding,
    pub consumption_fact: FactRef,
    pub child: VerifiedTaskResult,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ResultRead {
    Active,
    Historical,
}

pub(super) fn load_consumed<J: FactJournal, L: LedgerStore>(
    coordinator: &TaskCoordinator<J>,
    ledger: &L,
    task_id: &str,
    parent: &AttemptBinding,
    child: &AttemptBinding,
    max_bytes: usize,
) -> Result<Option<VerifiedConsumedResult>, TaskError> {
    load_consumed_for(
        coordinator,
        ledger,
        task_id,
        parent,
        child,
        max_bytes,
        ResultRead::Active,
    )
}

pub(super) fn load_consumed_for<J: FactJournal, L: LedgerStore>(
    coordinator: &TaskCoordinator<J>,
    ledger: &L,
    task_id: &str,
    parent: &AttemptBinding,
    child: &AttemptBinding,
    max_bytes: usize,
    access: ResultRead,
) -> Result<Option<VerifiedConsumedResult>, TaskError> {
    if max_bytes == 0 || max_bytes > 1024 * 1024 {
        return Err(invalid("invalid consumed result envelope ceiling"));
    }
    let state = coordinator.snapshot(task_id)?;
    let owner = state
        .invocations
        .get(&parent.invocation_id)
        .ok_or_else(|| invalid("unknown result consumer"))?;
    let attempt = state
        .attempts
        .get(&parent.attempt_id)
        .ok_or_else(|| invalid("unknown parent attempt"))?;
    if (access == ResultRead::Active
        && (state.state.is_terminal()
            || owner.cancellation_requested
            || attempt.cancellation_requested
            || !matches!(
                attempt.state,
                InvocationState::Running
                    | InvocationState::Suspended
                    | InvocationState::RecoveryRequired
            )
            || !matches!(
                owner.state,
                InvocationState::Running
                    | InvocationState::Suspended
                    | InvocationState::RecoveryRequired
            )))
        || owner.attempts.last() != Some(&parent.attempt_id)
        || attempt.binding != *parent
    {
        return Err(invalid("cancelled, terminal or foreign result consumer"));
    }
    if access == ResultRead::Historical {
        // Historical consumption is not active recovery authority. Independently
        // require the exact parent's physical terminal proof before reading its edge.
        load_for(coordinator, ledger, task_id, parent, max_bytes, access)?;
    }
    if access == ResultRead::Active
        && ledger
            .execution_events_after(&parent.execution.execution_id, 0)
            .map_err(|error| invalid(error.to_string()))?
            .iter()
            .any(|event| {
                matches!(
                    event.kind,
                    LedgerEventKind::ExecutionCancelled
                        | LedgerEventKind::TurnCancelled
                        | LedgerEventKind::TurnCompleted
                        | LedgerEventKind::TurnFailed
                        | LedgerEventKind::TurnTimedOut
                        | LedgerEventKind::EffectUncertain
                )
            })
    {
        return Err(invalid(
            "parent execution is terminal in the physical ledger",
        ));
    }
    if state
        .attempts
        .get(&child.attempt_id)
        .is_none_or(|attempt| attempt.binding != *child)
    {
        return Err(invalid("foreign child attempt"));
    }
    let successful = owner.consumed_results.get(&child.invocation_id);
    let terminal = owner.terminal_consumed_results.get(&child.invocation_id);
    if successful.is_some() && terminal.is_some() {
        return Err(invalid("conflicting consumption formats"));
    }
    if successful.is_none() && terminal.is_none() {
        return Ok(None);
    }
    let physical = load_for(coordinator, ledger, task_id, child, max_bytes, access)?;
    let (kind, expected) = if let Some(consumed) = terminal {
        let matches = match (&consumed.disposition, &physical.outcome) {
            (
                TerminalResultDisposition::Completed { .. },
                VerifiedTaskOutcome::Completed { .. },
            ) => true,
            (
                TerminalResultDisposition::Failed { reason },
                VerifiedTaskOutcome::Failed { reason: actual },
            )
            | (
                TerminalResultDisposition::Cancelled { reason },
                VerifiedTaskOutcome::Cancelled { reason: actual },
            ) => reason == actual,
            _ => false,
        };
        if consumed.parent != *parent
            || consumed.child != *child
            || consumed.terminal_fact != physical.terminal_fact
            || !matches
        {
            return Err(invalid(
                "consumed terminal disposition differs from physical proof",
            ));
        }
        (
            "task.terminal_result_consumed",
            TaskEvent::TerminalResultConsumed {
                invocation_id: parent.invocation_id.clone(),
                result: Box::new(consumed.clone()),
            },
        )
    } else {
        let consumed = successful.unwrap();
        if !matches!(physical.outcome, VerifiedTaskOutcome::Completed { .. })
            || consumed.completion_fact != physical.terminal_fact
        {
            return Err(invalid(
                "consumed result is not the exact successful child proof",
            ));
        }
        (
            "task.result_consumed",
            TaskEvent::ResultConsumed {
                invocation_id: parent.invocation_id.clone(),
                result: consumed.clone(),
            },
        )
    };
    let linked = ExecutionBinding {
        task_id: task_id.into(),
        invocation_id: parent.invocation_id.clone(),
        attempt_id: parent.attempt_id.clone(),
        session_id: parent.execution.session_id.clone(),
        turn_id: parent.execution.turn_id.clone(),
        execution_id: parent.execution.execution_id.clone(),
    };
    let id = format!("{}/task-binding", parent.execution.execution_id);
    let binding = ledger
        .event_by_id(&id)
        .map_err(|error| invalid(error.to_string()))?
        .ok_or_else(|| invalid("missing parent execution binding"))?;
    if binding.event_id != id
        || binding.idempotency_key != id
        || binding.kind != LedgerEventKind::ExecutionBound
        || binding.execution_id != parent.execution.execution_id
        || binding.turn_id != parent.execution.turn_id
        || binding.payload != json!({"binding":linked})
    {
        return Err(invalid("parent execution binding differs"));
    }
    let records = coordinator.records(task_id)?;
    let mut matching = records
        .iter()
        .filter(|record| record.draft.kind == kind && record.draft.payload == json!(expected));
    let record = matching
        .next()
        .ok_or_else(|| invalid("consumption fact missing"))?;
    if matching.next().is_some()
        || record.draft.schema_version != 1
        || !record.draft.critical
        || !record.draft.causes.contains(&physical.terminal_fact)
    {
        return Err(invalid("unknown or conflicting consumption proof"));
    }
    let result = VerifiedConsumedResult {
        parent: parent.clone(),
        consumption_fact: FactRef {
            stream_id: record.stream_id.clone(),
            position: record.position,
            fact_id: record.draft.fact_id.clone(),
        },
        child: physical,
    };
    if serde_json::to_vec(&result)
        .map_err(|error| invalid(error.to_string()))?
        .len()
        > max_bytes
    {
        return Err(invalid("consumed result envelope exceeds host ceiling"));
    }
    Ok(Some(result))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum VerifiedTaskOutcome {
    Completed {
        response: Box<ModelResponse>,
        result_digest: String,
    },
    Failed {
        reason: String,
    },
    Cancelled {
        reason: String,
    },
}

pub(super) fn load<J: FactJournal, L: LedgerStore>(
    coordinator: &TaskCoordinator<J>,
    ledger: &L,
    task_id: &str,
    binding: &AttemptBinding,
    max_bytes: usize,
) -> Result<VerifiedTaskResult, TaskError> {
    load_for(
        coordinator,
        ledger,
        task_id,
        binding,
        max_bytes,
        ResultRead::Active,
    )
}

pub(super) fn load_for<J: FactJournal, L: LedgerStore>(
    coordinator: &TaskCoordinator<J>,
    ledger: &L,
    task_id: &str,
    binding: &AttemptBinding,
    max_bytes: usize,
    access: ResultRead,
) -> Result<VerifiedTaskResult, TaskError> {
    if max_bytes == 0 || max_bytes > 1024 * 1024 {
        return Err(invalid("invalid terminal result envelope ceiling"));
    }
    let snapshot = coordinator.snapshot(task_id)?;
    let invocation = snapshot
        .invocations
        .get(&binding.invocation_id)
        .ok_or_else(|| invalid("unknown result invocation"))?;
    let attempt = snapshot
        .attempts
        .get(&binding.attempt_id)
        .ok_or_else(|| invalid("unknown result attempt"))?;
    if !matches!(
        invocation.state,
        InvocationState::Completed | InvocationState::Failed | InvocationState::Cancelled
    ) || attempt.state != invocation.state
        || attempt.binding != *binding
        || invocation.attempts.last() != Some(&binding.attempt_id)
    {
        return Err(invalid("child is not an exact successful terminal attempt"));
    }
    let observation = attempt
        .observation
        .as_ref()
        .ok_or_else(|| invalid("missing completed observation"))?;
    if access == ResultRead::Historical {
        let events = ledger
            .execution_events_after(&binding.execution.execution_id, 0)
            .map_err(|error| invalid(error.to_string()))?;
        for event in &events {
            if event.kind != LedgerEventKind::ExecutionCancelled
                && !evidence::is_physical_terminal(event)?
            {
                continue;
            }
            let conflict = matches!(
                (&observation.outcome, event.kind),
                (
                    AttemptOutcome::Completed { .. },
                    LedgerEventKind::TurnFailed
                        | LedgerEventKind::TurnTimedOut
                        | LedgerEventKind::TurnCancelled
                        | LedgerEventKind::ExecutionCancelled,
                ) | (
                    AttemptOutcome::Failed { .. },
                    LedgerEventKind::TurnCancelled | LedgerEventKind::ExecutionCancelled,
                ) | (
                    AttemptOutcome::Cancelled { .. },
                    LedgerEventKind::TurnCompleted
                        | LedgerEventKind::TurnFailed
                        | LedgerEventKind::TurnTimedOut,
                )
            ) || (matches!(observation.outcome, AttemptOutcome::Failed { .. })
                && event.kind == LedgerEventKind::TurnCompleted
                // Completed is a lifecycle notification, not necessarily success.
                // Only the admitted FinalAnswer boundary proves contradictory success.
                && event.payload["reason"] == "FinalAnswer")
                || (matches!(observation.outcome, AttemptOutcome::Completed { .. })
                    && event.kind == LedgerEventKind::TurnCompleted
                    && event.payload["reason"] != "FinalAnswer");
            if conflict {
                return Err(invalid(
                    "historical execution has conflicting physical terminal outcomes",
                ));
            }
        }
    }
    let records = coordinator.records(task_id)?;
    let record = records
        .iter()
        .rev()
        .find(|record| {
            record.draft.kind == "task.attempt_observed"
                && record.draft.payload == json!(TaskEvent::AttemptObserved(observation.clone()))
        })
        .ok_or_else(|| invalid("missing exact terminal observation fact"))?;
    let terminal_fact = FactRef {
        stream_id: record.stream_id.clone(),
        position: record.position,
        fact_id: record.draft.fact_id.clone(),
    };
    if record.stream_id != task_id
        || record.draft.kind != "task.attempt_observed"
        || record.draft.schema_version != 1
        || serde_json::from_value::<TaskEvent>(record.draft.payload.clone())
            .map_err(|error| invalid(error.to_string()))?
            != TaskEvent::AttemptObserved(observation.clone())
    {
        return Err(invalid("completion observation fact differs"));
    }
    let linked = ExecutionBinding {
        task_id: task_id.into(),
        invocation_id: binding.invocation_id.clone(),
        attempt_id: binding.attempt_id.clone(),
        session_id: binding.execution.session_id.clone(),
        turn_id: binding.execution.turn_id.clone(),
        execution_id: binding.execution.execution_id.clone(),
    };
    let id = format!("{}/task-binding", binding.execution.execution_id);
    let fact = ledger
        .event_by_id(&id)
        .map_err(|error| invalid(error.to_string()))?
        .ok_or_else(|| invalid("missing child execution admission binding"))?;
    if fact.kind != LedgerEventKind::ExecutionBound
        || fact.event_id != id
        || fact.idempotency_key != id
        || fact.execution_id != binding.execution.execution_id
        || fact.turn_id != binding.execution.turn_id
        || fact.payload != json!({"binding":linked})
    {
        return Err(invalid("child execution admission binding differs"));
    }
    let actual = inspect(ledger, binding, 0)?;
    if actual.source != observation.source {
        return Err(invalid(
            "child result is not a matching physical terminal fact",
        ));
    }
    let outcome = match (&observation.outcome, actual.stopped) {
        (AttemptOutcome::Failed { reason, .. }, StoppedOutcome::Failed(actual_reason))
            if reason == &actual_reason =>
        {
            VerifiedTaskOutcome::Failed {
                reason: reason.clone(),
            }
        }
        (AttemptOutcome::Cancelled { reason }, StoppedOutcome::Cancelled) => {
            VerifiedTaskOutcome::Cancelled {
                reason: reason.clone(),
            }
        }
        (AttemptOutcome::Completed { evidence }, StoppedOutcome::Completed) => {
            if (access == ResultRead::Active
                && (snapshot.state.is_terminal() && snapshot.state != crate::TaskState::Completed
                    || invocation.cancellation_requested
                    || attempt.cancellation_requested))
                || invocation.completion_fact.as_ref() != Some(&terminal_fact)
            {
                return Err(invalid(
                    "successful child has cancellation or a different completion fact",
                ));
            }
            let response = actual
                .response
                .ok_or_else(|| invalid("child final response is missing"))?;
            let result_digest = response_digest(&response)?;
            // Child invocations need not own a Task completion criterion.
            // Their actual response digest is derived from the physical Step;
            // any recorded criterion proof must still agree with that source.
            if evidence.iter().any(|proof| {
                proof.source() != &actual.source
                    || matches!(proof, CompletionEvidence::ExecutionResult { result_digest: digest, .. }
                        if digest != &result_digest)
            }) {
                return Err(invalid("child result criterion proof differs from physical response"));
            }
            VerifiedTaskOutcome::Completed {
                response: Box::new(response),
                result_digest,
            }
        }
        _ => {
            return Err(invalid(
                "child terminal outcome differs from physical evidence",
            ));
        }
    };
    let result = VerifiedTaskResult {
        binding: binding.clone(),
        terminal_fact,
        outcome,
    };
    if serde_json::to_vec(&result)
        .map_err(|error| invalid(error.to_string()))?
        .len()
        > max_bytes
    {
        return Err(invalid("terminal result envelope exceeds host ceiling"));
    }
    Ok(result)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "result/ordering_tests.rs"]
mod ordering_tests;
