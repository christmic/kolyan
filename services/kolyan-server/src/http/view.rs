//! Read-only display mapping from durable facts; never exposes checkpoints.
use kolyan_core::StepResult;
use kolyan_ledger::{LedgerEventKind, LedgerStore};
use kolyan_model::ContentBlock;
use kolyan_server::{ExecutionRef, ServerError};
use kolyan_storage::{SessionRecord, StorageError};
use serde_json::{Value, json};

use crate::assembly::App;

pub fn session(record: SessionRecord) -> Value {
    json!({"session_id":record.session_id, "version":record.version, "turns":record.turns.into_iter().map(|turn| json!({"turn_id":turn.turn_id,"status":turn.status})).collect::<Vec<_>>()})
}

pub fn owned(app: &App, session_id: &str, turn_id: &str) -> Result<ExecutionRef, ServerError> {
    let session = app.service.load_reconciled(session_id)?;
    let turn = session
        .turns
        .iter()
        .find(|turn| turn.turn_id == turn_id)
        .ok_or_else(|| StorageError::NotFound(turn_id.into()))?;
    Ok(ExecutionRef {
        session_id: session_id.into(),
        turn_id: turn_id.into(),
        execution_id: turn.execution_id.clone(),
    })
}

pub fn turn(app: &App, key: &ExecutionRef, active: bool) -> Result<Value, ServerError> {
    let events = app
        .service
        .execution()
        .server()
        .coordinator()
        .ledger()
        .execution_events_after(&key.execution_id, 0)
        .map_err(kolyan_server::CoordinatorError::from)?;
    project(key, &events, app.service.state(&key.execution_id)?, active)
}

fn project(
    key: &ExecutionRef,
    events: &[kolyan_ledger::LedgerEvent],
    state: kolyan_server::ExecutionState,
    active: bool,
) -> Result<Value, ServerError> {
    let terminal = events.iter().find(|event| {
        matches!(
            event.kind,
            LedgerEventKind::TurnCompleted
                | LedgerEventKind::TurnCancelled
                | LedgerEventKind::TurnFailed
                | LedgerEventKind::TurnTimedOut
        )
    });
    let suspension = kolyan_server::execution_suspension(key, events)?;
    // Admission alone is not a durable wait. No approval-only fallback exists.
    let suspended = terminal.is_none()
        && suspension.is_some()
        && matches!(state, kolyan_server::ExecutionState::Suspended);
    let state = match terminal.map(|event| &event.kind) {
        Some(LedgerEventKind::TurnCompleted) => "completed",
        Some(LedgerEventKind::TurnCancelled) => "cancelled",
        Some(_) => "failed",
        None if suspended => "suspended",
        None => "running",
    };
    let end_reason = if terminal.is_some() {
        events
            .iter()
            .rev()
            .filter(|event| {
                matches!(
                    event.kind,
                    LedgerEventKind::TurnCompleted
                        | LedgerEventKind::TurnCancelled
                        | LedgerEventKind::TurnFailed
                        | LedgerEventKind::TurnTimedOut
                )
            })
            .find_map(|event| event.payload.get("reason").and_then(Value::as_str))
            .map(str::to_owned)
            .or_else(|| match state {
                "cancelled" => Some("Cancelled".into()),
                "failed" => Some("Failed".into()),
                _ => None,
            })
    } else {
        None
    };
    let mut steps = Vec::new();
    for event in events {
        if event.kind == LedgerEventKind::StepCompleted {
            let step: StepResult = serde_json::from_value(event.payload["step"].clone())
                .map_err(StorageError::from)?;
            let content = step.response.content.into_iter().filter_map(|block| match block {
                ContentBlock::Text {text} => Some(json!({"type":"text","text":text})),
                ContentBlock::Reasoning {text,..} => Some(json!({"type":"reasoning","text":text})),
                ContentBlock::ToolCall {call} => Some(json!({"type":"tool_call","id":call.id,"name":call.name,"arguments":call.arguments})),
                _ => None,
            }).collect::<Vec<_>>();
            steps.push(json!({"step_id":step.step_id,"outcome":step.outcome,"content":content,"usage":step.response.usage,"structured_output":step.response.structured_output}));
        }
    }
    let waiting = if suspended {
        kolyan_server::suspension_view(suspension.as_ref().expect("validated compound suspension"))?
    } else {
        json!({"checkpoint_id":null,"pending_approvals":[],"external_waits":[]})
    };
    let mut tool_results = Vec::new();
    for event in events {
        if event.kind == LedgerEventKind::ToolExecutionCompleted
            && let Some(result) = event.payload.get("result")
        {
            let result: kolyan_model::ToolResult =
                serde_json::from_value(result.clone()).map_err(StorageError::from)?;
            tool_results.push(result);
        }
    }
    Ok(
        json!({"session_id":key.session_id,"turn_id":key.turn_id,"state":state,"end_reason":end_reason,
        "steps":steps,"tool_results":tool_results,"checkpoint_id":waiting["checkpoint_id"],
        "pending_approvals":waiting["pending_approvals"],"external_waits":waiting["external_waits"],
        "cancellation_requested":events.iter().any(|event| event.kind == LedgerEventKind::ExecutionCancelled),
        "execution_stopped":terminal.is_some() || suspended,
        "recovery_required":terminal.is_none() && !suspended && !active}),
    )
}

#[cfg(test)]
mod tests;
