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
        .events_after(0)
        .map_err(kolyan_server::CoordinatorError::from)?
        .into_iter()
        .filter(|event| event.execution_id == key.execution_id)
        .collect::<Vec<_>>();
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
    let suspension = events
        .iter()
        .rev()
        .find(|event| event.kind == LedgerEventKind::ExecutionSuspended);
    let checkpoint = events.iter().rev().find(|event| {
        event.kind == LedgerEventKind::ApprovalRequested
            && event.payload.get("continuation").is_some()
            && suspension.is_some_and(|suspended| {
                suspended.payload["approval_id"] == event.payload["approval_id"]
            })
    });
    // The boundary fact precedes checkpoint persistence. During that gap there
    // is not yet an approval a caller can decide or recover.
    let suspended = terminal.is_none()
        && checkpoint.is_some()
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
    let approval = if suspended {
        let checkpoint = checkpoint.expect("suspended requires a persisted checkpoint");
        let approval: kolyan_core::ApprovalRequest =
            serde_json::from_value(checkpoint.payload.clone()).map_err(StorageError::from)?;
        let call = approval
            .continuation
            .pending_calls
            .iter()
            .find(|call| call.id == approval.call_id)
            .ok_or_else(|| StorageError::Conflict("missing pending call".into()))?;
        json!({"approval_id":approval.approval_id,"call_id":approval.call_id,"tool_name":approval.tool_name,"arguments":call.arguments})
    } else {
        Value::Null
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
        "steps":steps,"tool_results":tool_results,"pending_approval":approval,
        "cancellation_requested":events.iter().any(|event| event.kind == LedgerEventKind::ExecutionCancelled),
        "execution_stopped":terminal.is_some() || suspended,
        "recovery_required":terminal.is_none() && !suspended && !active}),
    )
}

#[cfg(test)]
mod tests;
