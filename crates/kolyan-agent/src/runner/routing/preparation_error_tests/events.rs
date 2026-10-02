//! Lossless test-only event projection; diagnostics never substitute for typed fields.

use kolyan_core::{StepResult, TurnEvent, TurnOutcome};
use kolyan_model::{ContentBlock, ModelResponse, ToolCall};
use serde_json::{Value, json};

pub(super) fn serialize(event: &TurnEvent) -> Value {
    match event {
        TurnEvent::Started { turn_id } => json!({"kind":"started","turn_id":turn_id}),
        TurnEvent::StepStarted { turn_id, step_id } => {
            json!({"kind":"step_started","turn_id":turn_id,"step_id":step_id})
        }
        TurnEvent::StepCompleted { turn_id, step } => {
            json!({"kind":"step_completed","turn_id":turn_id,"step":step})
        }
        TurnEvent::ToolCallRequested { turn_id, call } => {
            json!({"kind":"tool_call_requested","turn_id":turn_id,"call":call})
        }
        TurnEvent::ToolExecutionStarted {
            turn_id,
            call_id,
            name,
        } => {
            json!({"kind":"tool_execution_started","turn_id":turn_id,"call_id":call_id,"name":name})
        }
        TurnEvent::ToolResult { turn_id, result } => {
            json!({"kind":"tool_result","turn_id":turn_id,"result":result})
        }
        TurnEvent::ToolExecutionFailed {
            turn_id,
            call_id,
            name,
            error,
        } => {
            json!({"kind":"tool_execution_failed","turn_id":turn_id,"call_id":call_id,"name":name,"error":error})
        }
        TurnEvent::ApprovalRequested {
            turn_id,
            call_id,
            name,
        } => json!({"kind":"approval_requested","turn_id":turn_id,"call_id":call_id,"name":name}),
        TurnEvent::ToolAwaitingExternal {
            turn_id,
            call_id,
            wait,
        } => {
            json!({"kind":"tool_awaiting_external","turn_id":turn_id,"call_id":call_id,"wait":wait})
        }
        TurnEvent::Failed { turn_id, error } => {
            json!({"kind":"failed","turn_id":turn_id,"error":error})
        }
        TurnEvent::Cancelled { turn_id } => json!({"kind":"cancelled","turn_id":turn_id}),
        TurnEvent::TimedOut { turn_id } => json!({"kind":"timed_out","turn_id":turn_id}),
        TurnEvent::Completed { turn_id, outcome } => {
            let outcome = match outcome {
                TurnOutcome::FinalAnswer { response } => {
                    json!({"kind":"final_answer","response":response})
                }
                TurnOutcome::Refused { response } => json!({"kind":"refused","response":response}),
                TurnOutcome::Incomplete { response } => {
                    json!({"kind":"incomplete","response":response})
                }
                TurnOutcome::Rejected { reason } => json!({"kind":"rejected","reason":reason}),
                TurnOutcome::Expired { reason } => json!({"kind":"expired","reason":reason}),
                TurnOutcome::MaxSteps => json!({"kind":"max_steps"}),
            };
            json!({"kind":"completed","turn_id":turn_id,"outcome":outcome})
        }
    }
}

/// Compare only after the caller has read the exported physical JSONL.
pub(super) fn compare(row: &Value) {
    let id = row["fixture_id"].as_str().unwrap();
    let events = row["full_events"].as_array().unwrap();
    assert_eq!(
        events.len(),
        row["events"].as_array().unwrap().len(),
        "{id}"
    );
    let responses: Vec<ModelResponse> = serde_json::from_value(row["responses"].clone()).unwrap();
    let steps: Vec<StepResult> = events
        .iter()
        .filter(|event| event["kind"] == "step_completed")
        .map(|event| serde_json::from_value(event["step"].clone()).unwrap())
        .collect();
    assert_eq!(steps.len(), responses.len(), "{id}");
    for (step, response) in steps.iter().zip(&responses) {
        assert_eq!(&step.response, response, "{id}: full response fields lost");
    }
    let emitted: Vec<_> = responses
        .iter()
        .flat_map(|response| &response.content)
        .filter_map(|block| match block {
            ContentBlock::ToolCall { call } => Some(call),
            _ => None,
        })
        .collect();
    for event in events
        .iter()
        .filter(|event| event["kind"] == "tool_call_requested")
    {
        let call: ToolCall = serde_json::from_value(event["call"].clone()).unwrap();
        assert!(
            emitted.contains(&&call),
            "{id}: call fields differ from actual response"
        );
        if call.id == row["original"]["id"].as_str().unwrap() {
            assert_eq!(event["call"], row["original"], "{id}");
        }
    }
    if !responses.is_empty() {
        assert!(
            events
                .iter()
                .any(|event| event["kind"] == "tool_call_requested"
                    && event["call"] == row["original"]),
            "{id}: original call event missing"
        );
    }
}
