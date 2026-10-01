use super::*;
use kolyan_core::{StepEvent, StepEventRecorder, TurnEvent, TurnEventRecorder};
use kolyan_ledger::{InMemoryLedger, LedgerError};
use kolyan_model::{TokenUsage, ToolCall};

#[tokio::test]
async fn approval_boundary_admission_does_not_publish_a_saved_suspension() {
    let ledger = InMemoryLedger::default();
    let control = LedgerBoundaryControl::new(ledger.clone(), key());
    control
        .admit(TurnBoundary {
            turn_id: "turn".into(),
            kind: TurnBoundaryKind::AwaitingApproval {
                approval_id: "approval".into(),
            },
        })
        .await
        .unwrap();
    let events = ledger.execution_events_after("exec", 0).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, LedgerEventKind::ExecutionBoundaryAdmitted);
    assert_eq!(events[0].payload, json!({"approval_id":"approval"}));
    assert!(
        events
            .iter()
            .all(|event| event.kind != LedgerEventKind::ExecutionSuspended)
    );
    assert!(
        events
            .iter()
            .all(|event| event.kind != LedgerEventKind::ApprovalRequested)
    );
}

#[test]
fn attempt_events_preserve_content_and_cursor_across_reconstruction() {
    let ledger = InMemoryLedger::default();
    let first = LedgerRecorder::new(ledger.clone(), key(), "start".into());
    TurnEventRecorder::record(
        &first,
        &TurnEvent::ToolResult {
            turn_id: "turn".into(),
            result: kolyan_model::ToolResult {
                call_id: "call".into(),
                content: "actual tool output".into(),
                is_error: false,
            },
        },
    )
    .unwrap();
    drop(first);
    let second = LedgerRecorder::new(ledger.clone(), key(), "resume/approval".into());
    TurnEventRecorder::record(
        &second,
        &TurnEvent::Cancelled {
            turn_id: "turn".into(),
        },
    )
    .unwrap();
    let events = ledger.events_after(0).unwrap();
    assert_eq!(events.len(), 2);
    assert!(events[0].cursor < events[1].cursor);
    assert_ne!(events[0].event_id, events[1].event_id);
    assert_eq!(events[0].payload["result"]["content"], "actual tool output");
    let duplicate = LedgerRecorder::new(ledger.clone(), key(), "start".into());
    assert!(
        TurnEventRecorder::record(
            &duplicate,
            &TurnEvent::Started {
                turn_id: "turn".into(),
            },
        )
        .is_err()
    );
    assert_eq!(ledger.events_after(0).unwrap().len(), 2);
}

#[test]
fn neutral_model_stream_events_share_the_attempt_cursor_without_raw_provider_events() {
    let ledger = InMemoryLedger::default();
    let recorder = LedgerRecorder::new(ledger.clone(), key(), "stream".into());
    let call = ToolCall {
        id: "call-1".into(),
        name: "read".into(),
        arguments: json!({"path":"notes.txt"}),
    };
    for event in [
        StepEvent::Started {
            step_id: "step-1".into(),
        },
        StepEvent::TextDelta {
            step_id: "step-1".into(),
            text: "hello".into(),
        },
        StepEvent::ReasoningDelta {
            step_id: "step-1".into(),
            text: "inspect".into(),
        },
        StepEvent::ToolCallStarted {
            step_id: "step-1".into(),
            id: call.id.clone(),
            name: call.name.clone(),
        },
        StepEvent::ToolCallArgumentsDelta {
            step_id: "step-1".into(),
            id: call.id.clone(),
            delta: r#"{"path":"notes.txt"}"#.into(),
        },
        StepEvent::ToolCallCompleted {
            step_id: "step-1".into(),
            call,
        },
        StepEvent::Usage {
            step_id: "step-1".into(),
            usage: TokenUsage {
                output_tokens: Some(7),
                ..TokenUsage::default()
            },
        },
        StepEvent::Cancelled {
            step_id: "step-1".into(),
        },
    ] {
        StepEventRecorder::record(&recorder, &event).unwrap();
    }

    let events = ledger.events_after(0).unwrap();
    assert_eq!(events.len(), 6);
    assert!(
        events
            .iter()
            .all(|event| event.kind == LedgerEventKind::ModelStreamEvent)
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.payload["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "text_delta",
            "reasoning_delta",
            "tool_call_started",
            "tool_call_arguments_delta",
            "tool_call_completed",
            "usage"
        ]
    );
    assert_eq!(events[3].payload["delta"], r#"{"path":"notes.txt"}"#);
    assert_eq!(events[4].payload["call"]["arguments"]["path"], "notes.txt");
}

#[derive(Clone)]
struct UnreadableLedger;
impl LedgerStore for UnreadableLedger {
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        panic!("must not append after failed control read")
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        Err(LedgerError::Storage("unavailable".into()))
    }
    fn query(&self, _: &kolyan_ledger::LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        Err(LedgerError::Storage("unavailable".into()))
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        panic!("must not claim")
    }
}

fn key() -> RuntimeTurnKey {
    RuntimeTurnKey {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "exec".into(),
    }
}

#[tokio::test]
async fn admission_rejects_a_boundary_from_another_turn() {
    let ledger = InMemoryLedger::default();
    let control = LedgerBoundaryControl::new(ledger.clone(), key());
    assert!(matches!(
        control
            .admit(TurnBoundary {
                turn_id: "other".into(),
                kind: TurnBoundaryKind::Step {
                    step_id: "step".into()
                }
            })
            .await,
        Err(TurnError::BoundaryControl { .. })
    ));
    assert!(ledger.events_after(0).unwrap().is_empty());
}

#[tokio::test]
async fn unreadable_ledger_cannot_admit_a_step() {
    let control = LedgerBoundaryControl::new(UnreadableLedger, key());
    assert!(matches!(
        control
            .admit(TurnBoundary {
                turn_id: "turn".into(),
                kind: TurnBoundaryKind::Step {
                    step_id: "step".into()
                }
            })
            .await,
        Err(TurnError::BoundaryControl { .. })
    ));
}

#[tokio::test]
async fn cancellation_prevents_successful_terminal_admission() {
    let ledger = InMemoryLedger::default();
    append_once(
        &ledger,
        "exec",
        "turn",
        "cancel",
        LedgerEventKind::ExecutionCancelled,
        Value::Null,
    )
    .unwrap();
    let control = LedgerBoundaryControl::new(ledger.clone(), key());
    assert!(matches!(
        control
            .admit(TurnBoundary {
                turn_id: "turn".into(),
                kind: TurnBoundaryKind::Terminal {
                    reason: kolyan_core::TurnEndReason::FinalAnswer
                }
            })
            .await,
        Err(TurnError::Cancelled)
    ));
    control
        .admit(TurnBoundary {
            turn_id: "turn".into(),
            kind: TurnBoundaryKind::Terminal {
                reason: kolyan_core::TurnEndReason::Cancelled,
            },
        })
        .await
        .unwrap();
    assert_eq!(
        ledger.events_after(0).unwrap().last().unwrap().kind,
        LedgerEventKind::TurnCancelled
    );
}

#[test]
fn repeated_event_ids_must_bind_the_same_payload() {
    let ledger = InMemoryLedger::default();
    let first = append_once(
        &ledger,
        "exec",
        "turn",
        "same",
        LedgerEventKind::StepStarted,
        json!({"step":"one"}),
    )
    .unwrap();
    assert_eq!(
        append_once(
            &ledger,
            "exec",
            "turn",
            "same",
            LedgerEventKind::StepStarted,
            json!({"step":"one"})
        )
        .unwrap(),
        first
    );
    assert!(
        append_once(
            &ledger,
            "exec",
            "turn",
            "same",
            LedgerEventKind::StepStarted,
            json!({"step":"two"})
        )
        .is_err()
    );
    assert!(
        append_once(
            &ledger,
            "exec",
            "other-turn",
            "same",
            LedgerEventKind::StepStarted,
            json!({"step":"one"})
        )
        .is_err()
    );
}
