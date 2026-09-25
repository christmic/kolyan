use super::*;
use kolyan_core::{TurnEvent, TurnEventRecorder};
use kolyan_ledger::{InMemoryLedger, LedgerError};

#[test]
fn attempt_events_preserve_content_and_cursor_across_reconstruction() {
    let ledger = InMemoryLedger::default();
    let first = LedgerRecorder::new(ledger.clone(), key(), "start".into());
    first
        .record(&TurnEvent::ToolResult {
            turn_id: "turn".into(),
            result: kolyan_model::ToolResult {
                call_id: "call".into(),
                content: "actual tool output".into(),
                is_error: false,
            },
        })
        .unwrap();
    drop(first);
    let second = LedgerRecorder::new(ledger.clone(), key(), "resume/approval".into());
    second
        .record(&TurnEvent::Cancelled {
            turn_id: "turn".into(),
        })
        .unwrap();
    let events = ledger.events_after(0).unwrap();
    assert_eq!(events.len(), 2);
    assert!(events[0].cursor < events[1].cursor);
    assert_ne!(events[0].event_id, events[1].event_id);
    assert_eq!(events[0].payload["result"]["content"], "actual tool output");
    let duplicate = LedgerRecorder::new(ledger.clone(), key(), "start".into());
    assert!(
        duplicate
            .record(&TurnEvent::Started {
                turn_id: "turn".into()
            })
            .is_err()
    );
    assert_eq!(ledger.events_after(0).unwrap().len(), 2);
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
