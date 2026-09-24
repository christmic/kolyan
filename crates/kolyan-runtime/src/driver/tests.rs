use super::*;
use kolyan_ledger::{InMemoryLedger, LedgerError};

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
