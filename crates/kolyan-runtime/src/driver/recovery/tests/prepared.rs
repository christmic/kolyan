use super::*;
use crate::driver::{DurableTurnResult, LedgerRecorder};
use kolyan_core::TurnEventRecorder;

#[tokio::test]
async fn prepared_checkpoint_repairs_admission_gap_without_polling_execution_ports() {
    let fixture = Fixture::new(true, ToolErrorPolicy::FailTurn);
    let recorder = LedgerRecorder::new(fixture.ledger.clone(), fixture.key.clone(), "start".into());
    recorder.record_checkpoint(&fixture.checkpoint).unwrap();
    assert!(
        fixture
            .ledger
            .events_after(0)
            .unwrap()
            .iter()
            .all(|event| event.kind != LedgerEventKind::ExecutionSuspended)
    );
    fixture.enter(0);
    fixture.receipt(0, Ok(result("head")));
    fixture.enter(1);
    let wait = ExternalWait {
        wait_id: "saved-child-admission".into(),
        kind: "fixture.child".into(),
        schema_version: 1,
        binding: json!({"durable_admission":"already-committed"}),
    };
    let driver = fixture
        .driver()
        .with_external_wait_verifier(Arc::new(ExactWait(
            fixture.bindings[1].issued(),
            wait.clone(),
        )));
    let saved = driver
        .load_current_suspension(&fixture.key.execution_id)
        .unwrap();
    assert_eq!(saved.checkpoint, fixture.checkpoint);
    assert!(saved.waiting.approvals.is_empty() && saved.waiting.external_waits.is_empty());
    let DurableTurnResult::Suspended { suspension, .. } = driver
        .resume_committed(
            TurnExecutor::with_tools(ForbiddenPorts, ForbiddenPorts),
            &fixture.key.session_id,
            &fixture.key.execution_id,
            &saved.checkpoint.checkpoint_id,
        )
        .await
        .unwrap()
    else {
        panic!("partial recovery must suspend");
    };
    assert_eq!(suspension.checkpoint.budget.tool_calls_used, 2);
    assert_eq!(suspension.checkpoint.calls[2], fixture.checkpoint.calls[2]);
    assert_eq!(suspension.waiting.external_waits.len(), 1);
    assert!(
        matches!(&suspension.checkpoint.calls[1].state,CheckpointCallState::AwaitingExternal {wait:saved,..} if saved==&wait)
    );
    assert!(
        fixture
            .ledger
            .event_by_id(&format!("{}/started", fixture.bindings[2].prefix()))
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .ledger
            .event_by_id(&format!("{}/receipt", fixture.bindings[1].prefix()))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        driver
            .load_current_suspension(&fixture.key.execution_id)
            .unwrap(),
        *suspension
    );
}

#[test]
fn uncommitted_model_response_cannot_be_saved_as_an_execution_barrier() {
    let fixture = Fixture::new(false, ToolErrorPolicy::FailTurn);
    let recorder = LedgerRecorder::new(fixture.ledger.clone(), fixture.key.clone(), "start".into());
    let mut changed = fixture.checkpoint.clone();
    changed.steps[0].response.metadata = json!({"uncommitted":true});
    changed.validate(&changed.scope).unwrap();
    let before = fixture.ledger.events_after(0).unwrap();
    assert!(recorder.record_checkpoint(&changed).is_err());
    assert_eq!(fixture.ledger.events_after(0).unwrap(), before);
}

#[test]
fn committed_cancellation_refuses_pre_effect_barrier_without_new_facts() {
    let fixture = Fixture::new(false, ToolErrorPolicy::FailTurn);
    fixture.append(
        "recovery-cancelled",
        LedgerEventKind::ExecutionCancelled,
        json!({"reason":"cancel before effect entry"}),
    );
    let before = fixture.ledger.events_after(0).unwrap();
    let recorder = LedgerRecorder::new(fixture.ledger.clone(), fixture.key.clone(), "start".into());
    assert!(matches!(
        recorder.record_checkpoint(&fixture.checkpoint),
        Err(kolyan_core::TurnError::Cancelled)
    ));
    assert_eq!(fixture.ledger.events_after(0).unwrap(), before);
}

#[test]
fn unrelated_execution_cancellation_does_not_block_pre_effect_barrier() {
    let fixture = Fixture::new(false, ToolErrorPolicy::FailTurn);
    fixture
        .ledger
        .append(kolyan_ledger::LedgerEvent {
            event_id: "other-execution-cancelled".into(),
            idempotency_key: "other-execution-cancelled".into(),
            execution_id: "other-execution".into(),
            turn_id: "other-turn".into(),
            cursor: 0,
            kind: LedgerEventKind::ExecutionCancelled,
            payload: json!({"reason":"unrelated cancellation"}),
        })
        .unwrap();
    let recorder = LedgerRecorder::new(fixture.ledger.clone(), fixture.key.clone(), "start".into());
    recorder.record_checkpoint(&fixture.checkpoint).unwrap();
    let saved = fixture
        .driver()
        .load_current_suspension(&fixture.key.execution_id)
        .unwrap();
    assert_eq!(saved.checkpoint, fixture.checkpoint);
    assert!(
        fixture
            .ledger
            .execution_events_after(&fixture.key.execution_id, 0)
            .unwrap()
            .iter()
            .all(|event| event.kind != LedgerEventKind::EffectStarted)
    );
}

#[tokio::test]
async fn cancelled_prepared_checkpoint_cannot_recover_or_poll_execution_ports() {
    let fixture = Fixture::new(false, ToolErrorPolicy::FailTurn);
    let recorder = LedgerRecorder::new(fixture.ledger.clone(), fixture.key.clone(), "start".into());
    recorder.record_checkpoint(&fixture.checkpoint).unwrap();
    let driver = fixture.driver();
    driver
        .cancel(&fixture.key.execution_id, &fixture.key.turn_id)
        .unwrap();
    let before = fixture.ledger.events_after(0).unwrap();
    for _ in 0..2 {
        assert!(matches!(
            driver
                .resume_committed(
                    TurnExecutor::with_tools(ForbiddenPorts, ForbiddenPorts),
                    &fixture.key.session_id,
                    &fixture.key.execution_id,
                    &fixture.checkpoint.checkpoint_id,
                )
                .await,
            Err(crate::RuntimeError::Turn(kolyan_core::TurnError::Cancelled))
        ));
        assert_eq!(fixture.ledger.events_after(0).unwrap(), before);
    }
}

#[tokio::test]
async fn repeated_waiting_recovery_keeps_receipts_and_tool_budget_without_polling() {
    let fixture = Fixture::new(true, ToolErrorPolicy::FailTurn);
    LedgerRecorder::new(fixture.ledger.clone(), fixture.key.clone(), "start".into())
        .record_checkpoint(&fixture.checkpoint)
        .unwrap();
    fixture.enter(0);
    fixture.receipt(0, Ok(result("head")));
    fixture.enter(1);
    let wait = ExternalWait {
        wait_id: "saved-child-admission".into(),
        kind: "fixture.child".into(),
        schema_version: 1,
        binding: json!({"durable_admission":"already-committed"}),
    };
    let driver = fixture
        .driver()
        .with_external_wait_verifier(Arc::new(ExactWait(fixture.bindings[1].issued(), wait)));
    for _ in 0..3 {
        let current = driver
            .load_current_suspension(&fixture.key.execution_id)
            .unwrap();
        let DurableTurnResult::Suspended { suspension, .. } = driver
            .resume_committed(
                TurnExecutor::with_tools(ForbiddenPorts, ForbiddenPorts),
                &fixture.key.session_id,
                &fixture.key.execution_id,
                &current.checkpoint.checkpoint_id,
            )
            .await
            .unwrap()
        else {
            panic!("pending external work must remain suspended");
        };
        assert_eq!(suspension.checkpoint.budget.tool_calls_used, 2);
        assert!(suspension.checkpoint.calls[0].charged);
        assert!(suspension.checkpoint.calls[1].charged);
        assert_eq!(suspension.checkpoint.calls[2], fixture.checkpoint.calls[2]);
        assert_eq!(suspension.waiting.external_waits.len(), 1);
        let events = fixture
            .ledger
            .execution_events_after(&fixture.key.execution_id, 0)
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == LedgerEventKind::EffectStarted)
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == LedgerEventKind::EffectAwaitingExternal)
                .count(),
            1
        );
        assert!(
            events
                .iter()
                .all(|event| event.kind != LedgerEventKind::ModelRequested)
        );
    }
}
