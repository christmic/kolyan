use super::*;

async fn pending() -> (
    ScopedLedger,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    kolyan_core::TurnSuspension,
) {
    let ledger = ScopedLedger::default();
    let models = Arc::new(AtomicUsize::new(0));
    let tools = Arc::new(AtomicUsize::new(0));
    let driver = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    let mut input = request("turn");
    input.config.max_steps = 2;
    let DurableTurnResult::Suspended { suspension, .. } = driver
        .start(
            executor(models.clone(), tools.clone()),
            input,
            "session",
            "target",
        )
        .await
        .unwrap()
    else {
        panic!("expected saved approval");
    };
    (ledger, models, tools, *suspension)
}

#[tokio::test]
async fn calling_runtime_resume_is_not_an_approval_decision() {
    let (ledger, models, tools, suspension) = pending().await;
    let rebuilt = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    let approval = &suspension.waiting.approvals[0];
    assert!(
        rebuilt
            .resume_approval(
                executor(models.clone(), tools.clone()),
                "session",
                "target",
                &approval.approval_id
            )
            .await
            .is_err()
    );
    assert_eq!(models.load(Ordering::SeqCst), 1);
    assert_eq!(tools.load(Ordering::SeqCst), 0);
    assert!(
        ledger
            .execution_events_after("target", 0)
            .unwrap()
            .iter()
            .all(|event| event.kind != LedgerEventKind::ApprovalResolved)
    );
    assert_eq!(
        rebuilt.load_current_suspension("target").unwrap(),
        suspension
    );
    ledger.assert_scoped_reads();
}

#[tokio::test]
async fn persisted_rejection_cannot_be_consumed_as_affirmative_authority() {
    let (ledger, models, tools, suspension) = pending().await;
    let saved = &suspension.checkpoint.approvals[0];
    let confirmation = ApprovalConfirmation {
        approval_id: saved.approval_id.clone(),
        prepared_digest: saved.prepared.digest().into(),
        policy_revision: saved.policy_revision.clone(),
        scope: saved.scope.clone(),
        evidence_id: "target-rejection".into(),
    };
    ledger
        .append(LedgerEvent {
            event_id: confirmation.evidence_id.clone(),
            idempotency_key: confirmation.evidence_id.clone(),
            execution_id: "target".into(),
            turn_id: "turn".into(),
            cursor: 0,
            kind: LedgerEventKind::ApprovalResolved,
            payload: approval_decision_payload(
                &suspension.checkpoint.checkpoint_id,
                &confirmation,
                "reject",
            ),
        })
        .unwrap();
    let rebuilt = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    assert!(
        rebuilt
            .resume(
                executor(models.clone(), tools.clone()),
                "session",
                "target",
                &suspension.checkpoint.checkpoint_id,
                ResumeInput::ApprovalConfirmed(confirmation)
            )
            .await
            .is_err()
    );
    assert_eq!(models.load(Ordering::SeqCst), 1);
    assert_eq!(tools.load(Ordering::SeqCst), 0);
    assert_eq!(
        rebuilt.load_current_suspension("target").unwrap(),
        suspension
    );
    ledger.assert_scoped_reads();
}
