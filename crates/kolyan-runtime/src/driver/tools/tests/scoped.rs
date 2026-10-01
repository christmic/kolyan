use super::*;
use crate::tests::scoped::ScopedLedger;

#[tokio::test]
async fn scoped_tool_receipt_recovery_never_repeats_an_effect() {
    let ledger = ScopedLedger::default();
    let count = Arc::new(AtomicUsize::new(0));
    let first = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()));
    let expected = first
        .execute_invocation(invocation_for(call()))
        .await
        .unwrap();
    drop(first);
    let rebuilt = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()));
    assert_eq!(
        rebuilt
            .execute_invocation(invocation_for(call()))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut changed = call();
    changed.arguments = json!({"content": "changed"});
    assert!(
        rebuilt
            .execute_invocation(invocation_for(changed))
            .await
            .is_err()
    );
    ledger.assert_scoped_reads();
}

#[tokio::test]
async fn scoped_tool_started_without_receipt_stays_uncertain() {
    let ledger = ScopedLedger::default();
    append_once(
        &ledger,
        "e",
        "t",
        "effect/step/c/started",
        LedgerEventKind::EffectStarted,
        json!({"effect_id": "step/c"}),
    )
    .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let tools = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()));
    assert!(
        tools
            .execute_invocation(invocation_for(call()))
            .await
            .unwrap_err()
            .to_string()
            .contains("uncertain")
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(
        ledger
            .event_by_id("e/effect/step/c/uncertain")
            .unwrap()
            .unwrap()
            .kind,
        LedgerEventKind::EffectUncertain
    );
    ledger.assert_scoped_reads();
}
