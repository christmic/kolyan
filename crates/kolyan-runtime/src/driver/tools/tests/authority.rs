//! Reject altered independent invocation coordinates before recording effects.

use super::*;

#[tokio::test]
async fn valid_grant_for_foreign_execution_cannot_replace_host_admission() {
    let ledger = InMemoryLedger::default();
    let effects = Arc::new(AtomicUsize::new(0));
    let tools = DurableTools::new(ledger.clone(), key(), CountingTool(effects.clone()));
    let mut scope = invocation_for(call()).scope;
    scope.execution.session_id = "foreign-session".into();
    let invocation = preparation::invocation_in_scope(call(), scope);
    invocation
        .grant
        .validate(
            &invocation.prepared,
            &invocation.policy_revision,
            &invocation.scope,
        )
        .unwrap();
    assert!(tools.execute_invocation(invocation).await.is_err());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(ledger.events_after(0).unwrap().is_empty());
}

#[tokio::test]
async fn invocation_scope_and_grant_must_match_before_fixture_effects() {
    for coordinate in 0..4 {
        let ledger = InMemoryLedger::default();
        let effects = Arc::new(AtomicUsize::new(0));
        let tools = DurableTools::new(ledger.clone(), key(), CountingTool(effects.clone()));
        let mut invocation = invocation_for(call());
        match coordinate {
            0 => invocation.scope.execution.session_id = "foreign-session".into(),
            1 => invocation.scope.execution.execution_id = "foreign-execution".into(),
            2 => invocation.scope.execution.turn_id = "foreign-turn".into(),
            _ => invocation.scope.step_id = "foreign-step".into(),
        }
        assert!(tools.execute_invocation(invocation).await.is_err());
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert!(
            !ledger
                .events_after(0)
                .unwrap()
                .iter()
                .any(|event| event.kind == LedgerEventKind::EffectStarted)
        );
    }
}

#[tokio::test]
async fn changed_preparation_cannot_reuse_a_grant_or_start_an_effect() {
    let ledger = InMemoryLedger::default();
    let effects = Arc::new(AtomicUsize::new(0));
    let tools = DurableTools::new(ledger.clone(), key(), CountingTool(effects.clone()));
    let mut invocation = invocation_for(call());
    let mut changed = call();
    changed.arguments = json!({"content":"unauthorized change"});
    invocation.prepared = prepare_call(changed).unwrap();
    assert!(tools.execute_invocation(invocation).await.is_err());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(
        !ledger
            .events_after(0)
            .unwrap()
            .iter()
            .any(|event| event.kind == LedgerEventKind::EffectStarted)
    );
}
