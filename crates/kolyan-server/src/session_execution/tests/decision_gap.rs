//! Durable deny survives restart before terminal and Session publication.
use super::super::*;
use kolyan_ledger::InMemoryLedger;
use kolyan_model::{ModelRequest, ProviderFuture};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;

struct NeverModel;
impl ModelProvider for NeverModel {
    fn stream(&self, _: ModelRequest) -> ProviderFuture<'_> {
        panic!("approval decision recovery must not call a model");
    }
}

#[tokio::test]
async fn saved_deny_is_retryable_and_cannot_be_overwritten_by_approval() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(directory.path()).unwrap();
    store.create("s").unwrap();
    store
        .begin_turn_with_input(
            "s",
            SessionTurn {
                turn_id: "t".into(),
                execution_id: "e".into(),
                status: SessionTurnStatus::Running,
            },
            0,
            vec![],
        )
        .unwrap();
    let ledger = InMemoryLedger::default();
    let suspension = crate::suspension::tests::persist_approval(ledger.clone()).await;
    store
        .update_turn("s", "t", SessionTurnStatus::Suspended, vec![])
        .unwrap();
    let key = ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    let approval_id = &suspension.waiting.approvals[0].approval_id;
    let denied = crate::suspension::record_approval_decision(
        &ledger,
        &key,
        &suspension,
        approval_id,
        "deny",
    )
    .unwrap();
    let before = ledger.events_after(0).unwrap();
    let service = SessionExecutionService::new(
        ExecutionService::new(ledger.clone(), NoopTraceSink),
        SessionService::new(store.clone()),
    );
    assert!(
        service
            .resume_approval(TurnExecutor::new(NeverModel), "s", "e", approval_id)
            .await
            .is_err()
    );
    assert!(
        service
            .resume(
                TurnExecutor::new(NeverModel),
                "s",
                "e",
                &suspension.checkpoint.checkpoint_id,
                ResumeInput::ApprovalConfirmed(denied)
            )
            .await
            .is_err()
    );
    assert_eq!(ledger.events_after(0).unwrap(), before);
    assert!(!service.execution().server().coordinator().is_active("e"));
    service
        .deny(TurnExecutor::new(NeverModel), &key, approval_id)
        .unwrap();
    assert_eq!(service.state("e").unwrap(), ExecutionState::Failed);
    assert_eq!(
        store.load("s").unwrap().turns[0].status,
        SessionTurnStatus::Failed
    );
    let facts = ledger.events_after(0).unwrap();
    assert_eq!(
        facts
            .iter()
            .filter(|fact| fact.kind == LedgerEventKind::ApprovalResolved)
            .count(),
        1
    );
    assert_eq!(
        facts
            .iter()
            .filter(|fact| fact.kind == LedgerEventKind::TurnFailed)
            .count(),
        1
    );
    assert!(!facts[before.len()..].iter().any(|fact| matches!(
        fact.kind,
        LedgerEventKind::ModelRequested
            | LedgerEventKind::EffectStarted
            | LedgerEventKind::ToolExecutionStarted
    )));
}
