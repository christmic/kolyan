use super::super::*;
use kolyan_ledger::InMemoryLedger;
use kolyan_model::{ModelRequest, ProviderFuture};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;

struct NeverModel;
impl ModelProvider for NeverModel {
    fn stream(&self, _: ModelRequest) -> ProviderFuture<'_> {
        panic!("denial must not invoke a model");
    }
}

#[tokio::test]
async fn denial_is_durable_exclusive_and_reconcilable_without_execution() {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    store.create("s").unwrap();
    let key = ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
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
    store
        .update_turn("s", "t", SessionTurnStatus::Suspended, vec![])
        .unwrap();
    let ledger = InMemoryLedger::default();
    let coordinator = ExecutionCoordinator::new(ledger.clone());
    coordinator.start(key.clone()).unwrap();
    coordinator.release("e");
    let suspension = crate::suspension::tests::persist_approval(ledger.clone()).await;
    let approval_id = &suspension.waiting.approvals[0].approval_id;
    let service = SessionExecutionService::new(
        ExecutionService::new(ledger.clone(), NoopTraceSink),
        SessionService::new(store.clone()),
    );
    let before = ledger.events_after(0).unwrap();
    let wrong = ExecutionRef {
        session_id: "other".into(),
        ..key.clone()
    };
    assert!(
        service
            .deny(TurnExecutor::new(NeverModel), &wrong, approval_id)
            .is_err()
    );
    assert!(
        service
            .deny(TurnExecutor::new(NeverModel), &key, "stale")
            .is_err()
    );
    assert_eq!(ledger.events_after(0).unwrap(), before);
    let suspended_record = store.load("s").unwrap();
    service
        .deny(TurnExecutor::new(NeverModel), &key, approval_id)
        .unwrap();
    assert_eq!(service.state("e").unwrap(), ExecutionState::Failed);
    assert_eq!(
        store.load("s").unwrap().turns[0].status,
        SessionTurnStatus::Failed
    );
    // Exclusivity is a durable bound deny decision, not a stranded resume claim.
    assert_eq!(
        ledger
            .events_after(0)
            .unwrap()
            .iter()
            .filter(|event| {
                event.kind == LedgerEventKind::ApprovalResolved
                    && event.payload["decision"] == "deny"
                    && event.payload["checkpoint_id"] == suspension.checkpoint.checkpoint_id
            })
            .count(),
        1
    );
    assert!(
        service
            .deny(TurnExecutor::new(NeverModel), &key, approval_id)
            .is_err()
    );
    let events = ledger.events_after(0).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == LedgerEventKind::TurnFailed)
            .count(),
        1
    );
    assert!(!events[before.len()..].iter().any(|e| matches!(
        e.kind,
        LedgerEventKind::ModelRequested
            | LedgerEventKind::ToolExecutionStarted
            | LedgerEventKind::EffectStarted
    )));
    assert_eq!(
        events
            .iter()
            .find(|e| e.kind == LedgerEventKind::TurnFailed)
            .unwrap()
            .payload["reason"],
        "ApprovalRejected"
    );
    // Reopening the Server does not require the rejected checkpoint in memory.
    // Reconstruct the crash window after terminal persistence but before the
    // Session snapshot commit, without adding a real persistence dependency.
    std::fs::write(
        root.path().join("s.json"),
        serde_json::to_vec(&suspended_record).unwrap(),
    )
    .unwrap();
    let reopened = SessionExecutionService::new(
        ExecutionService::new(ledger, NoopTraceSink),
        SessionService::new(store),
    );
    assert_eq!(
        reopened.load_reconciled("s").unwrap().turns[0].status,
        SessionTurnStatus::Failed
    );
}
