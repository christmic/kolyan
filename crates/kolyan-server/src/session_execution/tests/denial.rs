use super::super::*;
use kolyan_core::{ApprovalState, ToolDispatchPolicy, TurnContinuation};
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

#[test]
fn denial_is_durable_exclusive_and_reconcilable_without_execution() {
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
    let request: ModelRequest = serde_json::from_value(json!({
        "request_id":"r","model":{"provider":"fixture","model":"m"},
        "system":[],"messages":[],"tools":[],"tool_choice":"auto",
        "output_format":null,"prompt_cache":null,"reasoning":null,
        "max_output_tokens":null,"extensions":{}
    }))
    .unwrap();
    let approval = ApprovalRequest {
        approval_id: "a".into(),
        turn_id: "t".into(),
        call_id: "c".into(),
        tool_name: "file.write".into(),
        reason: "requires approval".into(),
        state: ApprovalState::Pending,
        expires_at_ms: None,
        continuation: TurnContinuation {
            continuation_id: "continuation".into(),
            approval_id: "a".into(),
            turn_id: "t".into(),
            model_request: request,
            assistant_content: vec![],
            pending_calls: vec![],
            steps: vec![],
            max_steps: 3,
            next_step_index: 1,
            call_id: "c".into(),
            tool_name: "file.write".into(),
            args_fingerprint: "args".into(),
            policy_version: "v1".into(),
            // Denial consumes an opaque checkpoint without preparing or executing tools.
            prepared_calls: Vec::new(),
            preparation_errors: Vec::new(),
            execution_scope: serde_json::from_value(json!({
                "execution": {"session_id":"s", "turn_id":"t", "execution_id":"e"},
                "step_id":"t-step-0", "agent_snapshot_digest":null
            }))
            .unwrap(),
            approved_call_ids: vec![],
            max_tool_calls: Some(3),
            tool_calls_used: 0,
            deadline_at_ms: None,
            tool_dispatch: ToolDispatchPolicy::default(),
            tool_timeout_ms: None,
        },
    };
    coordinator
        .append_once(
            &key,
            "approval/a/requested",
            LedgerEventKind::ApprovalRequested,
            json!(approval),
        )
        .unwrap();
    coordinator
        .append_once(
            &key,
            "paused",
            LedgerEventKind::ExecutionSuspended,
            json!({"approval_id":"a"}),
        )
        .unwrap();
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
            .deny(TurnExecutor::new(NeverModel), &wrong, "a")
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
        .deny(TurnExecutor::new(NeverModel), &key, "a")
        .unwrap();
    assert_eq!(service.state("e").unwrap(), ExecutionState::Failed);
    assert_eq!(
        store.load("s").unwrap().turns[0].status,
        SessionTurnStatus::Failed
    );
    assert!(!ledger.claim("e/attempt/resume/a").unwrap());
    assert!(
        service
            .deny(TurnExecutor::new(NeverModel), &key, "a")
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
    assert!(!events.iter().any(|e| matches!(
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
