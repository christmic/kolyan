//! Recover a committed pure merge without reapproving or remerging it.
use super::super::*;
use kolyan_core::{ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture, TurnControl};
use kolyan_ledger::InMemoryLedger;
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelRequest, ModelResponse, ProviderFuture,
    StopReason, TokenUsage, ToolCall, ToolResult,
};
use kolyan_policy::{ApprovalMode, PolicyEngine, PreparedCall, ToolManifest};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};

struct FinalProvider(Arc<AtomicUsize>);
impl ModelProvider for FinalProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let response = ModelResponse {
            id: "final".into(),
            model: request.model,
            content: vec![ContentBlock::Text {
                text: "completed".into(),
            }],
            structured_output: None,
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}
struct CountingTool {
    prepared: PreparedCall,
    calls: Arc<AtomicUsize>,
}
impl ToolExecutor for CountingTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        let prepared = self.prepared.clone();
        Box::pin(async move {
            assert_eq!(prepared.call(), &call);
            Ok(prepared)
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &invocation.policy_revision,
                    &invocation.scope,
                )
                .unwrap();
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "committed".into(),
                is_error: false,
            }))
        })
    }
}

#[tokio::test]
async fn merged_checkpoint_recovery_drives_once_without_consuming_confirmation_again() {
    checkpoint_recovery("merged", LedgerEventKind::TurnCheckpointMerged).await;
}

#[tokio::test]
async fn prepared_checkpoint_recovery_accepts_running_session_and_drives_once() {
    checkpoint_recovery("prepared", LedgerEventKind::TurnCheckpointPrepared).await;
}

async fn checkpoint_recovery(category: &str, kind: LedgerEventKind) {
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
    let saved = crate::suspension::tests::persist_approval(ledger.clone()).await;
    store
        .update_turn("s", "t", SessionTurnStatus::Suspended, vec![])
        .unwrap();
    let key = ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    let confirmation = crate::suspension::confirm_approval(
        &ledger,
        &key,
        &saved,
        &saved.waiting.approvals[0].approval_id,
    )
    .unwrap();
    let scope = saved.checkpoint.scope.clone();
    let checkpoint = TurnExecutor::new(FinalProvider(Arc::new(AtomicUsize::new(0))))
        .with_execution_key(scope.execution.clone())
        .merge_resume_with_control(
            saved,
            ResumeInput::ApprovalConfirmed(confirmation),
            scope,
            TurnControl::default(),
        )
        .unwrap();
    let publication = ledger
        .execution_events_after("e", 0)
        .unwrap()
        .last()
        .unwrap()
        .cursor;
    let payload =
        json!({"schema_version":1,"publication_cursor":publication,"checkpoint":checkpoint});
    let id = format!(
        "e/checkpoint/{category}/{:x}",
        Sha256::digest(serde_json::to_vec(&payload).unwrap())
    );
    ledger
        .append_unless_cancelled(LedgerEvent {
            event_id: id.clone(),
            idempotency_key: id,
            turn_id: "t".into(),
            execution_id: "e".into(),
            cursor: 0,
            kind,
            payload,
        })
        .unwrap();
    if kind == LedgerEventKind::TurnCheckpointPrepared {
        store
            .update_turn("s", "t", SessionTurnStatus::Running, vec![])
            .unwrap();
    }
    let prepared = checkpoint.calls[0].prepared.clone().unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: prepared.call().name.clone(),
        capabilities: prepared.claim().capabilities.clone(),
        effects: prepared.claim().effects.clone(),
        path_scopes: vec![],
        idempotency: prepared.claim().idempotency,
        approval: ApprovalMode::Always,
    });
    let models = Arc::new(AtomicUsize::new(0));
    let tools = Arc::new(AtomicUsize::new(0));
    let executor = || {
        TurnExecutor::with_tools(
            FinalProvider(models.clone()),
            CountingTool {
                prepared: prepared.clone(),
                calls: tools.clone(),
            },
        )
        .with_policy_engine(Arc::new(policy.clone()))
    };
    let service = SessionExecutionService::new(
        ExecutionService::new(ledger.clone(), NoopTraceSink),
        SessionService::new(store.clone()),
    );
    assert!(matches!(
        service
            .resume_committed(executor(), "s", "e", &checkpoint.checkpoint_id)
            .await
            .unwrap(),
        DurableTurnResult::Completed(..)
    ));
    let before = ledger.events_after(0).unwrap();
    assert!(
        service
            .resume_committed(executor(), "s", "e", &checkpoint.checkpoint_id)
            .await
            .is_err()
    );
    assert_eq!(ledger.events_after(0).unwrap(), before);
    assert_eq!(models.load(Ordering::SeqCst), 1);
    assert_eq!(tools.load(Ordering::SeqCst), 1);
    assert_eq!(
        before
            .iter()
            .filter(|fact| fact.kind == LedgerEventKind::ApprovalResolved)
            .count(),
        1
    );
    assert_eq!(
        before
            .iter()
            .filter(|fact| fact.kind == LedgerEventKind::EffectReceipt)
            .count(),
        1
    );
    assert_eq!(
        store.load("s").unwrap().turns[0].status,
        SessionTurnStatus::Completed
    );
}
