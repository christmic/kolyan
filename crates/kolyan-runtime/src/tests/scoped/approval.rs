use super::*;
use futures_util::stream;
use kolyan_core::{ToolExecutor, ToolFuture, TurnExecutor};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture, StopReason, TokenUsage, ToolCall, ToolResult,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, ExecutionGrant, Idempotency, PathScope, PolicyEngine,
    ToolManifest,
};

#[derive(Clone)]
struct ApprovalProvider(Arc<AtomicUsize>);

impl ModelProvider for ApprovalProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        let response = ModelResponse {
            id: request.request_id,
            model: request.model,
            content: if first {
                vec![ContentBlock::ToolCall {
                    call: ToolCall {
                        id: "call".into(),
                        name: "file.write".into(),
                        arguments: json!({"path": "safe/approved.txt", "content": "approved"}),
                    },
                }]
            } else {
                vec![ContentBlock::Text {
                    text: "done".into(),
                }]
            },
            structured_output: None,
            stop_reason: if first {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(
                Box::pin(stream::iter(vec![Ok(ModelEvent::Completed(response))]))
                    as ModelEventStream,
            )
        })
    }
}

struct ApprovedTool(Arc<AtomicUsize>);
impl ToolExecutor for ApprovedTool {
    fn execute_with_grant(&self, call: ToolCall, grant: ExecutionGrant) -> ToolFuture<'_> {
        assert_eq!(grant.call_id, call.id);
        assert_eq!(grant.tool_name, call.name);
        self.execute(call)
    }

    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolResult {
                call_id: call.id,
                content: "written".into(),
                is_error: false,
            })
        })
    }
}

fn executor(
    models: Arc<AtomicUsize>,
    tools: Arc<AtomicUsize>,
) -> TurnExecutor<ApprovalProvider, ApprovedTool> {
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Update].into_iter().collect(),
        path_scopes: vec![PathScope::new("safe")],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Always,
    });
    TurnExecutor::with_tools(ApprovalProvider(models), ApprovedTool(tools))
        .with_policy_engine(Arc::new(policy))
}

#[tokio::test]
async fn approval_checkpoint_resumes_after_reconstruction_using_only_scoped_reads() {
    let ledger = ScopedLedger::default();
    let models = Arc::new(AtomicUsize::new(0));
    let tools = Arc::new(AtomicUsize::new(0));
    let driver = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    let mut turn_request = request("turn");
    turn_request.config.max_steps = 2;
    let result = driver
        .start(
            executor(models.clone(), tools.clone()),
            turn_request,
            "session",
            "target",
        )
        .await
        .unwrap();
    let DurableTurnResult::AwaitingApproval {
        approval,
        trajectory,
    } = result
    else {
        panic!("expected approval checkpoint");
    };
    assert_eq!(models.load(Ordering::SeqCst), 1);
    assert_eq!(tools.load(Ordering::SeqCst), 0);
    // An unrelated checkpoint with the same approval id must never be selected.
    ledger
        .append(LedgerEvent {
            event_id: "other/approval".into(),
            turn_id: "other-turn".into(),
            execution_id: "other".into(),
            cursor: 0,
            kind: LedgerEventKind::ApprovalRequested,
            idempotency_key: "other/approval".into(),
            payload: json!({"approval_id": approval.approval_id}),
        })
        .unwrap();
    drop(driver);
    let rebuilt = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    assert!(
        rebuilt
            .load_approval("missing", &approval.approval_id)
            .is_err()
    );
    assert_eq!(
        rebuilt
            .load_approval("target", &approval.approval_id)
            .unwrap(),
        *approval
    );
    assert!(
        rebuilt
            .resume(
                executor(models.clone(), tools.clone()),
                "wrong-session",
                "target",
                &approval.approval_id
            )
            .await
            .is_err()
    );
    let result = rebuilt
        .resume(
            executor(models.clone(), tools.clone()),
            "session",
            "target",
            &approval.approval_id,
        )
        .await
        .unwrap();
    let DurableTurnResult::Completed(_, completed) = result else {
        panic!("expected completion");
    };
    assert!(completed.records.starts_with(&trajectory.records));
    assert!(
        completed
            .records
            .windows(2)
            .all(|rows| rows[0].sequence < rows[1].sequence)
    );
    assert_eq!(models.load(Ordering::SeqCst), 2);
    assert_eq!(tools.load(Ordering::SeqCst), 1);
    assert!(
        rebuilt
            .resume(
                executor(models.clone(), tools.clone()),
                "session",
                "target",
                &approval.approval_id
            )
            .await
            .is_err()
    );
    assert_eq!(tools.load(Ordering::SeqCst), 1);
    ledger.assert_scoped_reads();
}
