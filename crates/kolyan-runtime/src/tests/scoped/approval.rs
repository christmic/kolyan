use super::*;
use futures_util::stream;
use kolyan_core::{
    ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture, TurnExecutor,
};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture, StopReason, TokenUsage, ToolCall, ToolResult,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PathScope, PolicyEngine,
    PreparedCall, ResourceClaim, ToolManifest, ToolRequirements,
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
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            if call.name != "file.write" {
                return Err(ToolError::Unavailable { name: call.name });
            }
            let path = call.arguments["path"]
                .as_str()
                .ok_or_else(|| ToolError::Failed {
                    message: "fixture path is required".into(),
                })?
                .to_owned();
            let claim = InvocationClaim {
                tool_name: "file.write".into(),
                capabilities: [Capability::FilesystemWrite].into(),
                effects: [Effect::Update].into(),
                resource: ResourceClaim { path: Some(path) },
                idempotency: Idempotency::NonIdempotent,
            };
            PreparedCall::new(
                call,
                "approval-fixture-v1".into(),
                claim,
                ToolRequirements {
                    process_sandbox: false,
                    max_output_bytes: 65536,
                    timeout_ms: 30000,
                },
            )
            .map_err(|error| ToolError::Failed {
                message: error.to_string(),
            })
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &invocation.policy_revision,
                    &invocation.scope,
                )
                .map_err(|error| ToolError::PolicyDenied {
                    message: error.to_string(),
                })?;
            let call = invocation.prepared.call().clone();
            assert_eq!(call.name, "file.write");
            self.0.fetch_add(1, Ordering::SeqCst);
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
