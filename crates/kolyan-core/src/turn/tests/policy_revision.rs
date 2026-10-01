use super::*;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PolicyDecisionKind, ToolManifest,
};

#[tokio::test]
async fn rule_change_rejects_old_approval_even_when_decision_kind_is_unchanged() {
    let call = ToolCall {
        id: "policy-change".into(),
        name: "shell.query".into(),
        arguments: serde_json::json!({"command":"count_lines","path":"safe/file"}),
    };
    let provider = ScriptedProvider {
        calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        saw_tool_result: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: call.clone(),
    };
    let model_calls = provider.calls.clone();
    let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "shell.query".into(),
        capabilities: [Capability::ProcessInspect].into_iter().collect(),
        effects: [Effect::Read].into_iter().collect(),
        path_scopes: vec![],
        idempotency: Idempotency::Idempotent,
        approval: ApprovalMode::Always,
    });
    let mut changed = policy.clone();
    changed.restrict_workspace("safe");
    assert_eq!(
        policy.decide(&call).kind,
        PolicyDecisionKind::RequireApproval
    );
    assert_eq!(
        changed.decide(&call).kind,
        PolicyDecisionKind::RequireApproval
    );
    assert_ne!(policy.revision(), changed.revision());
    let executor = TurnExecutor::with_tools(
        provider,
        CountingTool {
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("revision-turn"))
    .with_policy_engine(fixture_policy())
    .with_policy_engine(Arc::new(policy));
    let approval = match executor
        .start_resumable(TurnRequest {
            turn_id: "revision-turn".into(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 2,
                ..Default::default()
            },
        })
        .await
        .unwrap()
    {
        ResumableTurn::AwaitingApproval(approval) => *approval,
        other => panic!("expected approval: {other:?}"),
    };
    let approval_id = approval.approval_id.clone();
    let error = executor
        .with_policy_engine(Arc::new(changed))
        .resume_approval(approval, &approval_id)
        .await
        .unwrap_err();
    assert!(matches!(error, TurnError::InvalidRequest {message} if message.contains("stale")));
    assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
}
