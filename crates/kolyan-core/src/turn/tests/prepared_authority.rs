//! Turn-bound authority refusals precede effects and model continuation.

use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

struct ChangingPreparation {
    mode: Arc<AtomicUsize>,
    executed: Arc<AtomicUsize>,
}

impl ToolExecutor for ChangingPreparation {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            let base = fixture_prepare(call)?;
            let mode = self.mode.load(Ordering::SeqCst);
            if mode == 3 {
                return std::future::pending().await;
            }
            let mut claim = base.claim().clone();
            if mode == 2 {
                claim.resource.path = Some("changed-target".into());
            }
            PreparedCall::new(
                base.call().clone(),
                if mode == 1 {
                    "changed-v2"
                } else {
                    "original-v1"
                }
                .into(),
                claim,
                base.requirements().clone(),
            )
            .and_then(|prepared| {
                prepared.with_execution_binding(serde_json::json!({
                    "root":{"dev":1,"ino":if mode == 6 { 2 } else { 1 }}
                }))
            })
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
                .unwrap();
            self.executed.fetch_add(1, Ordering::SeqCst);
            Ok(ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "committed".into(),
                is_error: false,
            })
        })
    }
}

fn provider() -> ScriptedProvider {
    ScriptedProvider {
        calls: Arc::new(AtomicUsize::new(0)),
        saw_tool_result: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        tool_call: ToolCall {
            id: "authority-call".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command":"count_lines","path":"original-target"}),
        },
    }
}

fn turn_request() -> TurnRequest {
    TurnRequest {
        turn_id: "prepared-authority".into(),
        model_request: request(),
        config: TurnConfig {
            max_steps: 2,
            ..Default::default()
        },
    }
}

fn approval_policy() -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    policy.register(kolyan_policy::ToolManifest {
        tool_name: "shell.query".into(),
        capabilities: [kolyan_policy::Capability::ProcessInspect].into(),
        effects: [kolyan_policy::Effect::Read].into(),
        path_scopes: vec![],
        idempotency: kolyan_policy::Idempotency::Idempotent,
        approval: kolyan_policy::ApprovalMode::Always,
    });
    Arc::new(policy)
}

#[tokio::test]
async fn missing_or_foreign_host_key_never_executes_a_tool() {
    for foreign in [false, true] {
        let executed = Arc::new(AtomicUsize::new(0));
        let executor = TurnExecutor::with_tools(
            provider(),
            CountingTool {
                executed: executed.clone(),
            },
        )
        .with_policy_engine(fixture_policy());
        let executor = if foreign {
            executor.with_execution_key(fixture_key("another-turn"))
        } else {
            executor
        };
        assert!(matches!(
            executor.execute(turn_request()).await,
            Err(TurnError::InvalidRequest { .. })
        ));
        assert_eq!(executed.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn resumed_approval_rejects_changed_implementation_resource_scope_or_snapshot() {
    for mutation in 1..=6 {
        let model = provider();
        let calls = model.calls.clone();
        let mode = Arc::new(AtomicUsize::new(0));
        let executed = Arc::new(AtomicUsize::new(0));
        let executor = TurnExecutor::with_tools(
            model,
            ChangingPreparation {
                mode: mode.clone(),
                executed: executed.clone(),
            },
        )
        .with_execution_key(fixture_key("prepared-authority"))
        .with_agent_snapshot_digest("a".repeat(64))
        .with_policy_engine(approval_policy());
        let approval = match executor.start_resumable(turn_request()).await.unwrap() {
            ResumableTurn::AwaitingApproval(approval) => *approval,
            other => panic!("expected durable approval: {other:?}"),
        };
        let bytes = serde_json::to_vec(&approval).unwrap();
        let restored: ApprovalRequest = serde_json::from_slice(&bytes).unwrap();
        let executor = match mutation {
            1 | 2 | 6 => {
                mode.store(mutation, Ordering::SeqCst);
                executor
            }
            3 | 4 => {
                let mut key = fixture_key("prepared-authority");
                if mutation == 3 {
                    key.session_id = "another-session".into();
                } else {
                    key.execution_id = "another-invocation".into();
                }
                executor.with_execution_key(key)
            }
            5 => executor.with_agent_snapshot_digest("b".repeat(64)),
            _ => unreachable!(),
        };
        assert!(
            matches!(
                executor
                    .resume_approval(restored, &approval.approval_id)
                    .await,
                Err(TurnError::InvalidRequest { .. })
            ),
            "mutation {mutation}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(executed.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn resumed_preparation_retains_tool_timeout_without_a_turn_deadline() {
    let model = provider();
    let calls = model.calls.clone();
    let mode = Arc::new(AtomicUsize::new(0));
    let executed = Arc::new(AtomicUsize::new(0));
    let executor = TurnExecutor::with_tools(
        model,
        ChangingPreparation {
            mode: mode.clone(),
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("prepared-authority"))
    .with_policy_engine(approval_policy())
    .with_tool_timeout(Duration::from_millis(10));
    let approval = match executor.start_resumable(turn_request()).await.unwrap() {
        ResumableTurn::AwaitingApproval(approval) => *approval,
        other => panic!("expected approval: {other:?}"),
    };
    mode.store(3, Ordering::SeqCst);
    let id = approval.approval_id.clone();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        executor.resume_approval(approval, &id),
    )
    .await
    .expect("saved tool timeout must bound resumed preparation")
    .unwrap_err();
    assert!(matches!(error, TurnError::Tool(ToolError::TimedOut)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
}
