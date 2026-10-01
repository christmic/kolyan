//! Trusted semantics and exact host coordinates for deterministic Turn fixtures.

use super::*;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, ResourceClaim, ToolManifest,
    ToolRequirements,
};

pub(crate) fn fixture_key(turn_id: &str) -> ExecutionKey {
    ExecutionKey {
        session_id: "core-fixture-session".into(),
        turn_id: turn_id.into(),
        execution_id: format!("fixture-{turn_id}"),
    }
}

fn manifest(name: &str) -> Result<ToolManifest, ToolError> {
    let (capability, effect, idempotency) = match name {
        "shell.query" => (
            Capability::ProcessInspect,
            Effect::Read,
            Idempotency::Idempotent,
        ),
        "read" | "file.read" => (
            Capability::FilesystemRead,
            Effect::Read,
            Idempotency::Idempotent,
        ),
        "file.write" => (
            Capability::FilesystemWrite,
            Effect::Update,
            Idempotency::NonIdempotent,
        ),
        _ => return Err(ToolError::Unavailable { name: name.into() }),
    };
    Ok(ToolManifest {
        tool_name: name.into(),
        capabilities: [capability].into(),
        effects: [effect].into(),
        path_scopes: Vec::new(),
        idempotency,
        approval: ApprovalMode::Never,
    })
}

pub(super) fn fixture_policy() -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    for name in ["shell.query", "read", "file.read", "file.write"] {
        policy.register(manifest(name).unwrap());
    }
    Arc::new(policy)
}

pub(crate) fn fixture_prepare(call: ToolCall) -> Result<PreparedCall, ToolError> {
    let declared = manifest(&call.name)?;
    let path = match call.arguments.get("path") {
        None => None,
        Some(Value::String(path)) => Some(path.clone()),
        _ => {
            return Err(ToolError::Failed {
                message: "fixture path must be a string".into(),
            });
        }
    };
    let claim = InvocationClaim {
        tool_name: declared.tool_name,
        capabilities: declared.capabilities,
        effects: declared.effects,
        resource: ResourceClaim { path },
        idempotency: declared.idempotency,
    };
    PreparedCall::new(
        call,
        "core-fixture-v1".into(),
        claim,
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 1024 * 1024,
            timeout_ms: 30000,
        },
    )
    .map_err(|error| ToolError::Failed {
        message: error.to_string(),
    })
}

pub(super) fn fixture_validate(invocation: &ToolInvocation) -> Result<ToolCall, ToolError> {
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
    if fixture_prepare(call.clone())? != invocation.prepared {
        return Err(ToolError::PolicyDenied {
            message: "fixture implementation preparation changed".into(),
        });
    }
    Ok(call)
}

#[tokio::test]
async fn failed_preparation_fail_turn_has_no_execution_started_event() {
    let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let executor = TurnExecutor::with_tools(
        MockProvider {
            stop_reason: StopReason::ToolUse,
            content: vec![ContentBlock::ToolCall {
                call: ToolCall {
                    id: "invalid-path".into(),
                    name: "file.write".into(),
                    arguments: serde_json::json!({"path":42}),
                },
            }],
        },
        CountingTool {
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("preparation-fail-turn"))
    .with_policy_engine(fixture_policy());
    let events = executor
        .execute_event_stream(
            TurnRequest {
                turn_id: "preparation-fail-turn".into(),
                model_request: request(),
                config: TurnConfig::default(),
            },
            TurnControl::default(),
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Err(TurnError::Tool(ToolError::Failed { .. }))))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Ok(TurnEvent::Failed { .. })))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(TurnEvent::ToolExecutionStarted { .. })))
    );
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_preparation_feedback_never_emits_execution_started() {
    let model_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let saw_result = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let executor = TurnExecutor::with_tools(
        ScriptedProvider {
            calls: model_calls.clone(),
            saw_tool_result: saw_result.clone(),
            tool_call: ToolCall {
                id: "invalid-path".into(),
                name: "file.write".into(),
                arguments: serde_json::json!({"path": 42, "content": "not written"}),
            },
        },
        CountingTool {
            executed: executed.clone(),
        },
    )
    .with_execution_key(fixture_key("preparation-feedback"))
    .with_policy_engine(fixture_policy())
    .with_tool_dispatch_policy(ToolDispatchPolicy {
        mode: ToolDispatchMode::Serial,
        on_error: ToolErrorPolicy::ContinueBatch,
    });
    let execution = executor
        .execute_with_events(
            TurnRequest {
                turn_id: "preparation-feedback".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..Default::default()
                },
            },
            TurnControl::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(saw_result.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(
        !execution
            .events
            .iter()
            .any(|event| matches!(event, TurnEvent::ToolExecutionStarted { .. }))
    );
    assert!(execution.events.iter().any(|event| matches!(event, TurnEvent::ToolResult { result, .. } if result.is_error && result.call_id == "invalid-path")));
}
