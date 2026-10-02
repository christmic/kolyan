//! Exact configured ports and ceilings survive model-side rebinding.

use super::*;

struct AllowBoundary;

impl TurnBoundaryControl for AllowBoundary {
    fn admit(&self, _: TurnBoundary) -> TurnBoundaryFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

struct Recorder;

struct MarkerTools(u32);

impl ToolExecutor for MarkerTools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        NoopToolExecutor.prepare(call)
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        NoopToolExecutor.execute_invocation(invocation)
    }
}

impl TurnEventRecorder for Recorder {
    fn record(&self, _: &TurnEvent) -> Result<(), TurnError> {
        Ok(())
    }
}

#[test]
fn rebinding_retains_all_turn_ports_and_limits() {
    let policy = fixture_policy();
    let boundary: Arc<dyn TurnBoundaryControl> = Arc::new(AllowBoundary);
    let recorder: Arc<dyn TurnEventRecorder> = Arc::new(Recorder);
    let dispatch = ToolDispatchPolicy {
        mode: ToolDispatchMode::Parallel,
        on_error: ToolErrorPolicy::ContinueBatch,
    };
    let key = fixture_key("mapping");
    let executor = TurnExecutor::with_tools(17_u32, MarkerTools(29))
        .with_policy_engine(policy.clone())
        .with_boundary_control(boundary.clone())
        .with_event_recorder(recorder.clone())
        .with_execution_key(key.clone())
        .with_agent_snapshot_digest("a".repeat(64))
        .with_tool_dispatch_policy(dispatch)
        .with_tool_timeout(Duration::from_millis(731))
        .with_absolute_deadline_at_ms(42_000);
    let mapped = executor
        .try_map_model_provider(|provider| {
            assert_eq!(provider, 17);
            Ok::<_, ()>(format!("wrapped-{provider}"))
        })
        .unwrap();
    assert!(Arc::ptr_eq(&policy, mapped.policy_engine.as_ref().unwrap()));
    assert!(Arc::ptr_eq(
        &boundary,
        mapped.boundary_control.as_ref().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &recorder,
        mapped.event_recorder.as_ref().unwrap()
    ));
    assert_eq!(mapped.execution_key, Some(key));
    assert_eq!(
        mapped.agent_snapshot_digest.as_deref(),
        Some("a".repeat(64).as_str())
    );
    assert_eq!(mapped.tool_dispatch, dispatch);
    assert_eq!(mapped.tool_timeout, Some(Duration::from_millis(731)));
    assert_eq!(mapped.absolute_deadline_at_ms, Some(42_000));
    assert_eq!(mapped.tool_executor.0, 29);
}

#[test]
fn failed_turn_binding_returns_original_error_once() {
    let calls = std::cell::Cell::new(0);
    let result = TurnExecutor::new(19_u32).try_map_model_provider(|_| {
        calls.set(calls.get() + 1);
        Err::<String, _>("exact host binding failure")
    });
    assert!(matches!(result, Err("exact host binding failure")));
    assert_eq!(calls.get(), 1);
}
