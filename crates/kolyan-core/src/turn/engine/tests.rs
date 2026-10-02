use super::*;
use kolyan_model::ToolChoice;
use kolyan_model::{ModelRef, ModelResponse, StopReason, TokenUsage};
use serde_json::json;

mod shared_deadline;

impl RunState {
    fn restore(checkpoint: &TurnCheckpoint, scope: &ToolExecutionScope) -> Result<Self, TurnError> {
        let deadline = TurnDeadline::restore(checkpoint.budget.deadline_at_ms)?;
        Self::restore_with_deadline(checkpoint, scope, deadline)
    }

    fn new(
        request: TurnRequest,
        dispatch: ToolDispatchPolicy,
        tool_timeout: Option<Duration>,
    ) -> Result<Self, TurnError> {
        let deadline = TurnDeadline::capture(request.config.deadline, None)?;
        Self::with_deadline(request, dispatch, tool_timeout, deadline)
    }
}

#[tokio::test]
async fn failed_fact_recording_prevents_model_invocation() {
    struct NeverProvider;
    impl ModelProvider for NeverProvider {
        fn stream(&self, _: ModelRequest) -> kolyan_model::ProviderFuture<'_> {
            panic!("model must not run after failed fact recording")
        }
    }
    struct BrokenRecorder;
    impl TurnEventRecorder for BrokenRecorder {
        fn record(&self, _: &TurnEvent) -> Result<(), TurnError> {
            Err(TurnError::BoundaryControl {
                message: "disk unavailable".into(),
            })
        }
    }
    let result = TurnExecutor::new(NeverProvider)
        .with_event_recorder(Arc::new(BrokenRecorder))
        .start_resumable(TurnRequest {
            turn_id: "record-failure".into(),
            model_request: state().model_request,
            config: TurnConfig::default(),
        })
        .await;
    assert!(matches!(result, Err(TurnError::BoundaryControl { .. })));
}

fn state() -> RunState {
    let request = TurnRequest {
        turn_id: "checkpoint".into(),
        model_request: ModelRequest {
            request_id: "initial".into(),
            model: ModelRef::new("test", "test"),
            system: vec![],
            messages: vec![],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: json!({}),
        },
        config: TurnConfig {
            max_steps: 4,
            max_tool_calls: Some(8),
            deadline: Some(Duration::from_secs(5)),
        },
    };
    let mut state = RunState::new(
        request,
        ToolDispatchPolicy {
            mode: ToolDispatchMode::Parallel,
            on_error: ToolErrorPolicy::ContinueBatch,
        },
        Some(Duration::from_secs(1)),
    )
    .unwrap();
    let call = ToolCall {
        id: "reused-id".into(),
        name: "file.write".into(),
        arguments: json!({"path":"safe/a","content":"a"}),
    };
    let content = vec![ContentBlock::ToolCall { call: call.clone() }];
    for index in 0..3 {
        state.steps.push(StepResult {
            step_id: format!("checkpoint-step-{index}"),
            outcome: StepOutcome::ToolCalls,
            response: ModelResponse {
                id: format!("r{index}"),
                model: ModelRef::new("test", "test"),
                content: content.clone(),
                structured_output: None,
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
                metadata: json!({}),
            },
        });
    }
    for step in &state.steps[..2] {
        append_tool_context(
            &mut state.model_request.messages,
            &step.response.content,
            vec![ToolResult {
                call_id: call.id.clone(),
                content: "historical result".into(),
                is_error: false,
            }],
        );
    }
    state.tool_calls_used = 2;
    let scope = ToolExecutionScope {
        execution: super::super::tests::preparation::fixture_key("checkpoint"),
        step_id: "checkpoint-step-2".into(),
        agent_snapshot_digest: None,
    };
    let prepared = super::super::tests::preparation::fixture_prepare(call.clone()).unwrap();
    state.pending = Some(
        TurnCheckpoint::reconstruct(
            CheckpointReconstruction {
                scope: scope.clone(),
                model_request: state.model_request.clone(),
                input_message_count: 0,
                steps: state.steps.clone(),
                calls: vec![CheckpointCall {
                    call,
                    prepared: Some(prepared),
                    charged: false,
                    state: CheckpointCallState::Ready,
                }],
                stages: vec![vec!["reused-id".into()]],
                stage_index: 0,
                approvals: vec![],
                budget: CheckpointBudget {
                    max_steps: 4,
                    max_tool_calls: Some(8),
                    deadline_at_ms: state.deadline_at_ms,
                    tool_timeout_ms: Some(1000),
                    prior_tool_calls_used: 2,
                    tool_calls_used: 2,
                },
                dispatch: state.dispatch,
            },
            &scope,
        )
        .unwrap(),
    );
    state
}

fn checkpoint(state: &RunState) -> TurnSuspension {
    let mut checkpoint = state.pending.clone().unwrap();
    checkpoint.scope.step_id = state.step_id();
    checkpoint.steps = state.steps.clone();
    checkpoint.next_step_index = state.steps.len();
    checkpoint.model_request = state.model_request.clone();
    checkpoint.budget.deadline_at_ms = state.deadline_at_ms;
    checkpoint.budget.prior_tool_calls_used = state.tool_calls_used;
    checkpoint.budget.tool_calls_used = state.tool_calls_used;
    let prepared = checkpoint.calls[0].prepared.clone().unwrap();
    checkpoint.approvals.push(CheckpointApproval {
        approval_id: super::super::checkpoint::host_identity(
            "approval",
            &(&checkpoint.scope, prepared.digest()),
        ),
        reason: "approve".into(),
        prepared,
        scope: checkpoint.scope.clone(),
        policy_revision: "v1".into(),
        evidence_id: None,
        expires_at_ms: None,
    });
    let scope = checkpoint.scope.clone();
    TurnSuspension::from_checkpoint(checkpoint, &scope).unwrap()
}

#[test]
fn checkpoint_preserves_original_deadline_indices_and_dispatch() {
    let state = state();
    let approval = checkpoint(&state);
    let restored = RunState::restore(&approval.checkpoint, &approval.checkpoint.scope).unwrap();
    assert_eq!(approval.checkpoint.next_step_index, 3);
    assert_eq!(restored.deadline_at_ms, state.deadline_at_ms);
    assert_eq!(restored.dispatch, state.dispatch);
    assert_eq!(restored.tool_timeout, state.tool_timeout);
    assert_eq!(restored.tool_calls_used, 2);
    assert_eq!(
        restored.pending.as_ref().unwrap().calls[0].prepared,
        state.pending.as_ref().unwrap().calls[0].prepared
    );
    assert_eq!(
        restored.pending.as_ref().unwrap().scope,
        state.pending.as_ref().unwrap().scope
    );
    assert_eq!(approval.checkpoint.scope.step_id, "checkpoint-step-2");
}

#[test]
fn checkpoint_does_not_replenish_elapsed_time() {
    let mut state = state();
    state.deadline_at_ms = Some(now_ms().saturating_sub(1));
    let restored = RunState::restore(
        &checkpoint(&state).checkpoint,
        &checkpoint(&state).checkpoint.scope,
    )
    .unwrap();
    assert_eq!(restored.deadline_at_ms, state.deadline_at_ms);
    assert!(matches!(
        restored.check(&TurnControl::default()),
        Err(TurnError::TimedOut)
    ));
}

#[test]
fn inconsistent_checkpoint_fields_are_rejected() {
    let approval = checkpoint(&state());
    for mutation in 0..5 {
        let mut altered = approval.clone();
        match mutation {
            0 => altered.checkpoint.next_step_index = 1,
            1 => altered.checkpoint.budget.max_steps = 2,
            2 => altered.checkpoint.calls[0].call.arguments = json!({"path":"other"}),
            3 => altered.checkpoint.approvals[0].approval_id = "unknown".into(),
            _ => altered.checkpoint.scope.execution.turn_id = "different-turn".into(),
        }
        assert!(
            altered.validate(&approval.checkpoint.scope).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn reused_model_call_ids_have_distinct_step_scoped_approvals() {
    let mut state = state();
    let later = checkpoint(&state);
    state.steps.pop();
    state.model_request.messages.truncate(2);
    state.tool_calls_used = 1;
    assert_ne!(
        checkpoint(&state).waiting.approvals[0].approval_id,
        later.waiting.approvals[0].approval_id
    );
}

#[tokio::test]
async fn inline_approval_is_consumed_once() {
    use futures_util::FutureExt;
    let control = TurnControl::default();
    control.approve_tool("file.write");
    assert!(
        control
            .wait_for_tool_approval("file.write")
            .now_or_never()
            .is_some()
    );
    assert!(
        control
            .wait_for_tool_approval("file.write")
            .now_or_never()
            .is_none()
    );
}

#[tokio::test]
async fn unavailable_admission_cannot_hold_a_timed_out_turn_open() {
    struct NeverProvider;
    impl ModelProvider for NeverProvider {
        fn stream(&self, _: ModelRequest) -> kolyan_model::ProviderFuture<'_> {
            panic!("provider must not run without admission")
        }
    }
    struct HangingControl;
    impl TurnBoundaryControl for HangingControl {
        fn admit(&self, _: TurnBoundary) -> TurnBoundaryFuture<'_> {
            Box::pin(std::future::pending())
        }
    }
    let executor = TurnExecutor::new(NeverProvider).with_boundary_control(Arc::new(HangingControl));
    let request = TurnRequest {
        turn_id: "admission-timeout".into(),
        model_request: state().model_request,
        config: TurnConfig {
            deadline: Some(Duration::from_millis(5)),
            ..Default::default()
        },
    };
    let result = tokio::time::timeout(Duration::from_millis(100), executor.execute(request))
        .await
        .unwrap();
    assert!(matches!(result, Err(TurnError::TimedOut)));
}
