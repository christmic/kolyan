use super::*;
use kolyan_model::{ModelRef, ModelResponse, StopReason, TokenUsage};
use serde_json::json;

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
    state.pending = Some(PendingTools {
        batch: ToolCallBatch::try_from(vec![call]).unwrap(),
        assistant_content: content,
        approved: vec![],
    });
    state.tool_calls_used = 2;
    state
}

fn checkpoint(state: &RunState) -> ApprovalRequest {
    state.checkpoint(
        &state.pending.as_ref().unwrap().batch.calls()[0],
        "approve".into(),
        "v1".into(),
    )
}

#[test]
fn checkpoint_preserves_original_deadline_indices_and_dispatch() {
    let state = state();
    let approval = checkpoint(&state);
    let restored = RunState::restore(&approval).unwrap();
    assert_eq!(approval.continuation.next_step_index, 3);
    assert_eq!(restored.deadline_at_ms, state.deadline_at_ms);
    assert_eq!(restored.dispatch, state.dispatch);
    assert_eq!(restored.tool_timeout, state.tool_timeout);
    assert_eq!(restored.tool_calls_used, 2);
    assert!(approval.approval_id.contains("step-2"));
}

#[test]
fn checkpoint_does_not_replenish_elapsed_time() {
    let mut state = state();
    state.deadline_at_ms = Some(now_ms().saturating_sub(1));
    let restored = RunState::restore(&checkpoint(&state)).unwrap();
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
            0 => altered.continuation.next_step_index = 1,
            1 => altered.continuation.max_steps = 2,
            2 => altered.continuation.pending_calls[0].arguments = json!({"path":"other"}),
            3 => altered.continuation.approved_call_ids = vec!["unknown".into()],
            _ => altered.turn_id = "different-turn".into(),
        }
        assert!(RunState::restore(&altered).is_err(), "mutation {mutation}");
    }
}

#[test]
fn reused_model_call_ids_have_distinct_step_scoped_approvals() {
    let mut state = state();
    let later = checkpoint(&state);
    state.steps.pop();
    assert_ne!(checkpoint(&state).approval_id, later.approval_id);
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
