//! Cross-check saved usage against actual tool batches of this Turn, never the
//! input Session history. This checks consistency, not trusted limit provenance.

use super::*;
use kolyan_model::Message;

fn history_fixture() -> TurnCheckpoint {
    let mut checkpoint = fixture(false);
    let mut previous = checkpoint.steps[0].clone();
    previous.response.content.truncate(2);
    // A complete old Session batch lies before the saved original input boundary.
    checkpoint.model_request.messages = vec![
        Message {
            role: MessageRole::Assistant,
            content: previous.response.content.clone(),
        },
        Message {
            role: MessageRole::User,
            content: vec![ContentBlock::ToolResult {
                result: result("a"),
            }],
        },
        Message {
            role: MessageRole::User,
            content: vec![ContentBlock::ToolResult {
                result: result("b"),
            }],
        },
        Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: "current Turn input".into(),
            }],
        },
    ];
    checkpoint.input_message_count = checkpoint.model_request.messages.len();
    super::super::super::append_tool_context(
        &mut checkpoint.model_request.messages,
        &previous.response.content,
        vec![result("a"), result("b")],
    );
    checkpoint.steps[0].step_id = "turn-step-1".into();
    checkpoint.steps.insert(0, previous);
    checkpoint.next_step_index = 2;
    checkpoint.scope.step_id = "turn-step-1".into();
    checkpoint.budget.prior_tool_calls_used = 2;
    checkpoint.budget.tool_calls_used = 3;
    let CheckpointCallState::AwaitingExternal { issued, .. } = &mut checkpoint.calls[0].state
    else {
        unreachable!()
    };
    issued.scope = checkpoint.scope.clone();
    issued.grant = PreparedGrant::issue(
        &issued.prepared,
        PolicyDecision {
            kind: PolicyDecisionKind::Allow,
            reason: "trusted historical fixture".into(),
            policy_version: issued.policy_revision.clone(),
            constraints: issued.grant.constraints().clone(),
        },
        ApprovalEvidence::NotConfirmed,
        checkpoint.scope.clone(),
    )
    .unwrap();
    checkpoint
}

#[test]
fn current_turn_history_is_charged_once_and_session_history_is_not_charged() {
    let checkpoint = history_fixture();
    checkpoint.validate(&checkpoint.scope).unwrap();
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let restored = TurnCheckpoint::from_json(&bytes, &checkpoint.scope).unwrap();
    let merged = restored
        .merge_external(&[resolution("a")], &checkpoint.scope)
        .unwrap();
    assert_eq!(merged.budget.prior_tool_calls_used, 2);
    assert_eq!(merged.budget.tool_calls_used, 3);
    assert_eq!(merged.steps, checkpoint.steps);
    let mut charges_session_history = checkpoint;
    charges_session_history.budget.prior_tool_calls_used += 2;
    charges_session_history.budget.tool_calls_used += 2;
    assert!(
        charges_session_history
            .validate(&charges_session_history.scope)
            .is_err()
    );
}

#[test]
fn jointly_lowering_prior_and_total_cannot_reset_historical_tool_usage() {
    let checkpoint = history_fixture();
    for reduction in [1, 2] {
        let mut bad = checkpoint.clone();
        bad.budget.prior_tool_calls_used -= reduction;
        bad.budget.tool_calls_used -= reduction;
        assert!(bad.validate(&bad.scope).is_err());
    }
}

#[test]
fn historical_final_outcome_and_incomplete_or_foreign_results_are_rejected() {
    let checkpoint = history_fixture();
    for index in 0..4 {
        let mut bad = checkpoint.clone();
        match index {
            0 => bad.steps[0].outcome = StepOutcome::FinalAnswer,
            1 => {
                bad.model_request.messages.pop();
            }
            2 => {
                let message = &mut bad.model_request.messages[bad.input_message_count + 1];
                message.content = vec![ContentBlock::ToolResult {
                    result: result("foreign"),
                }];
            }
            _ => {
                bad.model_request.messages[bad.input_message_count].role = MessageRole::User;
            }
        }
        assert!(bad.validate(&bad.scope).is_err(), "mutation {index}");
    }
}
