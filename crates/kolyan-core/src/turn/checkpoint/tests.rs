use super::*;
use kolyan_model::{ModelRef, ModelResponse, StopReason, TokenUsage, ToolChoice};
use kolyan_policy::{
    ApprovalEvidence, Capability, Effect, ExecutionConstraints, Idempotency, InvocationClaim,
    PolicyDecision, PolicyDecisionKind, ResourceClaim, ToolRequirements,
};
use kolyan_types::ExecutionKey;
use serde_json::{Value, json};

mod history;
mod opaque;

fn scope() -> ToolExecutionScope {
    ToolExecutionScope {
        execution: ExecutionKey {
            session_id: "session".into(),
            execution_id: "execution".into(),
            turn_id: "turn".into(),
        },
        step_id: "turn-step-0".into(),
        agent_snapshot_digest: Some("a".repeat(64)),
    }
}

fn prepared(id: &str) -> PreparedCall {
    PreparedCall::new(
        ToolCall {
            id: id.into(),
            name: "file.read".into(),
            arguments: json!({"path":id}),
        },
        "adapter-v1".into(),
        InvocationClaim {
            tool_name: "file.read".into(),
            capabilities: [Capability::FilesystemRead].into_iter().collect(),
            effects: [Effect::Read].into_iter().collect(),
            resource: ResourceClaim {
                path: Some(id.into()),
            },
            idempotency: Idempotency::Idempotent,
        },
        ToolRequirements {
            process_sandbox: true,
            max_output_bytes: 256,
            timeout_ms: 1000,
        },
    )
    .unwrap()
}

fn issued(prepared: &PreparedCall) -> IssuedToolAuthority {
    let grant = PreparedGrant::issue(
        prepared,
        PolicyDecision {
            kind: PolicyDecisionKind::Allow,
            reason: "trusted fixture".into(),
            policy_version: "policy-v1".into(),
            constraints: ExecutionConstraints {
                max_output_bytes: Some(256),
                timeout_ms: Some(1000),
            },
        },
        ApprovalEvidence::NotConfirmed,
        scope(),
    )
    .unwrap();
    IssuedToolAuthority {
        prepared: prepared.clone(),
        grant,
        scope: scope(),
        policy_revision: "policy-v1".into(),
    }
}

fn wait(id: &str) -> ExternalWait {
    ExternalWait {
        wait_id: format!("wait-{id}"),
        kind: "child-v1".into(),
        schema_version: 1,
        binding: json!({"admission":id, "fact":"verified-host-reference"}),
    }
}

fn result(id: &str) -> ToolResult {
    ToolResult {
        call_id: id.into(),
        content: "complete".into(),
        is_error: false,
    }
}

fn fixture(parallel: bool) -> TurnCheckpoint {
    let calls: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|id| {
            let prepared = prepared(id);
            let pending = id == "a" || parallel && id == "b";
            CheckpointCall {
                call: prepared.call().clone(),
                prepared: Some(prepared.clone()),
                charged: pending,
                state: if pending {
                    CheckpointCallState::AwaitingExternal {
                        wait: wait(id),
                        issued: issued(&prepared),
                    }
                } else {
                    CheckpointCallState::Ready
                },
            }
        })
        .collect();
    let model = ModelRef::new("test", "model");
    TurnCheckpoint {
        schema_version: TURN_CHECKPOINT_SCHEMA,
        checkpoint_id: "checkpoint-1".into(),
        scope: scope(),
        model_request: ModelRequest {
            request_id: "turn-step-0".into(),
            model: model.clone(),
            system: vec![],
            messages: vec![],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: Value::Null,
        },
        input_message_count: 0,
        steps: vec![StepResult {
            step_id: "turn-step-0".into(),
            response: ModelResponse {
                id: "response".into(),
                model,
                content: calls
                    .iter()
                    .map(|item| ContentBlock::ToolCall {
                        call: item.call.clone(),
                    })
                    .collect(),
                structured_output: None,
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage {
                    input_tokens: Some(42),
                    output_tokens: Some(12),
                    ..Default::default()
                },
                metadata: Value::Null,
            },
            outcome: StepOutcome::ToolCalls,
        }],
        next_step_index: 1,
        stages: if parallel {
            vec![vec!["a".into(), "b".into()], vec!["c".into()]]
        } else {
            vec![vec!["a".into()], vec!["b".into()], vec!["c".into()]]
        },
        stage_index: 0,
        approvals: vec![],
        budget: CheckpointBudget {
            max_steps: 10,
            max_tool_calls: Some(10),
            deadline_at_ms: Some(9000),
            tool_timeout_ms: Some(1000),
            prior_tool_calls_used: 0,
            tool_calls_used: if parallel { 2 } else { 1 },
        },
        dispatch: ToolDispatchPolicy {
            mode: if parallel {
                super::super::ToolDispatchMode::Parallel
            } else {
                super::super::ToolDispatchMode::Serial
            },
            on_error: super::super::ToolErrorPolicy::FailTurn,
        },
        calls,
    }
}

fn resolution(id: &str) -> ExternalResolution {
    ExternalResolution {
        call_id: id.into(),
        wait: wait(id),
        result: result(id),
    }
}

#[test]
fn serial_merge_preserves_tail_order_budget_and_model_usage_without_execution_ports() {
    // No provider or executor exists in this fixture or in the pure API.
    let checkpoint = fixture(false);
    let merged = checkpoint
        .merge_external(&[resolution("a")], &scope())
        .unwrap();
    assert!(matches!(
        merged.calls[0].state,
        CheckpointCallState::Completed { .. }
    ));
    assert_eq!(merged.calls[1..], checkpoint.calls[1..]);
    assert_eq!(merged.budget, checkpoint.budget);
    assert_eq!(merged.steps, checkpoint.steps);
    assert_eq!(merged.model_request, checkpoint.model_request);
    assert_eq!(merged.stages, checkpoint.stages);
    assert!(matches!(
        checkpoint.calls[0].state,
        CheckpointCallState::AwaitingExternal { .. }
    ));
    assert!(merged.merge_external(&[resolution("a")], &scope()).is_err());
}

#[test]
fn parallel_out_of_order_partial_merge_does_not_start_next_stage() {
    let checkpoint = fixture(true);
    let partial = checkpoint
        .merge_external(&[resolution("b")], &scope())
        .unwrap();
    assert!(matches!(
        partial.calls[0].state,
        CheckpointCallState::AwaitingExternal { .. }
    ));
    let complete = partial
        .merge_external(&[resolution("a")], &scope())
        .unwrap();
    assert!(matches!(
        complete.calls[2].state,
        CheckpointCallState::Ready
    ));
    assert_eq!(complete.stage_index, 0);
    assert_eq!(complete.budget.tool_calls_used, 2);
    assert_eq!(complete.steps[0].response.usage.input_tokens, Some(42));
}

#[test]
fn full_serialized_envelope_limit_is_enforced_without_truncation() {
    let checkpoint = fixture(false);
    let mut resolution = resolution("a");
    resolution.result.content = "x".repeat(256);
    assert!(resolution.result.content.len() <= 256);
    assert!(matches!(
        checkpoint.merge_external(&[resolution], &scope()),
        Err(CheckpointError::OutputLimit)
    ));
    assert!(matches!(
        checkpoint.calls[0].state,
        CheckpointCallState::AwaitingExternal { .. }
    ));
}

#[test]
fn foreign_coordinates_and_snapshot_are_rejected() {
    for index in 0..5 {
        let mut foreign = scope();
        match index {
            0 => foreign.execution.session_id.push('x'),
            1 => foreign.execution.execution_id.push('x'),
            2 => foreign.execution.turn_id.push('x'),
            3 => foreign.step_id.push('x'),
            _ => foreign.agent_snapshot_digest = None,
        }
        assert!(matches!(
            fixture(false).merge_external(&[resolution("a")], &foreign),
            Err(CheckpointError::ForeignScope)
        ));
    }
}

#[test]
fn duplicate_unknown_ready_and_mismatched_resolutions_fail_atomically() {
    let checkpoint = fixture(true);
    for mut bad in [
        resolution("a"),
        resolution("a"),
        resolution("c"),
        resolution("foreign"),
    ] {
        if bad.call_id == "a" {
            bad.wait.binding = json!({"admission":"foreign"});
        }
        assert!(
            checkpoint
                .merge_external(&[resolution("b"), bad], &scope())
                .is_err()
        );
        assert!(matches!(
            checkpoint.calls[1].state,
            CheckpointCallState::AwaitingExternal { .. }
        ));
    }
    assert!(
        checkpoint
            .merge_external(&[resolution("a"), resolution("a")], &scope())
            .is_err()
    );
    let mut bad = resolution("a");
    bad.result.call_id = "b".into();
    assert!(checkpoint.merge_external(&[bad], &scope()).is_err());
}

#[test]
fn every_top_level_and_budget_field_is_required_and_unknown_fields_fail() {
    let value = serde_json::to_value(fixture(false)).unwrap();
    for key in value.as_object().unwrap().keys() {
        let mut bad = value.clone();
        bad.as_object_mut().unwrap().remove(key);
        assert!(
            serde_json::from_value::<TurnCheckpoint>(bad).is_err(),
            "missing {key}"
        );
    }
    for key in value["budget"].as_object().unwrap().keys() {
        let mut bad = value.clone();
        bad["budget"].as_object_mut().unwrap().remove(key);
        assert!(
            serde_json::from_value::<TurnCheckpoint>(bad).is_err(),
            "missing budget.{key}"
        );
    }
    for pointer in [
        "",
        "/budget",
        "/scope",
        "/scope/execution",
        "/calls/0",
        "/calls/0/state",
        "/calls/0/state/issued",
        "/calls/0/state/issued/grant",
        "/calls/0/state/wait",
    ] {
        let mut bad = value.clone();
        bad.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown_critical".into(), json!(true));
        assert!(
            serde_json::from_value::<TurnCheckpoint>(bad).is_err(),
            "unknown at {pointer}"
        );
    }
}

#[test]
fn nested_optional_authority_fields_must_be_explicit() {
    let value = serde_json::to_value(fixture(false)).unwrap();
    for (pointer, key) in [
        ("/scope", "agent_snapshot_digest"),
        ("/calls/0/state/issued/grant", "approval_evidence_id"),
        (
            "/calls/0/state/issued/grant/constraints",
            "max_output_bytes",
        ),
        ("/calls/0", "prepared"),
        ("/calls/0/state/wait", "binding"),
    ] {
        let mut bad = value.clone();
        bad.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(
            serde_json::from_value::<TurnCheckpoint>(bad).is_err(),
            "missing {pointer}/{key}"
        );
    }
}

#[test]
fn explicit_unlimited_nulls_roundtrip() {
    let mut checkpoint = fixture(false);
    checkpoint.budget.max_tool_calls = None;
    checkpoint.budget.deadline_at_ms = None;
    checkpoint.budget.tool_timeout_ms = None;
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let restored: TurnCheckpoint = serde_json::from_slice(&bytes).unwrap();
    restored.validate(&scope()).unwrap();
    assert_eq!(restored, checkpoint);
}

#[test]
fn corrupt_authority_order_stage_and_charges_are_rejected() {
    for index in 0..9 {
        let mut checkpoint = fixture(false);
        match index {
            0 => checkpoint.schema_version += 1,
            1 => checkpoint.calls.swap(0, 1),
            2 => checkpoint.stages[1][0] = "a".into(),
            3 => checkpoint.stage_index = 1,
            4 => checkpoint.calls[0].charged = false,
            5 => checkpoint.calls[1].charged = true,
            6 => checkpoint.budget.tool_calls_used += 1,
            7 => checkpoint.budget.max_tool_calls = Some(0),
            _ => {
                let CheckpointCallState::AwaitingExternal { issued, .. } =
                    &mut checkpoint.calls[0].state
                else {
                    unreachable!()
                };
                issued.policy_revision = "foreign".into();
            }
        }
        assert!(checkpoint.validate(&scope()).is_err(), "mutation {index}");
    }
}

#[test]
fn wait_schema_identity_and_size_limits_are_real() {
    for index in 0..5 {
        let mut wait = wait("a");
        match index {
            0 => wait.schema_version = 0,
            1 => wait.wait_id.clear(),
            2 => wait.kind = "x".repeat(257),
            3 => wait.binding = Value::Null,
            _ => {
                wait.binding =
                    json!({"large":"x".repeat(super::super::outcome::MAX_EXTERNAL_BINDING_BYTES)})
            }
        }
        assert!(wait.validate().is_err());
    }
}

#[test]
fn stale_approval_is_not_accepted_for_new_preparation() {
    let mut checkpoint = fixture(false);
    checkpoint.approvals.push(CheckpointApproval {
        approval_id: "approval-b".into(),
        prepared: prepared("b"),
        scope: scope(),
        policy_revision: "policy-v1".into(),
        evidence_id: Some("confirmed-b".into()),
        expires_at_ms: None,
    });
    checkpoint.validate(&scope()).unwrap();
    let changed = prepared("b")
        .with_execution_binding(json!({"inode":999}))
        .unwrap();
    checkpoint.calls[1].prepared = Some(changed);
    assert!(checkpoint.validate(&scope()).is_err());
}

#[test]
fn duplicate_approval_for_one_call_and_invalid_host_authority_ids_are_rejected() {
    let mut checkpoint = fixture(false);
    let approval = CheckpointApproval {
        approval_id: "approval-b".into(),
        prepared: prepared("b"),
        scope: scope(),
        policy_revision: "policy-v1".into(),
        evidence_id: Some("evidence-b".into()),
        expires_at_ms: None,
    };
    checkpoint.approvals.push(approval.clone());
    checkpoint.validate(&scope()).unwrap();
    let mut duplicate = approval;
    duplicate.approval_id = "different-approval-b".into();
    checkpoint.approvals.push(duplicate);
    assert!(checkpoint.validate(&scope()).is_err());
    checkpoint.approvals.pop();
    for index in 0..4 {
        let mut bad = checkpoint.clone();
        match index {
            0 => bad.checkpoint_id = "x".repeat(257),
            1 => bad.approvals[0].policy_revision = " \t ".into(),
            2 => bad.approvals[0].evidence_id = Some("x".repeat(257)),
            _ => {
                let CheckpointCallState::AwaitingExternal { issued, .. } = &mut bad.calls[0].state
                else {
                    unreachable!()
                };
                issued.policy_revision = "x".repeat(257);
            }
        }
        assert!(bad.validate(&scope()).is_err());
    }
}

#[test]
fn total_history_bound_fails_without_truncating_and_decode_is_bounded() {
    let mut checkpoint = fixture(false);
    checkpoint.model_request.extensions =
        json!({"provider_source":"x".repeat(MAX_TURN_CHECKPOINT_BYTES)});
    assert!(matches!(
        checkpoint.validate(&scope()),
        Err(CheckpointError::CheckpointLimit)
    ));
    assert_eq!(
        checkpoint.model_request.extensions["provider_source"]
            .as_str()
            .unwrap()
            .len(),
        MAX_TURN_CHECKPOINT_BYTES
    );
    let bytes = vec![b' '; MAX_TURN_CHECKPOINT_BYTES + 1];
    assert!(matches!(
        TurnCheckpoint::from_json(&bytes, &scope()),
        Err(CheckpointError::CheckpointLimit)
    ));
}

#[test]
fn provider_extensions_are_not_interpreted_as_critical_authority() {
    let mut checkpoint = fixture(false);
    checkpoint.model_request.extensions =
        json!({"vendor_extra":{"future-field":[1.25,null,true]}, "budget_hint":123});
    let encoded = serde_json::to_vec(&checkpoint).unwrap();
    let restored = TurnCheckpoint::from_json(&encoded, &scope()).unwrap();
    assert_eq!(
        restored.model_request.extensions,
        checkpoint.model_request.extensions
    );
}

#[test]
fn completed_authority_and_error_feedback_presence_remain_strict() {
    let merged = fixture(false)
        .merge_external(&[resolution("a")], &scope())
        .unwrap();
    let encoded = serde_json::to_value(&merged).unwrap();
    let mut missing = encoded.clone();
    missing["calls"][0]["state"]
        .as_object_mut()
        .unwrap()
        .remove("issued");
    assert!(serde_json::from_value::<TurnCheckpoint>(missing).is_err());
    let mut missing_authority = merged.clone();
    let CheckpointCallState::Completed { issued, .. } = &mut missing_authority.calls[0].state
    else {
        unreachable!()
    };
    *issued = None;
    assert!(missing_authority.validate(&scope()).is_err());
    let mut oversize = merged;
    let CheckpointCallState::Completed { result, .. } = &mut oversize.calls[0].state else {
        unreachable!()
    };
    result.content = "x".repeat(256);
    assert!(matches!(
        oversize.validate(&scope()),
        Err(CheckpointError::OutputLimit)
    ));
}

#[test]
fn historical_message_boundary_is_exact_not_saturating_inferred() {
    let mut checkpoint = fixture(false);
    checkpoint
        .model_request
        .messages
        .push(kolyan_model::Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: "original input".into(),
            }],
        });
    assert!(checkpoint.validate(&scope()).is_err());
    checkpoint.input_message_count = 1;
    checkpoint.validate(&scope()).unwrap();
    checkpoint.input_message_count = 2;
    assert!(checkpoint.validate(&scope()).is_err());
}
