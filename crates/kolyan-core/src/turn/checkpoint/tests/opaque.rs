//! Model IDs and policy revisions retain the existing opaque policy contracts.
//! They are payload identities, not physical ledger coordinates.

use super::*;

fn replace_pending_identity(checkpoint: &mut TurnCheckpoint, id: &str, revision: &str) {
    let original = prepared(id);
    let mut requirements = original.requirements().clone();
    // The complete result includes the opaque call ID; it still needs an actual
    // grant large enough to carry that envelope, not a checkpoint exemption.
    requirements.max_output_bytes = 4096;
    let prepared = PreparedCall::new(
        original.call().clone(),
        original.tool_revision().into(),
        original.claim().clone(),
        requirements,
    )
    .unwrap();
    let grant = PreparedGrant::issue(
        &prepared,
        PolicyDecision {
            kind: PolicyDecisionKind::Allow,
            reason: "trusted opaque-identity fixture".into(),
            policy_version: revision.into(),
            constraints: ExecutionConstraints {
                max_output_bytes: Some(4096),
                timeout_ms: Some(1000),
            },
        },
        ApprovalEvidence::NotConfirmed,
        scope(),
    )
    .unwrap();
    checkpoint.calls[0] = CheckpointCall {
        call: prepared.call().clone(),
        prepared: Some(prepared.clone()),
        charged: true,
        state: CheckpointCallState::AwaitingExternal {
            // Host wait identity remains bounded ASCII, independent of call ID.
            wait: wait("host-a"),
            issued: IssuedToolAuthority {
                prepared,
                grant,
                scope: scope(),
                policy_revision: revision.into(),
            },
        },
    };
    checkpoint.steps[0].response.content[0] = ContentBlock::ToolCall {
        call: checkpoint.calls[0].call.clone(),
    };
    checkpoint.stages[0][0] = id.into();
}

#[test]
fn unicode_slash_and_long_opaque_call_ids_roundtrip_and_merge() {
    let ids = [
        "调用/原生 id".to_owned(),
        "opaque/tool/path".to_owned(),
        format!("调用/{}", "x".repeat(1024 - "调用/".len())),
    ];
    let revision = format!("策略/版本 {}", "r".repeat(2048));
    for id in ids {
        let mut checkpoint = fixture(false);
        replace_pending_identity(&mut checkpoint, &id, &revision);
        let bytes = serde_json::to_vec(&checkpoint).unwrap();
        let restored = TurnCheckpoint::from_json(&bytes, &scope()).unwrap();
        assert_eq!(restored, checkpoint);
        let merged = restored
            .merge_external(
                &[ExternalResolution {
                    call_id: id.clone(),
                    wait: wait("host-a"),
                    result: result(&id),
                }],
                &scope(),
            )
            .unwrap();
        let CheckpointCallState::Completed {
            result,
            issued: Some(authority),
        } = &merged.calls[0].state
        else {
            panic!("expected complete authorized result")
        };
        assert_eq!(result.call_id, id);
        assert_eq!(authority.policy_revision, revision);
        assert_eq!(merged.calls[0].call.id, id);
        assert_eq!(merged.budget, checkpoint.budget);
    }
}

#[test]
fn approval_revision_is_opaque_and_roundtrips_with_external_merge() {
    let mut checkpoint = fixture(false);
    let revision = format!("策略/审批/{}", "x".repeat(2048));
    checkpoint.approvals.push(CheckpointApproval {
        approval_id: "host-approval-b".into(),
        reason: "approve opaque revision".into(),
        prepared: prepared("b"),
        scope: scope(),
        policy_revision: revision.clone(),
        evidence_id: Some("host-evidence-b".into()),
        expires_at_ms: None,
    });
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let restored = TurnCheckpoint::from_json(&bytes, &scope()).unwrap();
    let merged = restored
        .merge_external(&[resolution("a")], &scope())
        .unwrap();
    assert_eq!(merged.approvals[0].policy_revision, revision);
    assert_eq!(merged.approvals, checkpoint.approvals);
}

#[test]
fn opaque_identity_does_not_bypass_output_limits_or_host_coordinate_constraints() {
    let id = format!("调用/{}", "x".repeat(900));
    let mut checkpoint = fixture(false);
    replace_pending_identity(&mut checkpoint, &id, "策略/版本");
    let mut large_result = result(&id);
    large_result.content = "x".repeat(4096);
    assert!(matches!(
        checkpoint.merge_external(
            &[ExternalResolution {
                call_id: id.clone(),
                wait: wait("host-a"),
                result: large_result
            }],
            &scope()
        ),
        Err(CheckpointError::OutputLimit)
    ));
    let CheckpointCallState::AwaitingExternal { wait, .. } = &mut checkpoint.calls[0].state else {
        unreachable!()
    };
    wait.wait_id = "foreign/host-id".into();
    assert!(checkpoint.validate(&scope()).is_err());
}

#[test]
fn preparation_error_feedback_still_obeys_original_tool_identity_contract() {
    for id in [" \t ".to_owned(), "x".repeat(1025)] {
        let mut checkpoint = fixture(false);
        checkpoint.calls[0].call.id = id.clone();
        checkpoint.calls[0].prepared = None;
        checkpoint.calls[0].state = CheckpointCallState::Completed {
            result: ToolResult {
                call_id: id.clone(),
                content: "preparation rejected".into(),
                is_error: true,
            },
            issued: None,
        };
        checkpoint.steps[0].response.content[0] = ContentBlock::ToolCall {
            call: checkpoint.calls[0].call.clone(),
        };
        checkpoint.stages[0][0] = id;
        assert!(checkpoint.validate(&scope()).is_err());
    }
}
