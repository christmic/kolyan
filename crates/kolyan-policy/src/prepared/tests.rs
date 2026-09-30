use super::*;
use crate::{
    ApprovalMode, Capability, Effect, Idempotency, PolicyContext, PolicyEngine, ResourceClaim,
    ToolManifest,
};
use serde_json::json;

fn prepared(revision: &str, text: &str) -> PreparedCall {
    PreparedCall::new(
        ToolCall {
            id: "call-1".into(),
            name: "file.edit".into(),
            arguments: json!({"path":"work/a.txt", "old_text":text, "new_text":"b"}),
        },
        revision.into(),
        InvocationClaim {
            tool_name: "file.edit".into(),
            capabilities: [Capability::FilesystemWrite].into_iter().collect(),
            effects: [Effect::Update].into_iter().collect(),
            resource: ResourceClaim {
                path: Some("work/a.txt".into()),
            },
            idempotency: Idempotency::NonIdempotent,
        },
        ToolRequirements {
            process_sandbox: true,
            max_output_bytes: 4096,
            timeout_ms: 1000,
        },
    )
    .unwrap()
}

fn policy(approval: ApprovalMode) -> PolicyEngine {
    let mut engine = PolicyEngine::default();
    engine.register(ToolManifest {
        tool_name: "file.edit".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Update].into_iter().collect(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval,
    });
    engine
}

#[test]
fn adapter_claims_not_tool_name_heuristics_drive_policy() {
    let call = prepared("edit-v1", "a");
    let engine = policy(ApprovalMode::Never);
    assert_eq!(
        engine
            .decide_prepared(&call, &PolicyContext::default())
            .kind,
        PolicyDecisionKind::Allow
    );
    // The pre-existing heuristic has no edit semantics. The new port must not
    // accidentally delegate its decision to that heuristic.
    assert_eq!(engine.decide(call.call()).kind, PolicyDecisionKind::Deny);
}

#[test]
fn exact_binding_rejects_changed_arguments_revision_and_policy() {
    let call = prepared("edit-v1", "a");
    let decision = policy(ApprovalMode::Never).decide_prepared(&call, &PolicyContext::default());
    let grant = PreparedGrant::issue(&call, decision, ApprovalEvidence::NotConfirmed).unwrap();
    grant.validate(&call, "v1").unwrap();
    assert_eq!(
        grant.validate(&prepared("edit-v1", "c"), "v1"),
        Err(PreparedError::BindingMismatch)
    );
    assert_eq!(
        grant.validate(&prepared("edit-v2", "a"), "v1"),
        Err(PreparedError::BindingMismatch)
    );
    assert_eq!(
        grant.validate(&call, "v2"),
        Err(PreparedError::BindingMismatch)
    );
    assert_eq!(grant.constraints().max_output_bytes, Some(4096));
    assert_eq!(grant.constraints().timeout_ms, Some(1000));
}

#[test]
fn persisted_input_tampering_is_denied_before_grant() {
    let call = prepared("edit-v1", "a");
    let mut value = serde_json::to_value(call).unwrap();
    value["call"]["arguments"]["new_text"] = json!("tampered");
    let changed: PreparedCall = serde_json::from_value(value).unwrap();
    assert!(changed.validate().is_err());
    let decision = policy(ApprovalMode::Never).decide_prepared(&changed, &PolicyContext::default());
    assert_eq!(decision.kind, PolicyDecisionKind::Deny);
    assert!(PreparedGrant::issue(&changed, decision, ApprovalEvidence::NotConfirmed).is_err());
}

#[test]
fn approval_never_overrides_a_denial_and_limits_cannot_expand() {
    let call = prepared("edit-v1", "a");
    let decision = policy(ApprovalMode::Always).decide_prepared(&call, &PolicyContext::default());
    assert!(PreparedGrant::issue(&call, decision.clone(), ApprovalEvidence::NotConfirmed).is_err());
    let approval = ApprovalEvidence::Confirmed {
        prepared_digest: call.digest().into(),
        policy_revision: decision.policy_version.clone(),
        evidence_id: "approval-fact-1".into(),
    };
    let grant = PreparedGrant::issue(&call, decision, approval.clone()).unwrap();
    grant.validate(&call, "v1").unwrap();
    assert!(PreparedGrant::issue(&call, PolicyDecision::denied("ceiling"), approval).is_err());
    let mut value = serde_json::to_value(grant).unwrap();
    value["constraints"]["timeout_ms"] = json!(2000);
    let changed: PreparedGrant = serde_json::from_value(value).unwrap();
    assert_eq!(
        changed.validate(&call, "v1"),
        Err(PreparedError::BindingMismatch)
    );
}

#[test]
fn invalid_claim_identity_budget_and_workspace_fail_closed() {
    let call = prepared("edit-v1", "a");
    let engine = policy(ApprovalMode::Never);
    let context = PolicyContext {
        remaining_tool_calls: Some(0),
        ..Default::default()
    };
    assert_eq!(
        engine.decide_prepared(&call, &context).kind,
        PolicyDecisionKind::Deny
    );
    let context = PolicyContext {
        workspace: Some("other".into()),
        ..Default::default()
    };
    assert_eq!(
        engine.decide_prepared(&call, &context).kind,
        PolicyDecisionKind::Deny
    );
    let mut claim = call.claim.clone();
    claim.tool_name = "file.write".into();
    assert!(
        PreparedCall::new(
            call.call.clone(),
            "v1".into(),
            claim,
            call.requirements.clone()
        )
        .is_err()
    );
}
