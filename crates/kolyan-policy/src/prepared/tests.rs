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
    let revision = decision.policy_version.clone();
    let grant = PreparedGrant::issue(&call, decision, ApprovalEvidence::NotConfirmed).unwrap();
    grant.validate(&call, &revision).unwrap();
    assert_eq!(
        grant.validate(&prepared("edit-v1", "c"), &revision),
        Err(PreparedError::BindingMismatch)
    );
    assert_eq!(
        grant.validate(&prepared("edit-v2", "a"), &revision),
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
    let revision = decision.policy_version.clone();
    assert!(PreparedGrant::issue(&call, decision.clone(), ApprovalEvidence::NotConfirmed).is_err());
    let approval = ApprovalEvidence::Confirmed {
        prepared_digest: call.digest().into(),
        policy_revision: decision.policy_version.clone(),
        evidence_id: "approval-fact-1".into(),
    };
    let grant = PreparedGrant::issue(&call, decision, approval.clone()).unwrap();
    grant.validate(&call, &revision).unwrap();
    assert!(PreparedGrant::issue(&call, PolicyDecision::denied("ceiling"), approval).is_err());
    let mut value = serde_json::to_value(grant).unwrap();
    value["constraints"]["timeout_ms"] = json!(2000);
    let changed: PreparedGrant = serde_json::from_value(value).unwrap();
    assert_eq!(
        changed.validate(&call, &revision),
        Err(PreparedError::BindingMismatch)
    );
}

#[test]
fn policy_revision_tracks_rules_not_insertion_order_or_dynamic_budget() {
    let mut engine = policy(ApprovalMode::Always);
    let initial = engine.revision();
    engine.register(policy(ApprovalMode::Always).manifests["file.edit"].clone());
    assert_eq!(engine.revision(), initial);
    let first = engine.manifests["file.edit"].clone();
    let mut second = first.clone();
    second.tool_name = "file.write".into();
    let mut left = PolicyEngine::default();
    left.register(first.clone());
    left.register(second.clone());
    let mut right = PolicyEngine::default();
    right.register(second);
    right.register(first);
    assert_eq!(left.revision(), right.revision());
    let call = prepared("edit-v1", "a");
    let original = engine.decide_prepared(&call, &PolicyContext::default());
    assert_eq!(original.policy_version, initial);
    let no_budget = PolicyContext {
        remaining_tool_calls: Some(0),
        ..Default::default()
    };
    assert_eq!(
        engine.decide_prepared(&call, &no_budget).policy_version,
        initial
    );
    engine.restrict_workspace("work");
    assert_ne!(engine.revision(), initial);
    let scoped = engine.revision();
    engine.deny_tool("file.edit");
    assert_ne!(engine.revision(), scoped);
    assert_eq!(
        engine
            .decide_prepared(&call, &PolicyContext::default())
            .policy_version,
        engine.revision()
    );
}

#[test]
fn canonical_preparation_sorts_nested_keys_but_preserves_array_order() {
    let original = prepared("edit-v1", "a");
    let build = |arguments| {
        let mut call = original.call.clone();
        call.arguments = arguments;
        PreparedCall::new(
            call,
            original.tool_revision.clone(),
            original.claim.clone(),
            original.requirements.clone(),
        )
        .unwrap()
    };
    let first = build(serde_json::from_str(r#"{"nested":{"b":2,"a":1},"ordered":[1,2]}"#).unwrap());
    let second =
        build(serde_json::from_str(r#"{"ordered":[1,2],"nested":{"a":1,"b":2}}"#).unwrap());
    assert_eq!(first.digest(), second.digest());
    let reversed = build(json!({"nested":{"a":1,"b":2},"ordered":[2,1]}));
    assert_ne!(first.digest(), reversed.digest());
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
