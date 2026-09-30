use super::*;
use crate::{
    ApprovalMode, Capability, Effect, Idempotency, PolicyContext, PolicyEngine, ResourceClaim,
    ToolManifest,
};
use serde_json::json;

fn scope() -> ToolExecutionScope {
    ToolExecutionScope {
        execution: ExecutionKey {
            session_id: "session-1".into(),
            execution_id: "execution-1".into(),
            turn_id: "turn-1".into(),
        },
        step_id: "step-1".into(),
        agent_snapshot_digest: Some("a".repeat(64)),
    }
}

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
    let grant =
        PreparedGrant::issue(&call, decision, ApprovalEvidence::NotConfirmed, scope()).unwrap();
    grant.validate(&call, &revision, &scope()).unwrap();
    assert_eq!(
        grant.validate(&prepared("edit-v1", "c"), &revision, &scope()),
        Err(PreparedError::BindingMismatch)
    );
    assert_eq!(
        grant.validate(&prepared("edit-v2", "a"), &revision, &scope()),
        Err(PreparedError::BindingMismatch)
    );
    assert_eq!(
        grant.validate(&call, "v2", &scope()),
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
    assert!(
        PreparedGrant::issue(&changed, decision, ApprovalEvidence::NotConfirmed, scope()).is_err()
    );
}

#[test]
fn approval_never_overrides_a_denial_and_limits_cannot_expand() {
    let call = prepared("edit-v1", "a");
    let decision = policy(ApprovalMode::Always).decide_prepared(&call, &PolicyContext::default());
    let revision = decision.policy_version.clone();
    assert!(
        PreparedGrant::issue(
            &call,
            decision.clone(),
            ApprovalEvidence::NotConfirmed,
            scope()
        )
        .is_err()
    );
    let approval = ApprovalEvidence::Confirmed {
        scope: scope(),
        prepared_digest: call.digest().into(),
        policy_revision: decision.policy_version.clone(),
        evidence_id: "approval-fact-1".into(),
    };
    let grant = PreparedGrant::issue(&call, decision, approval.clone(), scope()).unwrap();
    grant.validate(&call, &revision, &scope()).unwrap();
    assert!(
        PreparedGrant::issue(&call, PolicyDecision::denied("ceiling"), approval, scope()).is_err()
    );
    let mut value = serde_json::to_value(grant).unwrap();
    value["constraints"]["timeout_ms"] = json!(2000);
    let changed: PreparedGrant = serde_json::from_value(value).unwrap();
    assert_eq!(
        changed.validate(&call, &revision, &scope()),
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

fn different_scopes() -> Vec<ToolExecutionScope> {
    let mut scopes = Vec::new();
    for field in 0..6 {
        let mut changed = scope();
        match field {
            0 => changed.execution.session_id = "another-session".into(),
            1 => changed.execution.execution_id = "another-execution".into(),
            2 => changed.execution.turn_id = "another-turn".into(),
            3 => changed.step_id = "another-step".into(),
            4 => changed.agent_snapshot_digest = Some("b".repeat(64)),
            _ => changed.agent_snapshot_digest = None,
        }
        scopes.push(changed);
    }
    scopes
}

#[test]
fn same_call_id_and_input_cannot_reuse_a_grant_across_execution_scopes() {
    let call = prepared("edit-v1", "a");
    let decision = policy(ApprovalMode::Never).decide_prepared(&call, &PolicyContext::default());
    let revision = decision.policy_version.clone();
    let grant = PreparedGrant::issue(
        &call,
        decision.clone(),
        ApprovalEvidence::NotConfirmed,
        scope(),
    )
    .unwrap();
    assert_eq!(grant.scope(), &scope());
    grant.validate(&call, &revision, &scope()).unwrap();
    for other in different_scopes() {
        // The model may reuse the same ID and arguments; execution identity must not.
        assert_eq!(
            grant.validate(&call, &revision, &other),
            Err(PreparedError::BindingMismatch)
        );
        let fresh = PreparedGrant::issue(
            &call,
            decision.clone(),
            ApprovalEvidence::NotConfirmed,
            other.clone(),
        )
        .unwrap();
        fresh.validate(&call, &revision, &other).unwrap();
        assert_eq!(
            fresh.validate(&call, &revision, &scope()),
            Err(PreparedError::BindingMismatch)
        );
    }
}

#[test]
fn approval_evidence_is_exactly_scoped_even_when_call_digest_is_unchanged() {
    let call = prepared("edit-v1", "a");
    let decision = policy(ApprovalMode::Always).decide_prepared(&call, &PolicyContext::default());
    for foreign in different_scopes() {
        let approval = ApprovalEvidence::Confirmed {
            scope: foreign,
            prepared_digest: call.digest().into(),
            policy_revision: decision.policy_version.clone(),
            evidence_id: "durable-approval".into(),
        };
        assert_eq!(
            PreparedGrant::issue(&call, decision.clone(), approval, scope()),
            Err(PreparedError::BindingMismatch)
        );
    }
}

#[test]
fn malformed_and_empty_coordinates_and_snapshot_digests_are_rejected() {
    let call = prepared("edit-v1", "a");
    let decision = policy(ApprovalMode::Never).decide_prepared(&call, &PolicyContext::default());
    for field in 0..4 {
        for invalid in [
            "".into(),
            " ".into(),
            "id\n".into(),
            "非ASCII".into(),
            "x".repeat(257),
        ] {
            let mut invalid_scope = scope();
            match field {
                0 => invalid_scope.execution.session_id = invalid,
                1 => invalid_scope.execution.execution_id = invalid,
                2 => invalid_scope.execution.turn_id = invalid,
                _ => invalid_scope.step_id = invalid,
            }
            assert!(matches!(
                invalid_scope.validate(),
                Err(PreparedError::Invalid(_))
            ));
            assert!(matches!(
                PreparedGrant::issue(
                    &call,
                    decision.clone(),
                    ApprovalEvidence::NotConfirmed,
                    invalid_scope
                ),
                Err(PreparedError::Invalid(_))
            ));
        }
    }
    for invalid in [
        String::new(),
        "a".repeat(63),
        "a".repeat(65),
        "g".repeat(64),
    ] {
        let mut invalid_scope = scope();
        invalid_scope.agent_snapshot_digest = Some(invalid);
        assert!(matches!(
            invalid_scope.validate(),
            Err(PreparedError::Invalid(_))
        ));
    }
    let mut boundary = scope();
    boundary.execution.session_id = "s".repeat(256);
    boundary.execution.turn_id = "t".repeat(256);
    boundary.execution.execution_id = "e".repeat(256);
    boundary.step_id = "p".repeat(256);
    boundary.agent_snapshot_digest = Some("F".repeat(64));
    boundary.validate().unwrap();
    boundary.agent_snapshot_digest = None;
    boundary.validate().unwrap();
}

#[test]
fn persisted_grants_have_no_missing_scope_fallback_and_corruption_is_rejected() {
    let call = prepared("edit-v1", "a");
    let decision = policy(ApprovalMode::Never).decide_prepared(&call, &PolicyContext::default());
    let revision = decision.policy_version.clone();
    let grant =
        PreparedGrant::issue(&call, decision, ApprovalEvidence::NotConfirmed, scope()).unwrap();
    let encoded = serde_json::to_value(&grant).unwrap();
    let mut missing = encoded.clone();
    missing.as_object_mut().unwrap().remove("scope");
    assert!(serde_json::from_value::<PreparedGrant>(missing).is_err());
    for other in different_scopes() {
        let mut changed = encoded.clone();
        changed["scope"] = serde_json::to_value(other).unwrap();
        let restored: PreparedGrant = serde_json::from_value(changed).unwrap();
        assert_eq!(
            restored.validate(&call, &revision, &scope()),
            Err(PreparedError::BindingMismatch)
        );
    }
    let mut corrupt = encoded;
    corrupt["scope"]["execution"]["session_id"] = json!("");
    let restored: PreparedGrant = serde_json::from_value(corrupt).unwrap();
    assert!(matches!(
        restored.validate(&call, &revision, &scope()),
        Err(PreparedError::Invalid(_))
    ));
    let mut malformed_expected = scope();
    malformed_expected.step_id.clear();
    assert!(matches!(
        grant.validate(&call, &revision, &malformed_expected),
        Err(PreparedError::Invalid(_))
    ));
}
