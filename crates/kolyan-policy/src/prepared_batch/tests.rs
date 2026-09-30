use super::*;
use crate::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PolicyDecisionKind,
    ResourceClaim, ToolManifest, ToolRequirements,
};
use kolyan_model::ToolCall;
use serde_json::json;

fn prepared(id: &str, name: &str, path: &str, write: bool) -> PreparedCall {
    PreparedCall::new(
        ToolCall {
            id: id.into(),
            name: name.into(),
            arguments: json!({"path":path}),
        },
        "trusted-test-adapter/1".into(),
        InvocationClaim {
            tool_name: name.into(),
            capabilities: [if write {
                Capability::FilesystemWrite
            } else {
                Capability::FilesystemRead
            }]
            .into_iter()
            .collect(),
            effects: [if write { Effect::Update } else { Effect::Read }]
                .into_iter()
                .collect(),
            resource: ResourceClaim {
                path: Some(path.into()),
            },
            idempotency: Idempotency::Idempotent,
        },
        ToolRequirements {
            process_sandbox: true,
            max_output_bytes: 4096,
            timeout_ms: 1000,
        },
    )
    .unwrap()
}

fn policy() -> PolicyEngine {
    let mut engine = PolicyEngine::default();
    for (name, capability, effect) in [
        ("file.read", Capability::FilesystemRead, Effect::Read),
        ("file.edit", Capability::FilesystemWrite, Effect::Update),
    ] {
        let mut manifest = ToolManifest::new(name);
        manifest.capabilities.insert(capability);
        manifest.effects.insert(effect);
        manifest.approval = ApprovalMode::Never;
        engine.register(manifest);
    }
    engine
}

#[test]
fn prepared_semantics_order_conflicting_edits_without_name_inference() {
    let plan = policy()
        .resolve_prepared_batch(
            &PolicyContext::default(),
            &[
                prepared("edit", "file.edit", "/workspace/a", true),
                prepared("same", "file.read", "/workspace/a", false),
                prepared("other", "file.read", "/workspace/b", false),
            ],
        )
        .unwrap();
    assert!(
        plan.decisions
            .iter()
            .all(|item| item.decision.kind == PolicyDecisionKind::Allow)
    );
    assert_eq!(
        plan.decisions[0].claim.capabilities,
        [Capability::FilesystemWrite].into_iter().collect()
    );
    let edit_stage = plan
        .stages
        .iter()
        .position(|stage| stage.iter().any(|id| id == "edit"))
        .unwrap();
    let read_stage = plan
        .stages
        .iter()
        .position(|stage| stage.iter().any(|id| id == "same"))
        .unwrap();
    assert!(edit_stage < read_stage);
}

#[test]
fn invocation_workspace_can_narrow_but_never_replace_host_scope() {
    let mut engine = policy();
    engine.restrict_workspace("/workspace/private");
    let wide = PolicyContext {
        workspace: Some("/workspace".into()),
        ..Default::default()
    };
    let outside = prepared("outside", "file.read", "/workspace/public/a", false);
    assert_eq!(
        engine.decide_prepared(&outside, &wide).kind,
        PolicyDecisionKind::Deny
    );
    let narrow = PolicyContext {
        workspace: Some("/workspace/private/a".into()),
        ..Default::default()
    };
    let sibling = prepared("sibling", "file.read", "/workspace/private/b", false);
    assert_eq!(
        engine.decide_prepared(&sibling, &narrow).kind,
        PolicyDecisionKind::Deny
    );
    let inside = prepared("inside", "file.read", "/workspace/private/a/file", false);
    assert_eq!(
        engine.decide_prepared(&inside, &narrow).kind,
        PolicyDecisionKind::Allow
    );
}

#[test]
fn duplicate_ids_and_tampered_preparation_fail_before_a_plan_is_issued() {
    let call = prepared("same", "file.read", "/workspace/a", false);
    assert!(
        policy()
            .resolve_prepared_batch(&PolicyContext::default(), &[call.clone(), call.clone()])
            .is_err()
    );
    let mut encoded = serde_json::to_value(call).unwrap();
    encoded["claim"]["resource"]["path"] = json!("/workspace/b");
    let tampered: PreparedCall = serde_json::from_value(encoded).unwrap();
    assert!(
        policy()
            .resolve_prepared_batch(&PolicyContext::default(), &[tampered])
            .is_err()
    );
}
