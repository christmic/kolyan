//! Explicit synthetic write semantics; journal encoding uses production SSOT.

use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalEvidence, ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PathScope,
    PolicyContext, PolicyEngine, PreparedCall, PreparedGrant, ResourceClaim, ToolExecutionScope,
    ToolManifest, ToolRequirements,
};
use serde_json::json;

use crate::ExecutionKey;
use crate::reconciliation::receipt::PreparedEvidence;

pub(super) fn fixture_execution() -> ExecutionKey {
    ExecutionKey {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    }
}

pub(super) fn fixture_evidence(execution: &ExecutionKey) -> PreparedEvidence {
    let manifest = ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        path_scopes: vec![PathScope::new("safe")],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    };
    let call = ToolCall {
        id: "call-1".into(),
        name: manifest.tool_name.clone(),
        arguments: json!({"path":"safe/a","content":"done"}),
    };
    let prepared = PreparedCall::new(
        call,
        "1".into(),
        InvocationClaim {
            tool_name: manifest.tool_name.clone(),
            capabilities: manifest.capabilities.clone(),
            effects: manifest.effects.clone(),
            resource: ResourceClaim {
                path: Some("safe/a".into()),
            },
            idempotency: manifest.idempotency,
        },
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 1024 * 1024,
            timeout_ms: 30_000,
        },
    )
    .unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(manifest);
    let decision = policy.decide_prepared(&prepared, &PolicyContext::default());
    let revision = decision.policy_version.clone();
    let scope = ToolExecutionScope {
        execution: execution.clone(),
        step_id: "step-1".into(),
        agent_snapshot_digest: None,
    };
    let grant = PreparedGrant::issue(
        &prepared,
        decision,
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    PreparedEvidence::new(prepared, grant, scope, &revision, execution).unwrap()
}
