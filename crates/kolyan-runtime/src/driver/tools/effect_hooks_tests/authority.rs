//! Independent, explicitly trusted semantics for the scripted effect fixture.

use super::*;
use kolyan_policy::{
    ApprovalEvidence, ApprovalMode, Capability, Effect, Idempotency, InvocationClaim,
    PolicyContext, PolicyEngine, PreparedCall, PreparedGrant, ResourceClaim, ToolExecutionScope,
    ToolManifest, ToolRequirements,
};

fn manifest() -> ToolManifest {
    ToolManifest {
        tool_name: "scripted.write".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    }
}

pub(super) fn prepare_call(call: ToolCall) -> Result<PreparedCall, ToolError> {
    let declared = manifest();
    if call.name != declared.tool_name {
        return Err(ToolError::Unavailable { name: call.name });
    }
    PreparedCall::new(
        call,
        "effect-hook-scripted-v1".into(),
        InvocationClaim {
            tool_name: declared.tool_name,
            capabilities: declared.capabilities,
            effects: declared.effects,
            resource: ResourceClaim { path: None },
            idempotency: declared.idempotency,
        },
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 65536,
            timeout_ms: 30000,
        },
    )
    .map_err(|error| ToolError::InvalidBatch {
        message: error.to_string(),
    })
}

pub(super) fn invocation_for(call: ToolCall) -> ToolInvocation {
    let prepared = prepare_call(call).unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(manifest());
    let decision = policy.decide_prepared(&prepared, &PolicyContext::default());
    let revision = decision.policy_version.clone();
    let scope = ToolExecutionScope {
        execution: key(),
        step_id: "step".into(),
        agent_snapshot_digest: None,
    };
    let grant = PreparedGrant::issue(
        &prepared,
        decision,
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    ToolInvocation {
        prepared,
        grant,
        scope,
        policy_revision: revision,
        control: kolyan_core::TurnControl::default(),
        window: ToolExecutionWindow::at_deadline(Instant::now() + Duration::from_secs(30)),
    }
}
