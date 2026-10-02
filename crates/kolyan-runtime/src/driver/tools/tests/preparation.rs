//! Explicit write-fixture preparation and positively issued scoped authority.

use super::*;
use kolyan_core::{ToolInvocation, TurnControl};
use kolyan_policy::{
    ApprovalEvidence, ApprovalMode, Capability, Effect, Idempotency, InvocationClaim,
    PolicyContext, PolicyEngine, PreparedCall, PreparedGrant, ResourceClaim, ToolExecutionScope,
    ToolManifest, ToolRequirements,
};

pub(super) fn prepare_call(call: ToolCall) -> Result<PreparedCall, ToolError> {
    if call.name != "write" {
        return Err(ToolError::Unavailable { name: call.name });
    }
    let claim = InvocationClaim {
        tool_name: "write".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        resource: ResourceClaim { path: None },
        idempotency: Idempotency::NonIdempotent,
    };
    PreparedCall::new(
        call,
        "runtime-write-fixture-v1".into(),
        claim,
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 65536,
            timeout_ms: 30000,
        },
    )
    .map_err(|error| ToolError::Failed {
        message: error.to_string(),
    })
}

pub(super) fn invocation_for(call: ToolCall) -> ToolInvocation {
    invocation_in_scope(
        call,
        ToolExecutionScope {
            execution: key(),
            step_id: "step".into(),
            agent_snapshot_digest: None,
        },
    )
}

pub(super) fn invocation_in_scope(call: ToolCall, scope: ToolExecutionScope) -> ToolInvocation {
    let prepared = prepare_call(call).unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "write".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        path_scopes: Vec::new(),
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    let decision = policy.decide_prepared(&prepared, &PolicyContext::default());
    let policy_revision = decision.policy_version.clone();
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
        policy_revision,
        control: TurnControl::default(),
        window: kolyan_core::ToolExecutionWindow::at_deadline(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        ),
    }
}
