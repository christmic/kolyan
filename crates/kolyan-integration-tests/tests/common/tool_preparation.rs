//! Explicit trusted declarations for test adapters, never production inference.

use std::sync::Arc;

use kolyan_core::{ToolError, ToolInvocation};
use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PolicyEngine, PreparedCall,
    ResourceClaim, ToolManifest, ToolRequirements,
};
use kolyan_runtime::ExecutionKey;

pub fn key(turn_id: &str) -> ExecutionKey {
    ExecutionKey {
        session_id: "integration-fixture-session".into(),
        turn_id: turn_id.into(),
        execution_id: format!("fixture-{turn_id}"),
    }
}

/// Trusted fixture confirmation binds saved preparation and independently
/// admitted coordinates. It is not a production authority issuer.
pub fn approval_confirmation(
    suspension: &kolyan_core::TurnSuspension,
) -> (kolyan_core::ResumeInput, kolyan_policy::ToolExecutionScope) {
    assert!(suspension.waiting.external_waits.is_empty());
    let requested = suspension
        .waiting
        .approvals
        .first()
        .expect("approval suspension");
    let saved = suspension
        .checkpoint
        .approvals
        .iter()
        .find(|saved| saved.approval_id == requested.approval_id)
        .expect("approval must bind checkpoint authority");
    let scope = kolyan_policy::ToolExecutionScope {
        execution: key(&requested.turn_id),
        step_id: suspension.checkpoint.steps.last().unwrap().step_id.clone(),
        agent_snapshot_digest: None,
    };
    (
        kolyan_core::ResumeInput::ApprovalConfirmed(kolyan_core::ApprovalConfirmation {
            approval_id: requested.approval_id.clone(),
            prepared_digest: saved.prepared.digest().into(),
            policy_revision: saved.policy_revision.clone(),
            scope: scope.clone(),
            evidence_id: format!("fixture-confirmed-{}", requested.approval_id),
        }),
        scope,
    )
}

pub fn declaration(name: &str) -> Result<ToolManifest, ToolError> {
    let (capability, effect, idempotency) = match name {
        "file.read" => (
            Capability::FilesystemRead,
            Effect::Read,
            Idempotency::Idempotent,
        ),
        "file.write" => (
            Capability::FilesystemWrite,
            Effect::Update,
            Idempotency::NonIdempotent,
        ),
        "effect.write" => (
            Capability::FilesystemWrite,
            Effect::Create,
            Idempotency::NonIdempotent,
        ),
        "shell.query" | "fixture.lookup" | "test.tool" => (
            Capability::ProcessInspect,
            Effect::Read,
            Idempotency::Idempotent,
        ),
        _ => return Err(ToolError::Unavailable { name: name.into() }),
    };
    Ok(ToolManifest {
        tool_name: name.into(),
        capabilities: [capability].into(),
        effects: if name == "file.write" {
            [Effect::Create, Effect::Update].into()
        } else {
            [effect].into()
        },
        path_scopes: Vec::new(),
        idempotency,
        approval: ApprovalMode::Never,
    })
}

pub fn policy() -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    for name in [
        "file.read",
        "file.write",
        "effect.write",
        "shell.query",
        "fixture.lookup",
        "test.tool",
    ] {
        policy.register(declaration(name).unwrap());
    }
    Arc::new(policy)
}

pub fn prepare(call: ToolCall) -> Result<PreparedCall, ToolError> {
    let resource = match call.arguments.get("path") {
        Some(serde_json::Value::String(path)) => Some(path.clone()),
        None => None,
        _ => {
            return Err(ToolError::Failed {
                message: "fixture path must be a string".into(),
            });
        }
    };
    prepare_resource(call, resource)
}

/// Actual fixture effects may target a host-owned path absent from model arguments.
pub fn prepare_resource(call: ToolCall, path: Option<String>) -> Result<PreparedCall, ToolError> {
    let declared = declaration(&call.name)?;
    let claim = InvocationClaim {
        tool_name: declared.tool_name,
        capabilities: declared.capabilities,
        effects: declared.effects,
        resource: ResourceClaim { path },
        idempotency: declared.idempotency,
    };
    PreparedCall::new(
        call,
        "integration-fixture-v1".into(),
        claim,
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 1024 * 1024,
            timeout_ms: 30000,
        },
    )
    .map_err(|error| ToolError::Failed {
        message: error.to_string(),
    })
}

pub fn validate(invocation: &ToolInvocation) -> Result<ToolCall, ToolError> {
    invocation
        .grant
        .validate(
            &invocation.prepared,
            &invocation.policy_revision,
            &invocation.scope,
        )
        .map_err(|error| ToolError::PolicyDenied {
            message: error.to_string(),
        })?;
    Ok(invocation.prepared.call().clone())
}
