use super::*;

use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalEvidence, Capability, Effect, ExecutionConstraints, Idempotency, InvocationClaim,
    PolicyDecision, PolicyDecisionKind, PreparedCall, PreparedGrant, ResourceClaim,
    ToolRequirements,
};
use kolyan_types::ExecutionKey;
use serde_json::json;

fn context() -> ExternalWaitContext {
    let scope = ToolExecutionScope {
        execution: ExecutionKey {
            session_id: "session".into(),
            turn_id: "turn".into(),
            execution_id: "execution".into(),
        },
        step_id: "step".into(),
        agent_snapshot_digest: Some("a".repeat(64)),
    };
    let prepared = PreparedCall::new(
        ToolCall {
            id: "model/opaque-call".into(),
            name: "fixture.read".into(),
            arguments: json!({}),
        },
        "fixture-v1".into(),
        InvocationClaim {
            tool_name: "fixture.read".into(),
            capabilities: [Capability::FilesystemRead].into(),
            effects: [Effect::Read].into(),
            resource: ResourceClaim { path: None },
            idempotency: Idempotency::Idempotent,
        },
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 1024,
            timeout_ms: 1000,
        },
    )
    .unwrap();
    let grant = PreparedGrant::issue(
        &prepared,
        PolicyDecision {
            kind: PolicyDecisionKind::Allow,
            reason: "trusted test authority".into(),
            policy_version: "revision".into(),
            constraints: ExecutionConstraints {
                max_output_bytes: Some(1024),
                timeout_ms: Some(1000),
            },
        },
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    ExternalWaitContext {
        issued: IssuedToolAuthority {
            prepared,
            grant,
            scope,
            policy_revision: "revision".into(),
        },
        wait: ExternalWait {
            wait_id: "host-wait".into(),
            kind: "fixture.wait".into(),
            schema_version: 1,
            binding: json!({"admission":"must-be-verified-by-host"}),
        },
    }
}

#[tokio::test]
async fn default_host_refuses_wait_and_result_even_with_valid_tool_authority() {
    let context = context();
    context.validate(&context.issued.scope).unwrap();
    let verifier: &dyn ExternalWaitVerifier = &RefuseExternalWaits;
    let expected = not_configured();
    assert_eq!(
        verifier.verify_wait(context.clone()).await,
        Err(expected.clone())
    );
    assert_eq!(
        verifier
            .verify_result(
                context.clone(),
                ToolResult {
                    call_id: context.issued.prepared.call().id.clone(),
                    content: "unverified external output".into(),
                    is_error: false,
                },
            )
            .await,
        Err(expected)
    );
}

#[test]
fn framing_does_not_replace_independent_scope_or_durable_admission() {
    let original = context();
    original.validate(&original.issued.scope).unwrap();
    let mut scope = original.issued.scope.clone();
    scope.execution.execution_id = "foreign".into();
    assert!(original.validate(&scope).is_err());
    let mut scope = original.issued.scope.clone();
    scope.step_id = "foreign-step".into();
    assert!(original.validate(&scope).is_err());
    let mut changed = original.clone();
    changed.issued.scope.agent_snapshot_digest = Some("b".repeat(64));
    assert!(changed.validate(&original.issued.scope).is_err());
    let mut changed = original.clone();
    changed.wait.schema_version = 0;
    assert!(changed.validate(&original.issued.scope).is_err());
    let mut changed = original.clone();
    changed.wait.binding = json!({"oversized":"x".repeat(65 * 1024)});
    assert!(changed.validate(&original.issued.scope).is_err());
}

#[test]
fn resolved_output_keeps_the_original_call_and_full_envelope_ceiling() {
    let context = context();
    let valid = ToolResult {
        call_id: context.issued.prepared.call().id.clone(),
        content: "verified separately by the host".into(),
        is_error: false,
    };
    context
        .validate_result(&context.issued.scope, &valid)
        .unwrap();
    let mut foreign = valid.clone();
    foreign.call_id = "foreign".into();
    assert!(
        context
            .validate_result(&context.issued.scope, &foreign)
            .is_err()
    );
    let mut oversized = valid;
    oversized.content = "x".repeat(1024);
    assert!(
        context
            .validate_result(&context.issued.scope, &oversized)
            .is_err()
    );
}
