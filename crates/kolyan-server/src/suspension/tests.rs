use super::*;
use kolyan_core::{
    CheckpointApproval, CheckpointBudget, CheckpointCall, CheckpointCallState, StepOutcome,
    StepResult, TURN_CHECKPOINT_SCHEMA, ToolDispatchPolicy, TurnCheckpoint,
};
use kolyan_model::{ContentBlock, ModelRequest, ModelResponse, StopReason, TokenUsage, ToolCall};
use kolyan_policy::{
    Capability, Effect, Idempotency, InvocationClaim, PreparedCall, ResourceClaim, ToolRequirements,
};

struct ApprovalProvider(ModelResponse);
impl kolyan_model::ModelProvider for ApprovalProvider {
    fn stream(&self, _: ModelRequest) -> kolyan_model::ProviderFuture<'_> {
        let response = self.0.clone();
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(kolyan_model::ModelEvent::Started),
                Ok(kolyan_model::ModelEvent::Completed(response)),
            ])) as kolyan_model::ModelEventStream)
        })
    }
}

struct ApprovalTools(PreparedCall);
impl kolyan_core::ToolExecutor for ApprovalTools {
    fn prepare(&self, call: ToolCall) -> kolyan_core::ToolPreparationFuture<'_> {
        let prepared = self.0.clone();
        Box::pin(async move {
            assert_eq!(prepared.call(), &call);
            Ok(prepared)
        })
    }
    fn execute_invocation(&self, _: kolyan_core::ToolInvocation) -> kolyan_core::ToolFuture<'_> {
        panic!("pending approval must not execute a tool")
    }
}

/// Real Runtime publication, including immutable input admission and exact
/// checkpoint IDs. Does not synthesize facts accepted only by a weaker loader.
pub(crate) async fn persist_approval<L: LedgerStore + Clone + 'static>(
    ledger: L,
) -> TurnSuspension {
    use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
    use kolyan_policy::{ApprovalMode, PolicyEngine, ToolManifest};
    let fixture = fixture();
    let prepared = fixture.checkpoint.calls[0].prepared.clone().unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: prepared.call().name.clone(),
        capabilities: prepared.claim().capabilities.clone(),
        effects: prepared.claim().effects.clone(),
        path_scopes: vec![],
        idempotency: prepared.claim().idempotency,
        approval: ApprovalMode::Always,
    });
    let executor = TurnExecutor::with_tools(
        ApprovalProvider(fixture.checkpoint.steps[0].response.clone()),
        ApprovalTools(prepared),
    )
    .with_policy_engine(std::sync::Arc::new(policy));
    let result = kolyan_runtime::DurableTurnDriver::new(ledger, kolyan_trace::NoopTraceSink)
        .start(
            executor,
            TurnRequest {
                turn_id: "t".into(),
                model_request: fixture.checkpoint.model_request,
                config: TurnConfig {
                    max_steps: 3,
                    max_tool_calls: Some(3),
                    ..TurnConfig::default()
                },
            },
            "s",
            "e",
        )
        .await
        .unwrap();
    let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = result else {
        panic!("expected durable approval");
    };
    *suspension
}

pub(crate) fn fixture() -> TurnSuspension {
    let scope = serde_json::from_value(json!({
        "execution":{"session_id":"s","execution_id":"e","turn_id":"t"},
        "step_id":"t-step-0","agent_snapshot_digest":null
    }))
    .unwrap();
    let call = ToolCall {
        id: "c".into(),
        name: "file.write".into(),
        arguments: json!({"path":"safe/out","content":"text"}),
    };
    let prepared = PreparedCall::new(
        call.clone(),
        "adapter-v1".into(),
        InvocationClaim {
            tool_name: call.name.clone(),
            capabilities: [Capability::FilesystemWrite].into_iter().collect(),
            effects: [Effect::Create, Effect::Update].into_iter().collect(),
            resource: ResourceClaim {
                path: Some("safe/out".into()),
            },
            idempotency: Idempotency::NonIdempotent,
        },
        ToolRequirements {
            process_sandbox: true,
            max_output_bytes: 1024,
            timeout_ms: 1000,
        },
    )
    .unwrap();
    let request: ModelRequest = serde_json::from_value(json!({
        "request_id":"t-step-0","model":{"provider":"fixture","model":"m"},
        "system":[],"messages":[],"tools":[],"tool_choice":"auto", "output_format":null,
        "prompt_cache":null,"reasoning":null,"max_output_tokens":null,"extensions":{}
    }))
    .unwrap();
    let checkpoint = TurnCheckpoint {
        schema_version: TURN_CHECKPOINT_SCHEMA,
        checkpoint_id: "checkpoint".into(),
        scope,
        model_request: request.clone(),
        input_message_count: 0,
        steps: vec![StepResult {
            step_id: "t-step-0".into(),
            outcome: StepOutcome::ToolCalls,
            response: ModelResponse {
                id: "response".into(),
                model: request.model,
                content: vec![ContentBlock::ToolCall { call: call.clone() }],
                structured_output: None,
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
                metadata: Value::Null,
            },
        }],
        next_step_index: 1,
        calls: vec![CheckpointCall {
            call,
            prepared: Some(prepared.clone()),
            charged: false,
            state: CheckpointCallState::Ready,
        }],
        stages: vec![vec!["c".into()]],
        stage_index: 0,
        approvals: vec![CheckpointApproval {
            approval_id: "a".into(),
            prepared,
            scope: serde_json::from_value(json!({
            "execution":{"session_id":"s","execution_id":"e","turn_id":"t"},
            "step_id":"t-step-0","agent_snapshot_digest":null}))
            .unwrap(),
            policy_revision: "policy-v1".into(),
            reason: "requires approval".into(),
            evidence_id: None,
            expires_at_ms: None,
        }],
        budget: CheckpointBudget {
            max_steps: 3,
            max_tool_calls: Some(3),
            deadline_at_ms: None,
            tool_timeout_ms: Some(1000),
            prior_tool_calls_used: 0,
            tool_calls_used: 0,
        },
        dispatch: ToolDispatchPolicy::default(),
    };
    let scope = checkpoint.scope.clone();
    TurnSuspension::from_checkpoint(checkpoint, &scope).unwrap()
}

fn key() -> ExecutionRef {
    ExecutionRef {
        session_id: "s".into(),
        execution_id: "e".into(),
        turn_id: "t".into(),
    }
}

fn fact(kind: LedgerEventKind, payload: Value) -> LedgerEvent {
    LedgerEvent {
        event_id: "fact".into(),
        idempotency_key: "fact".into(),
        execution_id: "e".into(),
        turn_id: "t".into(),
        cursor: 1,
        kind,
        payload,
    }
}

#[test]
fn display_has_required_nullable_expiration_but_no_authority() {
    let suspension = fixture();
    let view = suspension_view(&suspension).unwrap();
    assert_eq!(view["pending_approvals"][0]["approval_id"], "a");
    assert_eq!(view["pending_approvals"][0]["arguments"]["content"], "text");
    assert!(
        view["pending_approvals"][0]
            .get("expires_at_ms")
            .unwrap()
            .is_null()
    );
    for private in [
        "prepared",
        "grant",
        "scope",
        "binding",
        "policy-v1",
        "adapter-v1",
    ] {
        assert!(!view.to_string().contains(private));
    }
}

#[test]
fn compound_checkpoint_rejects_foreign_corrupt_and_approval_only_formats() {
    let suspension = fixture();
    let saved = fact(
        LedgerEventKind::ExecutionSuspended,
        json!({"schema_version":1,"publication_cursor":0,"suspension":suspension}),
    );
    assert_eq!(
        current_suspension(&key(), std::slice::from_ref(&saved)).unwrap(),
        Some(suspension)
    );
    let foreign = ExecutionRef {
        session_id: "other".into(),
        ..key()
    };
    assert!(current_suspension(&foreign, std::slice::from_ref(&saved)).is_err());
    for payload in [
        json!({"approval_id":"a"}),
        json!({"checkpoint":{},"waiting":{}}),
    ] {
        assert!(
            current_suspension(
                &key(),
                &[fact(LedgerEventKind::ExecutionSuspended, payload)]
            )
            .is_err()
        );
    }
    for kind in [
        LedgerEventKind::ExecutionStarted,
        LedgerEventKind::TurnCheckpointPrepared,
        LedgerEventKind::ExecutionCancelled,
        LedgerEventKind::TurnFailed,
    ] {
        assert!(
            current_suspension(&key(), &[saved.clone(), fact(kind, Value::Null)])
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn forged_display_summary_is_not_a_checkpoint() {
    let mut suspension = fixture();
    suspension.waiting.approvals[0].reason = "forged".into();
    assert!(suspension_view(&suspension).is_err());
}

pub(crate) fn mixed_fixture() -> TurnSuspension {
    use kolyan_core::{ExternalWait, IssuedToolAuthority, ToolDispatchMode};
    use kolyan_policy::{
        ApprovalEvidence, ExecutionConstraints, PolicyDecision, PolicyDecisionKind, PreparedGrant,
    };
    let mut saved = fixture();
    let call = ToolCall {
        id: "child".into(),
        name: "agent.invoke".into(),
        arguments: json!({"input":"selected context"}),
    };
    let prepared = PreparedCall::new(
        call.clone(),
        "agent-host-v1".into(),
        InvocationClaim {
            tool_name: call.name.clone(),
            capabilities: [Capability::FilesystemRead].into_iter().collect(),
            effects: [Effect::Read].into_iter().collect(),
            resource: ResourceClaim {
                path: Some("other".into()),
            },
            idempotency: Idempotency::Idempotent,
        },
        ToolRequirements {
            process_sandbox: true,
            max_output_bytes: 1024,
            timeout_ms: 1000,
        },
    )
    .unwrap();
    let scope = saved.checkpoint.scope.clone();
    let grant = PreparedGrant::issue(
        &prepared,
        PolicyDecision {
            kind: PolicyDecisionKind::Allow,
            reason: "trusted fixture".into(),
            policy_version: "policy-v1".into(),
            constraints: ExecutionConstraints {
                max_output_bytes: Some(1024),
                timeout_ms: Some(1000),
            },
        },
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    saved.checkpoint.steps[0]
        .response
        .content
        .push(ContentBlock::ToolCall { call: call.clone() });
    saved.checkpoint.calls.push(CheckpointCall {
        call,
        prepared: Some(prepared.clone()),
        charged: true,
        state: CheckpointCallState::AwaitingExternal {
            wait: ExternalWait {
                wait_id: "wait-child".into(),
                kind: "agent-child".into(),
                schema_version: 1,
                binding: json!({"private_binding":"host proof coordinates"}),
            },
            issued: IssuedToolAuthority {
                prepared,
                grant,
                scope,
                policy_revision: "policy-v1".into(),
            },
        },
    });
    saved.checkpoint.stages[0].push("child".into());
    saved.checkpoint.budget.tool_calls_used = 1;
    saved.checkpoint.dispatch.mode = ToolDispatchMode::Parallel;
    let scope = saved.checkpoint.scope.clone();
    TurnSuspension::from_checkpoint(saved.checkpoint, &scope).unwrap()
}

#[test]
fn mixed_display_retains_both_wait_sets_and_hides_external_authority() {
    let saved = mixed_fixture();
    let view = suspension_view(&saved).unwrap();
    assert_eq!(view["pending_approvals"].as_array().unwrap().len(), 1);
    assert_eq!(view["external_waits"].as_array().unwrap().len(), 1);
    assert_eq!(view["external_waits"][0]["wait_id"], "wait-child");
    for private in [
        "private_binding",
        "prepared",
        "scope",
        "grant",
        "host proof coordinates",
    ] {
        assert!(!view.to_string().contains(private));
    }
    let waiting = task_waiting(&saved);
    assert_eq!(waiting.approval_ids, ["a"]);
    assert_eq!(waiting.external_wait_ids, ["wait-child"]);
}

#[test]
fn confirmation_requires_exact_affirmative_durable_evidence() {
    use kolyan_ledger::InMemoryLedger;
    let ledger = InMemoryLedger::default();
    let saved = fixture();
    let confirmation = confirm_approval(&ledger, &key(), &saved, "a").unwrap();
    let input = ResumeInput::ApprovalConfirmed(confirmation.clone());
    verify_resume_input(&ledger, &key(), &saved, &input).unwrap();
    assert_eq!(
        confirm_approval(&ledger, &key(), &saved, "a").unwrap(),
        confirmation
    );
    assert_eq!(ledger.execution_events_after("e", 0).unwrap().len(), 1);
    assert!(record_approval_decision(&ledger, &key(), &saved, "a", "deny").is_err());
    for field in ["evidence", "digest", "revision", "scope"] {
        let mut forged = confirmation.clone();
        match field {
            "evidence" => forged.evidence_id = "missing".into(),
            "digest" => forged.prepared_digest = "f".repeat(64),
            "revision" => forged.policy_revision = "foreign".into(),
            _ => forged.scope.execution.session_id = "foreign".into(),
        }
        assert!(
            verify_resume_input(
                &ledger,
                &key(),
                &saved,
                &ResumeInput::ApprovalConfirmed(forged)
            )
            .is_err()
        );
    }
    let denied = InMemoryLedger::default();
    let deny = record_approval_decision(&denied, &key(), &saved, "a", "deny").unwrap();
    assert!(
        verify_resume_input(
            &denied,
            &key(),
            &saved,
            &ResumeInput::ApprovalConfirmed(deny)
        )
        .is_err()
    );
}

#[test]
fn expiration_and_foreign_owner_block_decision_before_persistence() {
    use kolyan_ledger::InMemoryLedger;
    let ledger = InMemoryLedger::default();
    let mut saved = fixture();
    saved.checkpoint.approvals[0].expires_at_ms = Some(1);
    saved.waiting = saved
        .checkpoint
        .suspension_summary(&saved.checkpoint.scope)
        .unwrap();
    assert!(confirm_approval(&ledger, &key(), &saved, "a").is_err());
    let foreign = ExecutionRef {
        session_id: "other".into(),
        ..key()
    };
    assert!(confirm_approval(&ledger, &foreign, &fixture(), "a").is_err());
    assert!(ledger.execution_events_after("e", 0).unwrap().is_empty());
}

#[test]
fn persisted_merge_hides_old_approval_and_reuses_exact_historical_confirmation() {
    use kolyan_core::{TurnControl, TurnExecutor};
    use kolyan_ledger::InMemoryLedger;
    let ledger = InMemoryLedger::default();
    let saved = fixture();
    let confirmation = confirm_approval(&ledger, &key(), &saved, "a").unwrap();
    let scope = saved.checkpoint.scope.clone();
    let executor = TurnExecutor::new(ApprovalProvider(saved.checkpoint.steps[0].response.clone()))
        .with_execution_key(scope.execution.clone());
    let checkpoint = executor
        .merge_resume_with_control(
            saved.clone(),
            ResumeInput::ApprovalConfirmed(confirmation.clone()),
            scope.clone(),
            TurnControl::default(),
        )
        .unwrap();
    let merged = TurnSuspension::from_checkpoint(checkpoint.clone(), &scope).unwrap();
    let before = fact(
        LedgerEventKind::ExecutionSuspended,
        json!({"schema_version":1,"publication_cursor":0,"suspension":saved}),
    );
    let after = fact(
        LedgerEventKind::TurnCheckpointMerged,
        json!({"schema_version":1,"publication_cursor":0,"checkpoint":checkpoint}),
    );
    assert!(
        current_suspension(&key(), &[before, after])
            .unwrap()
            .is_none()
    );
    // A merge is recoverable authority, not a public stopped-state publication.
    let projected = merged;
    assert!(
        suspension_view(&projected).unwrap()["pending_approvals"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        confirm_approval(&ledger, &key(), &projected, "a").unwrap(),
        confirmation
    );
    verify_resume_input(
        &ledger,
        &key(),
        &projected,
        &ResumeInput::ApprovalConfirmed(confirmation),
    )
    .unwrap();
    assert_eq!(ledger.execution_events_after("e", 0).unwrap().len(), 1);
}
