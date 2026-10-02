//! Recovery uses production checkpoint and evidence constructors. No tool or
//! model port is available to hydration; waiting resume ports panic if polled.

use super::*;
mod prepared;
use kolyan_core::{
    CheckpointBudget, CheckpointCall, CheckpointReconstruction, ExternalWait, IssuedToolAuthority,
    ResumableTurn, StepOutcome, StepResult, ToolDispatchMode, ToolDispatchPolicy, ToolExecutor,
    ToolFuture, ToolInvocation, ToolPreparationFuture, TurnConfig, TurnControl, TurnExecutor,
    TurnRequest,
};
use kolyan_ledger::{InMemoryLedger, LedgerEvent};
use kolyan_model::{
    ContentBlock, ModelProvider, ModelRef, ModelRequest, ModelResponse, ProviderFuture, StopReason,
    TokenUsage, ToolCall, ToolChoice,
};
use kolyan_policy::{
    ApprovalEvidence, Capability, Effect, ExecutionConstraints, Idempotency, InvocationClaim,
    PolicyDecision, PolicyDecisionKind, PreparedCall, PreparedGrant, ResourceClaim,
    ToolExecutionScope, ToolRequirements,
};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};
use std::sync::Arc;

struct Fixture {
    ledger: InMemoryLedger,
    key: RuntimeTurnKey,
    checkpoint: TurnCheckpoint,
    bindings: Vec<PreparedEvidence>,
}

impl Fixture {
    fn new(parallel: bool, on_error: ToolErrorPolicy) -> Self {
        let key = RuntimeTurnKey {
            session_id: "recovery-session".into(),
            turn_id: "recovery-turn".into(),
            execution_id: "recovery-execution".into(),
        };
        let scope = ToolExecutionScope {
            execution: key.clone(),
            step_id: "recovery-turn-step-0".into(),
            agent_snapshot_digest: None,
        };
        let model = ModelRef::new("fixture", "recovery");
        let original = ModelRequest {
            request_id: "original-request".into(),
            model: model.clone(),
            system: vec![],
            messages: vec![],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: Value::Null,
        };
        let dispatch = ToolDispatchPolicy {
            mode: if parallel {
                ToolDispatchMode::Parallel
            } else {
                ToolDispatchMode::Serial
            },
            on_error,
        };
        let ledger = InMemoryLedger::default();
        let admission = InputAdmission::new(
            key.clone(),
            &TurnRequest {
                turn_id: key.turn_id.clone(),
                model_request: original.clone(),
                config: TurnConfig {
                    max_steps: 4,
                    max_tool_calls: Some(3),
                    deadline: None,
                },
            },
            dispatch,
            Some(1000),
            None,
        )
        .unwrap();
        let bindings: Vec<_> = ["head", "sibling", "tail"]
            .into_iter()
            .map(|id| {
                let call = ToolCall {
                    id: id.into(),
                    name: "file.read".into(),
                    arguments: json!({"path": id}),
                };
                let prepared = PreparedCall::new(
                    call,
                    "recovery-adapter-v1".into(),
                    InvocationClaim {
                        tool_name: "file.read".into(),
                        capabilities: [Capability::FilesystemRead].into(),
                        effects: [Effect::Read].into(),
                        resource: ResourceClaim {
                            path: Some(id.into()),
                        },
                        idempotency: Idempotency::Idempotent,
                    },
                    ToolRequirements {
                        process_sandbox: false,
                        max_output_bytes: 4096,
                        timeout_ms: 1000,
                    },
                )
                .unwrap();
                let grant = PreparedGrant::issue(
                    &prepared,
                    PolicyDecision {
                        kind: PolicyDecisionKind::Allow,
                        reason: "explicit recovery fixture admission".into(),
                        policy_version: "recovery-policy-v1".into(),
                        constraints: ExecutionConstraints {
                            max_output_bytes: Some(4096),
                            timeout_ms: Some(1000),
                        },
                    },
                    ApprovalEvidence::NotConfirmed,
                    scope.clone(),
                )
                .unwrap();
                PreparedEvidence::new(prepared, grant, scope.clone(), "recovery-policy-v1", &key)
                    .unwrap()
            })
            .collect();
        let calls: Vec<_> = bindings
            .iter()
            .map(|bound| CheckpointCall {
                call: bound.prepared.call().clone(),
                prepared: Some(bound.prepared.clone()),
                charged: false,
                state: CheckpointCallState::Ready,
            })
            .collect();
        let mut model_request = original;
        model_request.request_id = scope.step_id.clone();
        let checkpoint = TurnCheckpoint::reconstruct(
            CheckpointReconstruction {
                scope: scope.clone(),
                model_request,
                input_message_count: 0,
                steps: vec![StepResult {
                    step_id: scope.step_id.clone(),
                    response: ModelResponse {
                        id: "actual-completed-model-step".into(),
                        model,
                        content: calls
                            .iter()
                            .map(|call| ContentBlock::ToolCall {
                                call: call.call.clone(),
                            })
                            .collect(),
                        structured_output: None,
                        stop_reason: StopReason::ToolUse,
                        usage: TokenUsage {
                            input_tokens: Some(11),
                            output_tokens: Some(7),
                            ..Default::default()
                        },
                        metadata: Value::Null,
                    },
                    outcome: StepOutcome::ToolCalls,
                }],
                calls,
                stages: if parallel {
                    vec![vec!["head".into(), "sibling".into()], vec!["tail".into()]]
                } else {
                    vec![
                        vec!["head".into()],
                        vec!["sibling".into()],
                        vec!["tail".into()],
                    ]
                },
                stage_index: 0,
                approvals: vec![],
                budget: CheckpointBudget {
                    max_steps: 4,
                    max_tool_calls: Some(3),
                    deadline_at_ms: None,
                    tool_timeout_ms: Some(1000),
                    prior_tool_calls_used: 0,
                    tool_calls_used: 0,
                },
                dispatch,
            },
            &scope,
        )
        .unwrap();
        let fixture = Self {
            ledger,
            key,
            checkpoint,
            bindings,
        };
        fixture.append(
            "recovery-execution/execution-started",
            LedgerEventKind::ExecutionStarted,
            json!(fixture.key),
        );
        admission.persist(&fixture.ledger).unwrap();
        kolyan_core::TurnEventRecorder::record(
            &super::super::LedgerRecorder::new(
                fixture.ledger.clone(),
                fixture.key.clone(),
                "recovery-source".into(),
            ),
            &kolyan_core::TurnEvent::StepCompleted {
                turn_id: fixture.key.turn_id.clone(),
                step: fixture.checkpoint.steps[0].clone(),
            },
        )
        .unwrap();
        fixture
    }

    fn append(&self, id: &str, kind: LedgerEventKind, payload: Value) {
        self.ledger
            .append(LedgerEvent {
                event_id: id.into(),
                idempotency_key: id.into(),
                execution_id: self.key.execution_id.clone(),
                turn_id: self.key.turn_id.clone(),
                cursor: 0,
                kind,
                payload,
            })
            .unwrap();
    }

    fn enter(&self, index: usize) {
        let bound = &self.bindings[index];
        for (suffix, kind, payload) in [
            (
                "prepared",
                LedgerEventKind::EffectPrepared,
                bound.prepared_payload().unwrap(),
            ),
            (
                "authorized",
                LedgerEventKind::EffectAuthorized,
                bound.authorized_payload().unwrap(),
            ),
            (
                "started",
                LedgerEventKind::EffectStarted,
                bound.started_payload(),
            ),
        ] {
            self.append(&format!("{}/{suffix}", bound.prefix()), kind, payload);
        }
    }

    fn receipt(&self, index: usize, result: Result<ToolResult, ToolError>) {
        let bound = &self.bindings[index];
        self.append(
            &format!("{}/receipt", bound.prefix()),
            LedgerEventKind::EffectReceipt,
            bound.receipt_payload(&result).unwrap(),
        );
    }

    fn driver(&self) -> DurableTurnDriver<InMemoryLedger, NoopTraceSink> {
        DurableTurnDriver::new(self.ledger.clone(), NoopTraceSink)
    }

    fn assert_event(&self, index: usize, suffix: &str, kind: LedgerEventKind, payload: Value) {
        let id = format!("{}/{suffix}", self.bindings[index].prefix());
        let event = self.ledger.event_by_id(&id).unwrap().unwrap();
        check_event(&event, &self.key, &id, kind).unwrap();
        assert_eq!(event.payload, payload);
    }
}

fn result(id: &str) -> ToolResult {
    ToolResult {
        call_id: id.into(),
        content: "verified persisted output".into(),
        is_error: false,
    }
}

#[tokio::test]
async fn repeated_hydration_preserves_receipt_identity_and_charges_exactly_once() {
    let fixture = Fixture::new(false, ToolErrorPolicy::FailTurn);
    fixture.enter(0);
    fixture.receipt(0, Ok(result("head")));
    let first = fixture
        .driver()
        .hydrate_checkpoint(&fixture.key, fixture.checkpoint.clone())
        .await
        .unwrap();
    assert_eq!(first.budget.tool_calls_used, 1);
    assert_eq!(first.budget.prior_tool_calls_used, 0);
    assert!(first.calls[0].charged);
    assert_eq!(first.calls[1..], fixture.checkpoint.calls[1..]);
    assert_eq!(first.steps, fixture.checkpoint.steps);
    assert_eq!(first.stages, fixture.checkpoint.stages);
    assert_eq!(first.stage_index, 0);
    let events = fixture.ledger.events_after(0).unwrap();
    let second = fixture
        .driver()
        .hydrate_checkpoint(&fixture.key, first.clone())
        .await
        .unwrap();
    assert_eq!(second, first);
    // Replaying the older persisted snapshot also deduplicates ledger publication.
    let older = fixture
        .driver()
        .hydrate_checkpoint(&fixture.key, fixture.checkpoint.clone())
        .await
        .unwrap();
    assert_eq!(older, first);
    assert_eq!(fixture.ledger.events_after(0).unwrap(), events);
    let payload = fixture.bindings[0]
        .receipt_payload(&Ok(result("head")))
        .unwrap();
    fixture.assert_event(
        0,
        "receipt",
        LedgerEventKind::EffectReceipt,
        payload.clone(),
    );
    fixture.assert_event(0, "completed", LedgerEventKind::EffectCompleted, payload);
    for bound in &fixture.bindings[1..] {
        assert!(
            fixture
                .ledger
                .event_by_id(&format!("{}/started", bound.prefix()))
                .unwrap()
                .is_none()
        );
    }
}

struct ExactWait(IssuedToolAuthority, ExternalWait);
impl crate::ExternalWaitVerifier for ExactWait {
    fn verify_wait(&self, context: ExternalWaitContext) -> crate::ExternalVerificationFuture<'_> {
        Box::pin(async move {
            assert_eq!(context.issued, self.0);
            assert_eq!(context.wait, self.1);
            context.validate(&self.0.scope)
        })
    }
    fn verify_result(
        &self,
        _: ExternalWaitContext,
        _: ToolResult,
    ) -> crate::ExternalVerificationFuture<'_> {
        Box::pin(async { panic!("hydration must not resolve external work") })
    }
    fn recover_wait(&self, issued: IssuedToolAuthority) -> crate::ExternalRecoveryFuture<'_> {
        Box::pin(async move {
            assert_eq!(issued, self.0);
            Ok(Some(self.1.clone()))
        })
    }
}

struct ForbiddenPorts;
impl ModelProvider for ForbiddenPorts {
    fn stream(&self, _: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async { panic!("partial recovery must not poll a model") })
    }
}
impl ToolExecutor for ForbiddenPorts {
    fn prepare(&self, _: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async { panic!("partial recovery must not prepare the conflict tail") })
    }
    fn execute_invocation(&self, _: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async { panic!("partial recovery must not execute effects") })
    }
}

#[tokio::test]
async fn parallel_partial_recovery_preserves_wait_and_blocks_conflict_tail() {
    let fixture = Fixture::new(true, ToolErrorPolicy::FailTurn);
    fixture.enter(0);
    fixture.receipt(0, Ok(result("head")));
    fixture.enter(1);
    let wait = ExternalWait {
        wait_id: "saved-child-admission".into(),
        kind: "fixture.child".into(),
        schema_version: 1,
        binding: json!({"durable_admission":"already-committed"}),
    };
    let driver = fixture
        .driver()
        .with_external_wait_verifier(Arc::new(ExactWait(
            fixture.bindings[1].issued(),
            wait.clone(),
        )));
    let checkpoint = driver
        .hydrate_checkpoint(&fixture.key, fixture.checkpoint.clone())
        .await
        .unwrap();
    assert_eq!(checkpoint.budget.tool_calls_used, 2);
    assert!(checkpoint.calls[0].charged && checkpoint.calls[1].charged);
    assert_eq!(checkpoint.calls[2], fixture.checkpoint.calls[2]);
    assert_eq!(checkpoint.stage_index, 0);
    assert_eq!(checkpoint.stages, fixture.checkpoint.stages);
    assert!(
        matches!(&checkpoint.calls[0].state, CheckpointCallState::Completed { result: saved, .. } if saved == &result("head"))
    );
    assert!(
        matches!(&checkpoint.calls[1].state, CheckpointCallState::AwaitingExternal { wait: saved, issued } if saved == &wait && issued == &fixture.bindings[1].issued())
    );
    fixture.assert_event(
        1,
        "waiting",
        LedgerEventKind::EffectAwaitingExternal,
        fixture.bindings[1].wait_payload(&wait).unwrap(),
    );
    assert!(
        fixture
            .ledger
            .event_by_id(&format!("{}/receipt", fixture.bindings[1].prefix()))
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .ledger
            .event_by_id(&format!("{}/started", fixture.bindings[2].prefix()))
            .unwrap()
            .is_none()
    );
    let before = fixture.ledger.events_after(0).unwrap();
    let again = driver
        .hydrate_checkpoint(&fixture.key, checkpoint.clone())
        .await
        .unwrap();
    assert_eq!(again, checkpoint);
    assert_eq!(fixture.ledger.events_after(0).unwrap(), before);
    let outcome = TurnExecutor::with_tools(ForbiddenPorts, ForbiddenPorts)
        .with_execution_key(fixture.key.clone())
        .resume_checkpoint_with_control(
            checkpoint.clone(),
            checkpoint.scope.clone(),
            TurnControl::default(),
        )
        .await
        .unwrap();
    let ResumableTurn::Suspended(suspension) = outcome else {
        panic!("partial stage must stay suspended")
    };
    assert_eq!(suspension.checkpoint, checkpoint);
    assert_eq!(suspension.waiting.external_waits.len(), 1);
}

#[tokio::test]
async fn failed_receipts_preserve_fail_turn_and_fatal_semantics_without_fake_success() {
    for (policy, error, fatal) in [
        (
            ToolErrorPolicy::FailTurn,
            ToolError::Failed {
                message: "known failure".into(),
            },
            true,
        ),
        (
            ToolErrorPolicy::ContinueBatch,
            ToolError::Failed {
                message: "known failure".into(),
            },
            false,
        ),
        (ToolErrorPolicy::ContinueBatch, ToolError::Cancelled, true),
        (ToolErrorPolicy::ContinueBatch, ToolError::TimedOut, true),
    ] {
        let fixture = Fixture::new(false, policy);
        fixture.enter(0);
        fixture.receipt(0, Err(error.clone()));
        let recovered = fixture
            .driver()
            .hydrate_checkpoint(&fixture.key, fixture.checkpoint.clone())
            .await;
        if fatal {
            assert!(
                matches!(recovered, Err(RuntimeError::Turn(TurnError::Tool(ref saved))) if saved == &error)
            );
        } else {
            let checkpoint = recovered.unwrap();
            assert_eq!(checkpoint.budget.tool_calls_used, 1);
            assert!(checkpoint.calls[0].charged);
            assert!(
                matches!(&checkpoint.calls[0].state, CheckpointCallState::Completed { result, issued: Some(_) } if result.is_error && result.call_id == "head" && result.content == error.to_string())
            );
            assert_eq!(checkpoint.calls[1..], fixture.checkpoint.calls[1..]);
        }
        let payload = fixture.bindings[0].receipt_payload(&Err(error)).unwrap();
        fixture.assert_event(
            0,
            "receipt",
            LedgerEventKind::EffectReceipt,
            payload.clone(),
        );
        fixture.assert_event(0, "failed", LedgerEventKind::EffectFailed, payload);
        assert!(
            fixture
                .ledger
                .event_by_id(&format!("{}/completed", fixture.bindings[0].prefix()))
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn started_without_proven_admission_is_typed_uncertain_and_cannot_make_a_receipt() {
    let fixture = Fixture::new(false, ToolErrorPolicy::ContinueBatch);
    fixture.enter(0);
    let before = fixture.ledger.events_after(0).unwrap();
    let error = fixture
        .driver()
        .hydrate_checkpoint(&fixture.key, fixture.checkpoint.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeError::Turn(TurnError::Tool(ToolError::Uncertain { .. }))
    ));
    assert_eq!(fixture.ledger.events_after(0).unwrap(), before);
    assert_eq!(fixture.checkpoint.budget.tool_calls_used, 0);
    assert!(fixture.checkpoint.calls.iter().all(|call| !call.charged));
    assert!(
        fixture.bindings[0]
            .receipt_payload(&Err(ToolError::Uncertain {
                message: "not a definitive outcome".into()
            }))
            .is_err()
    );
    for suffix in ["receipt", "completed", "failed", "waiting"] {
        assert!(
            fixture
                .ledger
                .event_by_id(&format!("{}/{suffix}", fixture.bindings[0].prefix()))
                .unwrap()
                .is_none()
        );
    }
}
