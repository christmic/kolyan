//! Exact prepared evidence survives reconstruction and rejects corruption.

use super::*;
use crate::{
    AdmissionDecision, AdmissionPort, EffectDisposition, EffectExecutor, EffectOutcome,
    ExecutionRuntime, RuntimeExecutionError,
};
use kolyan_core::ToolError;
use kolyan_ledger::{FileLedger, InMemoryLedger};
use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalEvidence, ApprovalMode, Capability, Effect, Idempotency, InvocationClaim,
    PolicyContext, PolicyEngine, PreparedCall, PreparedGrant, ResourceClaim, ToolExecutionScope,
    ToolManifest, ToolRequirements,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Inspector {
    calls: AtomicUsize,
    resolution: ReconciliationResolution,
}
impl ToolEffectReconciler for Inspector {
    fn inspect(
        &self,
        _: &ReconciliationRequest,
        _: &EffectRequest,
        _: &EffectGrant,
    ) -> Result<ReconciliationResolution, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.resolution.clone())
    }
}

struct NeverReplay;
impl AdmissionPort for NeverReplay {
    fn decide(
        &self,
        _: &ExecutionKey,
        _: &EffectRequest,
    ) -> Result<AdmissionDecision, RuntimeExecutionError> {
        panic!("historical prepared effects cannot seek new admission")
    }
}
impl EffectExecutor for NeverReplay {
    fn execute(
        &self,
        _: &ExecutionKey,
        _: &EffectRequest,
        _: &EffectGrant,
    ) -> Result<EffectOutcome, RuntimeExecutionError> {
        panic!("uncertain or committed prepared effects cannot be executed")
    }
}

fn fixture() -> (ReconciliationRequest, PreparedEvidence, Inspector) {
    fixture_in(ExecutionKey {
        session_id: "s".into(),
        execution_id: "e".into(),
        turn_id: "t".into(),
    })
}

fn fixture_in(execution: ExecutionKey) -> (ReconciliationRequest, PreparedEvidence, Inspector) {
    let scope = ToolExecutionScope {
        execution,
        step_id: "step-1".into(),
        agent_snapshot_digest: Some("a".repeat(64)),
    };
    let prepared = PreparedCall::new(
        ToolCall {
            id: "call-1".into(),
            name: "file.write".into(),
            arguments: json!({"path":"safe/a", "content":"done"}),
        },
        "file-fixture-v1".into(),
        InvocationClaim {
            tool_name: "file.write".into(),
            capabilities: [Capability::FilesystemWrite].into(),
            effects: [Effect::Update].into(),
            resource: ResourceClaim { path: None },
            idempotency: Idempotency::NonIdempotent,
        },
        ToolRequirements {
            process_sandbox: true,
            max_output_bytes: 4096,
            timeout_ms: 30000,
        },
    )
    .unwrap()
    .with_execution_binding(json!({"fixture_target": "safe/a", "fixture_plan_revision": 1}))
    .unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        path_scopes: Vec::new(),
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    let decision = policy.decide_prepared(&prepared, &PolicyContext::default());
    let revision = decision.policy_version.clone();
    let grant = PreparedGrant::issue(
        &prepared,
        decision,
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    let bound =
        PreparedEvidence::new(prepared, grant, scope.clone(), &revision, &scope.execution).unwrap();
    let request = ReconciliationRequest {
        reconciliation_id: "inspection-1".into(),
        execution: scope.execution,
        effect_id: bound.request.effect_id.clone(),
    };
    let inspector = Inspector {
        calls: AtomicUsize::new(0),
        resolution: ReconciliationResolution::Committed {
            output: ToolResult {
                call_id: "call-1".into(),
                content: "wrote 4 bytes".into(),
                is_error: false,
            },
            executor_id: bound.executor_id(),
            executor_revision: bound.prepared.tool_revision().to_owned(),
            evidence: "trusted executor proof 73".into(),
        },
    };
    (request, bound, inspector)
}

#[test]
fn valid_foreign_scoped_authority_is_not_historical_owner_authority() {
    let (request, bound, inspector) = fixture();
    let mut foreign_execution = request.execution.clone();
    foreign_execution.session_id = "foreign-session".into();
    let (foreign_request, foreign_bound, _) = fixture_in(foreign_execution);
    let source = InMemoryLedger::default();
    seed(&source, &foreign_request, &foreign_bound);
    let ledger = reconstructed(source.events_after(0).unwrap(), |event| {
        if event.kind == LedgerEventKind::ExecutionStarted {
            event.payload = json!(request.execution);
        }
    });
    assert!(reconcile_tool_effect(&ledger, &request, &inspector).is_err());
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 0);
    assert!(
        ExecutionRuntime::new(ledger, NeverReplay, NeverReplay)
            .apply_effect(&request.execution, &bound.request)
            .is_err()
    );
}

fn seed(ledger: &impl LedgerStore, request: &ReconciliationRequest, bound: &PreparedEvidence) {
    for (id, kind, payload) in [
        (
            format!("{}/execution-started", request.execution.execution_id),
            LedgerEventKind::ExecutionStarted,
            json!(request.execution),
        ),
        (
            format!("{}/prepared", bound.prefix()),
            LedgerEventKind::EffectPrepared,
            bound.prepared_payload().unwrap(),
        ),
        (
            format!("{}/authorized", bound.prefix()),
            LedgerEventKind::EffectAuthorized,
            bound.authorized_payload().unwrap(),
        ),
        (
            format!("{}/started", bound.prefix()),
            LedgerEventKind::EffectStarted,
            bound.started_payload(),
        ),
    ] {
        ledger
            .append(LedgerEvent {
                event_id: id.clone(),
                turn_id: request.execution.turn_id.clone(),
                execution_id: request.execution.execution_id.clone(),
                cursor: 0,
                kind,
                idempotency_key: id,
                payload,
            })
            .unwrap();
    }
}

fn reconstructed(events: Vec<LedgerEvent>, mutate: impl Fn(&mut LedgerEvent)) -> InMemoryLedger {
    let ledger = InMemoryLedger::default();
    for mut event in events {
        mutate(&mut event);
        ledger.append(event).unwrap();
    }
    ledger
}

#[tokio::test]
async fn disk_reconciliation_and_generic_recovery_share_the_exact_receipt() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("facts.jsonl");
    let (request, bound, inspector) = fixture();
    {
        let ledger = FileLedger::open(&path).unwrap();
        seed(&ledger, &request, &bound);
    }
    {
        let ledger = FileLedger::open(&path).unwrap();
        let runtime = ExecutionRuntime::new(ledger.clone(), NeverReplay, NeverReplay);
        assert!(matches!(
            runtime
                .apply_effect(&request.execution, &bound.request)
                .unwrap(),
            EffectDisposition::Uncertain { .. }
        ));
        assert_eq!(ledger.events_after(0).unwrap().len(), 4);
        assert_eq!(
            reconcile_tool_effect(&ledger, &request, &inspector).unwrap(),
            inspector.resolution
        );
    }
    let ledger = FileLedger::open(&path).unwrap();
    assert_eq!(
        reconcile_tool_effect(&ledger, &request, &inspector).unwrap(),
        inspector.resolution
    );
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 1);
    let runtime = ExecutionRuntime::new(ledger.clone(), NeverReplay, NeverReplay);
    assert!(matches!(
        runtime
            .apply_effect(&request.execution, &bound.request)
            .unwrap(),
        EffectDisposition::Completed { .. }
    ));
    let receipt = ledger
        .event_by_id(&format!("{}/receipt", bound.prefix()))
        .unwrap()
        .unwrap();
    let output = bound.validate_receipt(&receipt.payload).unwrap().unwrap();
    let ReconciliationResolution::Committed {
        output: expected, ..
    } = &inspector.resolution
    else {
        unreachable!()
    };
    assert_eq!(output, *expected);
    assert_eq!(ledger.events_after(0).unwrap().len(), 6);
}

#[test]
fn prepared_digest_scope_grant_and_missing_bindings_fail_before_inspection() {
    let (request, bound, inspector) = fixture();
    let source = InMemoryLedger::default();
    seed(&source, &request, &bound);
    let events = source.events_after(0).unwrap();
    for mutation in 0..10 {
        let ledger = reconstructed(events.clone(), |event| match (mutation, event.kind) {
            (0, LedgerEventKind::EffectPrepared) => {
                event.payload["input_digest"] = json!("0".repeat(64))
            }
            (1, LedgerEventKind::EffectPrepared) => {
                event.payload["input"]["scope"]["execution"]["session_id"] = json!("foreign")
            }
            (2, LedgerEventKind::EffectPrepared) => {
                event.payload["input"]["scope"]["step_id"] = json!("foreign-step")
            }
            (3, LedgerEventKind::EffectPrepared) => {
                event.payload["input"]["scope"]["agent_snapshot_digest"] = json!("b".repeat(64))
            }
            (4, LedgerEventKind::EffectPrepared) => {
                event.payload["input"]
                    .as_object_mut()
                    .unwrap()
                    .remove("scope");
            }
            (5, LedgerEventKind::EffectAuthorized) => {
                event.payload["constraints_digest"] = json!("0".repeat(64))
            }
            (6, LedgerEventKind::EffectAuthorized) => {
                event
                    .payload
                    .as_object_mut()
                    .unwrap()
                    .remove("prepared_grant");
            }
            (7, LedgerEventKind::EffectPrepared) => {
                event.payload["input"]["prepared"]["digest"] = json!("0".repeat(64))
            }
            (8, LedgerEventKind::EffectPrepared) => {
                event
                    .payload
                    .as_object_mut()
                    .unwrap()
                    .remove("binding_kind");
            }
            (9, LedgerEventKind::EffectPrepared) => {
                event.payload["input"]["prepared"]["execution_binding"]["fixture_target"] =
                    json!("foreign/b");
            }
            _ => {}
        });
        assert!(
            reconcile_tool_effect(&ledger, &request, &inspector).is_err(),
            "mutation {mutation}"
        );
        assert!(
            ExecutionRuntime::new(ledger.clone(), NeverReplay, NeverReplay)
                .apply_effect(&request.execution, &bound.request)
                .is_err(),
            "mutation {mutation}"
        );
        assert_eq!(ledger.events_after(0).unwrap().len(), 4);
    }
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 0);
    for missing in [
        LedgerEventKind::EffectPrepared,
        LedgerEventKind::EffectAuthorized,
        LedgerEventKind::EffectStarted,
    ] {
        let ledger = reconstructed(
            events
                .clone()
                .into_iter()
                .filter(|event| event.kind != missing)
                .collect(),
            |_| {},
        );
        assert!(reconcile_tool_effect(&ledger, &request, &inspector).is_err());
        if missing != LedgerEventKind::EffectStarted {
            assert!(
                ExecutionRuntime::new(ledger, NeverReplay, NeverReplay)
                    .apply_effect(&request.execution, &bound.request)
                    .is_err()
            );
        }
    }
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn corrupt_receipt_is_not_hidden_by_an_existing_reconciliation_decision() {
    let (request, bound, inspector) = fixture();
    let source = InMemoryLedger::default();
    seed(&source, &request, &bound);
    reconcile_tool_effect(&source, &request, &inspector).unwrap();
    for mutation in 0..5 {
        let ledger = reconstructed(source.events_after(0).unwrap(), |event| {
            if event.kind == LedgerEventKind::EffectReceipt {
                match mutation {
                    0 => event.payload["receipt"]["result_digest"] = json!("0".repeat(64)),
                    1 => event.payload["receipt"]["executor_revision"] = json!("foreign-revision"),
                    2 => {
                        event.payload["prepared_grant"]["scope"]["execution"]["turn_id"] =
                            json!("foreign")
                    }
                    3 => {
                        event.payload.as_object_mut().unwrap().remove("input");
                    }
                    _ => {
                        event.payload["reconciliation"]["resolution"]["Committed"]["evidence"] =
                            json!("")
                    }
                }
            }
        });
        assert!(
            reconcile_tool_effect(&ledger, &request, &inspector).is_err(),
            "mutation {mutation}"
        );
        assert!(
            ExecutionRuntime::new(ledger, NeverReplay, NeverReplay)
                .apply_effect(&request.execution, &bound.request)
                .is_err()
        );
    }
    let absent = reconstructed(
        source
            .events_after(0)
            .unwrap()
            .into_iter()
            .filter(|event| event.kind != LedgerEventKind::EffectReceipt)
            .collect(),
        |_| {},
    );
    assert!(reconcile_tool_effect(&absent, &request, &inspector).is_err());
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn receipt_commit_gap_repairs_and_inspector_revision_mismatch_is_refused() {
    let (request, bound, inspector) = fixture();
    let source = InMemoryLedger::default();
    seed(&source, &request, &bound);
    reconcile_tool_effect(&source, &request, &inspector).unwrap();
    let rebuilt = reconstructed(
        source
            .events_after(0)
            .unwrap()
            .into_iter()
            .filter(|e| e.kind != LedgerEventKind::EffectReconciled)
            .collect(),
        |_| {},
    );
    assert_eq!(
        reconcile_tool_effect(&rebuilt, &request, &inspector).unwrap(),
        inspector.resolution
    );
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 1);
    let mut bad = fixture().2;
    if let ReconciliationResolution::Committed {
        executor_revision, ..
    } = &mut bad.resolution
    {
        *executor_revision = "wrong".into();
    }
    let fresh = InMemoryLedger::default();
    seed(&fresh, &request, &bound);
    assert!(reconcile_tool_effect(&fresh, &request, &bad).is_err());
    assert_eq!(fresh.events_after(0).unwrap().len(), 4);
}

#[test]
fn shared_receipt_preserves_cancel_and_timeout_and_rejects_extra_fields() {
    let (_, bound, _) = fixture();
    for error in [
        ToolError::Cancelled,
        ToolError::TimedOut,
        ToolError::Failed {
            message: "failed".into(),
        },
    ] {
        let result = Err(error);
        let mut payload = bound.receipt_payload(&result).unwrap();
        assert_eq!(bound.validate_receipt(&payload).unwrap(), result);
        payload["extra"] = json!("unexpected");
        assert!(bound.validate_receipt(&payload).is_err());
    }
}
