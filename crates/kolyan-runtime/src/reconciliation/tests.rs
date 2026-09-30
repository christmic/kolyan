use super::*;
use kolyan_ledger::InMemoryLedger;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Inspect {
    calls: AtomicUsize,
    resolution: ReconciliationResolution,
}
impl ToolEffectReconciler for Inspect {
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

fn setup() -> (InMemoryLedger, ReconciliationRequest) {
    let ledger = InMemoryLedger::default();
    let request = ReconciliationRequest {
        reconciliation_id: "inspection-1".into(),
        execution: ExecutionKey {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        },
        effect_id: "step-1/call-1".into(),
    };
    let prepared = EffectRequest {
        effect_id: request.effect_id.clone(),
        operation_kind: "file.write".into(),
        input_digest: json!({"name":"file.write","arguments":{"path":"safe/a","content":"done"}})
            .to_string(),
        requirements: vec![],
        policy_revision: "1".into(),
    };
    let grant = EffectGrant {
        authorization_id: "e/effect/step-1/call-1/authorized".into(),
        effect_id: prepared.effect_id.clone(),
        input_digest: prepared.input_digest.clone(),
        constraints_digest: "scope-1".into(),
        authority_revision: "1".into(),
    };
    for (id, kind, payload) in [
        (
            "e/execution-started",
            LedgerEventKind::ExecutionStarted,
            json!(request.execution),
        ),
        (
            "e/effect/step-1/call-1/prepared",
            LedgerEventKind::EffectPrepared,
            json!(prepared),
        ),
        (
            "e/effect/step-1/call-1/authorized",
            LedgerEventKind::EffectAuthorized,
            json!(grant),
        ),
        (
            "e/effect/step-1/call-1/started",
            LedgerEventKind::EffectStarted,
            json!({"effect_id":request.effect_id}),
        ),
    ] {
        ledger
            .append(LedgerEvent {
                event_id: id.into(),
                turn_id: "t".into(),
                execution_id: "e".into(),
                cursor: 0,
                kind,
                idempotency_key: id.into(),
                payload,
            })
            .unwrap();
    }
    (ledger, request)
}

fn committed() -> Inspect {
    Inspect {
        calls: AtomicUsize::new(0),
        resolution: ReconciliationResolution::Committed {
            output: ToolResult {
                call_id: "call-1".into(),
                content: "wrote 4 bytes".into(),
                is_error: false,
            },
            executor_id: "file-executor".into(),
            executor_revision: "1".into(),
            evidence: "verified external operation receipt #73".into(),
        },
    }
}

#[test]
fn committed_receipt_contains_evidence_and_repeats_without_inspection() {
    let (ledger, request) = setup();
    let inspector = committed();
    let result = reconcile_tool_effect(&ledger, &request, &inspector).unwrap();
    assert_eq!(result, inspector.resolution);
    assert_eq!(
        reconcile_tool_effect(&ledger, &request, &inspector).unwrap(),
        result
    );
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 1);
    let receipt = ledger
        .event_by_id("e/effect/step-1/call-1/receipt")
        .unwrap()
        .unwrap();
    assert_eq!(receipt.payload["reconciliation"]["request"], json!(request));
    assert_eq!(
        receipt.payload["reconciliation"]["resolution"],
        json!(result)
    );
    assert_eq!(receipt.payload["output"]["call_id"], "call-1");
    assert_eq!(ledger.execution_events_after("e", 0).unwrap().len(), 6);
}

#[test]
fn uncertain_and_not_committed_never_create_a_receipt_or_grant() {
    for resolution in [
        ReconciliationResolution::Unknown {
            evidence: "executor cannot prove outcome".into(),
        },
        ReconciliationResolution::NotCommitted {
            evidence: "executor transaction was rolled back".into(),
        },
    ] {
        let (ledger, request) = setup();
        let inspector = Inspect {
            calls: AtomicUsize::new(0),
            resolution,
        };
        let result = reconcile_tool_effect(&ledger, &request, &inspector).unwrap();
        assert_eq!(result, inspector.resolution);
        assert_eq!(
            reconcile_tool_effect(&ledger, &request, &inspector).unwrap(),
            result
        );
        assert_eq!(inspector.calls.load(Ordering::SeqCst), 1);
        assert!(
            ledger
                .event_by_id("e/effect/step-1/call-1/receipt")
                .unwrap()
                .is_none()
        );
        assert_eq!(ledger.execution_events_after("e", 0).unwrap().len(), 5);
    }
}

#[test]
fn foreign_execution_and_wrong_result_are_rejected_without_receipts() {
    let (ledger, mut request) = setup();
    let inspector = committed();
    request.execution.session_id = "foreign".into();
    assert!(reconcile_tool_effect(&ledger, &request, &inspector).is_err());
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 0);
    request.execution.session_id = "s".into();
    let bad = Inspect {
        calls: AtomicUsize::new(0),
        resolution: ReconciliationResolution::Committed {
            output: ToolResult {
                call_id: "wrong".into(),
                content: "written".into(),
                is_error: false,
            },
            executor_id: "x".into(),
            executor_revision: "1".into(),
            evidence: "external proof".into(),
        },
    };
    assert!(reconcile_tool_effect(&ledger, &request, &bad).is_err());
    assert!(
        ledger
            .event_by_id("e/effect/step-1/call-1/receipt")
            .unwrap()
            .is_none()
    );
}

#[test]
fn receipt_without_projection_decision_repairs_from_saved_evidence() {
    let (ledger, request) = setup();
    let inspector = committed();
    let result = reconcile_tool_effect(&ledger, &request, &inspector).unwrap();
    let rebuilt = InMemoryLedger::default();
    for event in ledger
        .execution_events_after("e", 0)
        .unwrap()
        .into_iter()
        .filter(|event| event.kind != LedgerEventKind::EffectReconciled)
    {
        rebuilt.append(event).unwrap();
    }
    assert_eq!(
        reconcile_tool_effect(&rebuilt, &request, &inspector).unwrap(),
        result
    );
    assert_eq!(inspector.calls.load(Ordering::SeqCst), 1);
    assert!(
        rebuilt
            .event_by_id("e/effect/step-1/call-1/reconciliation/inspection-1")
            .unwrap()
            .is_some()
    );
}
