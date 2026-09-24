use super::*;
use kolyan_ledger::InMemoryLedger;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Allow;
impl AdmissionPort for Allow {
    fn decide(
        &self,
        _: &ExecutionKey,
        effect: &EffectRequest,
    ) -> Result<AdmissionDecision, RuntimeExecutionError> {
        Ok(AdmissionDecision::Grant(EffectGrant {
            authorization_id: "auth".into(),
            effect_id: effect.effect_id.clone(),
            input_digest: effect.input_digest.clone(),
            constraints_digest: "constraints".into(),
            authority_revision: "1".into(),
        }))
    }
}

struct CrashingExecutor(Arc<AtomicUsize>);
impl EffectExecutor for CrashingExecutor {
    fn execute(
        &self,
        _: &ExecutionKey,
        _: &EffectRequest,
        _: &EffectGrant,
    ) -> Result<EffectOutcome, RuntimeExecutionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(RuntimeExecutionError::Executor(
            "crash after external effect".into(),
        ))
    }
}

fn key() -> ExecutionKey {
    ExecutionKey {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "exec".into(),
    }
}
fn effect() -> EffectRequest {
    EffectRequest {
        effect_id: "effect".into(),
        operation_kind: "write".into(),
        input_digest: "input-one".into(),
        requirements: vec![],
        policy_revision: "1".into(),
    }
}

#[test]
fn started_without_receipt_is_uncertain_and_never_replayed() {
    let ledger = InMemoryLedger::default();
    let count = Arc::new(AtomicUsize::new(0));
    let first = ExecutionRuntime::new(ledger.clone(), Allow, CrashingExecutor(count.clone()));
    first.start(&key()).unwrap();
    assert!(first.apply_effect(&key(), &effect()).is_err());
    let rebuilt = ExecutionRuntime::new(ledger, Allow, CrashingExecutor(count.clone()));
    assert!(matches!(
        rebuilt.apply_effect(&key(), &effect()).unwrap(),
        EffectDisposition::Uncertain { .. }
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut other = effect();
    other.input_digest = "different-input".into();
    assert!(rebuilt.apply_effect(&key(), &other).is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn receipt_status_must_match_outcome() {
    let receipt = EffectReceipt {
        receipt_id: "r".into(),
        effect_id: "e".into(),
        authorization_id: "a".into(),
        input_digest: "i".into(),
        executor_id: "executor".into(),
        executor_revision: "1".into(),
        result_digest: "result".into(),
        status: ReceiptStatus::Failed,
    };
    assert!(validate_receipt_status(&receipt, ReceiptStatus::Completed).is_err());
}
