//! Lifecycle projection only: checkpoint decoding and authority validation
//! remain Runtime responsibilities. No model or tool execution occurs here.

use super::*;

use kolyan_ledger::{InMemoryLedger, SqliteLedger};

fn key(id: &str) -> ExecutionRef {
    ExecutionRef {
        session_id: format!("session-{id}"),
        turn_id: format!("turn-{id}"),
        execution_id: id.into(),
    }
}

fn append<L: LedgerStore>(ledger: &L, key: &ExecutionRef, suffix: &str, kind: LedgerEventKind) {
    let id = format!("{}/{suffix}", key.execution_id);
    ledger
        .append(LedgerEvent {
            event_id: id.clone(),
            turn_id: key.turn_id.clone(),
            execution_id: key.execution_id.clone(),
            cursor: 0,
            kind,
            idempotency_key: id,
            payload: json!({"approval_id":"approval", "checkpoint_id":"checkpoint"}),
        })
        .unwrap();
}

fn export<L: LedgerStore>(ledger: &L, key: &ExecutionRef, label: &str) {
    let root = tempfile::Builder::new()
        .prefix("kolyan-server-boundary-")
        .tempdir()
        .unwrap()
        .keep();
    let facts = ledger.execution_events_after(&key.execution_id, 0).unwrap();
    std::fs::write(
        root.join("actual.json"),
        serde_json::to_vec_pretty(&json!({
            "case":label, "execution":key, "facts":facts,
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!(
        "coordinator boundary observations: {}",
        root.join("actual.json").display()
    );
}

fn boundary_then_checkpoint<L: LedgerStore + Clone>(ledger: L) {
    let target = key("target");
    let coordinator = ExecutionCoordinator::new(ledger.clone());
    coordinator.start(target.clone()).unwrap();
    coordinator.release(&target.execution_id);
    append(
        &ledger,
        &target,
        "boundary",
        LedgerEventKind::ExecutionBoundaryAdmitted,
    );
    let rebuilt = ExecutionCoordinator::new(ledger.clone());
    export(
        &ledger,
        &target,
        "boundary admission is not durable suspension",
    );
    assert_eq!(
        rebuilt.state(&target.execution_id).unwrap(),
        ExecutionState::Running
    );
    let before = ledger
        .execution_events_after(&target.execution_id, 0)
        .unwrap();
    assert!(matches!(
        rebuilt.resume(target.clone()),
        Err(CoordinatorError::NotSuspended { .. })
    ));
    let mut foreign_scope = target.clone();
    foreign_scope.session_id = "foreign-session".into();
    foreign_scope.turn_id = "foreign-turn".into();
    // Boundary admission is not resume authority, including for foreign callers.
    assert!(matches!(
        rebuilt.resume(foreign_scope),
        Err(CoordinatorError::NotSuspended { .. })
    ));
    assert_eq!(
        ledger
            .execution_events_after(&target.execution_id, 0)
            .unwrap(),
        before
    );
    assert!(!rebuilt.is_active(&target.execution_id));

    append(
        &ledger,
        &target,
        "checkpoint",
        LedgerEventKind::ApprovalRequested,
    );
    assert_eq!(
        rebuilt.state(&target.execution_id).unwrap(),
        ExecutionState::Running
    );
    assert!(matches!(
        rebuilt.resume(target.clone()),
        Err(CoordinatorError::NotSuspended { .. })
    ));
    append(
        &ledger,
        &target,
        "suspended",
        LedgerEventKind::ExecutionSuspended,
    );
    let rebuilt = ExecutionCoordinator::new(ledger.clone());
    export(
        &ledger,
        &target,
        "checkpoint persisted before durable suspension",
    );
    assert_eq!(
        rebuilt.state(&target.execution_id).unwrap(),
        ExecutionState::Suspended
    );
    let admission = rebuilt.resume(target.clone()).unwrap();
    assert_eq!(admission.execution, target);
    assert_eq!(admission.kind, AdmissionKind::Resume);
    assert_eq!(
        rebuilt.state(&target.execution_id).unwrap(),
        ExecutionState::Running
    );
    assert!(rebuilt.is_active(&target.execution_id));
    rebuilt.release(&target.execution_id);
}

#[test]
fn boundary_remains_running_and_only_persisted_suspension_admits_resume() {
    boundary_then_checkpoint(InMemoryLedger::default());
    let root = tempfile::tempdir().unwrap();
    boundary_then_checkpoint(SqliteLedger::open(root.path().join("ledger.sqlite")).unwrap());
}

fn cancellation_wins<L: LedgerStore + Clone>(ledger: L) {
    let target = key("cancelled");
    let coordinator = ExecutionCoordinator::new(ledger.clone());
    coordinator.start(target.clone()).unwrap();
    append(
        &ledger,
        &target,
        "boundary",
        LedgerEventKind::ExecutionBoundaryAdmitted,
    );
    coordinator.cancel(&target).unwrap();
    // Even late facts cannot resurrect a terminal cancellation during replay.
    append(
        &ledger,
        &target,
        "late-boundary",
        LedgerEventKind::ExecutionBoundaryAdmitted,
    );
    append(
        &ledger,
        &target,
        "late-checkpoint",
        LedgerEventKind::ApprovalRequested,
    );
    append(
        &ledger,
        &target,
        "late-suspension",
        LedgerEventKind::ExecutionSuspended,
    );
    let rebuilt = ExecutionCoordinator::new(ledger.clone());
    export(
        &ledger,
        &target,
        "cancellation precedes late boundary and suspension",
    );
    let before = ledger
        .execution_events_after(&target.execution_id, 0)
        .unwrap();
    assert_eq!(
        rebuilt.state(&target.execution_id).unwrap(),
        ExecutionState::Cancelled
    );
    assert!(matches!(
        rebuilt.resume(target.clone()),
        Err(CoordinatorError::Terminal {
            state: ExecutionState::Cancelled,
            ..
        })
    ));
    assert!(rebuilt.recover(target.clone()).is_err());
    assert_eq!(
        ledger
            .execution_events_after(&target.execution_id, 0)
            .unwrap(),
        before
    );
    assert!(!rebuilt.is_active(&target.execution_id));
}

#[test]
fn cancellation_is_not_revived_by_late_boundary_or_suspension() {
    cancellation_wins(InMemoryLedger::default());
    let root = tempfile::tempdir().unwrap();
    cancellation_wins(SqliteLedger::open(root.path().join("ledger.sqlite")).unwrap());
}

fn foreign_wait<L: LedgerStore + Clone>(ledger: L) {
    let target = key("running");
    let foreign = key("foreign");
    let coordinator = ExecutionCoordinator::new(ledger.clone());
    coordinator.start(target.clone()).unwrap();
    coordinator.release(&target.execution_id);
    append(
        &ledger,
        &target,
        "boundary",
        LedgerEventKind::ExecutionBoundaryAdmitted,
    );
    coordinator.start(foreign.clone()).unwrap();
    coordinator.release(&foreign.execution_id);
    append(
        &ledger,
        &foreign,
        "checkpoint",
        LedgerEventKind::ApprovalRequested,
    );
    append(
        &ledger,
        &foreign,
        "suspended",
        LedgerEventKind::ExecutionSuspended,
    );
    let rebuilt = ExecutionCoordinator::new(ledger.clone());
    export(
        &ledger,
        &target,
        "foreign suspended execution does not authorize target",
    );
    assert_eq!(
        rebuilt.state(&foreign.execution_id).unwrap(),
        ExecutionState::Suspended
    );
    assert_eq!(
        rebuilt.state(&target.execution_id).unwrap(),
        ExecutionState::Running
    );
    let before = ledger
        .execution_events_after(&target.execution_id, 0)
        .unwrap();
    assert!(matches!(
        rebuilt.resume(target.clone()),
        Err(CoordinatorError::NotSuspended { .. })
    ));
    assert_eq!(
        ledger
            .execution_events_after(&target.execution_id, 0)
            .unwrap(),
        before
    );
    assert!(!rebuilt.is_active(&target.execution_id));
}

#[test]
fn foreign_execution_checkpoint_and_suspension_cannot_authorize_boundary_resume() {
    foreign_wait(InMemoryLedger::default());
    let root = tempfile::tempdir().unwrap();
    foreign_wait(SqliteLedger::open(root.path().join("ledger.sqlite")).unwrap());
}
