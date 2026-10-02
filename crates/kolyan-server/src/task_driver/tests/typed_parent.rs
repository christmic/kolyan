//! Domain and physical lifecycle must both still admit the exact parent attempt.
use super::super::*;
use super::support::*;
use crate::input_fixture::SourceFixtureAdmission;
use crate::{ExecutionEvidence, TaskUsage};
use kolyan_ledger::FactJournal;

fn case() -> (Host, AttemptBinding) {
    let host = Host {
        coordinator: coordinator(),
        ledger: FaultLedger::default(),
    };
    let parent = binding("root");
    host.coordinator
        .start_attempt("task", "parent-start", parent.clone())
        .unwrap();
    host.coordinator
        .admit_fixture("task", "child-admit", invocation("child", Some("root")))
        .unwrap();
    host.coordinator
        .start_attempt("task", "child-start", binding("child"))
        .unwrap();
    (host, parent)
}

#[test]
fn superseded_failed_parent_attempt_cannot_be_reused_even_when_invocation_is_running() {
    let (host, parent) = case();
    let source = ExecutionEvidence {
        execution: parent.execution.clone(),
        event_id: "parent-failed".into(),
        cursor: 1,
    };
    host.coordinator
        .observe_attempt(
            "task",
            "parent-failed-observation",
            AttemptObservation {
                attempt_id: parent.attempt_id.clone(),
                execution: parent.execution.clone(),
                source,
                usage: TaskUsage::default(),
                outcome: AttemptOutcome::Failed {
                    reason: "host proved not entered".into(),
                    safe_to_retry: true,
                },
            },
        )
        .unwrap();
    let mut next = parent.clone();
    next.attempt_id = "next-parent-attempt".into();
    next.execution.turn_id = "next-turn".into();
    next.execution.execution_id = "next-execution".into();
    host.coordinator
        .start_attempt("task", "next-parent-start", next.clone())
        .unwrap();
    let before = host.coordinator.journal().read("task", 0, 1024).unwrap();
    assert!(
        result::load_consumed(
            &host.coordinator,
            &host.ledger,
            "task",
            &parent,
            &binding("child"),
            65536
        )
        .is_err()
    );
    assert!(
        result::load_consumed(
            &host.coordinator,
            &host.ledger,
            "task",
            &next,
            &binding("child"),
            65536
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(
        host.coordinator.journal().read("task", 0, 1024).unwrap(),
        before
    );
}

#[test]
fn physical_cancel_before_task_cancel_projection_refuses_external_proof_without_writes() {
    let (host, parent) = case();
    let id = "physical-parent-cancel";
    host.ledger
        .append(LedgerEvent {
            event_id: id.into(),
            idempotency_key: id.into(),
            turn_id: parent.execution.turn_id.clone(),
            execution_id: parent.execution.execution_id.clone(),
            cursor: 0,
            kind: LedgerEventKind::ExecutionCancelled,
            payload: json!(null),
        })
        .unwrap();
    assert_eq!(
        host.coordinator.snapshot("task").unwrap().invocations["root"].state,
        InvocationState::Running
    );
    let ledger = host.ledger.events_after(0).unwrap();
    let journal = host.coordinator.journal().read("task", 0, 1024).unwrap();
    assert!(
        result::load_consumed(
            &host.coordinator,
            &host.ledger,
            "task",
            &parent,
            &binding("child"),
            65536
        )
        .is_err()
    );
    assert_eq!(host.ledger.events_after(0).unwrap(), ledger);
    assert_eq!(
        host.coordinator.journal().read("task", 0, 1024).unwrap(),
        journal
    );
}
