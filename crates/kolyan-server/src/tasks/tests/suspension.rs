//! Mixed waits are projections of one checkpoint and never reset admission.
use super::*;
use kolyan_ledger::SqliteFactJournal;

fn mixed(approvals: &[&str], waits: &[&str]) -> AttemptOutcome {
    AttemptOutcome::Suspended {
        waiting: TaskSuspension {
            checkpoint_id: "checkpoint".into(),
            approval_ids: approvals.iter().map(|id| (*id).into()).collect(),
            external_wait_ids: waits.iter().map(|id| (*id).into()).collect(),
        },
    }
}

fn partial<J: FactJournal>(coordinator: TaskCoordinator<J>) {
    let attempt = binding("root", "a1");
    coordinator
        .start_attempt("task", "start", attempt.clone())
        .unwrap();
    let before = coordinator
        .observe_attempt(
            "task",
            "mixed",
            observation(&attempt, 3, mixed(&["approval"], &["wait-a", "wait-b"])),
        )
        .unwrap();
    assert_eq!(before.state, TaskState::Waiting);
    assert_eq!(before.usage.total(), Some(15));
    let mut observation = observation(&attempt, 4, mixed(&[], &["wait-b"]));
    observation.usage = TaskUsage::default();
    let partial = coordinator
        .observe_attempt("task", "partial", observation.clone())
        .unwrap();
    assert_eq!(partial.attempts.len(), 1);
    assert_eq!(partial.attempts["a1"].binding, attempt);
    assert_eq!(partial.usage, before.usage);
    assert_eq!(
        partial.waiting,
        vec![WaitingReason::Suspension(TaskSuspension {
            checkpoint_id: "checkpoint".into(),
            approval_ids: vec![],
            external_wait_ids: vec!["wait-b".into()]
        })]
    );
    assert_eq!(
        coordinator
            .observe_attempt("task", "partial", observation)
            .unwrap(),
        partial
    );
    assert!(
        coordinator
            .resume_attempt("task", "foreign", attempt.clone(), "foreign-checkpoint")
            .is_err()
    );
    let resumed = coordinator
        .resume_attempt("task", "resume", attempt, "checkpoint")
        .unwrap();
    assert_eq!(resumed.attempts.len(), 1);
    assert_eq!(resumed.usage, before.usage);
    assert_eq!(resumed.invocations["root"].state, InvocationState::Running);
}

#[test]
fn memory_partial_merge_retains_attempt_and_budget() {
    partial(setup(MemoryFactJournal::default()));
}

#[test]
fn sqlite_partial_merge_retains_attempt_and_budget() {
    let directory = tempfile::tempdir().unwrap();
    partial(setup(
        SqliteFactJournal::open(directory.path().join("journal.sqlite")).unwrap(),
    ));
}

#[test]
fn empty_or_duplicate_waits_are_rejected_without_charging_usage() {
    for outcome in [
        mixed(&[], &[]),
        mixed(&["duplicate", "duplicate"], &[]),
        mixed(&[], &["duplicate", "duplicate"]),
    ] {
        let coordinator = setup(MemoryFactJournal::default());
        let attempt = binding("root", "a1");
        let before = coordinator
            .start_attempt("task", "start", attempt.clone())
            .unwrap();
        assert!(
            coordinator
                .observe_attempt("task", "invalid", observation(&attempt, 3, outcome))
                .is_err()
        );
        assert_eq!(coordinator.snapshot("task").unwrap(), before);
    }
}
