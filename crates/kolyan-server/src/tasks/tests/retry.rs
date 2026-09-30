//! Retry authorization is a new fact, never a rewrite or implicit effect replay.

use kolyan_ledger::SqliteFactJournal;

use super::*;

fn stopped<J: FactJournal>(coordinator: &TaskCoordinator<J>, recovering: bool) -> AttemptBinding {
    let attempt = binding("root", "a1");
    coordinator
        .start_attempt("task", "start", attempt.clone())
        .unwrap();
    let outcome = if recovering {
        AttemptOutcome::RecoveryRequired {
            reason: "Started without receipt".into(),
        }
    } else {
        AttemptOutcome::Failed {
            reason: "host has not authorized retry".into(),
            safe_to_retry: false,
        }
    };
    coordinator
        .observe_attempt("task", "stopped", observation(&attempt, 4, outcome))
        .unwrap();
    attempt
}

#[test]
fn authorization_preserves_original_observation_and_usage_for_failed_or_uncertain_attempt() {
    for recovering in [false, true] {
        let coordinator = setup(MemoryFactJournal::default());
        let attempt = stopped(&coordinator, recovering);
        let before = coordinator.snapshot("task").unwrap();
        assert!(!before.attempts["a1"].retry_authorized);
        assert!(
            coordinator
                .start_attempt("task", "premature", binding("root", "a2"))
                .is_err()
        );
        let after = coordinator
            .authorize_retry(
                "task",
                "retry-authorized",
                "a1",
                source(&attempt, 5),
                "verified NotCommitted reconciliation",
            )
            .unwrap();
        assert!(after.attempts["a1"].retry_authorized);
        assert_eq!(
            after.attempts["a1"].observation,
            before.attempts["a1"].observation
        );
        assert_eq!(after.usage, before.usage);
        assert_eq!(after.invocations["root"].state, InvocationState::Failed);
        assert_eq!(
            coordinator
                .authorize_retry(
                    "task",
                    "retry-authorized",
                    "a1",
                    source(&attempt, 5),
                    "verified NotCommitted reconciliation"
                )
                .unwrap(),
            after
        );
        assert!(
            coordinator
                .authorize_retry(
                    "task",
                    "duplicate-decision",
                    "a1",
                    source(&attempt, 5),
                    "verified NotCommitted reconciliation"
                )
                .is_err()
        );
        assert!(
            coordinator
                .start_attempt("task", "reuse-old-attempt", attempt.clone())
                .is_err()
        );
        let mut execution_collision = binding("root", "a2");
        execution_collision.execution = attempt.execution.clone();
        assert!(
            coordinator
                .start_attempt("task", "reuse-old-execution", execution_collision)
                .is_err()
        );
        let started = coordinator
            .start_attempt("task", "new-attempt", binding("root", "a2"))
            .unwrap();
        assert_eq!(started.attempts.len(), 2);
        assert!(!started.attempts["a2"].retry_authorized);
        assert_eq!(
            started.attempts["a1"].observation,
            before.attempts["a1"].observation
        );
        assert!(
            coordinator
                .authorize_retry(
                    "task",
                    "older-attempt",
                    "a1",
                    source(&attempt, 6),
                    "late evidence"
                )
                .is_err()
        );
    }
}

#[test]
fn retry_authorization_requires_fresh_source_bound_to_original_execution() {
    let coordinator = setup(MemoryFactJournal::default());
    let attempt = stopped(&coordinator, false);
    let before = coordinator.snapshot("task").unwrap();
    for cursor in [0, 3, 4] {
        assert!(
            coordinator
                .authorize_retry(
                    "task",
                    &format!("stale-{cursor}"),
                    "a1",
                    source(&attempt, cursor),
                    "verified reconciliation"
                )
                .is_err()
        );
    }
    for field in ["execution", "session", "turn"] {
        let mut wrong = source(&attempt, 5);
        match field {
            "execution" => wrong.execution.execution_id = "foreign".into(),
            "session" => wrong.execution.session_id = "foreign".into(),
            _ => wrong.execution.turn_id = "foreign".into(),
        }
        assert!(
            coordinator
                .authorize_retry("task", field, "a1", wrong, "verified reconciliation")
                .is_err()
        );
    }
    assert!(
        coordinator
            .authorize_retry(
                "task",
                "missing-attempt",
                "absent",
                source(&attempt, 5),
                "verified reconciliation"
            )
            .is_err()
    );
    assert!(
        coordinator
            .authorize_retry("task", "missing-reason", "a1", source(&attempt, 5), "")
            .is_err()
    );
    assert_eq!(coordinator.snapshot("task").unwrap(), before);
}

#[test]
fn terminal_task_or_running_attempt_cannot_be_authorized_for_retry() {
    let coordinator = setup(MemoryFactJournal::default());
    let attempt = binding("root", "a1");
    coordinator
        .start_attempt("task", "start", attempt.clone())
        .unwrap();
    assert!(
        coordinator
            .authorize_retry("task", "running", "a1", source(&attempt, 5), "not stopped")
            .is_err()
    );
    for cancelled in [false, true] {
        let coordinator = setup(MemoryFactJournal::default());
        let attempt = stopped(&coordinator, false);
        if cancelled {
            coordinator
                .cancel_task("task", "cancel", "user cancellation")
                .unwrap();
        } else {
            coordinator
                .fail_task("task", "fail", "task failure")
                .unwrap();
        }
        let before = coordinator.snapshot("task").unwrap();
        assert!(
            coordinator
                .authorize_retry(
                    "task",
                    "terminal",
                    "a1",
                    source(&attempt, 5),
                    "verified reconciliation"
                )
                .is_err()
        );
        assert_eq!(coordinator.snapshot("task").unwrap(), before);
    }
}

#[test]
fn sqlite_replay_retains_authorization_and_original_failure_before_new_attempt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retry.db");
    let coordinator = setup(SqliteFactJournal::open(&path).unwrap());
    let attempt = stopped(&coordinator, true);
    let original = coordinator.snapshot("task").unwrap().attempts["a1"]
        .observation
        .clone();
    let authorized = coordinator
        .authorize_retry(
            "task",
            "safe-retry",
            "a1",
            source(&attempt, 5),
            "verified external reconciliation",
        )
        .unwrap();
    drop(coordinator);
    let coordinator = TaskCoordinator::new(SqliteFactJournal::open(&path).unwrap());
    assert_eq!(coordinator.snapshot("task").unwrap(), authorized);
    assert_eq!(
        coordinator.snapshot("task").unwrap().attempts["a1"].observation,
        original
    );
    let restarted = coordinator
        .start_attempt("task", "retry-start", binding("root", "a2"))
        .unwrap();
    assert_eq!(restarted.invocations["root"].attempts, ["a1", "a2"]);
    assert_eq!(restarted.attempts["a1"].observation, original);
}
