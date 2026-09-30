//! Approval, interruption, retry, cancellation and usage evidence across restarts.

use kolyan_ledger::SqliteFactJournal;

use super::*;

#[test]
fn sqlite_approval_restart_retains_binding_and_does_not_admit_new_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    let coordinator = setup(SqliteFactJournal::open(&path).unwrap());
    let attempt = binding("root", "a1");
    coordinator
        .start_attempt("task", "start", attempt.clone())
        .unwrap();
    coordinator
        .observe_attempt(
            "task",
            "wait",
            observation(
                &attempt,
                3,
                AttemptOutcome::Suspended {
                    approval_id: "approval".into(),
                },
            ),
        )
        .unwrap();
    drop(coordinator);
    let coordinator = TaskCoordinator::new(SqliteFactJournal::open(&path).unwrap());
    let state = coordinator.snapshot("task").unwrap();
    assert_eq!(state.state, TaskState::Waiting);
    assert_eq!(
        state.waiting,
        vec![WaitingReason::Approval {
            approval_id: "approval".into()
        }]
    );
    assert!(
        coordinator
            .start_attempt("task", "rerun", binding("root", "a2"))
            .is_err()
    );
    for field in ["revision", "instance", "constraints", "execution"] {
        let mut changed = attempt.clone();
        match field {
            "revision" => changed.agent.revision = "r2".into(),
            "instance" => changed.agent.instance_id = "other".into(),
            "constraints" => changed.constraints_digest = "e".repeat(64),
            _ => changed.execution.execution_id = "other-execution".into(),
        }
        assert!(
            coordinator
                .resume_attempt("task", field, changed, "approval")
                .is_err()
        );
    }
    assert!(
        coordinator
            .resume_attempt("task", "wrong-approval", attempt.clone(), "other")
            .is_err()
    );
    coordinator
        .resume_attempt("task", "resume", attempt.clone(), "approval")
        .unwrap();
    let result = coordinator
        .observe_attempt(
            "task",
            "done",
            observation(
                &attempt,
                7,
                AttemptOutcome::Completed {
                    evidence: evidence(&attempt),
                },
            ),
        )
        .unwrap();
    assert_eq!(result.attempts.len(), 1);
    assert_eq!(result.usage.total(), Some(30));
    assert_eq!(result.invocations["root"].state, InvocationState::Completed);
}

#[test]
fn interrupted_attempt_requires_reconciliation_and_never_blind_replay() {
    let journal = MemoryFactJournal::default();
    let coordinator = setup(journal.clone());
    let attempt = binding("root", "a1");
    coordinator
        .start_attempt("task", "start", attempt.clone())
        .unwrap();
    let restarted = TaskCoordinator::new(journal);
    assert_eq!(restarted.snapshot("task").unwrap().state, TaskState::Active);
    let state = restarted
        .mark_recovery("task", "interrupted", "a1", "effect receipt absent")
        .unwrap();
    assert_eq!(state.state, TaskState::RecoveryRequired);
    assert!(
        restarted
            .start_attempt("task", "unsafe", binding("root", "a2"))
            .is_err()
    );
    assert!(
        restarted
            .resume_attempt("task", "not-approval", attempt.clone(), "approval")
            .is_err()
    );
    let reconciled = restarted
        .observe_attempt(
            "task",
            "receipt-reconciled",
            observation(
                &attempt,
                9,
                AttemptOutcome::Completed {
                    evidence: evidence(&attempt),
                },
            ),
        )
        .unwrap();
    assert_eq!(
        reconciled.invocations["root"].state,
        InvocationState::Completed
    );
    assert_eq!(reconciled.attempts.len(), 1);
}

#[test]
fn explicit_safe_retry_uses_new_attempt_and_never_reuses_execution_identity() {
    for safe in [false, true] {
        let coordinator = setup(MemoryFactJournal::default());
        let attempt = binding("root", "a1");
        coordinator
            .start_attempt("task", "start", attempt.clone())
            .unwrap();
        coordinator
            .observe_attempt(
                "task",
                "failed",
                observation(
                    &attempt,
                    4,
                    AttemptOutcome::Failed {
                        reason: "verified pre-effect failure".into(),
                        safe_to_retry: safe,
                    },
                ),
            )
            .unwrap();
        let before = coordinator.snapshot("task").unwrap();
        let mut collision = binding("root", "a2");
        collision.execution = attempt.execution;
        assert!(
            coordinator
                .start_attempt("task", "collision", collision)
                .is_err()
        );
        let retried = coordinator.start_attempt("task", "retry", binding("root", "a2"));
        assert_eq!(retried.is_ok(), safe);
        if safe {
            assert_eq!(retried.unwrap().attempts.len(), 2);
        } else {
            assert_eq!(coordinator.snapshot("task").unwrap(), before);
        }
    }
}

#[test]
fn observed_overspend_is_retained_and_unknown_usage_is_not_zero() {
    for (hard_limit, unknown) in [(Some(12), 0), (Some(1000), 1), (None, 1)] {
        let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
        let mut task = definition();
        task.limits.max_tokens = hard_limit;
        coordinator.register_task("register", task).unwrap();
        coordinator
            .admit_invocation(
                "task",
                "root",
                invocation("root", None, InvocationRole::Root),
            )
            .unwrap();
        let attempt = binding("root", "a1");
        coordinator
            .start_attempt("task", "start", attempt.clone())
            .unwrap();
        let mut observed = observation(
            &attempt,
            3,
            AttemptOutcome::Suspended {
                approval_id: "approval".into(),
            },
        );
        observed.usage.unreported_steps = unknown;
        let state = coordinator
            .observe_attempt("task", "wait", observed.clone())
            .unwrap();
        assert_eq!(state.usage, observed.usage);
        assert_eq!(
            state.state,
            if hard_limit.is_some() {
                TaskState::Failed
            } else {
                TaskState::Waiting
            }
        );
        if hard_limit.is_some() {
            assert!(
                coordinator
                    .resume_attempt("task", "resume", attempt, "approval")
                    .is_err()
            );
        }
    }
}

#[test]
fn cancellation_policy_is_explicit_and_actual_stopped_usage_remains_recordable() {
    for policy in [
        CancellationPolicy::RootOnly,
        CancellationPolicy::AllInvocations,
    ] {
        let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
        let mut task = definition();
        task.cancellation_policy = policy;
        coordinator.register_task("register", task).unwrap();
        coordinator
            .admit_invocation(
                "task",
                "root",
                invocation("root", None, InvocationRole::Root),
            )
            .unwrap();
        coordinator
            .admit_invocation(
                "task",
                "child",
                invocation("child", Some("root"), InvocationRole::Delegation),
            )
            .unwrap();
        let child = binding("child", "a-child");
        coordinator
            .start_attempt("task", "start-child", child.clone())
            .unwrap();
        let state = coordinator
            .cancel_task("task", "cancel", "user request")
            .unwrap();
        assert_eq!(state.state, TaskState::Cancelled);
        assert_eq!(
            state.invocations["child"].cancellation_requested,
            policy == CancellationPolicy::AllInvocations
        );
        assert_eq!(state.attempts["a-child"].state, InvocationState::Running);
        assert!(
            coordinator
                .admit_invocation(
                    "task",
                    "new",
                    invocation("new", Some("root"), InvocationRole::Delegation)
                )
                .is_err()
        );
        let state = coordinator
            .observe_attempt(
                "task",
                "child-stopped",
                observation(
                    &child,
                    3,
                    AttemptOutcome::Cancelled {
                        reason: "stopped by host".into(),
                    },
                ),
            )
            .unwrap();
        assert_eq!(state.state, TaskState::Cancelled);
        assert_eq!(state.usage.total(), Some(15));
    }
}

#[test]
fn root_only_cancellation_does_not_revoke_existing_child_approval_resume() {
    for policy in [
        CancellationPolicy::RootOnly,
        CancellationPolicy::AllInvocations,
    ] {
        let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
        let mut task = definition();
        task.cancellation_policy = policy;
        coordinator.register_task("register", task).unwrap();
        coordinator
            .admit_invocation(
                "task",
                "root",
                invocation("root", None, InvocationRole::Root),
            )
            .unwrap();
        coordinator
            .admit_invocation(
                "task",
                "child",
                invocation("child", Some("root"), InvocationRole::Delegation),
            )
            .unwrap();
        let child = binding("child", "a-child");
        coordinator
            .start_attempt("task", "start-child", child.clone())
            .unwrap();
        coordinator
            .observe_attempt(
                "task",
                "wait",
                observation(
                    &child,
                    3,
                    AttemptOutcome::Suspended {
                        approval_id: "approval".into(),
                    },
                ),
            )
            .unwrap();
        coordinator
            .cancel_task("task", "cancel", "user stopped root")
            .unwrap();
        let result = coordinator.resume_attempt("task", "resume", child.clone(), "approval");
        assert_eq!(result.is_ok(), policy == CancellationPolicy::RootOnly);
        if policy == CancellationPolicy::RootOnly {
            assert_eq!(result.unwrap().state, TaskState::Cancelled);
            let stopped = coordinator
                .observe_attempt(
                    "task",
                    "done",
                    observation(
                        &child,
                        7,
                        AttemptOutcome::Completed {
                            evidence: Vec::new(),
                        },
                    ),
                )
                .unwrap();
            assert_eq!(stopped.state, TaskState::Cancelled);
            assert_eq!(
                stopped.invocations["child"].state,
                InvocationState::Completed
            );
        }
    }
}

#[test]
fn approval_completion_without_explicit_resume_is_rejected() {
    let coordinator = setup(MemoryFactJournal::default());
    let attempt = binding("root", "a1");
    coordinator
        .start_attempt("task", "start", attempt.clone())
        .unwrap();
    coordinator
        .observe_attempt(
            "task",
            "wait",
            observation(
                &attempt,
                3,
                AttemptOutcome::Suspended {
                    approval_id: "approval".into(),
                },
            ),
        )
        .unwrap();
    assert!(
        coordinator
            .observe_attempt(
                "task",
                "bypass",
                observation(
                    &attempt,
                    7,
                    AttemptOutcome::Completed {
                        evidence: evidence(&attempt)
                    }
                )
            )
            .is_err()
    );
    assert_eq!(
        coordinator.snapshot("task").unwrap().invocations["root"].state,
        InvocationState::Suspended
    );
}
