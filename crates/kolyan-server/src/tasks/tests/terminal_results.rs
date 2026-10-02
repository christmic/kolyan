//! Terminal feedback does not replace successful completion evidence.
use super::*;
use crate::input_fixture::SourceFixtureAdmission;
use kolyan_ledger::SqliteFactJournal;

fn terminal<J: FactJournal>(
    journal: J,
    cancelled: bool,
) -> (TaskCoordinator<J>, ConsumedTerminalResult) {
    let coordinator = setup(journal);
    coordinator
        .start_attempt("task", "parent-start", binding("root", "parent"))
        .unwrap();
    coordinator
        .admit_fixture(
            "task",
            "child-admit",
            invocation("child", Some("root"), InvocationRole::Delegation),
        )
        .unwrap();
    let child = binding("child", "child-attempt");
    coordinator
        .start_attempt("task", "child-start", child.clone())
        .unwrap();
    let reason = "child stopped\u{0000}exactly".to_owned();
    let disposition = if cancelled {
        TerminalResultDisposition::Cancelled {
            reason: reason.clone(),
        }
    } else {
        TerminalResultDisposition::Failed {
            reason: reason.clone(),
        }
    };
    let outcome = if cancelled {
        AttemptOutcome::Cancelled { reason }
    } else {
        AttemptOutcome::Failed {
            reason,
            safe_to_retry: false,
        }
    };
    let stopped = observation(&child, 4, outcome);
    let state = coordinator
        .observe_attempt("task", "child-terminal", stopped.clone())
        .unwrap();
    let result = ConsumedTerminalResult {
        parent: binding("root", "parent"),
        child,
        terminal_fact: state.invocations["child"].terminal_fact.clone().unwrap(),
        source: stopped.source,
        disposition,
    };
    (coordinator, result)
}

fn verify<J: FactJournal>(coordinator: &TaskCoordinator<J>, result: ConsumedTerminalResult) {
    let before = coordinator.journal().read("task", 0, 1024).unwrap();
    for forged in [0, 1, 2, 3, 4] {
        let mut wrong = result.clone();
        match forged {
            0 => wrong.terminal_fact.stream_id = "foreign".into(),
            1 => wrong.child.execution.execution_id = "foreign".into(),
            2 => wrong.source.cursor += 1,
            4 => wrong.parent.execution.session_id = "foreign-parent".into(),
            _ => wrong.disposition = TerminalResultDisposition::Completed { evidence: vec![] },
        }
        assert!(
            coordinator
                .consume_terminal_result("task", "forged", "root", wrong)
                .is_err()
        );
    }
    assert_eq!(coordinator.journal().read("task", 0, 1024).unwrap(), before);
    let state = coordinator
        .consume_terminal_result("task", "consume", "root", result.clone())
        .unwrap();
    assert_eq!(
        state.invocations["root"].terminal_consumed_results["child"],
        result
    );
    assert!(state.invocations["child"].completion_fact.is_none());
    assert!(
        !state
            .waiting
            .iter()
            .any(|wait| matches!(wait, WaitingReason::ChildResults { .. }))
    );
    let rows = coordinator.journal().read("task", 0, 1024).unwrap();
    let consumed = rows.last().unwrap();
    assert_eq!(consumed.draft.kind, "task.terminal_result_consumed");
    assert!(consumed.draft.causes.contains(&result.terminal_fact));
    coordinator
        .consume_terminal_result("task", "consume", "root", result.clone())
        .unwrap();
    assert!(
        coordinator
            .consume_terminal_result("task", "consume-again", "root", result.clone())
            .is_err()
    );
    assert!(
        coordinator
            .consume_child_result(
                "task",
                "fake-success",
                "root",
                ConsumedResult {
                    child_invocation_id: "child".into(),
                    completion_fact: result.terminal_fact,
                    evidence: vec![]
                }
            )
            .is_err()
    );
    assert_eq!(coordinator.journal().read("task", 0, 1024).unwrap(), rows);
    let parent = binding("root", "parent");
    let proof = evidence(&parent);
    coordinator
        .observe_attempt(
            "task",
            "parent-reports-failure",
            observation(
                &parent,
                5,
                AttemptOutcome::Completed {
                    evidence: proof.clone(),
                },
            ),
        )
        .unwrap();
    assert!(
        coordinator
            .complete_task("task", "false-task-success", proof)
            .is_err()
    );
}

#[test]
fn terminal_results_unknown_schema_and_cancelled_parent_fail_closed() {
    use kolyan_ledger::{FactDraft, FactSubject};
    let (coordinator, result) = terminal(MemoryFactJournal::default(), false);
    coordinator
        .cancel_task("task", "cancel-parent", "host cancelled")
        .unwrap();
    let before = coordinator.journal().read("task", 0, 1024).unwrap();
    assert!(
        coordinator
            .consume_terminal_result("task", "late", "root", result)
            .is_err()
    );
    assert_eq!(coordinator.journal().read("task", 0, 1024).unwrap(), before);
    let (coordinator, result) = terminal(MemoryFactJournal::default(), true);
    let position = coordinator.snapshot("task").unwrap().position;
    coordinator
        .journal()
        .append(
            "task",
            position,
            vec![FactDraft {
                fact_id: "unknown-terminal-schema".into(),
                subject: FactSubject {
                    kind: "task.invocation".into(),
                    id: "root".into(),
                },
                kind: "task.terminal_result_consumed".into(),
                schema_version: 99,
                critical: true,
                causes: vec![result.terminal_fact.clone()],
                payload: serde_json::json!(TaskEvent::TerminalResultConsumed {
                    invocation_id: "root".into(),
                    result: Box::new(result)
                }),
            }],
        )
        .unwrap();
    assert!(coordinator.snapshot("task").is_err());
}

#[test]
fn memory_failed_and_cancelled_dispositions_are_exact_feedback_not_success() {
    for cancelled in [false, true] {
        let (coordinator, result) = terminal(MemoryFactJournal::default(), cancelled);
        verify(&coordinator, result);
    }
}

#[test]
fn sqlite_rebuild_preserves_failed_and_cancelled_terminal_consumption() {
    for cancelled in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("journal.sqlite");
        let (coordinator, result) = terminal(SqliteFactJournal::open(&path).unwrap(), cancelled);
        verify(&coordinator, result.clone());
        let saved = coordinator.snapshot("task").unwrap();
        drop(coordinator);
        let rebuilt = TaskCoordinator::new(SqliteFactJournal::open(&path).unwrap());
        assert_eq!(rebuilt.snapshot("task").unwrap(), saved);
        assert_eq!(
            saved.invocations["root"].terminal_consumed_results["child"],
            result
        );
    }
}
