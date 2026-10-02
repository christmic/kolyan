//! Journal half of ResultConsumed -> Runtime receipt/checkpoint commit gaps.
//! These domain tests do not claim Agent or Runtime end-to-end verification.
use super::*;
use crate::input_fixture::SourceFixtureAdmission;
use kolyan_ledger::{FactDraft, FactRef, FactSubject, SqliteFactJournal};

fn admitted<J: FactJournal>(journal: J) -> (TaskCoordinator<J>, AttemptBinding, ConsumedResult) {
    let coordinator = setup(journal);
    let parent = binding("root", "parent-attempt");
    coordinator
        .start_attempt("task", "parent-start", parent.clone())
        .unwrap();
    coordinator
        .admit_fixture(
            "task",
            "child-admit",
            invocation("child", Some("root"), InvocationRole::Delegation),
        )
        .unwrap();
    complete_invocation(&coordinator, "child", vec![]);
    coordinator
        .observe_attempt(
            "task",
            "parent-wait",
            observation(
                &parent,
                5,
                AttemptOutcome::Suspended {
                    waiting: TaskSuspension {
                        checkpoint_id: "parent-checkpoint".into(),
                        approval_ids: vec![],
                        external_wait_ids: vec!["child-wait".into()],
                    },
                },
            ),
        )
        .unwrap();
    let child = coordinator.snapshot("task").unwrap().invocations["child"].clone();
    let result = ConsumedResult {
        child_invocation_id: "child".into(),
        completion_fact: child.completion_fact.unwrap(),
        evidence: vec![],
    };
    (coordinator, parent, result)
}

fn committed_proof<J: FactJournal>(
    coordinator: &TaskCoordinator<J>,
    result: &ConsumedResult,
) -> FactRef {
    let state = coordinator.snapshot("task").unwrap();
    assert_eq!(
        state.invocations["root"].consumed_results.get("child"),
        Some(result)
    );
    let rows = coordinator.journal().read("task", 0, 1024).unwrap();
    let facts: Vec<_> = rows
        .iter()
        .filter(|row| row.draft.kind == "task.result_consumed")
        .collect();
    assert_eq!(facts.len(), 1);
    let row = facts[0];
    assert_eq!(row.draft.schema_version, 1);
    assert_eq!(
        row.draft.payload,
        serde_json::json!(TaskEvent::ResultConsumed {
            invocation_id: "root".into(),
            result: result.clone()
        })
    );
    assert!(row.draft.causes.contains(&result.completion_fact));
    FactRef {
        stream_id: row.stream_id.clone(),
        position: row.position,
        fact_id: row.draft.fact_id.clone(),
    }
}

fn retry_and_read<J: FactJournal>(coordinator: &TaskCoordinator<J>, result: &ConsumedResult) {
    let admitted = coordinator.journal().read("task", 0, 1024).unwrap();
    let mut foreign = result.clone();
    foreign.completion_fact.stream_id = "foreign-task".into();
    assert!(
        coordinator
            .consume_child_result("task", "foreign-before-consume", "root", foreign)
            .is_err()
    );
    assert_eq!(
        coordinator.journal().read("task", 0, 1024).unwrap(),
        admitted
    );
    coordinator
        .consume_child_result("task", "consume-once", "root", result.clone())
        .unwrap();
    let proof = committed_proof(coordinator, result);
    let before = coordinator.journal().read("task", 0, 1024).unwrap();
    for _ in 0..3 {
        // Recovery uses only these reads, not a second consume command.
        assert_eq!(committed_proof(coordinator, result), proof);
        let row = coordinator
            .journal()
            .read("task", proof.position - 1, 1)
            .unwrap();
        assert_eq!(row[0].draft.fact_id, proof.fact_id);
        assert_eq!(row[0].position, proof.position);
    }
    assert_eq!(coordinator.journal().read("task", 0, 1024).unwrap(), before);
    coordinator
        .consume_child_result("task", "consume-once", "root", result.clone())
        .unwrap();
    assert!(
        coordinator
            .consume_child_result("task", "consume-twice", "root", result.clone())
            .is_err()
    );
    let mut foreign = result.clone();
    foreign.completion_fact.stream_id = "foreign-task".into();
    assert!(
        coordinator
            .consume_child_result("task", "foreign-proof", "root", foreign)
            .is_err()
    );
    assert_eq!(coordinator.journal().read("task", 0, 1024).unwrap(), before);
}

#[test]
fn memory_consumed_gap_recovery_reads_exact_proof_without_second_consumption() {
    let (coordinator, _, result) = admitted(MemoryFactJournal::default());
    retry_and_read(&coordinator, &result);
}

#[test]
fn sqlite_consumed_gap_rebuild_retains_proof_but_cancelled_root_cannot_resume() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("facts.sqlite");
    let (coordinator, parent, result) = admitted(SqliteFactJournal::open(&path).unwrap());
    retry_and_read(&coordinator, &result);
    let original = committed_proof(&coordinator, &result);
    drop(coordinator);
    let coordinator = TaskCoordinator::new(SqliteFactJournal::open(&path).unwrap());
    assert_eq!(committed_proof(&coordinator, &result), original);
    coordinator
        .cancel_task("task", "cancel-root", "host cancelled")
        .unwrap();
    let before = coordinator.journal().read("task", 0, 1024).unwrap();
    // Historical proof remains readable; it grants no late resume authority.
    assert_eq!(committed_proof(&coordinator, &result), original);
    assert!(
        coordinator
            .resume_attempt("task", "late-resume", parent, "parent-checkpoint")
            .is_err()
    );
    assert!(
        coordinator
            .consume_child_result("task", "late-consume", "root", result)
            .is_err()
    );
    assert_eq!(
        coordinator.snapshot("task").unwrap().state,
        TaskState::Cancelled
    );
    assert_eq!(coordinator.journal().read("task", 0, 1024).unwrap(), before);
}

#[test]
fn unknown_critical_consumption_schema_cannot_be_recovered_as_valid_proof() {
    let (coordinator, _, result) = admitted(MemoryFactJournal::default());
    coordinator
        .consume_child_result("task", "consume-once", "root", result)
        .unwrap();
    let position = coordinator.snapshot("task").unwrap().position;
    coordinator
        .journal()
        .append(
            "task",
            position,
            vec![FactDraft {
                fact_id: "unknown-consume".into(),
                subject: FactSubject {
                    kind: "task.invocation".into(),
                    id: "root".into(),
                },
                kind: "task.result_consumed".into(),
                schema_version: 99,
                critical: true,
                causes: vec![],
                payload: serde_json::json!({}),
            }],
        )
        .unwrap();
    assert!(coordinator.snapshot("task").is_err());
}
