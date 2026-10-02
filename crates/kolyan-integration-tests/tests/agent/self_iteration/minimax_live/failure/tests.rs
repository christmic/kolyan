//! Developer-owned regressions; none mutate the model candidate or old evidence.

use kolyan_ledger::{FactJournal, SqliteFactJournal};
use kolyan_server::{
    AgentIdentity, CancellationPolicy, CompletionCriterion, TaskCoordinator, TaskDefinition,
    TaskLimits,
};
use serde_json::{Value, json};

use super::{close, record};
use crate::evidence::Evidence;

fn definition() -> TaskDefinition {
    TaskDefinition {
        task_id: "feedback-task".into(),
        objective: "Independently verify a candidate".into(),
        criteria: vec![CompletionCriterion::ExecutionCompleted {
            id: "review".into(),
            invocation_id: "root".into(),
        }],
        agent: AgentIdentity {
            definition_id: "feedback-agent".into(),
            revision: "r1".into(),
            instance_id: "instance".into(),
        },
        constraints_digest: "c".repeat(64),
        limits: TaskLimits {
            max_depth: 0,
            max_invocations: 1,
            max_attempts: 1,
            max_tokens: None,
            max_steps_per_turn: 1,
        },
        cancellation_policy: CancellationPolicy::AllInvocations,
    }
}

#[test]
fn rejection_closure_exports_all_cases_then_checks_rebuild_and_idempotency() {
    let cases: Value = serde_json::from_str(include_str!("cases.json")).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut rows = Vec::new();
    for case in cases["cases"].as_array().unwrap() {
        let path = directory
            .path()
            .join(format!("{}.sqlite", case["id"].as_str().unwrap()));
        let journal = SqliteFactJournal::open(&path).unwrap();
        let coordinator = TaskCoordinator::new(journal.clone());
        if case["admitted"] == true {
            coordinator
                .register_task("registered", definition())
                .unwrap();
        }
        if case["cancelled"] == true {
            coordinator
                .cancel_task("feedback-task", "cancelled", "human cancelled")
                .unwrap();
        }
        let before = journal.read("feedback-task", 0, 100).unwrap();
        let result = close(
            &coordinator,
            "feedback-task",
            "independent format check failed",
        )
        .unwrap();
        let first = journal.read("feedback-task", 0, 100).unwrap();
        let reopened = TaskCoordinator::new(SqliteFactJournal::open(&path).unwrap());
        let duplicate = close(
            &reopened,
            "feedback-task",
            "independent format check failed",
        )
        .unwrap();
        rows.push(json!({"case":case,"result":result,"duplicate":duplicate,
            "before":before,"after":first,"after_duplicate":journal.read("feedback-task",0,100).unwrap(),
            "state":reopened.snapshot("feedback-task").ok().map(|snapshot|snapshot.state)}));
    }
    let path = directory.path().join("actual.jsonl");
    let evidence = Evidence::new(&path);
    for row in &rows {
        evidence.append(row.clone()).unwrap();
    }
    drop(evidence);
    let rows = super::super::tests::read_rows(&path);
    println!("SELF_CLOSURE_TRACE={}", path.display());
    for row in rows {
        assert_eq!(row["result"]["outcome"], row["case"]["outcome"]);
        assert_eq!(row["state"], row["case"]["state"]);
        assert_eq!(row["after"], row["after_duplicate"]);
        let delta =
            row["after"].as_array().unwrap().len() - row["before"].as_array().unwrap().len();
        assert_eq!(delta as u64, row["case"]["new_facts"].as_u64().unwrap());
    }
}

#[test]
fn rejection_before_admission_records_reason_without_creating_state() {
    let directory = tempfile::tempdir().unwrap();
    let evidence = Evidence::new(&directory.path().join("actual.jsonl"));
    record(
        directory.path(),
        "absent-task",
        "baseline mismatch",
        &evidence,
    )
    .unwrap();
    let observations = evidence.rows();
    println!("{}", serde_json::to_string(&observations).unwrap());
    assert!(!directory.path().join("state").exists());
    assert_eq!(observations[0]["original_error"], "baseline mismatch");
    assert_eq!(observations[0]["result"]["Ok"]["outcome"], "not_admitted");
    assert_eq!(observations[0]["execution_outcomes_synthesized"], false);
}

#[test]
fn closure_failure_is_recorded_separately_without_hiding_original_rejection() {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    std::fs::create_dir(&state).unwrap();
    std::fs::write(state.join("ledger.sqlite"), "corrupt retained journal").unwrap();
    let evidence = Evidence::new(&directory.path().join("actual.jsonl"));
    let result = record(
        directory.path(),
        "feedback-task",
        "review receipt missing",
        &evidence,
    );
    let observations = evidence.rows();
    println!("{}", serde_json::to_string(&observations).unwrap());
    assert!(result.is_err());
    assert_eq!(observations[0]["original_error"], "review receipt missing");
    assert!(observations[0]["result"]["Err"].is_string());
    assert_eq!(observations[0]["execution_outcomes_synthesized"], false);
    assert_eq!(
        std::fs::read_to_string(state.join("ledger.sqlite")).unwrap(),
        "corrupt retained journal"
    );
}
