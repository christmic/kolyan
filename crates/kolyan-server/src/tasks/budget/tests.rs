//! Independent domain cases, actual Memory/SQLite journals and physical JSONL.

use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

use kolyan_ledger::{FactJournal, MemoryFactJournal, SqliteFactJournal};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::input_fixture::{SourceFixtureAdmission, fixture_source};
use crate::{
    AgentIdentity, CancellationPolicy, CompletionCriterion, ExecutionRef, InvocationDefinition,
    InvocationRole, TaskCoordinator, TaskDefinition, TaskLimits,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: String,
    total: u64,
    first: u32,
    second: u32,
    accepted: bool,
    remaining: Option<u64>,
}

fn policy(total: u64) -> TaskExecutionBudgetPolicy {
    TaskExecutionBudgetPolicy {
        id: "budget".into(),
        revision: "r1".into(),
        max_reserved_steps: total,
        deadline_at_ms: 1_000_000,
    }
}

fn agent() -> AgentIdentity {
    AgentIdentity {
        definition_id: "fixture".into(),
        revision: "r1".into(),
        instance_id: "fixture".into(),
    }
}

fn setup<J: FactJournal>(coordinator: &TaskCoordinator<J>) {
    coordinator
        .register_task(
            "register",
            TaskDefinition {
                task_id: "task".into(),
                objective: "Budget actual admitted attempts".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent(),
                constraints_digest: "a".repeat(64),
                limits: TaskLimits {
                    max_depth: 2,
                    max_invocations: 4,
                    max_attempts: 4,
                    max_tokens: None,
                    max_steps_per_turn: 8,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    for id in ["root", "child"] {
        let role = if id == "root" {
            InvocationRole::Root
        } else {
            InvocationRole::SelfCall
        };
        coordinator
            .admit_fixture(
                "task",
                &format!("admit-{id}"),
                InvocationDefinition {
                    invocation_id: id.into(),
                    agent: agent(),
                    constraints_digest: "a".repeat(64),
                    role,
                    parent_invocation_id: (id != "root").then(|| "root".into()),
                    dependencies: vec![],
                    input_source: fixture_source(
                        "task",
                        id,
                        if id == "root" {
                            crate::InvocationInputKind::Standalone
                        } else {
                            crate::InvocationInputKind::Derived
                        },
                    ),
                },
            )
            .unwrap();
    }
}

fn binding<J: FactJournal>(coordinator: &TaskCoordinator<J>, id: &str) -> AttemptBinding {
    let state = coordinator.snapshot("task").unwrap();
    AttemptBinding {
        attempt_id: format!("attempt-{id}"),
        invocation_id: id.into(),
        execution: ExecutionRef {
            session_id: format!("session-{id}"),
            turn_id: format!("turn-{id}"),
            execution_id: format!("exec-{id}"),
        },
        agent: agent(),
        constraints_digest: "a".repeat(64),
        input_source: state.invocations[id].definition.input_source.clone(),
    }
}

fn run<J: FactJournal>(journal: J, case: &Case) -> Value {
    let coordinator = TaskCoordinator::new(journal);
    setup(&coordinator);
    let selected = policy(case.total);
    if !matches!(case.mode.as_str(), "late_policy" | "missing_policy")
        && !case.mode.starts_with("invalid_")
    {
        coordinator
            .configure_execution_budget("task", "policy", selected.clone())
            .unwrap();
    }
    let root = binding(&coordinator, "root");
    if case.first != 0 {
        if case.mode == "late_policy" {
            coordinator
                .start_attempt("task", "first", root.clone())
                .unwrap();
        } else {
            coordinator
                .start_budgeted_attempt("task", "first", root.clone(), case.first)
                .unwrap();
        }
    }
    if case.mode == "cancel" {
        coordinator
            .cancel_task("task", "cancel", "host cancellation")
            .unwrap();
    }
    let before = coordinator.snapshot("task").unwrap();
    let mut child = binding(&coordinator, "child");
    let result = match case.mode.as_str() {
        "reserve" | "cancel" | "missing_policy" => {
            coordinator.start_budgeted_attempt("task", "second", child, case.second)
        }
        "foreign" => {
            child.agent.revision = "foreign".into();
            coordinator.start_budgeted_attempt("task", "second", child, case.second)
        }
        "foreign_source" => {
            match &mut child.input_source {
                crate::InvocationInputSource::Derived { fact } => {
                    fact.fact_id = "missing-source".into()
                }
                _ => unreachable!(),
            }
            coordinator.start_budgeted_attempt("task", "second", child, case.second)
        }
        "invalid_total" | "invalid_deadline" | "invalid_id" | "invalid_revision" => {
            let mut invalid = selected;
            match case.mode.as_str() {
                "invalid_total" => invalid.max_reserved_steps = 0,
                "invalid_deadline" => invalid.deadline_at_ms = 0,
                "invalid_id" => invalid.id = " ".into(),
                "invalid_revision" => invalid.revision = "x".repeat(257),
                _ => unreachable!(),
            }
            coordinator.configure_execution_budget("task", "invalid-policy", invalid)
        }
        "duplicate" => coordinator.start_budgeted_attempt("task", "second", root, case.second),
        "bypass" => coordinator.start_attempt("task", "second", child),
        "changed_policy" => {
            let mut changed = selected;
            changed.revision = "r2".into();
            coordinator.configure_execution_budget("task", "policy", changed)
        }
        "second_policy" | "late_policy" => {
            coordinator.configure_execution_budget("task", "second-policy", selected)
        }
        "same_policy" => coordinator.configure_execution_budget("task", "policy", selected),
        other => panic!("unknown data operation {other}"),
    };
    let after = coordinator.snapshot("task").unwrap();
    let error = result.as_ref().err().map(ToString::to_string);
    json!({"id":case.id,"accepted":result.is_ok(),"error":error,"before":before,"after":after,
        "remaining":after.execution_budget.as_ref().map(|b| b.remaining_steps().unwrap()),
        "facts":coordinator.journal().read("task",0,100).unwrap()})
}

#[test]
fn execution_budget_data_matrix_memory_and_reopened_sqlite() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    for sqlite in [false, true] {
        let root = tempfile::Builder::new()
            .prefix("kolyan-task-budget-")
            .tempdir()
            .unwrap()
            .keep();
        let path = root.join("actual.jsonl");
        let mut output = File::create(&path).unwrap();
        for case in &cases {
            let actual = if sqlite {
                let db = root.join(format!("{}.sqlite", case.id));
                let mut row = run(SqliteFactJournal::open(&db).unwrap(), case);
                let rebuilt = TaskCoordinator::new(SqliteFactJournal::open(db).unwrap())
                    .snapshot("task")
                    .unwrap();
                row["rebuilt"] = serde_json::to_value(rebuilt).unwrap();
                row
            } else {
                run(MemoryFactJournal::default(), case)
            };
            writeln!(output, "{actual}").unwrap();
        }
        output.flush().unwrap();
        output.sync_all().unwrap();
        drop(output);
        println!("TASK_BUDGET_ACTUAL={}", path.display());
        let actual: Vec<Value> = BufReader::new(File::open(path).unwrap())
            .lines()
            .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
            .collect();
        assert_eq!(actual.len(), cases.len());
        for (row, case) in actual.iter().zip(&cases) {
            assert_eq!(row["id"], case.id);
            if sqlite {
                assert_eq!(row["rebuilt"], row["after"], "{}", case.id);
            }
            assert_eq!(row["accepted"], case.accepted, "{}: {row}", case.id);
            assert_eq!(row["remaining"], json!(case.remaining), "{}", case.id);
            if !case.accepted {
                assert_eq!(row["before"], row["after"], "{}", case.id);
            }
        }
    }
}

/// Synchronize at the real journal CAS, after both commands validated the same prefix.
struct RacingJournal<J> {
    inner: J,
    barrier: std::sync::Barrier,
}

impl<J: FactJournal> FactJournal for RacingJournal<J> {
    fn read(
        &self,
        stream: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        self.inner.read(stream, after, limit)
    }

    fn append(
        &self,
        stream: &str,
        expected: u64,
        batch: Vec<kolyan_ledger::FactDraft>,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        if batch
            .iter()
            .any(|draft| draft.kind == "task.budgeted_attempt_started")
        {
            self.barrier.wait();
        }
        self.inner.append(stream, expected, batch)
    }
}

struct MissingPolicyCause<J>(J);

impl<J: FactJournal> FactJournal for MissingPolicyCause<J> {
    fn read(
        &self,
        stream: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        let mut records = self.0.read(stream, after, limit)?;
        for record in &mut records {
            if record.draft.kind == "task.budgeted_attempt_started" {
                record
                    .draft
                    .causes
                    .retain(|cause| cause.fact_id != "policy");
            }
        }
        Ok(records)
    }

    fn append(
        &self,
        _: &str,
        _: u64,
        _: Vec<kolyan_ledger::FactDraft>,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        panic!("read-only corruption probe must never append")
    }
}

fn race<J: FactJournal + Clone>(journal: J) -> Value {
    let setup_coordinator = TaskCoordinator::new(journal.clone());
    setup(&setup_coordinator);
    setup_coordinator
        .configure_execution_budget("task", "policy", policy(4))
        .unwrap();
    let root = binding(&setup_coordinator, "root");
    let child = binding(&setup_coordinator, "child");
    let coordinator = TaskCoordinator::new(RacingJournal {
        inner: journal.clone(),
        barrier: std::sync::Barrier::new(2),
    });
    let (first, second) = std::thread::scope(|scope| {
        let first =
            scope.spawn(|| coordinator.start_budgeted_attempt("task", "racing-root", root, 4));
        let second =
            scope.spawn(|| coordinator.start_budgeted_attempt("task", "racing-child", child, 4));
        (first.join().unwrap(), second.join().unwrap())
    });
    let snapshot = setup_coordinator.snapshot("task").unwrap();
    let facts = journal.read("task", 0, 100).unwrap();
    let corrupted = TaskCoordinator::new(MissingPolicyCause(journal.clone())).snapshot("task");
    json!({"accepted":[first.is_ok(),second.is_ok()],
        "errors":[first.err().map(|e|e.to_string()),second.err().map(|e|e.to_string())],
        "after":snapshot,"facts":facts,"remaining":snapshot.execution_budget.as_ref().unwrap().remaining_steps().unwrap(),
        "tampered_replay_rejected":corrupted.is_err(),"tampered_error":corrupted.err().map(|e|e.to_string()),
        "unchanged":journal.read("task",0,100).unwrap()==facts})
}

#[test]
fn same_prefix_cas_cannot_double_spend_and_replay_requires_policy_cause() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-task-budget-race-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    let memory = race(MemoryFactJournal::default());
    writeln!(output, "{memory}").unwrap();
    let db = root.join("race.sqlite");
    let mut sqlite = race(SqliteFactJournal::open(&db).unwrap());
    sqlite["rebuilt"] = serde_json::to_value(
        TaskCoordinator::new(SqliteFactJournal::open(db).unwrap())
            .snapshot("task")
            .unwrap(),
    )
    .unwrap();
    writeln!(output, "{sqlite}").unwrap();
    output.sync_all().unwrap();
    drop(output);
    println!("TASK_BUDGET_RACE_ACTUAL={}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(
            row["accepted"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|accepted| **accepted == json!(true))
                .count(),
            1,
            "{row}"
        );
        assert_eq!(
            row["after"]["execution_budget"]["reservations"]
                .as_object()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(row["remaining"], 0);
        assert_eq!(row["tampered_replay_rejected"], true, "{row}");
        assert_eq!(row["unchanged"], true);
    }
    assert_eq!(rows[1]["rebuilt"], rows[1]["after"]);
}
