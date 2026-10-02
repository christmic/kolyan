use super::*;
use crate::input_fixture::SourceFixtureAdmission;
mod historical;
mod incomplete;
use crate::{
    AgentIdentity, AttemptObservation, CancellationPolicy, CompletionCriterion, ExecutionRef,
    InvocationDefinition, InvocationRole, TaskDefinition, TaskLimits, TaskUsage,
};
use kolyan_ledger::{InMemoryLedger, MemoryFactJournal, SqliteFactJournal};
use kolyan_model::{ContentBlock, ModelRef, StopReason, TokenUsage};

fn case<J: FactJournal>(
    journal: J,
    successful: bool,
) -> (TaskCoordinator<J>, InMemoryLedger, AttemptBinding) {
    let coordinator = TaskCoordinator::new(journal);
    let agent = AgentIdentity {
        definition_id: "a".into(),
        revision: "r".into(),
        instance_id: "i".into(),
    };
    coordinator
        .register_task(
            "register",
            TaskDefinition {
                task_id: "task".into(),
                objective: "verified result".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "child".into(),
                }],
                agent: agent.clone(),
                constraints_digest: "c".repeat(64),
                limits: TaskLimits {
                    max_depth: 2,
                    max_invocations: 4,
                    max_attempts: 4,
                    max_tokens: None,
                    max_steps_per_turn: 4,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    coordinator
        .admit_fixture(
            "task",
            "invocation",
            InvocationDefinition {
                input_source: crate::input_fixture::fixture_source(
                    "task",
                    "child",
                    if (InvocationRole::Root) == crate::InvocationRole::Root {
                        crate::InvocationInputKind::Standalone
                    } else {
                        crate::InvocationInputKind::Derived
                    },
                ),
                invocation_id: "child".into(),
                agent: agent.clone(),
                constraints_digest: "c".repeat(64),
                role: InvocationRole::Root,
                parent_invocation_id: None,
                dependencies: vec![],
            },
        )
        .unwrap();
    let binding = AttemptBinding {
        input_source: crate::input_fixture::fixture_source(
            "task",
            "child",
            crate::InvocationInputKind::Standalone,
        ),
        attempt_id: "attempt".into(),
        invocation_id: "child".into(),
        execution: ExecutionRef {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        },
        agent,
        constraints_digest: "c".repeat(64),
    };
    coordinator
        .start_attempt("task", "start", binding.clone())
        .unwrap();
    let ledger = InMemoryLedger::default();
    append(
        &ledger,
        "e/execution-started",
        LedgerEventKind::ExecutionStarted,
        json!(binding.execution),
    );
    let linked = ExecutionBinding {
        task_id: "task".into(),
        invocation_id: "child".into(),
        attempt_id: "attempt".into(),
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    append(
        &ledger,
        "e/task-binding",
        LedgerEventKind::ExecutionBound,
        json!({"binding":linked}),
    );
    let response = ModelResponse {
        id: "response".into(),
        model: ModelRef::new("fixture", "model"),
        content: vec![ContentBlock::Text {
            text: "exact child result".into(),
        }],
        structured_output: None,
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::default(),
        metadata: serde_json::Value::Null,
    };
    let step = kolyan_core::StepResult {
        step_id: "step".into(),
        response: response.clone(),
        outcome: kolyan_core::StepOutcome::FinalAnswer,
    };
    append(
        &ledger,
        "step",
        LedgerEventKind::StepCompleted,
        json!({"step_id":"step","step":step}),
    );
    let terminal = append(
        &ledger,
        "terminal",
        if successful {
            LedgerEventKind::TurnCompleted
        } else {
            LedgerEventKind::TurnFailed
        },
        json!({"reason":"FinalAnswer"}),
    );
    let source = evidence::source_ref(&binding, &terminal);
    let outcome = if successful {
        AttemptOutcome::Completed {
            evidence: vec![CompletionEvidence::ExecutionResult {
                criterion_id: "done".into(),
                source: source.clone(),
                result_digest: response_digest(&response).unwrap(),
            }],
        }
    } else {
        AttemptOutcome::Failed {
            reason: terminal.payload.to_string(),
            safe_to_retry: false,
        }
    };
    coordinator
        .observe_attempt(
            "task",
            "observation",
            AttemptObservation {
                attempt_id: "attempt".into(),
                execution: binding.execution.clone(),
                source,
                usage: TaskUsage::default(),
                outcome,
            },
        )
        .unwrap();
    (coordinator, ledger, binding)
}

fn append(
    ledger: &InMemoryLedger,
    id: &str,
    kind: LedgerEventKind,
    payload: serde_json::Value,
) -> LedgerEvent {
    ledger
        .append(LedgerEvent {
            event_id: id.into(),
            idempotency_key: id.into(),
            execution_id: "e".into(),
            turn_id: "t".into(),
            cursor: 0,
            kind,
            payload,
        })
        .unwrap()
}

#[test]
fn exact_terminal_result_read_is_idempotent_and_foreign_attempt_is_rejected() {
    let (coordinator, ledger, binding) = case(MemoryFactJournal::default(), true);
    let before = coordinator.snapshot("task").unwrap();
    let facts = ledger.events_after(0).unwrap();
    let proof = load(&coordinator, &ledger, "task", &binding, 4096).unwrap();
    assert_eq!(
        load(&coordinator, &ledger, "task", &binding, 4096).unwrap(),
        proof
    );
    let VerifiedTaskOutcome::Completed {
        response,
        result_digest,
    } = &proof.outcome
    else {
        panic!("expected successful proof")
    };
    assert_eq!(result_digest, &response_digest(response).unwrap());
    let bytes = serde_json::to_vec(&proof).unwrap().len();
    assert_eq!(
        load(&coordinator, &ledger, "task", &binding, bytes).unwrap(),
        proof
    );
    assert!(load(&coordinator, &ledger, "task", &binding, bytes - 1).is_err());
    let mut foreign = binding.clone();
    foreign.execution.session_id = "foreign".into();
    assert!(load(&coordinator, &ledger, "task", &foreign, 4096).is_err());
    assert_eq!(coordinator.snapshot("task").unwrap(), before);
    assert_eq!(ledger.events_after(0).unwrap(), facts);
}

#[test]
fn failed_child_is_never_a_completed_result_proof() {
    let (coordinator, ledger, binding) = case(MemoryFactJournal::default(), false);
    let proof = load(&coordinator, &ledger, "task", &binding, 4096).unwrap();
    assert!(matches!(proof.outcome, VerifiedTaskOutcome::Failed { .. }));
    let bytes = serde_json::to_vec(&proof).unwrap().len();
    assert!(load(&coordinator, &ledger, "task", &binding, bytes - 1).is_err());
}

#[test]
fn missing_physical_binding_and_cancelled_execution_cannot_fake_child_completion() {
    let (coordinator, ledger, binding) = case(MemoryFactJournal::default(), true);
    for cancelled in [false, true] {
        let damaged = InMemoryLedger::default();
        for event in ledger.events_after(0).unwrap() {
            if !cancelled && event.kind == LedgerEventKind::ExecutionBound {
                continue;
            }
            if cancelled && event.kind == LedgerEventKind::TurnCompleted {
                append(
                    &damaged,
                    "cancelled",
                    LedgerEventKind::ExecutionCancelled,
                    json!(null),
                );
            }
            damaged.append(event).unwrap();
        }
        assert!(load(&coordinator, &damaged, "task", &binding, 4096).is_err());
    }
}

#[test]
fn sqlite_reconstruction_resolves_the_original_exact_completion_fact() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("facts.sqlite");
    let (coordinator, ledger, binding) = case(SqliteFactJournal::open(&path).unwrap(), true);
    let proof = load(&coordinator, &ledger, "task", &binding, 4096).unwrap();
    drop(coordinator);
    let rebuilt = TaskCoordinator::new(SqliteFactJournal::open(&path).unwrap());
    assert_eq!(
        load(&rebuilt, &ledger, "task", &binding, 4096).unwrap(),
        proof
    );
}
