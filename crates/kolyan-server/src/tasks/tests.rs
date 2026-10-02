//! Deterministic task-domain scenarios; no model or execution mocks in production.

use crate::input_fixture::SourceFixtureAdmission;
mod budgets;
mod consume_gap;
mod recovery;
mod retry;
mod suspension;
mod terminal_results;
mod topology;
mod validation;

use kolyan_ledger::{FactJournal, MemoryFactJournal};

use super::*;
use crate::ExecutionRef;

fn definition() -> TaskDefinition {
    TaskDefinition {
        task_id: "task".into(),
        objective: "Produce a verified final result".into(),
        criteria: vec![CompletionCriterion::ExecutionCompleted {
            id: "done".into(),
            invocation_id: "root".into(),
        }],
        agent: agent(),
        constraints_digest: "c".repeat(64),
        limits: TaskLimits {
            max_depth: 3,
            max_invocations: 8,
            max_attempts: 12,
            max_tokens: Some(1000),
            max_steps_per_turn: 8,
        },
        cancellation_policy: CancellationPolicy::AllInvocations,
    }
}

fn agent() -> AgentIdentity {
    AgentIdentity {
        definition_id: "agent".into(),
        revision: "r1".into(),
        instance_id: "instance".into(),
    }
}

fn invocation(id: &str, parent: Option<&str>, role: InvocationRole) -> InvocationDefinition {
    InvocationDefinition {
        input_source: crate::input_fixture::fixture_source(
            "task",
            id,
            if (role) == crate::InvocationRole::Root {
                crate::InvocationInputKind::Standalone
            } else {
                crate::InvocationInputKind::Derived
            },
        ),
        invocation_id: id.into(),
        agent: agent(),
        constraints_digest: "c".repeat(64),
        role,
        parent_invocation_id: parent.map(str::to_owned),
        dependencies: Vec::new(),
    }
}

fn binding(invocation_id: &str, attempt_id: &str) -> AttemptBinding {
    AttemptBinding {
        input_source: crate::input_fixture::fixture_source(
            "task",
            invocation_id,
            if (invocation_id) == "root" {
                crate::InvocationInputKind::Standalone
            } else {
                crate::InvocationInputKind::Derived
            },
        ),
        attempt_id: attempt_id.into(),
        invocation_id: invocation_id.into(),
        execution: ExecutionRef {
            session_id: format!("session-{invocation_id}"),
            turn_id: format!("turn-{attempt_id}"),
            execution_id: format!("exec-{attempt_id}"),
        },
        agent: agent(),
        constraints_digest: "c".repeat(64),
    }
}

fn source(binding: &AttemptBinding, cursor: u64) -> ExecutionEvidence {
    ExecutionEvidence {
        execution: binding.execution.clone(),
        event_id: format!("{}-{cursor}", binding.execution.execution_id),
        cursor,
    }
}

fn observation(
    binding: &AttemptBinding,
    cursor: u64,
    outcome: AttemptOutcome,
) -> AttemptObservation {
    AttemptObservation {
        attempt_id: binding.attempt_id.clone(),
        execution: binding.execution.clone(),
        source: source(binding, cursor),
        usage: TaskUsage {
            input_tokens: 10,
            output_tokens: 5,
            unreported_steps: 0,
        },
        outcome,
    }
}

fn evidence(binding: &AttemptBinding) -> Vec<CompletionEvidence> {
    vec![CompletionEvidence::ExecutionResult {
        criterion_id: "done".into(),
        source: source(binding, 3),
        result_digest: "d".repeat(64),
    }]
}

fn setup<J: FactJournal>(journal: J) -> TaskCoordinator<J> {
    let coordinator = TaskCoordinator::new(journal);
    coordinator.register_task("register", definition()).unwrap();
    coordinator
        .admit_fixture(
            "task",
            "admit-root",
            invocation("root", None, InvocationRole::Root),
        )
        .unwrap();
    coordinator
}

fn complete_invocation<J: FactJournal>(
    coordinator: &TaskCoordinator<J>,
    id: &str,
    proof: Vec<CompletionEvidence>,
) -> AttemptBinding {
    let attempt = binding(id, &format!("attempt-{id}"));
    coordinator
        .start_attempt("task", &format!("start-{id}"), attempt.clone())
        .unwrap();
    coordinator
        .observe_attempt(
            "task",
            &format!("complete-{id}"),
            observation(&attempt, 4, AttemptOutcome::Completed { evidence: proof }),
        )
        .unwrap();
    attempt
}

#[test]
fn empty_query_and_task_completion_require_real_bound_evidence() {
    let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
    assert!(matches!(
        coordinator.snapshot("task"),
        Err(TaskError::NotFound(_))
    ));
    let coordinator = setup(coordinator.journal().clone());
    let attempt = binding("root", "attempt-root");
    assert!(
        coordinator
            .complete_task("task", "premature", evidence(&attempt))
            .is_err()
    );
    let proof = evidence(&attempt);
    complete_invocation(&coordinator, "root", proof.clone());
    assert!(
        coordinator
            .complete_task("task", "without-proof", Vec::new())
            .is_err()
    );
    let mut fabricated = proof.clone();
    if let CompletionEvidence::ExecutionResult { result_digest, .. } = &mut fabricated[0] {
        *result_digest = "e".repeat(64);
    }
    assert!(
        coordinator
            .complete_task("task", "fabricated", fabricated)
            .is_err()
    );
    let result = coordinator
        .complete_task("task", "task-done", proof.clone())
        .unwrap();
    assert_eq!(result.state, TaskState::Completed);
    assert_eq!(result.success_evidence, proof);
    assert_eq!(result.usage.total(), Some(15));
    assert_eq!(coordinator.snapshot("task").unwrap(), result);
}
