//! Call ancestry and result-dependency graphs are distinct DAGs.

use super::*;

fn consume<J: FactJournal>(coordinator: &TaskCoordinator<J>, parent: &str, child: &str) {
    let state = coordinator.snapshot("task").unwrap();
    let completed = &state.invocations[child];
    let last = &state.attempts[completed.attempts.last().unwrap()];
    let AttemptOutcome::Completed { evidence } = &last.observation.as_ref().unwrap().outcome else {
        panic!("completed child");
    };
    coordinator
        .consume_child_result(
            "task",
            &format!("consume-{parent}-{child}"),
            parent,
            ConsumedResult {
                child_invocation_id: child.into(),
                completion_fact: completed.completion_fact.clone().unwrap(),
                evidence: evidence.clone(),
            },
        )
        .unwrap();
}

#[test]
fn fanout_join_requires_exact_consumption_before_root_execution() {
    let coordinator = setup(MemoryFactJournal::default());
    coordinator
        .admit_invocation(
            "task",
            "self",
            invocation("self", Some("root"), InvocationRole::SelfCall),
        )
        .unwrap();
    let mut delegate = invocation("delegate", Some("root"), InvocationRole::Delegation);
    delegate.agent.definition_id = "different-agent".into();
    coordinator
        .admit_invocation("task", "delegate", delegate.clone())
        .unwrap();
    for child in ["self", "delegate"] {
        coordinator
            .admit_dependency("task", &format!("join-{child}"), "root", child)
            .unwrap();
    }
    assert!(
        coordinator
            .start_attempt("task", "premature-root", binding("root", "a-root"))
            .is_err()
    );
    let before = coordinator.snapshot("task").unwrap();
    assert_ne!(
        before.invocations["self"].definition.invocation_id,
        before.invocations["root"].definition.invocation_id
    );
    complete_invocation(&coordinator, "self", Vec::new());
    let mut attempt = binding("delegate", "a-delegate");
    attempt.agent = delegate.agent;
    coordinator
        .start_attempt("task", "start-delegate", attempt.clone())
        .unwrap();
    coordinator
        .observe_attempt(
            "task",
            "done-delegate",
            observation(
                &attempt,
                4,
                AttemptOutcome::Completed {
                    evidence: Vec::new(),
                },
            ),
        )
        .unwrap();
    assert_ne!(
        coordinator.snapshot("task").unwrap().state,
        TaskState::Completed
    );
    assert!(
        coordinator
            .complete_task("task", "child-not-parent", Vec::new())
            .is_err()
    );
    consume(&coordinator, "root", "self");
    assert!(
        coordinator
            .start_attempt("task", "one-of-two", binding("root", "a-root"))
            .is_err()
    );
    consume(&coordinator, "root", "delegate");
    let attempt = binding("root", "attempt-root");
    complete_invocation(&coordinator, "root", evidence(&attempt));
    assert_eq!(
        coordinator
            .complete_task("task", "all-done", evidence(&attempt))
            .unwrap()
            .state,
        TaskState::Completed
    );
}

#[test]
fn parent_already_running_cannot_complete_before_child_result_consumption() {
    let coordinator = setup(MemoryFactJournal::default());
    let root = binding("root", "a-root");
    coordinator
        .start_attempt("task", "start-root", root.clone())
        .unwrap();
    coordinator
        .admit_invocation(
            "task",
            "child",
            invocation("child", Some("root"), InvocationRole::SelfCall),
        )
        .unwrap();
    complete_invocation(&coordinator, "child", Vec::new());
    let observed = observation(
        &root,
        4,
        AttemptOutcome::Completed {
            evidence: evidence(&root),
        },
    );
    assert!(
        coordinator
            .observe_attempt("task", "before-consume", observed.clone())
            .is_err()
    );
    let state = coordinator.snapshot("task").unwrap();
    let mut wrong = ConsumedResult {
        child_invocation_id: "child".into(),
        completion_fact: state.invocations["child"].completion_fact.clone().unwrap(),
        evidence: Vec::new(),
    };
    wrong.completion_fact.fact_id = "forged".into();
    assert!(
        coordinator
            .consume_child_result("task", "forged-consumption", "root", wrong)
            .is_err()
    );
    consume(&coordinator, "root", "child");
    assert!(
        coordinator
            .observe_attempt("task", "after-consume", observed)
            .is_ok()
    );
}

#[test]
fn graph_cycles_recursion_and_shared_invocation_budgets_fail_without_writes() {
    let coordinator = setup(MemoryFactJournal::default());
    let mut child = invocation("child", Some("root"), InvocationRole::SelfCall);
    child.dependencies.push("root".into());
    coordinator
        .admit_invocation("task", "child", child)
        .unwrap();
    let before = coordinator.snapshot("task").unwrap();
    assert!(
        coordinator
            .admit_dependency("task", "cycle", "root", "child")
            .is_err()
    );
    assert!(
        coordinator
            .admit_dependency("task", "self-cycle", "child", "child")
            .is_err()
    );
    assert_eq!(coordinator.snapshot("task").unwrap(), before);
    coordinator
        .admit_invocation(
            "task",
            "grandchild",
            invocation("grandchild", Some("child"), InvocationRole::SelfCall),
        )
        .unwrap();
    coordinator
        .admit_invocation(
            "task",
            "greatgrandchild",
            invocation(
                "greatgrandchild",
                Some("grandchild"),
                InvocationRole::SelfCall,
            ),
        )
        .unwrap();
    assert!(
        coordinator
            .admit_invocation(
                "task",
                "too-deep",
                invocation(
                    "too-deep",
                    Some("greatgrandchild"),
                    InvocationRole::SelfCall
                )
            )
            .is_err()
    );
    for n in 0..4 {
        coordinator
            .admit_invocation(
                "task",
                &format!("child-{n}"),
                invocation(
                    &format!("child-{n}"),
                    Some("root"),
                    InvocationRole::SelfCall,
                ),
            )
            .unwrap();
    }
    assert!(
        coordinator
            .admit_invocation(
                "task",
                "too-many",
                invocation("too-many", Some("root"), InvocationRole::SelfCall)
            )
            .is_err()
    );
}

#[test]
fn criterion_is_bound_to_invocation_not_any_successful_child() {
    let coordinator = setup(MemoryFactJournal::default());
    coordinator
        .admit_invocation(
            "task",
            "child",
            invocation("child", Some("root"), InvocationRole::SelfCall),
        )
        .unwrap();
    let child = binding("child", "a-child");
    coordinator
        .start_attempt("task", "start-child", child.clone())
        .unwrap();
    assert!(
        coordinator
            .observe_attempt(
                "task",
                "steal-root-criterion",
                observation(
                    &child,
                    4,
                    AttemptOutcome::Completed {
                        evidence: evidence(&child)
                    }
                )
            )
            .is_err()
    );
    assert_eq!(
        coordinator.snapshot("task").unwrap().invocations["child"].state,
        InvocationState::Running
    );
}

#[test]
fn continuation_is_a_new_admission_that_consumes_completed_predecessor() {
    let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
    let mut task = definition();
    task.limits.max_depth = 0;
    task.criteria = vec![CompletionCriterion::ExecutionCompleted {
        id: "done".into(),
        invocation_id: "next".into(),
    }];
    coordinator.register_task("register", task).unwrap();
    coordinator
        .admit_invocation(
            "task",
            "root",
            invocation("root", None, InvocationRole::Root),
        )
        .unwrap();
    let mut next = invocation("next", Some("root"), InvocationRole::Continuation);
    next.dependencies = vec!["root".into()];
    assert!(
        coordinator
            .admit_invocation("task", "too-early", next.clone())
            .is_err()
    );
    complete_invocation(&coordinator, "root", Vec::new());
    let mut missing = next.clone();
    missing.dependencies.clear();
    assert!(
        coordinator
            .admit_invocation("task", "missing-edge", missing)
            .is_err()
    );
    next.agent.revision = "r2".into();
    next.constraints_digest = "e".repeat(64);
    coordinator
        .admit_invocation("task", "next", next.clone())
        .unwrap();
    let mut attempt = binding("next", "a-next");
    attempt.agent = next.agent;
    attempt.constraints_digest = next.constraints_digest;
    assert!(
        coordinator
            .start_attempt("task", "unconsumed", attempt.clone())
            .is_err()
    );
    consume(&coordinator, "next", "root");
    coordinator
        .start_attempt("task", "start-next", attempt.clone())
        .unwrap();
    let proof = evidence(&attempt);
    coordinator
        .observe_attempt(
            "task",
            "done-next",
            observation(
                &attempt,
                4,
                AttemptOutcome::Completed {
                    evidence: proof.clone(),
                },
            ),
        )
        .unwrap();
    assert_eq!(
        coordinator
            .complete_task("task", "complete", proof)
            .unwrap()
            .state,
        TaskState::Completed
    );
    let state = coordinator.snapshot("task").unwrap();
    assert_eq!(state.invocations.len(), 2);
    assert_eq!(state.attempts.len(), 2);
    assert!(state.invocations["root"].consumed_results.is_empty());
}
