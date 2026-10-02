//! Fixture-owned boundary values for shared governance limits.

use crate::input_fixture::SourceFixtureAdmission;
use serde::Deserialize;

use super::*;

#[derive(Deserialize)]
struct BudgetCase {
    name: String,
    operation: String,
    ceiling: u64,
    admitted: bool,
}

#[test]
fn fixture_boundaries_reject_without_partial_writes() {
    let cases: Vec<BudgetCase> =
        serde_json::from_str(include_str!("fixtures/budgets.json")).unwrap();
    assert_eq!(cases.len(), 8);
    for case in cases {
        let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
        let mut task = definition();
        match case.operation.as_str() {
            "recursion" => task.limits.max_depth = case.ceiling as u32,
            "invocations" => task.limits.max_invocations = case.ceiling,
            "attempts" => task.limits.max_attempts = case.ceiling,
            "tokens" => task.limits.max_tokens = Some(case.ceiling),
            other => panic!("unrecognized budget operation {other}"),
        }
        coordinator.register_task("register", task).unwrap();
        coordinator
            .admit_fixture(
                "task",
                "root",
                invocation("root", None, InvocationRole::Root),
            )
            .unwrap();
        let result = match case.operation.as_str() {
            "recursion" | "invocations" => {
                let before = coordinator.snapshot("task").unwrap();
                let result = coordinator.admit_fixture(
                    "task",
                    "child",
                    invocation("child", Some("root"), InvocationRole::SelfCall),
                );
                if result.is_err() {
                    assert_eq!(
                        coordinator.snapshot("task").unwrap(),
                        before,
                        "{}",
                        case.name
                    );
                }
                result
            }
            _ => {
                let attempt = binding("root", "a1");
                coordinator
                    .start_attempt("task", "start-1", attempt.clone())
                    .unwrap();
                coordinator
                    .observe_attempt(
                        "task",
                        "failure-1",
                        observation(
                            &attempt,
                            4,
                            AttemptOutcome::Failed {
                                reason: "verified safe retry".into(),
                                safe_to_retry: true,
                            },
                        ),
                    )
                    .unwrap();
                let before = coordinator.snapshot("task").unwrap();
                let result = coordinator.start_attempt("task", "start-2", binding("root", "a2"));
                if result.is_err() {
                    assert_eq!(
                        coordinator.snapshot("task").unwrap(),
                        before,
                        "{}",
                        case.name
                    );
                }
                result
            }
        };
        assert_eq!(result.is_ok(), case.admitted, "{}", case.name);
    }
}
