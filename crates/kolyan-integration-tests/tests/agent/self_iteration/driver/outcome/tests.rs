//! Typed result-mapping fixtures; not model, OS or durable-stop acceptance.

use kolyan_core::{StepError, TurnExecution, TurnResult};
use kolyan_model::{ModelRef, ModelResponse, ProviderErrorKind, ProviderErrorPhase, StopReason};
use kolyan_runtime::{RuntimeError, Trajectory};
use kolyan_server::{ServerError, TaskExecutionError};
use serde::Deserialize;

use super::*;
use crate::evidence::Evidence;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    input: String,
    expected_result: String,
    expected_terminal: String,
    failure_contains: Option<String>,
}

fn response() -> ModelResponse {
    ModelResponse {
        id: "typed-fixture".into(),
        model: ModelRef::new("fixture", "fixture"),
        content: vec![],
        structured_output: None,
        stop_reason: StopReason::EndTurn,
        usage: Default::default(),
        metadata: Value::Null,
    }
}

fn input(case: &Case) -> Result<DurableTurnResult, TaskExecutionError> {
    let completed = match case.input.as_str() {
        "final" => Some((
            TurnOutcome::FinalAnswer {
                response: response(),
            },
            TurnEndReason::FinalAnswer,
        )),
        "max_steps" => Some((TurnOutcome::MaxSteps, TurnEndReason::MaxSteps)),
        "refused" => Some((
            TurnOutcome::Refused {
                response: response(),
            },
            TurnEndReason::Refused,
        )),
        "incomplete" => Some((
            TurnOutcome::Incomplete {
                response: response(),
            },
            TurnEndReason::Incomplete,
        )),
        "rejected" => Some((
            TurnOutcome::Rejected {
                reason: "actual approval rejection".into(),
            },
            TurnEndReason::ApprovalRejected,
        )),
        "expired" => Some((
            TurnOutcome::Expired {
                reason: "actual approval expiry".into(),
            },
            TurnEndReason::ApprovalExpired,
        )),
        _ => None,
    };
    if let Some((outcome, end_reason)) = completed {
        return Ok(DurableTurnResult::Completed(
            Box::new(TurnExecution {
                result: TurnResult {
                    turn_id: "fixture-turn".into(),
                    outcome,
                    end_reason,
                    steps: vec![],
                },
                events: vec![],
            }),
            Trajectory::default(),
        ));
    }
    let error = match case.input.as_str() {
        "cancelled" => TurnError::Cancelled,
        "timed_out" => TurnError::TimedOut,
        "max_steps_error" => TurnError::MaxSteps,
        "no_progress" => TurnError::NoProgress {
            tool_name: "file.write".into(),
        },
        "provider" | "provider_diagnostics" => {
            let mut error = ProviderError::new(
                ProviderErrorKind::Authentication,
                ProviderErrorPhase::Open,
                "current HTTP authentication failure",
            );
            error.provider = Some("anthropic".into());
            error.status = Some(401);
            if case.input == "provider_diagnostics" {
                error.diagnostics = Some(
                    json!({"kind":"local_http_opening_retry","report":{"attempts":2,"terminal_status":401}}),
                );
            }
            TurnError::Step(StepError::Provider(error))
        }
        _ => panic!("unknown typed outcome fixture"),
    };
    Err(TaskExecutionError::Server(ServerError::Runtime(
        RuntimeError::Turn(error),
    )))
}

#[test]
fn typed_outcomes_and_original_errors_export_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let output = tempfile::Builder::new()
        .prefix("kolyan-stage-outcome-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = Evidence::new(&output.join("actual.jsonl"));
    println!("STAGE_OUTCOME_TRACE={}", output.display());
    let actual: Vec<_> = cases
        .iter()
        .map(|case| {
            let result = input(case);
            let observed = observe(
                "repair",
                result.as_ref().map_err(|e| e as &(dyn Error + 'static)),
            );
            evidence
                .append(
                    json!({"case_id":case.id,"source":"offline_typed_mapping_fixture",
            "observed":observed.record,"failure":observed.failure}),
                )
                .unwrap();
            observed
        })
        .collect();
    for (case, observed) in cases.iter().zip(actual) {
        let detail = &observed.record["detail"];
        assert_eq!(detail["result"], case.expected_result, "{}", case.id);
        let terminal = if case.expected_result == "completed" {
            &detail["outcome"]
        } else {
            &detail["turn_end_reason"]
        };
        assert_eq!(terminal, &json!(case.expected_terminal), "{}", case.id);
        match &case.failure_contains {
            Some(text) => assert!(
                observed.failure.as_ref().unwrap().contains(text),
                "{}: {:?}",
                case.id,
                observed.failure
            ),
            None => assert!(observed.failure.is_none()),
        }
        if case.input.starts_with("provider") {
            assert_eq!(detail["provider_error"]["kind"], "authentication");
            assert_eq!(detail["provider_error"]["phase"], "open");
            assert_eq!(detail["provider_error"]["status"], 401);
            assert_eq!(
                detail["provider_error"]["message"],
                "current HTTP authentication failure"
            );
            assert_eq!(
                detail["provider_error"]["diagnostics"].is_null(),
                case.input == "provider"
            );
            if case.input == "provider_diagnostics" {
                assert_eq!(
                    detail["provider_error"]["diagnostics"]["report"]["attempts"],
                    2
                );
                assert!(
                    observed
                        .failure
                        .unwrap()
                        .contains("local_http_opening_retry")
                );
            }
        }
    }
}
