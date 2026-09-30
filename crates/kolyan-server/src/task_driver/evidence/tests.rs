//! Host evidence inspection against native execution facts; no model calls.

use kolyan_core::{StepOutcome, StepResult};
use kolyan_ledger::InMemoryLedger;
use kolyan_model::{ContentBlock, ModelRef, ModelRequest, StopReason, ToolChoice};
use serde_json::{Value, json};

use super::*;
use crate::{AgentIdentity, ExecutionRef};

fn binding() -> AttemptBinding {
    AttemptBinding {
        attempt_id: "attempt-1".into(),
        invocation_id: "invocation-1".into(),
        execution: ExecutionRef {
            session_id: "session-1".into(),
            turn_id: "turn-1".into(),
            execution_id: "execution-1".into(),
        },
        agent: AgentIdentity {
            definition_id: "agent-1".into(),
            revision: "revision-1".into(),
            instance_id: "instance-1".into(),
        },
        constraints_digest: "c".repeat(64),
    }
}

fn append(
    ledger: &InMemoryLedger,
    binding: &AttemptBinding,
    kind: LedgerEventKind,
    payload: Value,
) -> LedgerEvent {
    let count = ledger.events_after(0).unwrap().len();
    let id = if kind == LedgerEventKind::ExecutionStarted {
        format!("{}/execution-started", binding.execution.execution_id)
    } else {
        format!("{}/test-event/{count}", binding.execution.execution_id)
    };
    ledger
        .append(LedgerEvent {
            event_id: id.clone(),
            turn_id: binding.execution.turn_id.clone(),
            execution_id: binding.execution.execution_id.clone(),
            cursor: 0,
            kind,
            idempotency_key: id,
            payload,
        })
        .unwrap()
}

fn seed(binding: &AttemptBinding) -> InMemoryLedger {
    let ledger = InMemoryLedger::default();
    append(
        &ledger,
        binding,
        LedgerEventKind::ExecutionStarted,
        json!(binding.execution),
    );
    ledger
}

fn request(ledger: &InMemoryLedger, binding: &AttemptBinding, step_id: &str) {
    let request = ModelRequest {
        request_id: step_id.into(),
        model: ModelRef::new("fixture-provider", "fixture-model"),
        system: Vec::new(),
        messages: Vec::new(),
        tools: Vec::new(),
        tool_choice: ToolChoice::Auto,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: Some(64),
        extensions: Value::Null,
    };
    append(
        ledger,
        binding,
        LedgerEventKind::ModelRequested,
        json!({"request": request}),
    );
}

fn reported(input: u64, output: u64) -> TokenUsage {
    TokenUsage {
        input_tokens: Some(input),
        output_tokens: Some(output),
        ..TokenUsage::default()
    }
}

fn usage(ledger: &InMemoryLedger, binding: &AttemptBinding, step_id: &str, value: TokenUsage) {
    append(
        ledger,
        binding,
        LedgerEventKind::ModelStreamEvent,
        json!({"type": "usage", "step_id": step_id, "usage": value}),
    );
}

fn step(
    ledger: &InMemoryLedger,
    binding: &AttemptBinding,
    step_id: &str,
    usage: TokenUsage,
) -> ModelResponse {
    let response = ModelResponse {
        id: format!("response-{step_id}"),
        model: ModelRef::new("fixture-provider", "fixture-model"),
        content: vec![ContentBlock::Text {
            text: format!("verified result for {step_id}"),
        }],
        structured_output: None,
        stop_reason: StopReason::EndTurn,
        usage,
        metadata: json!({"fixture": step_id}),
    };
    let result = StepResult {
        step_id: step_id.into(),
        response: response.clone(),
        outcome: StepOutcome::FinalAnswer,
    };
    append(
        ledger,
        binding,
        LedgerEventKind::StepCompleted,
        json!({"step_id": step_id, "outcome": "FinalAnswer", "step": result}),
    );
    response
}

fn terminal(ledger: &InMemoryLedger, binding: &AttemptBinding) -> LedgerEvent {
    append(
        ledger,
        binding,
        LedgerEventKind::TurnCompleted,
        json!({"reason": "FinalAnswer"}),
    )
}

#[test]
fn membership_rejects_wrong_execution_session_and_turn_bindings() {
    let original = binding();
    let ledger = seed(&original);
    for field in ["execution", "session", "turn"] {
        let mut wrong = original.clone();
        match field {
            "execution" => wrong.execution.execution_id = "unrelated-execution".into(),
            "session" => wrong.execution.session_id = "unrelated-session".into(),
            _ => wrong.execution.turn_id = "unrelated-turn".into(),
        }
        assert!(inspect(&ledger, &wrong, 0).is_err(), "{field}");
    }
}

#[test]
fn membership_rejects_forged_identity_payload_or_a_foreign_turn_event() {
    let original = binding();
    for field in ["execution", "session", "turn"] {
        let ledger = InMemoryLedger::default();
        let mut forged = original.execution.clone();
        match field {
            "execution" => forged.execution_id = "forged".into(),
            "session" => forged.session_id = "forged".into(),
            _ => forged.turn_id = "forged".into(),
        }
        append(
            &ledger,
            &original,
            LedgerEventKind::ExecutionStarted,
            json!(forged),
        );
        assert!(inspect(&ledger, &original, 0).is_err(), "{field}");
    }
    let ledger = seed(&original);
    let mut wrong_turn = original.clone();
    wrong_turn.execution.turn_id = "foreign-turn".into();
    request(&ledger, &wrong_turn, "foreign-step");
    assert!(inspect(&ledger, &original, 0).is_err());
}

#[test]
fn missing_or_wrong_kind_execution_admission_is_not_evidence() {
    let original = binding();
    let ledger = InMemoryLedger::default();
    terminal(&ledger, &original);
    assert!(inspect(&ledger, &original, 0).is_err());
    let ledger = InMemoryLedger::default();
    let id = format!("{}/execution-started", original.execution.execution_id);
    ledger
        .append(LedgerEvent {
            event_id: id.clone(),
            idempotency_key: id,
            cursor: 0,
            turn_id: original.execution.turn_id.clone(),
            execution_id: original.execution.execution_id.clone(),
            kind: LedgerEventKind::TurnStarted,
            payload: json!(original.execution),
        })
        .unwrap();
    assert!(inspect(&ledger, &original, 0).is_err());
}

#[test]
fn absent_counts_are_unknown_but_reported_zero_is_known() {
    for (input, output, unknown) in [
        (None, None, 1),
        (None, Some(0), 1),
        (Some(0), None, 1),
        (Some(0), Some(0), 0),
    ] {
        let original = binding();
        let ledger = seed(&original);
        request(&ledger, &original, "step-1");
        step(
            &ledger,
            &original,
            "step-1",
            TokenUsage {
                input_tokens: input,
                output_tokens: output,
                ..TokenUsage::default()
            },
        );
        terminal(&ledger, &original);
        let observed = inspect(&ledger, &original, 0).unwrap();
        assert_eq!((observed.input_tokens, observed.output_tokens), (0, 0));
        assert_eq!(observed.unreported_steps, unknown);
        assert!(matches!(observed.stopped, StoppedOutcome::Completed));
    }
}

#[test]
fn repeated_stream_usage_is_replaced_by_final_step_usage_without_double_count() {
    let original = binding();
    let ledger = seed(&original);
    request(&ledger, &original, "step-1");
    usage(
        &ledger,
        &original,
        "step-1",
        TokenUsage {
            input_tokens: Some(10),
            ..TokenUsage::default()
        },
    );
    usage(&ledger, &original, "step-1", reported(15, 5));
    usage(&ledger, &original, "step-1", reported(15, 5));
    step(
        &ledger,
        &original,
        "step-1",
        TokenUsage {
            cache_read_tokens: Some(3),
            ..reported(20, 8)
        },
    );
    terminal(&ledger, &original);
    let observed = inspect(&ledger, &original, 0).unwrap();
    assert_eq!(
        (
            observed.input_tokens,
            observed.output_tokens,
            observed.unreported_steps
        ),
        (23, 8, 0)
    );
}

#[test]
fn cache_tokens_are_input_and_reasoning_is_not_added_to_output_again() {
    let original = binding();
    let ledger = seed(&original);
    request(&ledger, &original, "step-1");
    step(
        &ledger,
        &original,
        "step-1",
        TokenUsage {
            cache_read_tokens: Some(5),
            cache_write_tokens: Some(7),
            reasoning_tokens: Some(3),
            ..reported(10, 4)
        },
    );
    terminal(&ledger, &original);
    let observed = inspect(&ledger, &original, 0).unwrap();
    assert_eq!(
        (
            observed.input_tokens,
            observed.output_tokens,
            observed.unreported_steps
        ),
        (22, 4, 0)
    );
}

#[test]
fn interrupted_model_request_retains_unknown_usage_without_inventing_a_response() {
    let original = binding();
    let ledger = seed(&original);
    request(&ledger, &original, "started-step");
    let last = append(
        &ledger,
        &original,
        LedgerEventKind::ModelStreamEvent,
        json!({"type": "reasoning_delta", "step_id": "started-step", "text": "partial reasoning"}),
    );
    let observed = inspect(&ledger, &original, 0).unwrap();
    assert_eq!(
        (
            observed.input_tokens,
            observed.output_tokens,
            observed.unreported_steps
        ),
        (0, 0, 1)
    );
    assert!(observed.response.is_none());
    assert!(matches!(observed.stopped, StoppedOutcome::RecoveryRequired));
    assert_eq!(observed.source.cursor, last.cursor);
}

#[test]
fn interrupted_request_preserves_partial_reported_usage_without_claiming_completion() {
    let original = binding();
    let ledger = seed(&original);
    request(&ledger, &original, "started-step");
    usage(
        &ledger,
        &original,
        "started-step",
        TokenUsage {
            input_tokens: Some(11),
            output_tokens: None,
            cache_read_tokens: Some(9),
            ..TokenUsage::default()
        },
    );
    let observed = inspect(&ledger, &original, 0).unwrap();
    assert_eq!(
        (
            observed.input_tokens,
            observed.output_tokens,
            observed.unreported_steps
        ),
        (20, 0, 1)
    );
    assert!(matches!(observed.stopped, StoppedOutcome::RecoveryRequired));
}

#[test]
fn effect_started_without_matching_receipt_requires_recovery_even_with_final_answer() {
    for receipt in [None, Some("different-effect"), Some("write-effect")] {
        let original = binding();
        let ledger = seed(&original);
        request(&ledger, &original, "step-1");
        step(&ledger, &original, "step-1", reported(10, 5));
        append(
            &ledger,
            &original,
            LedgerEventKind::EffectStarted,
            json!({"effect_id": "write-effect", "call_id": "write-call"}),
        );
        if let Some(id) = receipt {
            append(
                &ledger,
                &original,
                LedgerEventKind::EffectReceipt,
                json!({"effect_id": id, "result": "written"}),
            );
        }
        terminal(&ledger, &original);
        let observed = inspect(&ledger, &original, 0).unwrap();
        assert_eq!(
            matches!(observed.stopped, StoppedOutcome::Completed),
            receipt == Some("write-effect")
        );
        assert_eq!(
            matches!(observed.stopped, StoppedOutcome::RecoveryRequired),
            receipt != Some("write-effect")
        );
        assert_eq!((observed.input_tokens, observed.output_tokens), (10, 5));
    }
}

#[test]
fn final_response_and_source_are_actual_and_foreign_execution_cannot_contaminate_them() {
    let original = binding();
    let ledger = seed(&original);
    request(&ledger, &original, "step-1");
    let response = step(&ledger, &original, "step-1", reported(8, 3));
    let final_fact = terminal(&ledger, &original);
    let mut foreign = original.clone();
    foreign.execution.execution_id = "foreign-execution".into();
    append(
        &ledger,
        &foreign,
        LedgerEventKind::ExecutionStarted,
        json!(foreign.execution),
    );
    request(&ledger, &foreign, "foreign-step");
    step(&ledger, &foreign, "foreign-step", reported(9000, 9000));
    terminal(&ledger, &foreign);
    let observed = inspect(&ledger, &original, 0).unwrap();
    assert_eq!(observed.response, Some(response.clone()));
    assert_eq!(observed.source, source_ref(&original, &final_fact));
    assert_eq!(
        (
            observed.input_tokens,
            observed.output_tokens,
            observed.unreported_steps
        ),
        (8, 3, 0)
    );
    assert!(matches!(observed.stopped, StoppedOutcome::Completed));
    assert_eq!(
        response_digest(&response).unwrap(),
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&response).unwrap())
        )
    );
}

#[test]
fn exclusive_observation_cursor_counts_only_new_step_usage_but_keeps_actual_final_response() {
    let original = binding();
    let ledger = seed(&original);
    request(&ledger, &original, "step-1");
    step(&ledger, &original, "step-1", reported(10, 5));
    let wait = append(
        &ledger,
        &original,
        LedgerEventKind::ExecutionSuspended,
        json!({"approval_id": "approval-1"}),
    );
    let suspended = inspect(&ledger, &original, 0).unwrap();
    assert!(matches!(suspended.stopped, StoppedOutcome::Approval(ref id) if id == "approval-1"));
    assert_eq!((suspended.input_tokens, suspended.output_tokens), (10, 5));
    request(&ledger, &original, "step-2");
    usage(&ledger, &original, "step-2", reported(18, 3));
    let final_response = step(&ledger, &original, "step-2", reported(20, 4));
    let final_fact = terminal(&ledger, &original);
    let delta = inspect(&ledger, &original, wait.cursor).unwrap();
    assert_eq!(
        (
            delta.input_tokens,
            delta.output_tokens,
            delta.unreported_steps
        ),
        (20, 4, 0)
    );
    assert_eq!(delta.response, Some(final_response.clone()));
    let repeated = inspect(&ledger, &original, final_fact.cursor).unwrap();
    assert_eq!(
        (
            repeated.input_tokens,
            repeated.output_tokens,
            repeated.unreported_steps
        ),
        (0, 0, 0)
    );
    assert_eq!(repeated.response, Some(final_response));
    assert_eq!(repeated.source.cursor, final_fact.cursor);
}

#[test]
fn malformed_step_identity_and_malformed_usage_fail_instead_of_losing_evidence() {
    let original = binding();
    let ledger = seed(&original);
    request(&ledger, &original, "step-1");
    let response = step(&ledger, &original, "step-1", reported(10, 5));
    let result = StepResult {
        step_id: "different-step".into(),
        response,
        outcome: StepOutcome::FinalAnswer,
    };
    append(
        &ledger,
        &original,
        LedgerEventKind::StepCompleted,
        json!({"step_id": "step-1", "step": result}),
    );
    assert!(inspect(&ledger, &original, 0).is_err());
    let ledger = seed(&original);
    append(
        &ledger,
        &original,
        LedgerEventKind::ModelStreamEvent,
        json!({"type": "usage", "step_id": "step-1", "usage": {"input_tokens": "not-a-number"}}),
    );
    assert!(inspect(&ledger, &original, 0).is_err());
}
