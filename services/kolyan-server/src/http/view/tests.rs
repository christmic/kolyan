use super::*;
use kolyan_ledger::LedgerEvent;
use kolyan_server::ExecutionState;

fn key() -> ExecutionRef {
    ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    }
}
fn fact(kind: LedgerEventKind, payload: Value) -> LedgerEvent {
    LedgerEvent {
        event_id: "fact".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
        cursor: 1,
        kind,
        idempotency_key: "fact".into(),
        payload,
    }
}
#[test]
fn public_view_retains_output_but_excludes_internal_model_data() {
    let step = fact(
        LedgerEventKind::StepCompleted,
        json!({"step":{
            "step_id":"t-step-0","outcome":"FinalAnswer",
            "response":{"id":"r","model":{"provider":"fixture","model":"m"},
                "content":[{"type":"reasoning","text":"actual reasoning","opaque":{"signature":"hidden-signature"}},
                    {"type":"text","text":"actual answer"}],
                "structured_output":{"answer":42},"stop_reason":{"reason":"end_turn"},
                "usage":{"input_tokens":null,"output_tokens":9,"cache_read_tokens":null,"cache_write_tokens":null,"reasoning_tokens":null},
                "metadata":{"secret":"hidden-provider-metadata"}}
        }}),
    );
    let tool = fact(
        LedgerEventKind::ToolExecutionCompleted,
        json!({"result":{
            "call_id":"c","content":"actual tool result","is_error":false,"unexpected":"hidden-extra"
        }}),
    );
    let terminal = fact(LedgerEventKind::TurnCompleted, json!({"reason":"MaxSteps"}));
    let view = project(
        &key(),
        &[step, tool, terminal],
        ExecutionState::Completed,
        false,
    )
    .unwrap();
    assert_eq!(view["end_reason"], "MaxSteps");
    assert_eq!(view["steps"][0]["content"][0]["text"], "actual reasoning");
    assert_eq!(view["steps"][0]["structured_output"]["answer"], 42);
    assert!(view["steps"][0]["usage"]["input_tokens"].is_null());
    assert_eq!(view["tool_results"][0]["content"], "actual tool result");
    assert!(!view.to_string().contains("hidden-"));
}
#[test]
fn cancellation_intent_is_not_stopping_and_lost_worker_requires_recovery() {
    let cancel = fact(LedgerEventKind::ExecutionCancelled, Value::Null);
    let running = project(
        &key(),
        std::slice::from_ref(&cancel),
        ExecutionState::Cancelled,
        true,
    )
    .unwrap();
    assert_eq!(running["cancellation_requested"], true);
    assert_eq!(running["execution_stopped"], false);
    assert_eq!(running["recovery_required"], false);
    let lost = project(&key(), &[cancel], ExecutionState::Cancelled, false).unwrap();
    assert_eq!(lost["recovery_required"], true);
}
#[test]
fn approval_boundary_without_matching_checkpoint_is_not_public_suspension() {
    let old = fact(
        LedgerEventKind::ApprovalRequested,
        json!({"schema_version":1,"approval":{"approval_id":"old","turn_id":"t",
            "call_id":"c","tool_name":"file.write","reason":"requires approval","expires_at_ms":null}}),
    );
    let boundary = fact(
        LedgerEventKind::ExecutionBoundaryAdmitted,
        json!({"checkpoint_id":"new"}),
    );
    let view = project(&key(), &[old, boundary], ExecutionState::Suspended, true).unwrap();
    assert_eq!(view["state"], "running");
    assert_eq!(view["pending_approvals"], json!([]));
    assert_eq!(view["external_waits"], json!([]));
    assert_eq!(view["execution_stopped"], false);
}
#[test]
fn malformed_step_fact_does_not_become_empty_success() {
    let corrupt = fact(
        LedgerEventKind::StepCompleted,
        json!({"step":{"invalid":true}}),
    );
    assert!(project(&key(), &[corrupt], ExecutionState::Completed, false).is_err());
}

#[test]
fn unsupported_or_approval_only_suspension_is_an_error_not_an_empty_wait() {
    for payload in [
        json!({"approval_id":"legacy"}),
        json!({"schema_version":77,"suspension":{}}),
        json!({"schema_version":1,"suspension":{},"unknown":true}),
    ] {
        let corrupt = fact(LedgerEventKind::ExecutionSuspended, payload);
        assert!(project(&key(), &[corrupt], ExecutionState::Suspended, false).is_err());
    }
}
