//! Constructors for real prepared receipts and adversarial stored evidence.

use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use kolyan_ledger::{LedgerQuery, LedgerStore};
use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalEvidence, ApprovalMode, Capability, Effect, Idempotency, InvocationClaim,
    PolicyContext, PolicyEngine, PreparedGrant, ResourceClaim, ToolManifest, ToolRequirements,
};
use serde_json::json;

use super::*;

pub(super) struct ObservedLedger {
    pub inner: Box<dyn LedgerStore>,
    pub writes: AtomicUsize,
    pub unbounded: AtomicUsize,
    pub reads: Mutex<Vec<Value>>,
    fault: String,
}

impl ObservedLedger {
    pub fn new(inner: Box<dyn LedgerStore>, fault: &str) -> Self {
        Self {
            inner,
            writes: AtomicUsize::new(0),
            unbounded: AtomicUsize::new(0),
            reads: Mutex::new(Vec::new()),
            fault: fault.into(),
        }
    }
}

impl LedgerStore for ObservedLedger {
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.reads
            .lock()
            .unwrap()
            .push(json!({"event_id":query.event_id,
            "execution_id":query.execution_id,"after":query.after,
            "through":query.through,"limit":query.limit}));
        if self.fault == "storage_error" {
            return Err(LedgerError::Storage("fixture unavailable".into()));
        }
        let mut events = self.inner.query(query)?;
        if self.fault == "oversized_query_page"
            && let Some(event) = events.first().cloned()
        {
            events.push(event);
        }
        Ok(events)
    }
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage("proof reader must not append".into()))
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.append(event)
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.unbounded.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage(
            "proof reader must not audit history".into(),
        ))
    }
    fn execution_events_after(&self, _: &str, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.unbounded.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage(
            "proof reader must not scan execution history".into(),
        ))
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(LedgerError::Storage("proof reader must not claim".into()))
    }
}

pub(super) fn fixture(case: &str) -> (EffectProofRequest, Vec<LedgerEvent>) {
    let execution = ExecutionKey {
        session_id: "proof-session".into(),
        turn_id: "proof-turn".into(),
        execution_id: "proof-execution".into(),
    };
    let call_id = match case {
        "native_long_unicode_slash" => "原生/调用/".repeat(40),
        "native_maximum_id" => "x".repeat(1024),
        "native_control_id" => "native\ncall".into(),
        _ => "proof-call".into(),
    };
    let mut scope = ToolExecutionScope {
        execution: execution.clone(),
        step_id: if case == "native_maximum_id" {
            "s".repeat(256)
        } else {
            "proof-step".into()
        },
        agent_snapshot_digest: Some("a".repeat(64)),
    };
    if case == "valid_foreign_session_authority" {
        scope.execution.session_id = "foreign-session".into();
    }
    let prepared = PreparedCall::new(
        ToolCall {
            id: call_id.clone(),
            name: "file.write".into(),
            arguments: json!({"path":"report.txt","content":"done"}),
        },
        "proof-tool-v1".into(),
        InvocationClaim {
            tool_name: "file.write".into(),
            capabilities: [Capability::FilesystemWrite].into(),
            effects: [Effect::Update].into(),
            resource: ResourceClaim {
                path: Some("/fixture/report.txt".into()),
            },
            idempotency: Idempotency::NonIdempotent,
        },
        ToolRequirements {
            process_sandbox: true,
            max_output_bytes: 4096,
            timeout_ms: 30000,
        },
    )
    .unwrap()
    .with_execution_binding(json!({"physical_target":"/fixture/report.txt"}))
    .unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        path_scopes: Vec::new(),
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    let decision = policy.decide_prepared(&prepared, &PolicyContext::default());
    let revision = decision.policy_version.clone();
    let grant = PreparedGrant::issue(
        &prepared,
        decision,
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    let bound_key = scope.execution.clone();
    let bound = PreparedEvidence::new(prepared, grant, scope, &revision, &bound_key).unwrap();
    let output = ToolResult {
        call_id,
        content: "wrote four bytes".into(),
        is_error: case == "completed_error_result",
    };
    let result = match case {
        "failed_receipt" => Err(ToolError::Failed {
            message: "fixture failed".into(),
        }),
        "cancelled_receipt" => Err(ToolError::Cancelled),
        "timed_out_receipt" => Err(ToolError::TimedOut),
        _ => Ok(output.clone()),
    };
    let mut receipt = bound.receipt_payload(&result).unwrap();
    if matches!(
        case,
        "reconciled_receipt" | "changed_reconciled_result" | "reconciliation_result_bound"
    ) {
        receipt["reconciliation"] = json!({"request":crate::ReconciliationRequest {
            reconciliation_id:"proof-inspection".into(),execution:execution.clone(),
            effect_id:bound.request.effect_id.clone() },
            "resolution":crate::ReconciliationResolution::Committed { output,
                executor_id:bound.executor_id(),executor_revision:bound.prepared.tool_revision().into(),
                evidence:"trusted saved inspection".into() }});
    }
    let mut events = [
        (
            format!("{}/execution-started", execution.execution_id),
            LedgerEventKind::ExecutionStarted,
            json!(execution),
        ),
        (
            format!("{}/prepared", bound.prefix()),
            LedgerEventKind::EffectPrepared,
            bound.prepared_payload().unwrap(),
        ),
        (
            format!("{}/authorized", bound.prefix()),
            LedgerEventKind::EffectAuthorized,
            bound.authorized_payload().unwrap(),
        ),
        (
            format!("{}/started", bound.prefix()),
            LedgerEventKind::EffectStarted,
            bound.started_payload(),
        ),
        (
            format!("{}/receipt", bound.prefix()),
            LedgerEventKind::EffectReceipt,
            receipt,
        ),
        (
            format!("{}/terminal/FinalAnswer", execution.execution_id),
            LedgerEventKind::TurnCompleted,
            json!({"reason":"FinalAnswer"}),
        ),
    ]
    .into_iter()
    .map(|(id, kind, payload)| LedgerEvent {
        event_id: id.clone(),
        idempotency_key: id,
        execution_id: execution.execution_id.clone(),
        turn_id: execution.turn_id.clone(),
        cursor: 0,
        kind,
        payload,
    })
    .collect::<Vec<_>>();
    let mut request = EffectProofRequest {
        execution,
        effect_id: bound.request.effect_id,
        terminal: EffectProofCoordinate {
            event_id: events[5].event_id.clone(),
            cursor: 6,
        },
        max_input_bytes: 65536,
        max_result_bytes: 4096,
    };
    mutate(case, &mut request, &mut events);
    (request, events)
}

fn mutate(case: &str, request: &mut EffectProofRequest, events: &mut Vec<LedgerEvent>) {
    match case {
        "changed_reconciled_result" => {
            events[4].payload["reconciliation"]["resolution"]["Committed"]["output"]["content"] =
                json!("changed")
        }
        "reconciliation_result_bound" => {
            events[4].payload["reconciliation"]["resolution"]["Committed"]["output"]["content"] =
                json!("x".repeat(4096))
        }
        "wrong_admission_session" => events[0].payload["session_id"] = json!("foreign"),
        "wrong_admission_kind" => events[0].kind = LedgerEventKind::StepStarted,
        "wrong_prepared_kind" => events[1].kind = LedgerEventKind::StepStarted,
        "wrong_authorized_kind" => events[2].kind = LedgerEventKind::StepStarted,
        "wrong_started_kind" => events[3].kind = LedgerEventKind::StepStarted,
        "wrong_receipt_kind" => events[4].kind = LedgerEventKind::EffectCompleted,
        "wrong_receipt_turn" => events[4].turn_id = "foreign".into(),
        "wrong_receipt_execution" => events[4].execution_id = "foreign".into(),
        "wrong_receipt_idempotency" => events[4].idempotency_key = "foreign".into(),
        "wrong_prepared_session" => {
            events[1].payload["input"]["scope"]["execution"]["session_id"] = json!("foreign")
        }
        "wrong_prepared_step" => events[1].payload["input"]["scope"]["step_id"] = json!("foreign"),
        "changed_grant" => {
            events[2].payload["prepared_grant"]["prepared_digest"] = json!("b".repeat(64))
        }
        "changed_revision" => {
            events[1].payload["input"]["prepared"]["tool_revision"] = json!("changed-v2")
        }
        "changed_started_binding" => events[3].payload["input_digest"] = json!("b".repeat(64)),
        "changed_result_digest" => {
            events[4].payload["receipt"]["result_digest"] = json!("b".repeat(64))
        }
        "changed_result_content" => events[4].payload["output"]["content"] = json!("altered"),
        "wrong_result_call" => events[4].payload["output"]["call_id"] = json!("foreign"),
        "extra_receipt_field" => events[4].payload["extra"] = json!(true),
        "uncertain_receipt" => events[4].payload["receipt"]["status"] = json!("Uncertain"),
        "malformed_receipt_status" => events[4].payload["receipt"]["status"] = json!("Unknown"),
        "foreign_terminal_execution" => events[5].execution_id = "foreign".into(),
        "foreign_terminal_turn" => events[5].turn_id = "foreign".into(),
        "terminal_cursor_mismatch" => request.terminal.cursor = 7,
        "diagnostic_completed_terminal" => events[5].payload = json!({"outcome":"Completed"}),
        "unknown_completed_reason" => events[5].payload = json!({"reason":"Unknown"}),
        "cancellation_intent_terminal" => events[5].kind = LedgerEventKind::ExecutionCancelled,
        "suspension_terminal" => events[5].kind = LedgerEventKind::ExecutionSuspended,
        "failed_terminal" => {
            events[5].kind = LedgerEventKind::TurnFailed;
            events[5].payload = json!({"error":"stopped failure"});
        }
        "cancelled_terminal" => {
            events[5].kind = LedgerEventKind::TurnCancelled;
            events[5].payload = json!({"reason":"Cancelled"});
        }
        "timed_out_terminal" => {
            events[5].kind = LedgerEventKind::TurnTimedOut;
            events[5].payload = json!({"reason":"TimedOut"});
        }
        "refused_terminal" => events[5].payload = json!({"reason":"Refused"}),
        "admission_after_preparation" => events.swap(0, 1),
        "authorization_before_preparation" => events.swap(1, 2),
        "start_before_authorization" => events.swap(2, 3),
        "receipt_before_start" => events.swap(3, 4),
        "receipt_after_terminal" => {
            events.swap(4, 5);
            request.terminal.cursor = 5;
        }
        "oversize_admission" => events[0].payload["extra"] = json!("x".repeat(65536)),
        "oversize_prepared" => events[1].payload["extra"] = json!("x".repeat(65536)),
        "oversize_authorized" => events[2].payload["extra"] = json!("x".repeat(65536)),
        "oversize_started" => events[3].payload["extra"] = json!("x".repeat(65536)),
        "oversize_receipt" => events[4].payload["extra"] = json!("x".repeat(70000)),
        "oversize_terminal" => events[5].payload["extra"] = json!("x".repeat(65536)),
        "exact_input_bound" => {
            request.max_input_bytes = events[..4]
                .iter()
                .map(|e| serde_json::to_vec(&e.payload).unwrap().len())
                .max()
                .unwrap()
        }
        "exact_result_bound" => {
            request.max_result_bytes = serde_json::to_vec(&events[4].payload["output"])
                .unwrap()
                .len()
        }
        "result_bound_too_small" => {
            request.max_result_bytes = serde_json::to_vec(&events[4].payload["output"])
                .unwrap()
                .len()
                - 1
        }
        "invalid_execution" => request.execution.session_id = "bad/session".into(),
        "oversize_execution_key" => request.execution.session_id = "s".repeat(257),
        "empty_effect" => request.effect_id.clear(),
        "oversize_effect" => request.effect_id = "x".repeat(1282),
        "zero_terminal_cursor" => request.terminal.cursor = 0,
        "empty_terminal_id" => request.terminal.event_id.clear(),
        "oversize_terminal_id" => request.terminal.event_id = "x".repeat(2049),
        "zero_input_bound" => request.max_input_bytes = 0,
        "zero_result_bound" => request.max_result_bytes = 0,
        "input_above_hard_bound" => request.max_input_bytes = MAX_EFFECT_PROOF_INPUT_BYTES + 1,
        "result_above_hard_bound" => request.max_result_bytes = MAX_EFFECT_PROOF_RESULT_BYTES + 1,
        _ => {}
    }
    if let Some(index) = match case {
        "missing_admission" => Some(0),
        "missing_prepared" => Some(1),
        "missing_authorized" => Some(2),
        "missing_started" => Some(3),
        "missing_receipt" => Some(4),
        "missing_terminal" => Some(5),
        _ => None,
    } {
        events.remove(index);
        if index < 5 {
            request.terminal.cursor = 5;
        }
    }
}
