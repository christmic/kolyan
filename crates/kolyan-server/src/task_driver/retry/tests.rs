//! Retry facade tests use native Ledger facts and the actual trusted reconciler.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use kolyan_ledger::{FactDraft, FactError, FactRecord, InMemoryLedger, MemoryFactJournal};
use kolyan_model::ToolResult;
use kolyan_runtime::{ExecutionKey, RuntimeError, ToolEffectReconciler, reconcile_tool_effect};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;

use super::*;
use crate::{
    AgentIdentity, CancellationPolicy, CompletionCriterion, ExecutionService, InvocationDefinition,
    InvocationRole, SessionService, TaskDefinition, TaskLimits,
};

type Service =
    TaskExecutionService<MemoryFactJournal, InMemoryLedger, NoopTraceSink, FileSessionStore>;

struct Harness {
    _root: tempfile::TempDir,
    service: Service,
    binding: AttemptBinding,
}

fn harness(alteration: Option<&str>, active: bool) -> Harness {
    let root = tempfile::tempdir().unwrap();
    let agent = AgentIdentity {
        definition_id: "agent".into(),
        revision: "r1".into(),
        instance_id: "instance".into(),
    };
    let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
    coordinator
        .register_task(
            "registered",
            TaskDefinition {
                task_id: "task".into(),
                objective: "Verify a retry before effects".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent.clone(),
                constraints_digest: "c".repeat(64),
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
    coordinator
        .admit_invocation(
            "task",
            "admitted",
            InvocationDefinition {
                invocation_id: "root".into(),
                agent: agent.clone(),
                constraints_digest: "c".repeat(64),
                role: InvocationRole::Root,
                parent_invocation_id: None,
                dependencies: vec![],
            },
        )
        .unwrap();
    let binding = AttemptBinding {
        attempt_id: "a1".into(),
        invocation_id: "root".into(),
        agent,
        constraints_digest: "c".repeat(64),
        execution: crate::ExecutionRef {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        },
    };
    coordinator
        .start_attempt("task", "attempt-start", binding.clone())
        .unwrap();
    let ledger = InMemoryLedger::default();
    let execution = ExecutionService::new(ledger, NoopTraceSink);
    let sessions =
        SessionService::new(FileSessionStore::new(root.path().join("sessions")).unwrap());
    let service = TaskExecutionService::new(
        coordinator,
        SessionExecutionService::new(execution, sessions),
    );
    let linked = ExecutionBinding {
        task_id: if alteration == Some("task") {
            "foreign-task".into()
        } else {
            "task".into()
        },
        invocation_id: "root".into(),
        attempt_id: "a1".into(),
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    append(
        &service,
        "e/task-binding",
        LedgerEventKind::ExecutionBound,
        json!({"binding": linked}),
    );
    let mut physical = binding.execution.clone();
    match alteration {
        Some("session") => physical.session_id = "foreign-session".into(),
        Some("turn") => physical.turn_id = "foreign-turn".into(),
        Some("execution") => physical.execution_id = "foreign-execution".into(),
        _ => {}
    }
    service
        .execution
        .execution()
        .server()
        .coordinator()
        .start(physical.clone())
        .unwrap();
    if !active {
        service
            .execution
            .execution()
            .server()
            .coordinator()
            .release(&physical.execution_id);
    }
    Harness {
        _root: root,
        service,
        binding,
    }
}

fn append(
    service: &Service,
    id: &str,
    kind: LedgerEventKind,
    payload: serde_json::Value,
) -> LedgerEvent {
    service
        .ledger()
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

fn stop(harness: &Harness, recovery: bool) {
    let fact = append(
        &harness.service,
        "e/failed",
        LedgerEventKind::TurnFailed,
        json!({"message": "verified stopped attempt"}),
    );
    harness
        .service
        .coordinator
        .observe_attempt(
            "task",
            "stopped",
            AttemptObservation {
                attempt_id: "a1".into(),
                execution: harness.binding.execution.clone(),
                source: evidence::source_ref(&harness.binding, &fact),
                usage: TaskUsage {
                    input_tokens: 3,
                    output_tokens: 2,
                    unreported_steps: 0,
                },
                outcome: if recovery {
                    AttemptOutcome::RecoveryRequired {
                        reason: "effect outcome initially unknown".into(),
                    }
                } else {
                    AttemptOutcome::Failed {
                        reason: "stopped before committed effects".into(),
                        safe_to_retry: false,
                    }
                },
            },
        )
        .unwrap();
}

fn effect(harness: &Harness, id: &str) -> ReconciliationRequest {
    let prefix = format!("e/effect/{id}");
    let prepared = EffectRequest {
        effect_id: id.into(),
        operation_kind: "file.write".into(),
        input_digest:
            json!({"name": "file.write", "arguments": {"path": "safe/a", "content": "result"}})
                .to_string(),
        requirements: vec![],
        policy_revision: "p1".into(),
    };
    let grant = EffectGrant {
        authorization_id: format!("{prefix}/authorized"),
        effect_id: id.into(),
        input_digest: prepared.input_digest.clone(),
        constraints_digest: "executor-scope".into(),
        authority_revision: "p1".into(),
    };
    for (suffix, kind, payload) in [
        ("prepared", LedgerEventKind::EffectPrepared, json!(prepared)),
        (
            "authorized",
            LedgerEventKind::EffectAuthorized,
            json!(grant),
        ),
        (
            "started",
            LedgerEventKind::EffectStarted,
            json!({"effect_id": id}),
        ),
    ] {
        append(
            &harness.service,
            &format!("{prefix}/{suffix}"),
            kind,
            payload,
        );
    }
    ReconciliationRequest {
        reconciliation_id: "inspection".into(),
        execution: ExecutionKey {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        },
        effect_id: id.into(),
    }
}

struct Inspector(ReconciliationResolution);
impl ToolEffectReconciler for Inspector {
    fn inspect(
        &self,
        request: &ReconciliationRequest,
        prepared: &EffectRequest,
        grant: &EffectGrant,
    ) -> Result<ReconciliationResolution, RuntimeError> {
        assert_eq!(request.effect_id, prepared.effect_id);
        assert_eq!(prepared.input_digest, grant.input_digest);
        Ok(self.0.clone())
    }
}

fn resolve(
    harness: &Harness,
    request: &ReconciliationRequest,
    resolution: ReconciliationResolution,
) {
    reconcile_tool_effect(harness.service.ledger(), request, &Inspector(resolution)).unwrap();
}

fn proof_count(service: &Service) -> usize {
    service
        .ledger()
        .execution_events_after("e", 0)
        .unwrap()
        .iter()
        .filter(|event| event.kind == LedgerEventKind::ExecutionRetryAuthorized)
        .count()
}

#[test]
fn stopped_before_effect_authorizes_only_a_new_attempt_and_keeps_original_usage() {
    let harness = harness(None, false);
    stop(&harness, false);
    let before = harness.service.coordinator.snapshot("task").unwrap();
    let authorized = harness
        .service
        .authorize_retry("task", "safe-retry", "a1", "no external effect started")
        .unwrap();
    assert!(authorized.attempts["a1"].retry_authorized);
    assert_eq!(authorized.attempts.len(), 1);
    assert_eq!(authorized.usage, before.usage);
    assert_eq!(
        authorized.attempts["a1"].observation,
        before.attempts["a1"].observation
    );
    assert_eq!(proof_count(&harness.service), 1);
    let mut next = harness.binding.clone();
    next.attempt_id = "a2".into();
    next.execution.execution_id = "e2".into();
    next.execution.turn_id = "t2".into();
    let admitted = harness
        .service
        .coordinator
        .start_attempt("task", "fresh-attempt", next)
        .unwrap();
    assert_eq!(admitted.attempts.len(), 2);
    assert!(
        harness
            .service
            .ledger()
            .execution_events_after("e2", 0)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn every_started_effect_requires_trusted_not_committed_inspection() {
    let harness = harness(None, false);
    let first = effect(&harness, "step/call-1");
    let second = effect(&harness, "step/call-2");
    stop(&harness, true);
    resolve(
        &harness,
        &first,
        ReconciliationResolution::NotCommitted {
            evidence: "first transaction rolled back".into(),
        },
    );
    assert!(
        harness
            .service
            .authorize_retry("task", "safe-retry", "a1", "checked effects")
            .is_err()
    );
    assert_eq!(proof_count(&harness.service), 0);
    resolve(
        &harness,
        &second,
        ReconciliationResolution::NotCommitted {
            evidence: "second transaction rolled back".into(),
        },
    );
    let snapshot = harness
        .service
        .authorize_retry("task", "safe-retry", "a1", "checked effects")
        .unwrap();
    assert!(snapshot.attempts["a1"].retry_authorized);
    let fact = harness
        .service
        .ledger()
        .execution_events_after("e", 0)
        .unwrap()
        .into_iter()
        .find(|event| event.kind == LedgerEventKind::ExecutionRetryAuthorized)
        .unwrap();
    let proof: RetryProof = serde_json::from_value(fact.payload["proof"].clone()).unwrap();
    assert_eq!(proof.reconciliations.len(), 2);
    for reference in proof.reconciliations {
        let actual = harness
            .service
            .ledger()
            .event_by_id(&reference.event_id)
            .unwrap()
            .unwrap();
        assert_eq!(reference, evidence::source_ref(&harness.binding, &actual));
        assert_eq!(actual.kind, LedgerEventKind::EffectReconciled);
    }
}

#[test]
fn unknown_or_missing_inspection_never_grants_retry() {
    for inspected in [false, true] {
        let harness = harness(None, false);
        let request = effect(&harness, "step/call");
        stop(&harness, true);
        if inspected {
            resolve(
                &harness,
                &request,
                ReconciliationResolution::Unknown {
                    evidence: "executor cannot determine transaction outcome".into(),
                },
            );
        }
        let before = harness.service.coordinator.snapshot("task").unwrap();
        assert!(
            harness
                .service
                .authorize_retry("task", "unsafe", "a1", "cannot prove absence")
                .is_err()
        );
        assert_eq!(
            harness.service.coordinator.snapshot("task").unwrap(),
            before
        );
        assert_eq!(proof_count(&harness.service), 0);
    }
}

#[test]
fn any_committed_receipt_blocks_whole_attempt_retry() {
    let harness = harness(None, false);
    let request = effect(&harness, "step/call");
    stop(&harness, true);
    resolve(
        &harness,
        &request,
        ReconciliationResolution::Committed {
            output: ToolResult {
                call_id: "call".into(),
                content: "written".into(),
                is_error: false,
            },
            executor_id: "file-executor".into(),
            executor_revision: "r1".into(),
            evidence: "verified committed external transaction".into(),
        },
    );
    assert!(
        harness
            .service
            .authorize_retry("task", "unsafe", "a1", "already committed")
            .is_err()
    );
    assert_eq!(proof_count(&harness.service), 0);
}

#[test]
fn cancellation_and_active_worker_block_retry_before_any_proof_write() {
    for active in [false, true] {
        let harness = harness(None, active);
        stop(&harness, false);
        if !active {
            harness
                .service
                .coordinator
                .cancel_task("task", "cancel", "user stopped task")
                .unwrap();
        }
        assert!(
            harness
                .service
                .authorize_retry("task", "unsafe", "a1", "must remain stopped")
                .is_err()
        );
        assert_eq!(proof_count(&harness.service), 0);
    }
    let harness = harness(None, false);
    harness
        .service
        .execution
        .execution()
        .cancel(&harness.binding.execution)
        .unwrap();
    // Capture a cancellation committed before the host's stopped observation.
    // Cancelling an already failed execution is intentionally a coordinator no-op.
    stop(&harness, false);
    assert!(
        harness
            .service
            .authorize_retry("task", "unsafe", "a1", "physical cancellation")
            .is_err()
    );
    assert_eq!(proof_count(&harness.service), 0);
}

#[test]
fn foreign_task_execution_session_or_turn_binding_cannot_authorize_retry() {
    for field in ["task", "execution", "session", "turn"] {
        let harness = harness(Some(field), false);
        stop(&harness, false);
        assert!(
            harness
                .service
                .authorize_retry("task", "foreign", "a1", "wrong binding")
                .is_err(),
            "{field}"
        );
        assert_eq!(proof_count(&harness.service), 0);
    }
}

#[test]
fn repeats_are_identical_and_changed_reason_or_physical_source_fails() {
    let harness = harness(None, false);
    stop(&harness, false);
    let first = harness
        .service
        .authorize_retry("task", "safe", "a1", "verified stopped before effect")
        .unwrap();
    assert_eq!(
        harness
            .service
            .authorize_retry("task", "safe", "a1", "verified stopped before effect")
            .unwrap(),
        first
    );
    assert!(
        harness
            .service
            .authorize_retry("task", "safe", "a1", "changed reason")
            .is_err()
    );
    assert!(
        harness
            .service
            .authorize_retry(
                "task",
                "different-fact",
                "a1",
                "verified stopped before effect"
            )
            .is_err()
    );
    append(
        &harness.service,
        "e/late-evidence",
        LedgerEventKind::TurnFailed,
        json!({"message": "later execution evidence"}),
    );
    assert!(
        harness
            .service
            .authorize_retry("task", "safe", "a1", "verified stopped before effect")
            .is_err()
    );
    assert_eq!(proof_count(&harness.service), 1);
    assert_eq!(harness.service.coordinator.snapshot("task").unwrap(), first);
}

#[test]
fn foreign_not_committed_inspection_cannot_resolve_this_effect() {
    let harness = harness(None, false);
    let request = effect(&harness, "step/call");
    stop(&harness, true);
    let mut foreign = request;
    foreign.execution.session_id = "other-session".into();
    append(
        &harness.service,
        "e/effect/step/call/reconciliation/inspection",
        LedgerEventKind::EffectReconciled,
        json!({"request": foreign, "resolution": ReconciliationResolution::NotCommitted { evidence: "another transaction rolled back".into() }}),
    );
    assert!(
        harness
            .service
            .authorize_retry("task", "unsafe", "a1", "foreign inspection")
            .is_err()
    );
    assert_eq!(proof_count(&harness.service), 0);
}

#[derive(Clone)]
struct FailingJournal {
    inner: MemoryFactJournal,
    fail_once: Arc<AtomicBool>,
}
impl FactJournal for FailingJournal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.inner.read(stream, after, limit)
    }
    fn append(
        &self,
        stream: &str,
        expected: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        if batch
            .iter()
            .any(|draft| draft.kind == "task.retry_authorized")
            && self.fail_once.swap(false, Ordering::SeqCst)
        {
            return Err(FactError::Storage("injected domain commit failure".into()));
        }
        self.inner.append(stream, expected, batch)
    }
}

#[test]
fn repeat_repairs_proof_to_domain_crash_boundary_without_second_proof() {
    let harness = harness(None, false);
    stop(&harness, false);
    let journal = FailingJournal {
        inner: harness.service.coordinator.journal().clone(),
        fail_once: Arc::new(AtomicBool::new(true)),
    };
    let service = TaskExecutionService::new(
        TaskCoordinator::new(journal),
        harness.service.execution.clone(),
    );
    assert!(
        service
            .authorize_retry("task", "safe", "a1", "verified stopped before effect")
            .is_err()
    );
    assert_eq!(proof_count(&harness.service), 1);
    assert!(!service.coordinator.snapshot("task").unwrap().attempts["a1"].retry_authorized);
    let repaired = service
        .authorize_retry("task", "safe", "a1", "verified stopped before effect")
        .unwrap();
    assert!(repaired.attempts["a1"].retry_authorized);
    assert_eq!(proof_count(&harness.service), 1);
}
