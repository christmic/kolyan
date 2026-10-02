//! Historical proof reads cannot be used as active recovery/consumption ports.
use super::*;
use crate::input_fixture::SourceFixtureAdmission;
use crate::{
    ConsumedTerminalResult, ExecutionEvidence, ExecutionService, SessionExecutionService,
    SessionService, TaskState,
};
use kolyan_ledger::{FactDraft, FactError, FactRecord};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;
use std::io::Write;

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct Row {
    backend: String,
    task: String,
    child: String,
    check: String,
}

/// Flush each observation before its caller compares it; Drop also exports on panic.
struct Evidence<J: FactJournal + Clone> {
    journal: J,
    visible: ReadFault<J>,
    ledger: InMemoryLedger,
    path: std::path::PathBuf,
}
impl<J: FactJournal + Clone> Evidence<J> {
    fn record(&self, phase: &str, result: serde_json::Value) {
        let coordinator = TaskCoordinator::new(self.journal.clone());
        let visible = TaskCoordinator::new(self.visible.clone());
        let entry = json!({
            "phase": phase,
            "actual": result,
            "journal": coordinator.records("history").map_err(|e|e.to_string()),
            "visible_journal": visible.records("history").map_err(|e|e.to_string()),
            "physical_ledger": self.ledger.events_after(0).map_err(|e|e.to_string()),
            "snapshot": coordinator.snapshot("history").map_err(|e|e.to_string()),
        });
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .expect("open historical evidence");
        serde_json::to_writer(&mut file, &entry).expect("encode historical evidence");
        writeln!(file).expect("write historical evidence");
        file.sync_all()
            .expect("persist historical evidence before comparison");
    }
}
impl<J: FactJournal + Clone> Drop for Evidence<J> {
    fn drop(&mut self) {
        self.record("after", json!({"panicking":std::thread::panicking()}));
    }
}

struct RecordedService<'a, J: FactJournal + Clone> {
    service: TaskExecutionService<ReadFault<J>, InMemoryLedger, NoopTraceSink, FileSessionStore>,
    evidence: &'a Evidence<J>,
}
macro_rules! recorded_loader {
    ($name:ident, $result:ty, $($binding:ident),+) => {
        fn $name(&self, task: &str, $($binding: &AttemptBinding,)+ cap: usize)
            -> Result<$result, TaskExecutionError> {
            self.evidence.record("before_loader", json!({
                "method":stringify!($name),"task":task,"cap":cap,
                $(stringify!($binding):$binding,)+
            }));
            let result = self.service.$name(task, $($binding,)+ cap);
            self.evidence.record("loader_result", json!({
                "method":stringify!($name),"ok":result.is_ok(),
                "result":result.as_ref().map_err(|error|error.to_string()),
                "diagnostic":format!("{result:?}")
            }));
            result
        }
    };
}
impl<J: FactJournal + Clone> RecordedService<'_, J> {
    recorded_loader!(load_verified_historical_result, VerifiedTaskResult, binding);
    recorded_loader!(load_verified_result, VerifiedTaskResult, binding);
    recorded_loader!(
        load_verified_historical_consumed_result,
        Option<VerifiedConsumedResult>,
        parent,
        child
    );
    recorded_loader!(
        load_verified_consumed_result,
        Option<VerifiedConsumedResult>,
        parent,
        child
    );
}

#[derive(Clone)]
struct ReadFault<J> {
    journal: J,
    mode: String,
}
impl<J: FactJournal> FactJournal for ReadFault<J> {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        let mut records = self.journal.read(stream, after, limit)?;
        for record in &mut records {
            if record.draft.kind == "task.terminal_result_consumed" {
                match self.mode.as_str() {
                    "unknown_schema" => record.draft.schema_version = 2,
                    "corrupt_consumption" => {
                        record.draft.payload = json!({"kind":"not_a_consumption"})
                    }
                    "corrupt_cause" => record.draft.causes.clear(),
                    _ => {}
                }
            }
        }
        Ok(records)
    }
    fn append(&self, _: &str, _: u64, _: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        panic!("historical verifier must never append")
    }
}

fn physical(
    ledger: &InMemoryLedger,
    binding: &AttemptBinding,
    kind: LedgerEventKind,
    payload: serde_json::Value,
) -> LedgerEvent {
    let cursor = ledger
        .execution_events_after(&binding.execution.execution_id, 0)
        .unwrap()
        .len();
    let id = if kind == LedgerEventKind::ExecutionStarted {
        format!("{}/execution-started", binding.execution.execution_id)
    } else if kind == LedgerEventKind::ExecutionBound {
        format!("{}/task-binding", binding.execution.execution_id)
    } else {
        format!("{}/{cursor}", binding.execution.execution_id)
    };
    ledger
        .append(LedgerEvent {
            event_id: id.clone(),
            idempotency_key: id,
            execution_id: binding.execution.execution_id.clone(),
            turn_id: binding.execution.turn_id.clone(),
            cursor: 0,
            kind,
            payload,
        })
        .unwrap()
}

fn terminal<J: FactJournal>(
    coordinator: &TaskCoordinator<J>,
    ledger: &InMemoryLedger,
    binding: &AttemptBinding,
    status: &str,
) -> (FactRef, ExecutionEvidence, Vec<CompletionEvidence>) {
    physical(
        ledger,
        binding,
        LedgerEventKind::ExecutionStarted,
        json!(binding.execution),
    );
    physical(
        ledger,
        binding,
        LedgerEventKind::ExecutionBound,
        json!({"binding":ExecutionBinding {task_id:"history".into(),invocation_id:binding.invocation_id.clone(),attempt_id:binding.attempt_id.clone(),session_id:binding.execution.session_id.clone(),turn_id:binding.execution.turn_id.clone(),execution_id:binding.execution.execution_id.clone()}}),
    );
    let response = ModelResponse {
        id: format!("response-{}", binding.invocation_id),
        model: ModelRef::new("fixture", "history"),
        content: vec![ContentBlock::Text {
            text: format!("actual fixture response {}", binding.invocation_id),
        }],
        structured_output: None,
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::default(),
        metadata: serde_json::Value::Null,
    };
    if status == "completed" {
        physical(
            ledger,
            binding,
            LedgerEventKind::StepCompleted,
            json!({"step_id":format!("{}-step",binding.invocation_id),"step":kolyan_core::StepResult {step_id:format!("{}-step",binding.invocation_id),response:response.clone(),outcome:kolyan_core::StepOutcome::FinalAnswer}}),
        );
    }
    let kind = match status {
        "completed" => LedgerEventKind::TurnCompleted,
        "failed" => LedgerEventKind::TurnFailed,
        "cancelled" => LedgerEventKind::TurnCancelled,
        _ => panic!("unknown fixture status"),
    };
    let event = physical(
        ledger,
        binding,
        kind,
        if status == "completed" {
            json!({"reason":"FinalAnswer"})
        } else {
            json!({"reason":format!("fixture {status}")})
        },
    );
    let source = evidence::source_ref(binding, &event);
    let proofs = if binding.invocation_id == "root" {
        vec![CompletionEvidence::ExecutionResult {
            criterion_id: "done".into(),
            source: source.clone(),
            result_digest: response_digest(&response).unwrap(),
        }]
    } else {
        vec![]
    };
    let outcome = match status {
        "completed" => AttemptOutcome::Completed {
            evidence: proofs.clone(),
        },
        "failed" => AttemptOutcome::Failed {
            reason: event.payload.to_string(),
            safe_to_retry: false,
        },
        "cancelled" => AttemptOutcome::Cancelled {
            reason: event.payload.to_string(),
        },
        _ => unreachable!(),
    };
    coordinator
        .observe_attempt(
            "history",
            &format!("observed/{}", binding.invocation_id),
            AttemptObservation {
                attempt_id: binding.attempt_id.clone(),
                execution: binding.execution.clone(),
                source: source.clone(),
                usage: TaskUsage::default(),
                outcome,
            },
        )
        .unwrap();
    let records = coordinator.records("history").unwrap();
    let last = records.last().unwrap();
    (
        FactRef {
            stream_id: last.stream_id.clone(),
            position: last.position,
            fact_id: last.draft.fact_id.clone(),
        },
        source,
        proofs,
    )
}

fn run<J: FactJournal + Clone + 'static>(journal: J, row: &Row) {
    let root = tempfile::tempdir().unwrap();
    let coordinator = TaskCoordinator::new(journal.clone());
    let ledger = InMemoryLedger::default();
    let export = tempfile::Builder::new()
        .prefix("kolyan-historical-proof-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = Evidence {
        journal: journal.clone(),
        visible: ReadFault {
            journal: journal.clone(),
            mode: row.check.clone(),
        },
        ledger: ledger.clone(),
        path: export.join("actual.jsonl"),
    };
    eprintln!(
        "historical row={} evidence={}",
        serde_json::to_string(row).unwrap(),
        evidence.path.display()
    );
    evidence.record("row", json!(row));
    let agent = AgentIdentity {
        definition_id: "history-agent".into(),
        revision: "1".into(),
        instance_id: "root-instance".into(),
    };
    coordinator
        .register_task(
            "registered",
            TaskDefinition {
                task_id: "history".into(),
                objective: "historical verification".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                limits: TaskLimits {
                    max_depth: 2,
                    max_invocations: 2,
                    max_attempts: 2,
                    max_tokens: None,
                    max_steps_per_turn: 4,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    let mut bindings = vec![];
    for (id, role, parent) in [
        ("root", InvocationRole::Root, None),
        ("child", InvocationRole::Delegation, Some("root".into())),
    ] {
        let mut identity = agent.clone();
        identity.instance_id = format!("{id}-instance");
        coordinator
            .admit_fixture(
                "history",
                &format!("admitted/{id}"),
                InvocationDefinition {
                    input_source: crate::input_fixture::fixture_source(
                        "history",
                        id,
                        if (role) == crate::InvocationRole::Root {
                            crate::InvocationInputKind::Standalone
                        } else {
                            crate::InvocationInputKind::Derived
                        },
                    ),
                    invocation_id: id.into(),
                    agent: identity.clone(),
                    constraints_digest: "a".repeat(64),
                    role,
                    parent_invocation_id: parent,
                    dependencies: vec![],
                },
            )
            .unwrap();
        let binding = AttemptBinding {
            input_source: crate::input_fixture::fixture_source(
                "history",
                id,
                if role == crate::InvocationRole::Root {
                    crate::InvocationInputKind::Standalone
                } else {
                    crate::InvocationInputKind::Derived
                },
            ),
            attempt_id: format!("attempt-{id}"),
            invocation_id: id.into(),
            agent: identity,
            constraints_digest: "a".repeat(64),
            execution: ExecutionRef {
                session_id: format!("session-{id}"),
                turn_id: format!("turn-{id}"),
                execution_id: format!("execution-{id}"),
            },
        };
        coordinator
            .start_attempt("history", &format!("started/{id}"), binding.clone())
            .unwrap();
        bindings.push(binding);
    }
    let parent = &bindings[0];
    let child = &bindings[1];
    let (fact, source, proofs) = terminal(&coordinator, &ledger, child, &row.child);
    let disposition = match row.child.as_str() {
        "completed" => TerminalResultDisposition::Completed { evidence: proofs },
        "failed" => TerminalResultDisposition::Failed {
            reason: json!({"reason":"fixture failed"}).to_string(),
        },
        "cancelled" => TerminalResultDisposition::Cancelled {
            reason: json!({"reason":"fixture cancelled"}).to_string(),
        },
        _ => unreachable!(),
    };
    coordinator
        .consume_terminal_result(
            "history",
            "consumed",
            &parent.invocation_id,
            ConsumedTerminalResult {
                parent: parent.clone(),
                child: child.clone(),
                terminal_fact: fact,
                source,
                disposition,
            },
        )
        .unwrap();
    let (_, _, proofs) = terminal(&coordinator, &ledger, parent, "completed");
    match row.task.as_str() {
        "Completed" => {
            coordinator
                .complete_task("history", "finalized", proofs)
                .unwrap();
        }
        "Failed" => {
            coordinator
                .fail_task("history", "finalized", "verified child failure")
                .unwrap();
        }
        "Cancelled" => {
            coordinator
                .cancel_task("history", "finalized", "root cancellation")
                .unwrap();
        }
        _ => panic!("unknown task status"),
    }
    let service = RecordedService {
        evidence: &evidence,
        service: TaskExecutionService::new(
            TaskCoordinator::new(ReadFault {
                journal: journal.clone(),
                mode: row.check.clone(),
            }),
            SessionExecutionService::new(
                ExecutionService::new(ledger.clone(), NoopTraceSink),
                SessionService::new(FileSessionStore::new(root.path()).unwrap()),
            ),
        ),
    };
    let before = coordinator.records("history").unwrap();
    let physical_before = ledger.events_after(0).unwrap();
    if row.check == "unknown_schema" || row.check.starts_with("corrupt_") {
        assert!(
            service
                .load_verified_historical_consumed_result("history", parent, child, 1024 * 1024)
                .is_err()
        );
        assert_eq!(coordinator.records("history").unwrap(), before);
        assert_eq!(ledger.events_after(0).unwrap(), physical_before);
        return;
    }
    let historical = service
        .load_verified_historical_result("history", parent, 1024 * 1024)
        .unwrap();
    assert!(matches!(
        historical.outcome,
        VerifiedTaskOutcome::Completed { .. }
    ));
    assert!(
        service
            .load_verified_consumed_result("history", parent, child, 1024 * 1024)
            .is_err()
    );
    if row.task != "Completed" {
        assert!(
            service
                .load_verified_result("history", parent, 1024 * 1024)
                .is_err()
        );
    }
    let mut parent = parent.clone();
    let mut child = child.clone();
    match row.check.as_str() {
        "read" => {
            let saved = service
                .load_verified_historical_consumed_result("history", &parent, &child, 1024 * 1024)
                .unwrap()
                .unwrap();
            assert_eq!(
                saved.child,
                service
                    .load_verified_historical_result("history", &child, 1024 * 1024)
                    .unwrap()
            );
            assert_eq!(
                saved,
                service
                    .load_verified_historical_consumed_result(
                        "history",
                        &parent,
                        &child,
                        1024 * 1024
                    )
                    .unwrap()
                    .unwrap()
            );
        }
        "foreign_parent" => {
            parent.execution.session_id = "foreign".into();
            assert!(
                service
                    .load_verified_historical_consumed_result(
                        "history",
                        &parent,
                        &child,
                        1024 * 1024
                    )
                    .is_err()
            );
        }
        "foreign_child" => {
            child.constraints_digest = "b".repeat(64);
            assert!(
                service
                    .load_verified_historical_consumed_result(
                        "history",
                        &parent,
                        &child,
                        1024 * 1024
                    )
                    .is_err()
            );
        }
        "small_cap" => {
            assert!(
                service
                    .load_verified_historical_consumed_result("history", &parent, &child, 1)
                    .is_err()
            );
        }
        "late_physical_cancel" => {
            physical(
                &ledger,
                &parent,
                LedgerEventKind::TurnCancelled,
                json!({"reason":"late conflicting physical terminal"}),
            );
            assert!(
                service
                    .load_verified_historical_consumed_result(
                        "history",
                        &parent,
                        &child,
                        1024 * 1024
                    )
                    .is_err()
            );
        }
        "missing_consumption" => {
            assert!(
                service
                    .load_verified_historical_consumed_result(
                        "history",
                        &parent,
                        &parent,
                        1024 * 1024
                    )
                    .unwrap()
                    .is_none()
            );
        }
        _ => panic!("unknown fixture check"),
    }
    assert_eq!(
        coordinator.records("history").unwrap(),
        before,
        "read must not finalize or consume again"
    );
    if row.check != "late_physical_cancel" {
        assert_eq!(
            ledger.events_after(0).unwrap(),
            physical_before,
            "read must not execute or publish"
        );
    }
    assert!(matches!(
        coordinator.snapshot("history").unwrap().state,
        TaskState::Completed | TaskState::Failed | TaskState::Cancelled
    ));
}

#[test]
fn historical_result_and_consumption_data_reconstruct_without_writes() {
    let rows: Vec<Row> = serde_json::from_str(include_str!("historical.json")).unwrap();
    for row in rows {
        match row.backend.as_str() {
            "memory" => run(MemoryFactJournal::default(), &row),
            "sqlite" => {
                let database = tempfile::tempdir().unwrap();
                run(
                    SqliteFactJournal::open(database.path().join("journal.sqlite")).unwrap(),
                    &row,
                );
            }
            _ => panic!("unknown backend"),
        }
    }
}
