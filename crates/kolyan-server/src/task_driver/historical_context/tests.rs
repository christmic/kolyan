//! Real Core/Runtime/Session completion, then frozen proof reads without effects.
use super::*;
use crate::input_fixture::SourceFixtureAdmission;
use crate::{
    AgentIdentity, CancellationPolicy, CompletionCriterion, ExecutionService, InvocationDefinition,
    InvocationRole, SessionService, TaskDefinition, TaskLimits,
};
use futures_util::stream;
use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{InMemoryLedger, MemoryFactJournal};
use kolyan_model::{
    ModelEvent, ModelEventStream, ModelRef, ModelRequest, ModelResponse, ProviderFuture,
    StopReason, TokenUsage, ToolChoice,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy, SessionTurn, SessionTurnStatus};
use kolyan_trace::NoopTraceSink;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    Read,
    QueryScope,
    AdmissionSchema,
    PreparedUnknown,
    PreparedDelta,
    MissingCommit,
    TerminalRef,
    Endpoint,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    max_bytes: usize,
    expected: Expected,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Expected {
    ExactContext,
    Refused,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum DeltaMutation {
    Read,
    OrphanResult,
    MissingResult,
    StepAfterFinal,
    ToolAfterFinal,
    ForeignStep,
    WrongIdentity,
    WrongOutcome,
    MissingOutcome,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DeltaCase {
    id: String,
    mutation: DeltaMutation,
    accepted: bool,
}

struct Provider;
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            Ok(
                Box::pin(stream::iter([Ok(ModelEvent::Completed(ModelResponse {
                    id: request.request_id,
                    model: request.model,
                    content: vec![
                        ContentBlock::Reasoning {
                            text: "retained reasoning".into(),
                            opaque: None,
                        },
                        ContentBlock::Text {
                            text: "actual Core final response".into(),
                        },
                    ],
                    structured_output: None,
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::default(),
                    metadata: json!(null),
                }))])) as ModelEventStream,
            )
        })
    }
}
fn text(value: &str) -> Message {
    Message {
        role: MessageRole::User,
        content: vec![ContentBlock::Text { text: value.into() }],
    }
}

#[derive(Clone)]
struct ReadOnly {
    ledger: InMemoryLedger,
    mode: Mutation,
}
impl LedgerStore for ReadOnly {
    fn query(&self, q: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        assert!(q.through.is_some(), "all physical reads must be frozen");
        let mut records = self.ledger.query(q)?;
        if self.mode == Mutation::MissingCommit {
            records.retain(|row| row.kind != LedgerEventKind::SessionCommitted);
        }
        for record in &mut records {
            match self.mode {
                Mutation::QueryScope => record.execution_id = "foreign".into(),
                Mutation::AdmissionSchema
                    if record.kind == LedgerEventKind::ExecutionInputAdmitted =>
                {
                    record.payload["schema_version"] = json!(2)
                }
                Mutation::PreparedUnknown
                    if record.kind == LedgerEventKind::SessionCommitPrepared =>
                {
                    record.payload["unknown"] = json!(true)
                }
                Mutation::PreparedDelta
                    if record.kind == LedgerEventKind::SessionCommitPrepared =>
                {
                    record.payload["context_messages"] = json!([])
                }
                _ => {}
            }
        }
        Ok(records)
    }
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        panic!("context reader executes")
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        panic!("context reader global audit")
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        panic!("context reader claims")
    }
}

#[tokio::test]
async fn historical_context_data_freezes_exact_admission_and_commit_not_latest_history() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-historical-context-")
        .tempdir()
        .unwrap()
        .keep();
    let ledger = InMemoryLedger::default();
    let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
    let store = FileSessionStore::new(root.join("sessions")).unwrap();
    store.create("s").unwrap();
    store
        .append_turn(
            "s",
            SessionTurn {
                turn_id: "prior".into(),
                execution_id: "prior-e".into(),
                status: SessionTurnStatus::Completed,
            },
            vec![text("immutable prior history")],
        )
        .unwrap();
    let agent = AgentIdentity {
        definition_id: "agent".into(),
        revision: "r".into(),
        instance_id: "instance".into(),
    };
    coordinator
        .register_task(
            "registered",
            TaskDefinition {
                task_id: "context-task".into(),
                objective: "source proof".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                limits: TaskLimits {
                    max_depth: 0,
                    max_invocations: 1,
                    max_attempts: 1,
                    max_tokens: None,
                    max_steps_per_turn: 3,
                },
                cancellation_policy: CancellationPolicy::RootOnly,
            },
        )
        .unwrap();
    coordinator
        .admit_fixture(
            "context-task",
            "admitted",
            InvocationDefinition {
                input_source: crate::input_fixture::fixture_source(
                    "context-task",
                    "root",
                    if (InvocationRole::Root) == crate::InvocationRole::Root {
                        crate::InvocationInputKind::Standalone
                    } else {
                        crate::InvocationInputKind::Derived
                    },
                ),
                invocation_id: "root".into(),
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                role: InvocationRole::Root,
                parent_invocation_id: None,
                dependencies: vec![],
            },
        )
        .unwrap();
    let binding = AttemptBinding {
        input_source: crate::input_fixture::fixture_source(
            "context-task",
            "root",
            crate::InvocationInputKind::Standalone,
        ),
        attempt_id: "attempt".into(),
        invocation_id: "root".into(),
        agent,
        constraints_digest: "a".repeat(64),
        execution: ExecutionRef {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        },
    };
    let sessions = |ledger| {
        SessionExecutionService::new(
            ExecutionService::new(ledger, NoopTraceSink),
            SessionService::new(store.clone()),
        )
        .with_context_policy(SessionContextPolicy::FullTrajectory)
    };
    let service = TaskExecutionService::new(coordinator.clone(), sessions(ledger.clone()));
    let request = TurnRequest {
        turn_id: "t".into(),
        config: TurnConfig {
            max_steps: 3,
            ..Default::default()
        },
        model_request: ModelRequest {
            request_id: "input".into(),
            model: ModelRef::new("fixture", "context"),
            system: vec![],
            messages: vec![text("exact current input")],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(40),
            extensions: json!(null),
        },
    };
    service
        .run(
            "context-task",
            binding.clone(),
            TurnExecutor::new(Provider).with_agent_snapshot_digest("a".repeat(64)),
            request,
        )
        .await
        .unwrap();
    let physical = service
        .load_verified_historical_result("context-task", &binding, 1024 * 1024)
        .unwrap();
    let events = ledger.execution_events_after("e", 0).unwrap();
    let endpoint = |kind| {
        evidence::source_ref(
            &binding,
            events
                .iter()
                .find(|event| event.kind == kind && event.payload["status"] == "completed")
                .unwrap(),
        )
    };
    let request = HistoricalContextRequest {
        binding: binding.clone(),
        terminal_fact: physical.terminal_fact,
        prepared: endpoint(LedgerEventKind::SessionCommitPrepared),
        committed: endpoint(LedgerEventKind::SessionCommitted),
        max_bytes: 16 * 1024 * 1024,
    };
    store
        .append_turn(
            "s",
            SessionTurn {
                turn_id: "future".into(),
                execution_id: "future-e".into(),
                status: SessionTurnStatus::Completed,
            },
            vec![text("not part of frozen context")],
        )
        .unwrap();
    // A later physical event cannot change the chosen completed through boundary.
    ledger
        .append(LedgerEvent {
            event_id: "e/later-boundary".into(),
            idempotency_key: "e/later-boundary".into(),
            execution_id: "e".into(),
            turn_id: "t".into(),
            cursor: 0,
            kind: LedgerEventKind::ExecutionBoundaryAdmitted,
            payload: json!(null),
        })
        .unwrap();
    let before = store.load("s").unwrap();
    let facts = coordinator.records("context-task").unwrap();
    let physical_before = ledger.events_after(0).unwrap();
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    use std::io::Write;
    println!(
        "HISTORICAL_CONTEXT_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let mut observations = Vec::new();
    for case in cases {
        let read = TaskExecutionService::new(
            coordinator.clone(),
            SessionExecutionService::new(
                ExecutionService::new(
                    ReadOnly {
                        ledger: ledger.clone(),
                        mode: case.mutation,
                    },
                    NoopTraceSink,
                ),
                SessionService::new(store.clone()),
            ),
        );
        let mut requested = request.clone();
        requested.max_bytes = case.max_bytes;
        match case.mutation {
            Mutation::TerminalRef => requested.terminal_fact.fact_id = "foreign".into(),
            Mutation::Endpoint => requested.prepared.cursor = requested.committed.cursor,
            _ => {}
        }
        let result = read.load_verified_historical_context("context-task", &requested);
        let session_after = store.load("s").unwrap();
        let facts_after = coordinator.records("context-task").unwrap();
        let physical_after = ledger.events_after(0).unwrap();
        let repeated = if matches!(case.expected, Expected::ExactContext) {
            Some(read.load_verified_historical_context("context-task", &requested))
        } else {
            None
        };
        writeln!(output,"{}",json!({"case":case,"request":{"binding":requested.binding,"terminal":requested.terminal_fact,"prepared":requested.prepared,"committed":requested.committed,"max_bytes":requested.max_bytes},"before":{"session":before,"journal":facts,"physical":physical_before},"result":result.as_ref().map_err(ToString::to_string),"repeated":repeated.as_ref().map(|result| result.as_ref().map_err(ToString::to_string)),"after":{"session":session_after,"journal":facts_after,"physical":physical_after}})).unwrap();
        observations.push((
            case,
            result,
            repeated,
            session_after,
            facts_after,
            physical_after,
        ));
    }
    output.sync_all().unwrap();
    for (case, result, repeated, session_after, facts_after, physical_after) in observations {
        if matches!(case.expected, Expected::ExactContext) {
            let value = result.unwrap();
            assert_eq!(
                &value.messages[..2],
                &[text("immutable prior history"), text("exact current input")]
            );
            assert_eq!(value.messages.len(), 3);
            assert_eq!(value.messages[2].content.len(), 2);
            assert!(!value.messages.contains(&text("not part of frozen context")));
            assert_eq!(value, repeated.unwrap().unwrap());
        } else {
            assert!(result.is_err(), "{}", case.id);
        }
        assert_eq!(session_after, before, "{}", case.id);
        assert_eq!(facts_after, facts, "{}", case.id);
        assert_eq!(physical_after, physical_before, "{}", case.id);
    }
}

#[test]
fn context_delta_preserves_tool_pairs_and_refuses_orphan_results() {
    let execution = ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    let response = |content| ModelResponse {
        id: "response".into(),
        model: ModelRef::new("fixture", "model"),
        content,
        structured_output: None,
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::default(),
        metadata: json!(null),
    };
    let event = |cursor, kind, payload| LedgerEvent {
        event_id: format!("e/turn-event/start/{cursor}"),
        idempotency_key: format!("e/turn-event/start/{cursor}"),
        execution_id: "e".into(),
        turn_id: "t".into(),
        cursor,
        kind,
        payload,
    };
    let first = StepResult {
        step_id: "t-step-0".into(),
        response: response(vec![ContentBlock::ToolCall {
            call: kolyan_model::ToolCall {
                id: "call".into(),
                name: "read".into(),
                arguments: json!({}),
            },
        }]),
        outcome: kolyan_core::StepOutcome::ToolCalls,
    };
    let result = ToolResult {
        call_id: "call".into(),
        is_error: false,
        content: "exact\u{0000}tool bytes".into(),
    };
    let last = StepResult {
        step_id: "t-step-1".into(),
        response: response(vec![ContentBlock::Text {
            text: "final".into(),
        }]),
        outcome: kolyan_core::StepOutcome::FinalAnswer,
    };
    let events = vec![
        event(
            1,
            LedgerEventKind::StepCompleted,
            json!({"step_id":first.step_id,"outcome":first.outcome,"step":first}),
        ),
        event(
            2,
            LedgerEventKind::ToolExecutionCompleted,
            json!({"call_id":"call","is_error":false,"result":result}),
        ),
        event(
            3,
            LedgerEventKind::StepCompleted,
            json!({"step_id":last.step_id,"outcome":last.outcome,"step":last}),
        ),
    ];
    let cases: Vec<DeltaCase> = serde_json::from_str(include_str!("delta_cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-context-delta-")
        .tempdir()
        .unwrap()
        .keep();
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    use std::io::Write;
    println!(
        "CONTEXT_DELTA_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let mut observations = Vec::new();
    for case in cases {
        let mut source = events.clone();
        match case.mutation {
            DeltaMutation::Read => {}
            DeltaMutation::OrphanResult => {
                source.remove(0);
            }
            DeltaMutation::MissingResult => {
                source.remove(1);
            }
            DeltaMutation::StepAfterFinal | DeltaMutation::ToolAfterFinal => {
                let mut extra = source[if matches!(case.mutation, DeltaMutation::StepAfterFinal) {
                    2
                } else {
                    1
                }]
                .clone();
                extra.cursor = 4;
                extra.event_id = "e/turn-event/start/4".into();
                extra.idempotency_key = extra.event_id.clone();
                if matches!(case.mutation, DeltaMutation::StepAfterFinal) {
                    extra.payload["step_id"] = json!("t-step-2");
                    extra.payload["step"]["step_id"] = json!("t-step-2");
                }
                source.push(extra);
            }
            DeltaMutation::ForeignStep => {
                source[0].payload["step_id"] = json!("foreign-turn-step-0");
                source[0].payload["step"]["step_id"] = json!("foreign-turn-step-0");
            }
            DeltaMutation::WrongIdentity => source[1].idempotency_key = "foreign".into(),
            DeltaMutation::WrongOutcome => source[0].payload["outcome"] = json!("FinalAnswer"),
            DeltaMutation::MissingOutcome => {
                source[0].payload.as_object_mut().unwrap().remove("outcome");
            }
        }
        let actual = delta(&source, &execution, 0, 5);
        writeln!(output, "{}", json!({"case":case,"execution":execution,"physical":source,"result":actual.as_ref().map_err(ToString::to_string)})).unwrap();
        observations.push((case, actual));
    }
    output.sync_all().unwrap();
    for (case, actual) in observations {
        assert_eq!(actual.is_ok(), case.accepted, "{}", case.id);
        if case.accepted {
            assert_eq!(
                actual.unwrap().0[1].content,
                vec![ContentBlock::ToolResult {
                    result: result.clone()
                }]
            );
        }
    }
}
