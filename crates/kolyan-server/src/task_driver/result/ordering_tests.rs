//! Core/Runtime-produced stop facts, independent corrupt copies, export then compare.

use super::*;
use crate::input_fixture::SourceFixtureAdmission;
use crate::{
    AgentIdentity, CancellationPolicy, CompletionCriterion, ExecutionRef, ExecutionService,
    InvocationDefinition, InvocationRole, SessionExecutionService, SessionService, TaskDefinition,
    TaskExecutionService, TaskLimits,
};
use kolyan_core::{
    ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture, TurnConfig,
    TurnExecutor, TurnRequest,
};
use kolyan_ledger::{InMemoryLedger, MemoryFactJournal};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRef, ModelRequest,
    ProviderFuture, TokenUsage, ToolCall, ToolChoice, ToolResult,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PolicyEngine, PreparedCall,
    ResourceClaim, ToolManifest, ToolRequirements,
};
use kolyan_storage::{FileSessionStore, SessionStore};
use kolyan_trace::NoopTraceSink;
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Seed {
    Completed,
    Cancelled,
    MaxSteps,
    Refused,
    Incomplete,
    NoProgress,
    Failed,
    TimedOut,
    ApprovalRejected,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    Notification,
    RepeatedBoundary,
    LateIncomplete,
    LateRefused,
    LateStep,
    BackfilledStep,
    LateRequest,
    LateDelta,
    IntentOnly,
    DebugOnly,
    MissingStep,
    CorruptStep,
    FailureNotification,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    seed: Seed,
    mutation: Mutation,
    expected_inspect: String,
    expected_result: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Data {
    cases: Vec<Case>,
    completed: Vec<ContentBlock>,
    max_steps: Vec<ContentBlock>,
    refused: Vec<ContentBlock>,
}

struct Provider {
    seed: Seed,
    response: ModelResponse,
    cancel: Option<(
        ExecutionService<InMemoryLedger, NoopTraceSink>,
        ExecutionRef,
    )>,
    requests: Arc<AtomicUsize>,
}
impl ModelProvider for Provider {
    fn stream(&self, _: ModelRequest) -> ProviderFuture<'_> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if matches!(self.seed, Seed::TimedOut) {
                return std::future::pending().await;
            }
            if matches!(self.seed, Seed::Failed) {
                return Err(kolyan_model::ProviderError::new(
                    kolyan_model::ProviderErrorKind::Unavailable,
                    kolyan_model::ProviderErrorPhase::Open,
                    "fixture provider failure",
                ));
            }
            if let Some((service, key)) = &self.cancel {
                service.cancel(key).unwrap();
            }
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(self.response.clone())),
            ])) as ModelEventStream)
        })
    }
}
struct Query {
    calls: Arc<AtomicUsize>,
}
impl ToolExecutor for Query {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            assert_eq!(call.name, "fixture.query");
            Ok(PreparedCall::new(
                call,
                "ordering-fixture-v1".into(),
                InvocationClaim {
                    tool_name: "fixture.query".into(),
                    capabilities: [Capability::ProcessInspect].into(),
                    effects: [Effect::Read].into(),
                    resource: ResourceClaim { path: None },
                    idempotency: Idempotency::Idempotent,
                },
                ToolRequirements {
                    process_sandbox: false,
                    max_output_bytes: 4096,
                    timeout_ms: 1000,
                },
            )
            .unwrap())
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &invocation.policy_revision,
                    &invocation.scope,
                )
                .unwrap();
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "fixture query returned".into(),
                is_error: false,
            }))
        })
    }
}

fn add(
    ledger: &InMemoryLedger,
    key: &ExecutionRef,
    id: &str,
    kind: LedgerEventKind,
    payload: serde_json::Value,
) {
    ledger
        .append(LedgerEvent {
            event_id: id.into(),
            idempotency_key: id.into(),
            execution_id: key.execution_id.clone(),
            turn_id: key.turn_id.clone(),
            cursor: 0,
            kind,
            payload,
        })
        .unwrap();
}

async fn scenario(data: &Data, case: &Case) -> serde_json::Value {
    let directory = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(directory.path()).unwrap();
    store.create("session").unwrap();
    let journal = MemoryFactJournal::default();
    let coordinator = TaskCoordinator::new(journal);
    let agent = AgentIdentity {
        definition_id: "ordering-agent".into(),
        revision: "r1".into(),
        instance_id: "ordering-instance".into(),
    };
    coordinator
        .register_task(
            "register",
            TaskDefinition {
                task_id: "task".into(),
                objective: case.id.clone(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent.clone(),
                constraints_digest: "c".repeat(64),
                limits: TaskLimits {
                    max_depth: 1,
                    max_invocations: 1,
                    max_attempts: 1,
                    max_tokens: None,
                    max_steps_per_turn: if matches!(case.seed, Seed::NoProgress) {
                        6
                    } else {
                        1
                    },
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    let source = crate::input_fixture::fixture_source(
        "task",
        "root",
        crate::InvocationInputKind::Standalone,
    );
    coordinator
        .admit_fixture(
            "task",
            "admit",
            InvocationDefinition {
                invocation_id: "root".into(),
                agent: agent.clone(),
                constraints_digest: "c".repeat(64),
                role: InvocationRole::Root,
                parent_invocation_id: None,
                dependencies: vec![],
                input_source: source.clone(),
            },
        )
        .unwrap();
    let key = ExecutionRef {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "execution".into(),
    };
    let binding = AttemptBinding {
        attempt_id: "attempt".into(),
        invocation_id: "root".into(),
        execution: key.clone(),
        agent,
        constraints_digest: "c".repeat(64),
        input_source: source,
    };
    let ledger = InMemoryLedger::default();
    let execution = ExecutionService::new(ledger.clone(), NoopTraceSink);
    let service = TaskExecutionService::new(
        coordinator.clone(),
        SessionExecutionService::new(execution.clone(), SessionService::new(store)),
    );
    let requests = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let content = match case.seed {
        Seed::MaxSteps | Seed::NoProgress | Seed::ApprovalRejected => data.max_steps.clone(),
        Seed::Refused => data.refused.clone(),
        _ => data.completed.clone(),
    };
    let response = ModelResponse {
        id: "actual-response".into(),
        model: ModelRef::new("fixture", "ordering"),
        content,
        structured_output: None,
        stop_reason: match case.seed {
            Seed::MaxSteps | Seed::NoProgress | Seed::ApprovalRejected => {
                kolyan_model::StopReason::ToolUse
            }
            Seed::Refused => kolyan_model::StopReason::Refusal,
            Seed::Incomplete => kolyan_model::StopReason::MaxOutputTokens,
            _ => kolyan_model::StopReason::EndTurn,
        },
        usage: TokenUsage {
            input_tokens: Some(7),
            output_tokens: Some(3),
            ..Default::default()
        },
        metadata: serde_json::Value::Null,
    };
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "fixture.query".into(),
        capabilities: [Capability::ProcessInspect].into(),
        effects: [Effect::Read].into(),
        path_scopes: vec![],
        idempotency: Idempotency::Idempotent,
        approval: if matches!(case.seed, Seed::ApprovalRejected) {
            ApprovalMode::Always
        } else {
            ApprovalMode::Never
        },
    });
    if matches!(case.seed, Seed::NoProgress) {
        policy = policy
            .with_progress_policy(kolyan_policy::ProgressPolicy {
                repeat_limit: 2,
                polling_tools: Default::default(),
            })
            .unwrap();
    }
    let policy = Arc::new(policy);
    let executor = TurnExecutor::with_tools(
        Provider {
            seed: case.seed,
            response,
            cancel: matches!(case.seed, Seed::Cancelled).then_some((execution, key.clone())),
            requests: requests.clone(),
        },
        Query {
            calls: calls.clone(),
        },
    )
    .with_policy_engine(policy.clone());
    let original_result = service
        .run(
            "task",
            binding.clone(),
            executor,
            TurnRequest {
                turn_id: key.turn_id.clone(),
                config: TurnConfig {
                    max_steps: if matches!(case.seed, Seed::NoProgress) {
                        6
                    } else {
                        1
                    },
                    deadline: matches!(case.seed, Seed::TimedOut)
                        .then_some(std::time::Duration::from_millis(50)),
                    ..Default::default()
                },
                model_request: ModelRequest {
                    request_id: "request".into(),
                    model: ModelRef::new("fixture", "ordering"),
                    system: vec![],
                    messages: vec![],
                    tools: vec![],
                    tool_choice: ToolChoice::Auto,
                    output_format: None,
                    prompt_cache: None,
                    reasoning: None,
                    max_output_tokens: Some(64),
                    extensions: serde_json::Value::Null,
                },
            },
        )
        .await;
    if matches!(case.seed, Seed::ApprovalRejected) {
        let saved = service
            .sessions()
            .execution()
            .load_current_suspension(&key.execution_id)
            .unwrap();
        let id = &saved.checkpoint.approvals[0].approval_id;
        service
            .sessions()
            .deny(
                TurnExecutor::with_tools(
                    Provider {
                        seed: Seed::Completed,
                        response: ModelResponse {
                            id: "not-called".into(),
                            model: ModelRef::new("fixture", "ordering"),
                            content: data.completed.clone(),
                            structured_output: None,
                            stop_reason: kolyan_model::StopReason::EndTurn,
                            usage: TokenUsage::default(),
                            metadata: serde_json::Value::Null,
                        },
                        cancel: None,
                        requests: requests.clone(),
                    },
                    Query {
                        calls: calls.clone(),
                    },
                )
                .with_policy_engine(policy),
                &key,
                id,
            )
            .unwrap();
        service.reconcile("task", &binding.attempt_id).unwrap();
    }
    let original = ledger.events_after(0).unwrap();
    let damaged = InMemoryLedger::default();
    let mut held = None;
    for mut event in original.clone() {
        if matches!(case.mutation, Mutation::IntentOnly)
            && evidence::is_physical_terminal(&event).unwrap()
        {
            continue;
        }
        if matches!(case.mutation, Mutation::DebugOnly)
            && evidence::is_physical_terminal(&event).unwrap()
        {
            continue;
        }
        if event.kind == LedgerEventKind::StepCompleted {
            match case.mutation {
                Mutation::MissingStep => continue,
                Mutation::BackfilledStep => {
                    held = Some(event);
                    continue;
                }
                Mutation::CorruptStep => event.payload["step_id"] = json!("foreign-step"),
                _ => {}
            }
        }
        event.cursor = 0;
        damaged.append(event).unwrap();
    }
    match case.mutation {
        Mutation::Notification => add(
            &damaged,
            &key,
            "notification",
            LedgerEventKind::TurnCompleted,
            json!({"outcome":"Completed { diagnostic: not authority }"}),
        ),
        Mutation::RepeatedBoundary => add(
            &damaged,
            &key,
            "repeat",
            LedgerEventKind::TurnCompleted,
            json!({"reason":"FinalAnswer"}),
        ),
        Mutation::LateIncomplete => add(
            &damaged,
            &key,
            "late-boundary",
            LedgerEventKind::TurnCompleted,
            json!({"reason":"Incomplete"}),
        ),
        Mutation::LateRefused => add(
            &damaged,
            &key,
            "late-boundary",
            LedgerEventKind::TurnCompleted,
            json!({"reason":"Refused"}),
        ),
        Mutation::LateStep => {
            let mut step = original
                .iter()
                .find(|event| event.kind == LedgerEventKind::StepCompleted)
                .unwrap()
                .clone();
            step.event_id = "late-step".into();
            step.idempotency_key = step.event_id.clone();
            step.cursor = 0;
            damaged.append(step).unwrap();
        }
        Mutation::BackfilledStep => {
            let mut step = held.unwrap();
            step.cursor = 0;
            damaged.append(step).unwrap();
        }
        Mutation::LateRequest => {
            let event = original
                .iter()
                .find(|event| event.kind == LedgerEventKind::ModelRequested)
                .unwrap();
            add(
                &damaged,
                &key,
                "late-request",
                event.kind,
                event.payload.clone(),
            );
        }
        Mutation::LateDelta => add(
            &damaged,
            &key,
            "late-delta",
            LedgerEventKind::ModelStreamEvent,
            json!({"type":"text_delta","step_id":"turn-step-0","text":"late data"}),
        ),
        Mutation::FailureNotification => {
            let event = original
                .iter()
                .find(|event| {
                    matches!(
                        event.kind,
                        LedgerEventKind::TurnFailed | LedgerEventKind::TurnTimedOut
                    )
                })
                .unwrap();
            add(
                &damaged,
                &key,
                "failure-notification",
                event.kind,
                event.payload.clone(),
            );
        }
        _ => {}
    }
    let before = damaged.events_after(0).unwrap();
    let journal_before = coordinator.records("task").unwrap();
    let work_before = [
        requests.load(Ordering::SeqCst),
        calls.load(Ordering::SeqCst),
    ];
    let actual = evidence::inspect(&damaged, &binding, 0);
    let inspect_value = match actual {
        Ok(actual) => {
            json!({"status":match actual.stopped {StoppedOutcome::Completed=>"completed",StoppedOutcome::Cancelled=>"cancelled",StoppedOutcome::Failed(_)=>"failed",StoppedOutcome::RecoveryRequired=>"recovery",StoppedOutcome::Suspended(_)=>"suspended"},"source":actual.source,"response":actual.response})
        }
        Err(error) => json!({"status":"error","error":error.to_string()}),
    };
    let read = || {
        load_for(
            &coordinator,
            &damaged,
            "task",
            &binding,
            65536,
            ResultRead::Historical,
        )
        .map_err(|error| error.to_string())
    };
    let first = read();
    let second = read();
    json!({"id":case.id,"original_run_ok":original_result.is_ok(),"original_run_error":original_result.as_ref().err().map(ToString::to_string),"original_physical":original,"physical":before,"journal":journal_before,"inspect":inspect_value,"first":first,"second":second,
        "unchanged":before==damaged.events_after(0).unwrap() && journal_before==coordinator.records("task").unwrap(),"no_new_work":work_before==[requests.load(Ordering::SeqCst),calls.load(Ordering::SeqCst)],"requests":requests.load(Ordering::SeqCst),"tools":calls.load(Ordering::SeqCst)})
}

#[tokio::test]
async fn physical_terminal_ordering_exports_actual_core_facts_before_assertions() {
    let data: Data = serde_json::from_str(include_str!("ordering.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-physical-ordering-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("PHYSICAL_ORDERING_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    let mut rows = vec![];
    for case in &data.cases {
        let row = scenario(&data, case).await;
        serde_json::to_writer(&mut output, &row).unwrap();
        writeln!(output).unwrap();
        output.sync_all().unwrap();
        rows.push(row);
    }
    for (case, row) in data.cases.iter().zip(rows) {
        assert_eq!(
            row["inspect"]["status"], case.expected_inspect,
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["first"], row["second"],
            "{}: non-idempotent read",
            case.id
        );
        assert_eq!(row["unchanged"], true, "{}: facts mutated", case.id);
        assert_eq!(
            row["no_new_work"], true,
            "{}: reader reentered execution",
            case.id
        );
        match &case.expected_result {
            Some(status) => assert_eq!(
                row["first"]["Ok"]["outcome"]["status"], *status,
                "{}: {row}",
                case.id
            ),
            None => assert!(row["first"]["Err"].is_string(), "{}: {row}", case.id),
        }
        if case.expected_result.as_deref() == Some("completed") {
            let original = row["original_physical"].as_array().unwrap();
            let boundary = original
                .iter()
                .find(|event| {
                    event["kind"] == "turn_completed" && event["payload"]["reason"] == "FinalAnswer"
                })
                .unwrap();
            let step = original
                .iter()
                .rev()
                .find(|event| {
                    event["kind"] == "step_completed"
                        && event["cursor"].as_u64().unwrap() < boundary["cursor"].as_u64().unwrap()
                })
                .unwrap();
            assert_eq!(
                row["first"]["Ok"]["outcome"]["response"], step["payload"]["step"]["response"],
                "{}: response not frozen at physical stop",
                case.id
            );
            assert_eq!(
                row["inspect"]["source"]["event_id"], boundary["event_id"],
                "{}: duplicate notification replaced source",
                case.id
            );
        }
        if matches!(case.seed, Seed::MaxSteps) {
            assert_eq!(row["tools"], 1);
            assert!(
                row["original_physical"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|event| event["kind"] == "step_completed"
                        && event["payload"]["step"]["outcome"] == "ToolCalls"),
                "actual Core Step must be ToolCalls: {row}"
            );
            assert!(
                row["original_physical"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|event| event["kind"] == "turn_completed"
                        && event["payload"]["reason"] == "MaxSteps")
            );
        }
    }
}
