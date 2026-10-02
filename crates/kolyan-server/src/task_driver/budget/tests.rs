//! Actual durable approval production followed by private, read-only budget checks.
//! Synthetic Provider/Tools are counted ports, not SDK or native-effect evidence.

use std::{
    io::Write,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use kolyan_core::{ToolFuture, ToolInvocation, ToolPreparationFuture, TurnConfig, TurnRequest};
use kolyan_ledger::{InMemoryLedger, MemoryFactJournal, SqliteFactJournal, SqliteLedger};
use kolyan_model::{
    ModelEvent, ModelEventStream, ModelRequest, ModelResponse, ProviderFuture, ToolCall,
};
use kolyan_policy::{ApprovalMode, PolicyEngine, PreparedCall, ToolManifest};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::{
    AgentIdentity, CancellationPolicy, ExecutionRef, InvocationDefinition, InvocationInputKind,
    InvocationRole, SessionService, TaskDefinition, TaskExecutionBudgetPolicy, TaskLimits,
    input_fixture::{SourceFixtureAdmission, fixture_source},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: String,
    accepted: bool,
    error_contains: Option<String>,
}

#[derive(Default)]
struct Activity {
    requests: Mutex<Vec<ModelRequest>>,
    prepared: AtomicUsize,
    effects: AtomicUsize,
}

struct Provider {
    activity: Arc<Activity>,
    response: ModelResponse,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.activity.requests.lock().unwrap().push(request);
        let response = self.response.clone();
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

struct Tools {
    activity: Arc<Activity>,
    prepared: PreparedCall,
}
impl ToolExecutor for Tools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        self.activity.prepared.fetch_add(1, Ordering::SeqCst);
        let prepared = self.prepared.clone();
        Box::pin(async move {
            if prepared.call() != &call {
                return Err(kolyan_core::ToolError::Failed {
                    message: "fixture call differs".into(),
                });
            }
            Ok(prepared)
        })
    }
    fn execute_invocation(&self, _: ToolInvocation) -> ToolFuture<'_> {
        self.activity.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(kolyan_core::ToolError::Failed {
                message: "no execution authorized by this test".into(),
            })
        })
    }
}

fn executor(activity: Arc<Activity>) -> TurnExecutor<Provider, Tools> {
    // Only fixture response/arguments seed the real run; its hand-built checkpoint
    // is never loaded, persisted, passed to the checker or treated as authority.
    let stimulus = crate::suspension::tests::fixture();
    let prepared = stimulus.checkpoint.calls[0].prepared.clone().unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: prepared.call().name.clone(),
        capabilities: prepared.claim().capabilities.clone(),
        effects: prepared.claim().effects.clone(),
        path_scopes: vec![],
        idempotency: prepared.claim().idempotency,
        approval: ApprovalMode::Always,
    });
    TurnExecutor::with_tools(
        Provider {
            activity: activity.clone(),
            response: stimulus.checkpoint.steps[0].response.clone(),
        },
        Tools { activity, prepared },
    )
    .with_policy_engine(Arc::new(policy))
}

fn service<J: FactJournal, L: LedgerStore + Clone + 'static>(
    journal: J,
    ledger: L,
    sessions: &Path,
) -> TaskExecutionService<J, L, NoopTraceSink, FileSessionStore> {
    TaskExecutionService::new(
        TaskCoordinator::new(journal),
        SessionExecutionService::new(
            crate::ExecutionService::new(ledger, NoopTraceSink),
            SessionService::new(FileSessionStore::new(sessions).unwrap()),
        ),
    )
}

fn setup<J: FactJournal>(coordinator: &TaskCoordinator<J>, cutoff: u64) -> AttemptBinding {
    let agent = AgentIdentity {
        definition_id: "fixture".into(),
        revision: "r1".into(),
        instance_id: "fixture".into(),
    };
    coordinator
        .register_task(
            "register",
            TaskDefinition {
                task_id: "task".into(),
                objective: "Exact durable resume budget".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                limits: TaskLimits {
                    max_depth: 1,
                    max_invocations: 1,
                    max_attempts: 1,
                    max_tokens: None,
                    max_steps_per_turn: 3,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    coordinator
        .admit_fixture(
            "task",
            "admit",
            InvocationDefinition {
                invocation_id: "root".into(),
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                role: InvocationRole::Root,
                parent_invocation_id: None,
                dependencies: vec![],
                input_source: fixture_source("task", "root", InvocationInputKind::Standalone),
            },
        )
        .unwrap();
    coordinator
        .configure_execution_budget(
            "task",
            "configure-budget",
            TaskExecutionBudgetPolicy {
                id: "budget".into(),
                revision: "r1".into(),
                max_reserved_steps: 8,
                deadline_at_ms: cutoff,
            },
        )
        .unwrap();
    AttemptBinding {
        attempt_id: "attempt".into(),
        invocation_id: "root".into(),
        execution: ExecutionRef {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        },
        agent,
        constraints_digest: "a".repeat(64),
        input_source: coordinator.snapshot("task").unwrap().invocations["root"]
            .definition
            .input_source
            .clone(),
    }
}

async fn scenario<J: FactJournal, L: LedgerStore + Clone + 'static>(
    open: impl Fn() -> (J, L),
    root: &Path,
    case: &Case,
) -> Value {
    let sessions = root.join("sessions");
    FileSessionStore::new(&sessions)
        .unwrap()
        .create("s")
        .unwrap();
    let activity = Arc::new(Activity::default());
    let cutoff = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 600_000;
    let binding = {
        let (journal, ledger) = open();
        let current = service(journal, ledger, &sessions);
        let binding = setup(current.coordinator(), cutoff);
        let stimulus = crate::suspension::tests::fixture();
        let result = current
            .run(
                "task",
                binding.clone(),
                executor(activity.clone()).with_absolute_deadline_at_ms(
                    if case.mutation == "earlier_cutoff" {
                        cutoff - 60_000
                    } else {
                        cutoff
                    },
                ),
                TurnRequest {
                    turn_id: "t".into(),
                    model_request: stimulus.checkpoint.model_request,
                    config: TurnConfig {
                        max_steps: 3,
                        max_tool_calls: Some(3),
                        deadline: None,
                    },
                },
            )
            .await
            .unwrap();
        if !matches!(result.1, DurableTurnResult::Suspended { .. }) {
            panic!("real Runtime must produce an approval checkpoint");
        }
        binding
    }; // Drop the original service before reopening SQLite and session state.
    let (journal, ledger) = open();
    let rebuilt = service(journal, ledger.clone(), &sessions);
    let saved = rebuilt
        .sessions()
        .execution()
        .load_current_suspension("e")
        .unwrap();
    let before = rebuilt.coordinator().snapshot("task").unwrap();
    let before_facts = rebuilt
        .coordinator()
        .journal()
        .read("task", 0, 100)
        .unwrap();
    let before_ledger = ledger.events_after(0).unwrap();
    let before_session = rebuilt.sessions().sessions().load("s").unwrap();
    let before_preparations = activity.prepared.load(Ordering::SeqCst);
    let verified_source = rebuilt
        .coordinator()
        .load_verified_invocation_input_source(
            &binding.input_source,
            &crate::InvocationInputScope {
                task_id: "task".into(),
                invocation_id: binding.invocation_id.clone(),
                agent: binding.agent.clone(),
                constraints_digest: binding.constraints_digest.clone(),
            },
            16 * 1024 * 1024,
        )
        .unwrap();
    let mut submitted = binding.clone();
    let mut checkpoint = saved.checkpoint.clone();
    match case.mutation.as_str() {
        "none" => {}
        "earlier_cutoff" => {}
        "missing_reservation" => submitted.attempt_id = "unreserved-attempt".into(),
        "invocation" => submitted.invocation_id = "foreign".into(),
        "execution" => submitted.execution.execution_id = "foreign".into(),
        "session" => submitted.execution.session_id = "foreign".into(),
        "turn" => submitted.execution.turn_id = "foreign".into(),
        "agent" => submitted.agent.instance_id = "foreign".into(),
        "constraints" => submitted.constraints_digest = "b".repeat(64),
        "source" => match &mut submitted.input_source {
            crate::InvocationInputSource::Standalone { fact } => fact.fact_id = "foreign".into(),
            _ => unreachable!(),
        },
        "max_steps" => checkpoint.budget.max_steps += 1,
        "missing_cutoff" => checkpoint.budget.deadline_at_ms = None,
        "widened_cutoff" => checkpoint.budget.deadline_at_ms = Some(cutoff + 1),
        other => panic!("unknown fixture mutation {other}"),
    }
    let structural = checkpoint.validate(&checkpoint.scope);
    let outcome =
        rebuilt.budget_resume_executor("task", &submitted, &checkpoint, executor(activity.clone()));
    json!({"case_id":case.id,"verification":"private_budget_checker_not_service_resume",
        "original_binding":binding,"submitted_binding":submitted,"original_suspension":saved,
        "submitted_checkpoint":checkpoint,"structurally_valid":structural.is_ok(),
        "structural_error":structural.err().map(|e|e.to_string()),
        "accepted":outcome.is_ok(),"error":outcome.err().map(|e|e.to_string()),
        "before":before,"after":rebuilt.coordinator().snapshot("task").unwrap(),
        "before_facts":before_facts,"after_facts":rebuilt.coordinator().journal().read("task",0,100).unwrap(),
        "before_ledger":before_ledger,"after_ledger":ledger.events_after(0).unwrap(),
        "before_session":before_session,"after_session":rebuilt.sessions().sessions().load("s").unwrap(),
        "verified_source":verified_source,"tool_preparations_before":before_preparations,
        "requests":activity.requests.lock().unwrap().clone(),
        "tool_preparations":activity.prepared.load(Ordering::SeqCst),"tool_effects":activity.effects.load(Ordering::SeqCst)})
}

#[tokio::test]
async fn actual_runtime_checkpoint_budget_negatives_memory_and_reopened_sqlite() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-task-budget-resume-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for backend in ["memory", "reopened_sqlite"] {
        for case in &cases {
            let case_root = root.join(format!("{backend}-{}", case.id));
            std::fs::create_dir(&case_root).unwrap();
            let mut row = if backend == "memory" {
                let journal = MemoryFactJournal::default();
                let ledger = InMemoryLedger::default();
                scenario(|| (journal.clone(), ledger.clone()), &case_root, case).await
            } else {
                scenario(
                    || {
                        (
                            SqliteFactJournal::open(case_root.join("facts.sqlite")).unwrap(),
                            SqliteLedger::open(case_root.join("executions.sqlite")).unwrap(),
                        )
                    },
                    &case_root,
                    case,
                )
                .await
            };
            row["backend"] = json!(backend);
            writeln!(export, "{row}").unwrap();
        }
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    eprintln!("TASK_BUDGET_RESUME_EVIDENCE={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len() * 2);
    for row in rows {
        let case = cases.iter().find(|case| row["case_id"] == case.id).unwrap();
        assert_eq!(row["accepted"], case.accepted, "{row}");
        if let Some(expected) = &case.error_contains {
            assert!(row["error"].as_str().unwrap().contains(expected), "{row}");
        }
        assert_eq!(row["structurally_valid"], true, "{row}");
        assert_eq!(row["requests"].as_array().unwrap().len(), 1, "{row}");
        assert!(
            row["tool_preparations_before"].as_u64().unwrap() > 0,
            "{row}"
        );
        assert_eq!(
            row["tool_preparations"], row["tool_preparations_before"],
            "{row}"
        );
        assert_eq!(row["tool_effects"], 0, "{row}");
        for (before, after) in [
            ("before", "after"),
            ("before_facts", "after_facts"),
            ("before_ledger", "after_ledger"),
            ("before_session", "after_session"),
        ] {
            assert_eq!(row[before], row[after], "{row}");
        }
        assert_eq!(
            row["before"]["execution_budget"]["reservations"]
                .as_object()
                .unwrap()
                .len(),
            1
        );
        let original = row["original_suspension"]["checkpoint"]["budget"]["deadline_at_ms"]
            .as_u64()
            .unwrap();
        let policy = row["before"]["execution_budget"]["policy"]["deadline_at_ms"]
            .as_u64()
            .unwrap();
        assert_eq!(
            original,
            if case.mutation == "earlier_cutoff" {
                policy - 60_000
            } else {
                policy
            }
        );
        assert_eq!(
            row["verified_source"]["reference"],
            row["original_binding"]["input_source"]["Standalone"]["fact"]
        );
        assert_eq!(
            row["verified_source"]["envelope"]["scope"]["agent"],
            row["original_binding"]["agent"]
        );
        assert_eq!(
            row["verified_source"]["envelope"]["scope"]["constraints_digest"],
            row["original_binding"]["constraints_digest"]
        );
    }
}
