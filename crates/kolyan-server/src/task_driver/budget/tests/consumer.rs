//! Actual Service approval resume over a narrow returned-payload corruption view.
//! Only positive production execution writes; the view never rewrites source facts.

use super::*;
use kolyan_core::{ToolOutcome, TurnEndReason, TurnOutcome};
use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerQuery};
use kolyan_model::{ContentBlock, StopReason, ToolResult};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumerCase {
    id: String,
    mutation: String,
    accepted: bool,
    additional_models: usize,
    additional_effects: usize,
    error_contains: Option<String>,
}

#[derive(Default)]
struct Observation {
    reads: Mutex<Vec<Value>>,
    writes: AtomicUsize,
    claims: AtomicUsize,
}

#[derive(Clone)]
struct ReadView<L> {
    source: L,
    target: LedgerEvent,
    selectors: Vec<Value>,
    mutation: String,
    observation: Arc<Observation>,
}

fn coordinate(event: &LedgerEvent) -> Value {
    let checkpoint = &event.payload["suspension"]["checkpoint"];
    json!({"event_id":event.event_id,"execution_id":event.execution_id,
        "turn_id":event.turn_id,"cursor":event.cursor,"kind":event.kind,
        "checkpoint_id":checkpoint["checkpoint_id"],
        "scope":checkpoint["scope"]})
}

impl<L: LedgerStore> LedgerStore for ReadView<L> {
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        let mut rows = self.source.query(query)?;
        for event in &mut rows {
            if event.event_id != self.target.event_id {
                continue;
            }
            let input = event.clone();
            let refusal = if self.selectors.len() != 1 {
                Some("multiple suspension read coordinates")
            } else if coordinate(event) != self.selectors[0] || input != self.target {
                Some("read selector differs from exact suspension")
            } else {
                None
            };
            if let Some(reason) = refusal {
                self.observation.reads.lock().unwrap().push(json!({
                    "source":input,"observed":null,"selectors":self.selectors,"refusal":reason}));
                return Err(LedgerError::Storage(reason.into()));
            }
            let budget = &mut event.payload["suspension"]["checkpoint"]["budget"];
            match self.mutation.as_str() {
                "missing_cutoff" => budget["deadline_at_ms"] = Value::Null,
                "widened_cutoff" => {
                    budget["deadline_at_ms"] = json!(budget["deadline_at_ms"].as_u64().unwrap() + 1)
                }
                "max_steps" => {
                    budget["max_steps"] = json!(budget["max_steps"].as_u64().unwrap() + 1)
                }
                "none" | "foreign_selector" | "multiple_selectors" => {}
                other => panic!("unknown new consumer mutation {other}"),
            }
            self.observation.reads.lock().unwrap().push(json!({
                "source":input,"observed":event,"selectors":self.selectors,"refusal":null,
                "query":{"execution_id":query.execution_id,"event_id":query.event_id,
                    "after":query.after,"through":query.through,"limit":query.limit}}));
        }
        Ok(rows)
    }
    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.source.events_after(cursor)
    }
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.observation.writes.fetch_add(1, Ordering::SeqCst);
        self.source.append(event)
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.observation.writes.fetch_add(1, Ordering::SeqCst);
        self.source.append_unless_cancelled(event)
    }
    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.observation.claims.fetch_add(1, Ordering::SeqCst);
        self.source.claim(key)
    }
}

struct ConsumerProvider(Arc<Activity>);
impl ModelProvider for ConsumerProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let mut requests = self.0.requests.lock().unwrap();
        let first = requests.is_empty();
        requests.push(request.clone());
        drop(requests);
        let mut response = crate::suspension::tests::fixture().checkpoint.steps[0]
            .response
            .clone();
        response.model = request.model;
        if !first {
            response.content = vec![ContentBlock::Text {
                text: "actual resumed final answer".into(),
            }];
            response.stop_reason = StopReason::EndTurn;
        }
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}
struct ConsumerTools(Arc<Activity>);
impl ToolExecutor for ConsumerTools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        self.0.prepared.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let prepared = crate::suspension::tests::fixture().checkpoint.calls[0]
                .prepared
                .clone()
                .unwrap();
            if prepared.call() != &call {
                return Err(kolyan_core::ToolError::Failed {
                    message: "fixture call differs".into(),
                });
            }
            Ok(prepared)
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
                .map_err(|e| kolyan_core::ToolError::PolicyDenied {
                    message: e.to_string(),
                })?;
            self.0.effects.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "actual counted fixture execution".into(),
                is_error: false,
            }))
        })
    }
}
fn consumer_executor(activity: Arc<Activity>) -> TurnExecutor<ConsumerProvider, ConsumerTools> {
    let prepared = crate::suspension::tests::fixture().checkpoint.calls[0]
        .prepared
        .clone()
        .unwrap();
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: prepared.call().name.clone(),
        capabilities: prepared.claim().capabilities.clone(),
        effects: prepared.claim().effects.clone(),
        path_scopes: vec![],
        idempotency: prepared.claim().idempotency,
        approval: ApprovalMode::Always,
    });
    TurnExecutor::with_tools(ConsumerProvider(activity.clone()), ConsumerTools(activity))
        .with_policy_engine(Arc::new(policy))
}

fn outcome(value: &DurableTurnResult) -> Value {
    match value {
        DurableTurnResult::Suspended { suspension, .. } => {
            json!({"kind":"suspended","suspension":suspension})
        }
        DurableTurnResult::Completed(execution, _) => {
            let result = &execution.result;
            let stopped = match &result.outcome {
                TurnOutcome::FinalAnswer { response } => {
                    json!({"kind":"final_answer","response":response})
                }
                TurnOutcome::Refused { response } => json!({"kind":"refused","response":response}),
                TurnOutcome::Incomplete { response } => {
                    json!({"kind":"incomplete","response":response})
                }
                TurnOutcome::Rejected { reason } => json!({"kind":"rejected","reason":reason}),
                TurnOutcome::Expired { reason } => json!({"kind":"expired","reason":reason}),
                TurnOutcome::MaxSteps => json!({"kind":"max_steps"}),
            };
            json!({"kind":"completed","turn_id":result.turn_id,"outcome":stopped,"steps":result.steps,
                "final_answer":matches!(result.end_reason, TurnEndReason::FinalAnswer)})
        }
    }
}

async fn consumer_scenario<J: FactJournal + Clone, L: LedgerStore + Clone + 'static>(
    open: impl Fn() -> (J, L),
    root: &Path,
    case: &ConsumerCase,
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
        current
            .run(
                "task",
                binding.clone(),
                consumer_executor(activity.clone()),
                TurnRequest {
                    turn_id: "t".into(),
                    model_request: crate::suspension::tests::fixture().checkpoint.model_request,
                    config: TurnConfig {
                        max_steps: 3,
                        max_tool_calls: Some(3),
                        deadline: None,
                    },
                },
            )
            .await
            .unwrap();
        binding
    };
    let (journal, ledger) = open();
    let original = service(journal, ledger.clone(), &sessions);
    let saved = original
        .sessions()
        .execution()
        .load_current_suspension("e")
        .unwrap();
    let before = original.coordinator().snapshot("task").unwrap();
    let before_facts = original
        .coordinator()
        .journal()
        .read("task", 0, 100)
        .unwrap();
    let before_session = original.sessions().sessions().load("s").unwrap();
    let before_ledger = ledger.events_after(0).unwrap();
    let target = before_ledger
        .iter()
        .find(|e| {
            e.kind == LedgerEventKind::ExecutionSuspended
                && e.payload["suspension"]["checkpoint"]["checkpoint_id"]
                    == saved.checkpoint.checkpoint_id
        })
        .unwrap()
        .clone();
    let mut selectors = vec![coordinate(&target)];
    if case.mutation == "foreign_selector" {
        selectors[0]["scope"]["execution"]["session_id"] = json!("foreign");
    }
    if case.mutation == "multiple_selectors" {
        selectors.push(selectors[0].clone());
    }
    let observation = Arc::new(Observation::default());
    let view = ReadView {
        source: ledger.clone(),
        target,
        selectors,
        mutation: case.mutation.clone(),
        observation: observation.clone(),
    };
    let resumed = service(original.coordinator().journal().clone(), view, &sessions);
    let before_models = activity.requests.lock().unwrap().len();
    let before_prepared = activity.prepared.load(Ordering::SeqCst);
    let before_effects = activity.effects.load(Ordering::SeqCst);
    let actual = resumed
        .resume_approval(
            "task",
            binding.clone(),
            &saved.checkpoint.approvals[0].approval_id,
            consumer_executor(activity.clone()),
        )
        .await;
    let historical = if actual.is_ok() {
        Some(original.load_verified_historical_result("task", &binding, 1024 * 1024))
    } else {
        None
    };
    json!({"case_id":case.id,"verification":"actual_service_resume_approval",
        "binding":binding,"saved_suspension":saved,"accepted":actual.is_ok(),
        "result":actual.as_ref().ok().map(|(snapshot,result)|json!({"snapshot":snapshot,"execution":outcome(result)})),
        "error":actual.err().map(|e|e.to_string()),
        "verified_physical_result":historical.as_ref().and_then(|r|r.as_ref().ok()),
        "physical_error":historical.as_ref().and_then(|r|r.as_ref().err()).map(|e|e.to_string()),
        "source_before":before_ledger,"source_after":ledger.events_after(0).unwrap(),
        "observed_reads":observation.reads.lock().unwrap().clone(),
        "ledger_writes":observation.writes.load(Ordering::SeqCst),"ledger_claims":observation.claims.load(Ordering::SeqCst),
        "task_before":before,"task_after":original.coordinator().snapshot("task").unwrap(),
        "facts_before":before_facts,"facts_after":original.coordinator().journal().read("task",0,100).unwrap(),
        "session_before":before_session,"session_after":original.sessions().sessions().load("s").unwrap(),
        "models_before":before_models,"models_after":activity.requests.lock().unwrap().len(),
        "preparations_before":before_prepared,"preparations_after":activity.prepared.load(Ordering::SeqCst),
        "effects_before":before_effects,"effects_after":activity.effects.load(Ordering::SeqCst),
        "requests":activity.requests.lock().unwrap().clone()})
}

#[tokio::test]
async fn service_approval_consumer_corrupt_read_matrix() {
    let cases: Vec<ConsumerCase> =
        serde_json::from_str(include_str!("consumer_cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-task-budget-consumer-")
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
                consumer_scenario(|| (journal.clone(), ledger.clone()), &case_root, case).await
            } else {
                consumer_scenario(
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
    eprintln!("TASK_BUDGET_CONSUMER_EVIDENCE={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len() * 2);
    for row in rows {
        let case = cases.iter().find(|case| row["case_id"] == case.id).unwrap();
        assert_eq!(row["accepted"], case.accepted, "{row}");
        assert_eq!(
            row["models_after"].as_u64().unwrap() - row["models_before"].as_u64().unwrap(),
            case.additional_models as u64,
            "{row}"
        );
        assert_eq!(
            row["effects_after"].as_u64().unwrap() - row["effects_before"].as_u64().unwrap(),
            case.additional_effects as u64,
            "{row}"
        );
        assert!(
            !row["observed_reads"].as_array().unwrap().is_empty(),
            "{row}"
        );
        let source = row["source_before"].as_array().unwrap();
        assert_eq!(
            source,
            &row["source_after"].as_array().unwrap()[..source.len()],
            "{row}"
        );
        for read in row["observed_reads"].as_array().unwrap() {
            if read["observed"].is_null() {
                assert!(read["refusal"].is_string(), "{row}");
                continue;
            }
            let mut observed = read["observed"].clone();
            let field = match case.mutation.as_str() {
                "missing_cutoff" | "widened_cutoff" => Some("deadline_at_ms"),
                "max_steps" => Some("max_steps"),
                _ => None,
            };
            if let Some(field) = field {
                observed["payload"]["suspension"]["checkpoint"]["budget"][field] =
                    read["source"]["payload"]["suspension"]["checkpoint"]["budget"][field].clone();
            }
            assert_eq!(
                observed, read["source"],
                "only the selected returned budget field may differ: {row}"
            );
        }
        if case.accepted {
            assert_eq!(
                row["result"]["execution"]["outcome"]["kind"], "final_answer",
                "{row}"
            );
            assert!(row["verified_physical_result"].is_object(), "{row}");
            assert!(row["physical_error"].is_null(), "{row}");
            assert!(
                row["source_after"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["kind"] == "turn_completed"),
                "{row}"
            );
            assert_eq!(
                row["task_before"]["execution_budget"], row["task_after"]["execution_budget"],
                "{row}"
            );
        } else {
            assert!(
                row["error"]
                    .as_str()
                    .unwrap()
                    .contains(case.error_contains.as_ref().unwrap()),
                "{row}"
            );
            assert_eq!(
                row["preparations_before"], row["preparations_after"],
                "{row}"
            );
            assert_eq!(row["ledger_writes"], 0, "{row}");
            assert_eq!(row["ledger_claims"], 0, "{row}");
            for (before, after) in [
                ("source_before", "source_after"),
                ("facts_before", "facts_after"),
                ("task_before", "task_after"),
                ("session_before", "session_after"),
            ] {
                assert_eq!(row[before], row[after], "{row}");
            }
        }
    }
}
