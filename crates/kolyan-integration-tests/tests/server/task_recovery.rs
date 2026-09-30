//! Offline fixture-driven recovery through the public task execution service.
//! Seeded execution windows are explicit orchestration inputs, not live models.

use std::{
    collections::BTreeSet,
    error::Error,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::stream;
use kolyan_core::{ToolExecutor, ToolFuture, TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{
    FactJournal, LedgerEvent, LedgerEventKind, LedgerStore, SqliteFactJournal, SqliteLedger,
};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderFuture, StopReason, TokenUsage,
    ToolCall, ToolResult,
};
use kolyan_runtime::{
    EffectGrant, EffectRequest, ReconciliationRequest, ReconciliationResolution, RuntimeError,
    ToolEffectReconciler, reconcile_tool_effect,
};
use kolyan_server::*;
use kolyan_storage::FileSessionStore;
use kolyan_trace::{ArtifactStore, NoopTraceSink, Retention};
use serde::Serialize;
use serde_json::{Value, json};

type Failure = Box<dyn Error>;
type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;

#[derive(Default)]
struct Counters {
    models: AtomicUsize,
    tools: AtomicUsize,
    inspections: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}
struct FixtureProvider {
    script: Value,
    counters: Arc<Counters>,
}
impl ModelProvider for FixtureProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.counters.models.fetch_add(1, Ordering::SeqCst);
        self.counters.requests.lock().unwrap().push(request.clone());
        let script = self.script.clone();
        Box::pin(async move {
            let has_result = request.messages.iter().flat_map(|m| &m.content).any(|block|
                matches!(block, ContentBlock::ToolResult { result } if script["call"]["id"] == result.call_id));
            if let Some(message) = script["failure"].as_str()
                && (!script["failure_when_observed"].as_bool().unwrap() || has_result)
            {
                return Err(ProviderError::new(
                    ProviderErrorKind::Unavailable,
                    ProviderErrorPhase::Open,
                    message,
                ));
            }
            let call: Option<ToolCall> = serde_json::from_value(script["call"].clone()).unwrap();
            let pending = call.filter(|call| !request.messages.iter().flat_map(|m| &m.content)
                .any(|block| matches!(block, ContentBlock::ToolResult { result } if result.call_id == call.id)));
            let stop_reason = if pending.is_some() {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            };
            let content = match pending {
                Some(call) => vec![ContentBlock::ToolCall { call }],
                None => vec![ContentBlock::Text {
                    text: script["answer"].as_str().unwrap().into(),
                }],
            };
            let response = ModelResponse {
                id: request.request_id,
                model: request.model,
                content,
                structured_output: None,
                stop_reason,
                usage: TokenUsage {
                    input_tokens: Some(12),
                    output_tokens: Some(6),
                    ..Default::default()
                },
                metadata: Value::Null,
            };
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}
struct FixtureTool {
    result: ToolResult,
    counters: Arc<Counters>,
}
impl ToolExecutor for FixtureTool {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        assert_eq!(call.id, self.result.call_id);
        self.counters.tools.fetch_add(1, Ordering::SeqCst);
        let result = self.result.clone();
        Box::pin(async move { Ok(result) })
    }
}
struct FixtureInspector {
    resolution: ReconciliationResolution,
    counters: Arc<Counters>,
}
impl ToolEffectReconciler for FixtureInspector {
    fn inspect(
        &self,
        _: &ReconciliationRequest,
        _: &EffectRequest,
        _: &EffectGrant,
    ) -> Result<ReconciliationResolution, RuntimeError> {
        self.counters.inspections.fetch_add(1, Ordering::SeqCst);
        Ok(self.resolution.clone())
    }
}

struct Context<'a> {
    root: PathBuf,
    data: &'a Value,
    service: Option<Service>,
    counters: Arc<Counters>,
}
impl Context<'_> {
    fn service(&self) -> &Service {
        self.service.as_ref().unwrap()
    }
    fn task(&self) -> &str {
        self.data["definition"]["task_id"].as_str().unwrap()
    }
    fn binding(&self, operation: &Value) -> AttemptBinding {
        serde_json::from_value(
            self.data["bindings"][operation["binding"].as_str().unwrap_or("original")].clone(),
        )
        .unwrap()
    }
}

fn service(root: &Path) -> Result<Service, Failure> {
    Ok(TaskExecutionService::new(
        TaskCoordinator::new(SqliteFactJournal::open(root.join("ledger.sqlite"))?),
        SessionExecutionService::new(
            ExecutionService::new(
                SqliteLedger::open(root.join("ledger.sqlite"))?,
                NoopTraceSink,
            ),
            SessionService::new(FileSessionStore::new(root.join("sessions"))?),
        ),
    ))
}

#[tokio::test]
async fn fixture_task_recovery_preserves_evidence_and_never_replays_uncertain_effects() {
    let data: Value = serde_json::from_str(include_str!("../fixtures/task_recovery.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-task-recovery-")
        .tempdir()
        .unwrap()
        .keep();
    println!("Task recovery evidence: {}", root.display());
    let cases = data["cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    write_jsonl(root.join("cases.planned.jsonl"), cases).unwrap();
    let mut names = BTreeSet::new();
    let mut summaries = Vec::new();
    for case in cases {
        let name = case["name"].as_str().unwrap();
        assert!(names.insert(name));
        assert!(name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
        let directory = root.join(name);
        fs::create_dir_all(&directory).unwrap();
        let counters = Arc::new(Counters::default());
        let mut context = Context {
            root: directory.clone(),
            data: &data,
            service: Some(service(&directory).unwrap()),
            counters,
        };
        context
            .service()
            .coordinator()
            .register_task(
                "register",
                serde_json::from_value(data["definition"].clone()).unwrap(),
            )
            .unwrap();
        context
            .service()
            .coordinator()
            .admit_invocation(
                context.task(),
                "admit-root",
                serde_json::from_value(data["invocation"].clone()).unwrap(),
            )
            .unwrap();
        for binding in data["bindings"].as_object().unwrap().values() {
            context
                .service()
                .sessions()
                .sessions()
                .create(binding["execution"]["session_id"].as_str().unwrap())
                .unwrap();
        }
        let _capture_on_failure = Capture {
            root: directory.clone(),
            task: context.task().into(),
            counters: context.counters.clone(),
        };
        let mut comparisons = Vec::new();
        for (index, operation) in case["operations"].as_array().unwrap().iter().enumerate() {
            let before = context
                .service()
                .coordinator()
                .snapshot(context.task())
                .unwrap();
            let durable_before = durable(&directory, context.task()).unwrap();
            let counters_before = measured(&context);
            let result = perform(&mut context, operation).await;
            let after = context
                .service()
                .coordinator()
                .snapshot(context.task())
                .unwrap();
            let actual = json!({"operation":operation,"ok":result.is_ok(),"error":result.as_ref().err().map(|e|e.to_string()),
                "state":after.state,"attempts":after.attempts.len(),"counters":measured(&context)});
            comparisons.push(actual);
            capture(
                &directory,
                context.task(),
                &context.counters,
                &format!("operation-{index:03}"),
            )
            .unwrap();
            write_jsonl(directory.join("comparison.actual.jsonl"), &comparisons).unwrap();
            assert_eq!(
                result.is_ok(),
                operation["expect_ok"].as_bool().unwrap(),
                "{name} operation {index}: {result:?}"
            );
            if let Some(expected) = operation["error_contains"].as_str() {
                assert!(result.as_ref().unwrap_err().to_string().contains(expected));
            }
            if operation["unchanged"] == true {
                assert_eq!(after, before);
                assert_eq!(durable(&directory, context.task()).unwrap(), durable_before);
                assert_eq!(measured(&context), counters_before);
            }
            let binding = context.binding(operation);
            if let Some(expected) = operation["attempt_state"].as_str() {
                assert_eq!(
                    serde_json::to_value(after.attempts[&binding.attempt_id].state).unwrap(),
                    expected
                );
            }
            if operation["preserve_observation"] == true {
                assert_eq!(
                    before.attempts[&binding.attempt_id].observation,
                    after.attempts[&binding.attempt_id].observation
                );
                assert_eq!(before.usage, after.usage);
                assert_eq!(measured(&context), counters_before);
            }
        }
        let snapshot = context
            .service()
            .coordinator()
            .snapshot(context.task())
            .unwrap();
        let ledger = SqliteLedger::open(directory.join("ledger.sqlite"))
            .unwrap()
            .events_after(0)
            .unwrap();
        let mut summary = measured(&context);
        summary["state"] = json!(snapshot.state);
        summary["attempts"] = json!(snapshot.attempts.len());
        summary["receipts"] = json!(
            ledger
                .iter()
                .filter(|e| e.kind == LedgerEventKind::EffectReceipt)
                .count()
        );
        summary["retry_proofs"] = json!(
            ledger
                .iter()
                .filter(|e| e.kind == LedgerEventKind::ExecutionRetryAuthorized)
                .count()
        );
        write_jsonl(directory.join("summary.actual.jsonl"), &[summary.clone()]).unwrap();
        summaries.push(json!({"case":name,"actual":summary,"expected":case["expected"]}));
        write_jsonl(root.join("summary.actual.jsonl"), &summaries).unwrap();
        assert_eq!(summary, case["expected"], "{name}");
        if let Some(expected) = case["expected_receipt_outputs"].as_array() {
            let receipts: Vec<_> = ledger
                .iter()
                .filter(|e| e.kind == LedgerEventKind::EffectReceipt)
                .collect();
            assert_eq!(receipts.len(), expected.len());
            for (receipt, key) in receipts.iter().zip(expected) {
                let output = &data[key.as_str().unwrap()];
                assert_eq!(&receipt.payload["output"], output);
                let output: ToolResult = serde_json::from_value(output.clone()).unwrap();
                assert!(
                    context
                        .counters
                        .requests
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|request| {
                            request.messages.iter().flat_map(|message| &message.content).any(|block|
                        matches!(block, ContentBlock::ToolResult { result } if result == &output))
                        })
                );
            }
        }
        if snapshot.attempts.len() > 1 {
            let bindings: Vec<_> = snapshot.attempts.values().map(|a| &a.binding).collect();
            assert_ne!(bindings[0].attempt_id, bindings[1].attempt_id);
            assert_ne!(bindings[0].execution.turn_id, bindings[1].execution.turn_id);
            assert_ne!(
                bindings[0].execution.execution_id,
                bindings[1].execution.execution_id
            );
        }
    }
}

async fn perform(context: &mut Context<'_>, operation: &Value) -> Result<(), Failure> {
    let binding = context.binding(operation);
    let id = operation["id"].as_str().unwrap_or("unused");
    match operation["op"].as_str().unwrap() {
        "admit" => {
            context
                .service()
                .coordinator()
                .start_attempt(context.task(), id, binding)?;
        }
        "seed" => {
            let ledger = SqliteLedger::open(context.root.join("ledger.sqlite"))?;
            for seed in context.data["seed_groups"][operation["group"].as_str().unwrap()]
                .as_array()
                .unwrap()
            {
                let seed = substitute(seed, context, &binding);
                let event_id = seed["event_id"].as_str().unwrap().to_owned();
                ledger.append(LedgerEvent {
                    event_id: event_id.clone(),
                    idempotency_key: event_id,
                    turn_id: binding.execution.turn_id.clone(),
                    execution_id: binding.execution.execution_id.clone(),
                    cursor: 0,
                    kind: serde_json::from_value(seed["kind"].clone())?,
                    payload: seed["payload"].clone(),
                })?;
            }
        }
        "reopen" => {
            drop(context.service.take());
            context.service = Some(service(&context.root)?);
        }
        "reconcile" => {
            context
                .service()
                .reconcile(context.task(), &binding.attempt_id)?;
        }
        "retry" => {
            context.service().authorize_retry(
                context.task(),
                id,
                &binding.attempt_id,
                operation["reason"].as_str().unwrap(),
            )?;
        }
        "run" => {
            let executor = TurnExecutor::with_tools(
                FixtureProvider {
                    script: context.data["providers"][operation["provider"].as_str().unwrap()]
                        .clone(),
                    counters: context.counters.clone(),
                },
                FixtureTool {
                    result: serde_json::from_value(context.data["tool_result"].clone())?,
                    counters: context.counters.clone(),
                },
            );
            let request = TurnRequest {
                turn_id: binding.execution.turn_id.clone(),
                model_request: serde_json::from_value(context.data["request"].clone())?,
                config: TurnConfig {
                    max_steps: context.data["max_steps"].as_u64().unwrap() as usize,
                    ..Default::default()
                },
            };
            if operation["via"] == "session" {
                context
                    .service()
                    .sessions()
                    .start(
                        executor,
                        request,
                        &binding.execution.session_id,
                        &binding.execution.execution_id,
                    )
                    .await?;
            } else {
                context
                    .service()
                    .run(context.task(), binding, executor, request)
                    .await?;
            }
        }
        "inspect" => {
            let request = ReconciliationRequest {
                reconciliation_id: id.into(),
                execution: serde_json::from_value(json!(binding.execution))?,
                effect_id: context.data["effect_id"].as_str().unwrap().into(),
            };
            let inspector = FixtureInspector {
                resolution: serde_json::from_value(operation["resolution"].clone())?,
                counters: context.counters.clone(),
            };
            reconcile_tool_effect(
                &SqliteLedger::open(context.root.join("ledger.sqlite"))?,
                &request,
                &inspector,
            )?;
        }
        "forge" => {
            let events = SqliteLedger::open(context.root.join("ledger.sqlite"))?
                .execution_events_after(&binding.execution.execution_id, 0)?;
            let terminal = events
                .iter()
                .find(|e| {
                    e.kind == LedgerEventKind::TurnCompleted && e.payload["reason"] == "FinalAnswer"
                })
                .unwrap();
            let mut source = json!(ExecutionEvidence {
                execution: binding.execution.clone(),
                event_id: terminal.event_id.clone(),
                cursor: terminal.cursor
            });
            for (key, value) in operation["source_patch"].as_object().unwrap() {
                source[key] = value.clone();
            }
            let response = events
                .iter()
                .rev()
                .find(|e| e.kind == LedgerEventKind::StepCompleted)
                .unwrap()
                .payload["step"]["response"]
                .clone();
            let actual_digest = ArtifactStore::new(context.root.join("artifacts"), 65536)?
                .put(&serde_json::to_vec(&response)?, Retention::Required)?
                .digest;
            let source: ExecutionEvidence = serde_json::from_value(source)?;
            context.service().coordinator().observe_attempt(
                context.task(),
                id,
                AttemptObservation {
                    attempt_id: binding.attempt_id.clone(),
                    execution: binding.execution,
                    source: source.clone(),
                    usage: TaskUsage::default(),
                    outcome: AttemptOutcome::Completed {
                        evidence: vec![
                            CompletionEvidence::ExecutionResult {
                                criterion_id:
                                    context.data["definition"]["criteria"][0]["ExecutionCompleted"]
                                        ["id"]
                                        .as_str()
                                        .unwrap()
                                        .into(),
                                source,
                                result_digest: operation["digest"]
                                    .as_str()
                                    .unwrap_or(&actual_digest)
                                    .into(),
                            },
                        ],
                    },
                },
            )?;
        }
        "complete" => {
            context.service().complete(context.task(), id)?;
        }
        other => panic!("unknown fixture operation {other}"),
    }
    Ok(())
}

fn substitute(value: &Value, context: &Context<'_>, binding: &AttemptBinding) -> Value {
    match value {
        Value::String(text) => {
            let mut text = text.clone();
            for (key, value) in [
                ("task", context.task()),
                ("attempt", binding.attempt_id.as_str()),
                ("invocation", binding.invocation_id.as_str()),
                ("execution", binding.execution.execution_id.as_str()),
                ("session", binding.execution.session_id.as_str()),
                ("turn", binding.execution.turn_id.as_str()),
                ("effect", context.data["effect_id"].as_str().unwrap()),
            ] {
                text = text.replace(&format!("${{{key}}}"), value);
            }
            json!(text)
        }
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|v| substitute(v, context, binding))
                .collect(),
        ),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(k, v)| (k.clone(), substitute(v, context, binding)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn measured(context: &Context<'_>) -> Value {
    json!({"model_calls":context.counters.models.load(Ordering::SeqCst),
        "tool_calls":context.counters.tools.load(Ordering::SeqCst),"inspections":context.counters.inspections.load(Ordering::SeqCst)})
}
fn durable(root: &Path, task: &str) -> Result<Value, Failure> {
    let journal = SqliteFactJournal::open(root.join("ledger.sqlite"))?;
    let mut records = Vec::new();
    let mut after = 0;
    loop {
        let page = journal.read(task, after, 512)?;
        let count = page.len();
        if let Some(last) = page.last() {
            after = last.position;
        }
        records.extend(page);
        if count < 512 {
            break;
        }
    }
    Ok(
        json!({"task":records,"execution":SqliteLedger::open(root.join("ledger.sqlite"))?.events_after(0)?}),
    )
}
fn capture(root: &Path, task: &str, counters: &Counters, prefix: &str) -> Result<(), Failure> {
    let actual = durable(root, task)?;
    write_jsonl(
        root.join(format!("{prefix}.task.jsonl")),
        actual["task"].as_array().unwrap(),
    )?;
    write_jsonl(
        root.join(format!("{prefix}.execution.jsonl")),
        actual["execution"].as_array().unwrap(),
    )?;
    write_jsonl(
        root.join("requests.actual.jsonl"),
        &counters.requests.lock().unwrap(),
    )?;
    let snapshot = TaskCoordinator::new(SqliteFactJournal::open(root.join("ledger.sqlite"))?)
        .snapshot(task)?;
    write_jsonl(root.join(format!("{prefix}.snapshot.jsonl")), &[snapshot])?;
    Ok(())
}
struct Capture {
    root: PathBuf,
    task: String,
    counters: Arc<Counters>,
}
impl Drop for Capture {
    fn drop(&mut self) {
        if let Err(error) = capture(&self.root, &self.task, &self.counters, "final") {
            eprintln!("recovery evidence export failed: {error}");
        }
    }
}
fn write_jsonl(path: impl AsRef<Path>, rows: &[impl Serialize]) -> Result<(), Failure> {
    let mut file = fs::File::create(path)?;
    for row in rows {
        serde_json::to_writer(&mut file, row)?;
        writeln!(file)?;
    }
    Ok(())
}
