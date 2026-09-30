//! Offline data-driven integration through the real Core and durable driver.
//! Scripted provider/tool data is explicit, not presented as real-model evidence.

use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures_util::stream;
use kolyan_core::{
    ToolError, ToolExecutor, ToolFuture, TurnConfig, TurnExecutor, TurnOutcome, TurnRequest,
};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore, SqliteLedger};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelEvent, ModelEventStream, ModelProvider, ModelRef,
    ModelRequest, ModelResponse, ProviderFuture, StopReason, TokenUsage, ToolCall, ToolChoice,
    ToolDefinition, ToolResult,
};
use kolyan_runtime::{
    ContentPolicy, DurableTurnDriver, DurableTurnResult, ExecutionBinding, LinkedTrajectory,
    LinkedTrajectoryRecord,
};
use kolyan_trace::{ArtifactError, ArtifactStore, Retention, TraceError, TraceRecord, TraceSink};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Dataset {
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    binding: ExecutionBinding,
    model: ModelRef,
    input: String,
    tools: Vec<ToolDefinition>,
    max_steps: usize,
    page_limit: usize,
    artifact_max_bytes: u64,
    steps: Vec<Script>,
    tool_results: Vec<ToolResult>,
    expected: Expected,
}
#[derive(Clone, Deserialize)]
struct Script {
    content: Vec<ContentBlock>,
    stop_reason: StopReason,
    usage: TokenUsage,
    events: Vec<ModelEvent>,
}
#[derive(Deserialize)]
struct Expected {
    counts: BTreeMap<String, usize>,
    terminal_payload_keys: Vec<Vec<String>>,
    stream_types: Vec<String>,
    redacted_strings: Vec<String>,
    neighbor_marker: String,
}
#[derive(Clone)]
struct FixtureProvider {
    steps: Arc<Mutex<VecDeque<Script>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}
impl ModelProvider for FixtureProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.requests.lock().unwrap().push(request.clone());
        let script = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("fixture exhausted");
        let response = ModelResponse {
            id: request.request_id,
            model: request.model,
            content: script.content,
            structured_output: None,
            stop_reason: script.stop_reason,
            usage: script.usage,
            metadata: Value::Null,
        };
        let mut events = vec![Ok(ModelEvent::Started)];
        events.extend(script.events.into_iter().map(Ok));
        events.push(Ok(ModelEvent::Completed(response)));
        Box::pin(async move { Ok(Box::pin(stream::iter(events)) as ModelEventStream) })
    }
}
#[derive(Clone)]
struct FixtureTools {
    calls: Arc<Mutex<Vec<ToolCall>>>,
    results: Vec<ToolResult>,
    expected_calls: Vec<ToolCall>,
}
impl ToolExecutor for FixtureTools {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        assert!(self.expected_calls.contains(&call), "unexpected tool input");
        self.calls.lock().unwrap().push(call.clone());
        let result = self
            .results
            .iter()
            .find(|result| result.call_id == call.id)
            .cloned();
        Box::pin(async move {
            result.ok_or_else(|| ToolError::Failed {
                message: "missing fixture result".into(),
            })
        })
    }
}
struct FailedTrace;
impl TraceSink for FailedTrace {
    fn record(&self, _: TraceRecord) -> Result<(), TraceError> {
        Err(TraceError {
            message: "fixture diagnostic sink unavailable".into(),
        })
    }
}

#[tokio::test]
async fn actual_core_execution_links_reopened_evidence_and_verified_artifacts() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("../fixtures/linked_trajectory.json")).unwrap();
    assert!(!dataset.cases.is_empty());
    let root = tempfile::Builder::new()
        .prefix("kolyan-linked-trajectory-")
        .tempdir()
        .unwrap()
        .keep();
    println!("linked trajectory evidence: {}", root.display());
    for case in dataset.cases {
        run_case(&root, case).await;
    }
}

async fn run_case(root: &Path, case: Case) {
    case.binding.validate().unwrap();
    let directory = root.join(&case.binding.execution_id);
    std::fs::create_dir_all(&directory).unwrap();
    let database = directory.join("ledger.sqlite");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let expected_calls: Vec<_> = case
        .steps
        .iter()
        .flat_map(|step| &step.content)
        .filter_map(|block| match block {
            ContentBlock::ToolCall { call } => Some(call.clone()),
            _ => None,
        })
        .collect();
    let scripts = Arc::new(Mutex::new(VecDeque::from(case.steps.clone())));
    let request = ModelRequest {
        request_id: case.binding.turn_id.clone(),
        model: case.model.clone(),
        system: Vec::new(),
        messages: vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: case.input.clone(),
            }],
        }],
        tools: case.tools.clone(),
        tool_choice: ToolChoice::Auto,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: None,
        extensions: Value::Null,
    };
    let initial = request.clone();
    let ledger = SqliteLedger::open(&database).unwrap();
    ledger
        .append(LedgerEvent {
            event_id: "neighbor".into(),
            execution_id: "neighbor".into(),
            turn_id: "neighbor".into(),
            cursor: 0,
            kind: LedgerEventKind::ModelRequested,
            idempotency_key: "neighbor".into(),
            payload: json!({"text":case.expected.neighbor_marker}),
        })
        .unwrap();
    let driver = DurableTurnDriver::new(ledger.clone(), FailedTrace);
    let result = driver
        .start(
            TurnExecutor::with_tools(
                FixtureProvider {
                    steps: scripts.clone(),
                    requests: requests.clone(),
                },
                FixtureTools {
                    calls: calls.clone(),
                    results: case.tool_results.clone(),
                    expected_calls: expected_calls.clone(),
                },
            ),
            TurnRequest {
                turn_id: case.binding.turn_id.clone(),
                model_request: request,
                config: TurnConfig {
                    max_steps: case.max_steps,
                    ..Default::default()
                },
            },
            case.binding.session_id.clone(),
            case.binding.execution_id.clone(),
        )
        .await
        .unwrap();
    let DurableTurnResult::Completed(execution, trajectory) = result else {
        panic!("fixture must complete");
    };
    assert!(matches!(
        execution.result.outcome,
        TurnOutcome::FinalAnswer { .. }
    ));
    assert!(!trajectory.trace_errors.is_empty());
    assert!(scripts.lock().unwrap().is_empty());
    assert_eq!(*calls.lock().unwrap(), expected_calls);
    let before = ledger.events_after(0).unwrap();
    drop(driver);
    drop(ledger);
    let ledger = SqliteLedger::open(database).unwrap();
    let full = pages(&ledger, &case, ContentPolicy::Full);
    let metadata = pages(&ledger, &case, ContentPolicy::MetadataOnly);
    let expected_links: Vec<_> = before
        .iter()
        .filter(|event| event.execution_id == case.binding.execution_id)
        .map(|event| (event.event_id.as_str(), event.cursor))
        .collect();
    let actual_links: Vec<_> = full
        .iter()
        .map(|row| (row.event_id.as_str(), row.cursor))
        .collect();
    assert_eq!(
        actual_links, expected_links,
        "bounded pages must preserve every execution fact"
    );
    write_jsonl(directory.join("linked.full.jsonl"), &full);
    write_jsonl(directory.join("linked.metadata.jsonl"), &metadata);
    write_jsonl(directory.join("ledger.actual.jsonl"), &before);
    compare(
        &directory,
        &case,
        &full,
        &metadata,
        &requests.lock().unwrap(),
        &initial,
    );
    let artifacts =
        ArtifactStore::new(directory.join("artifacts"), case.artifact_max_bytes).unwrap();
    let mut references = Vec::new();
    for row in &full {
        if row.kind == LedgerEventKind::StepCompleted {
            let bytes = serde_json::to_vec(&row.payload["step"]["response"]).unwrap();
            let reference = artifacts.put(&bytes, Retention::Required).unwrap();
            let reopened =
                ArtifactStore::new(directory.join("artifacts"), case.artifact_max_bytes).unwrap();
            assert_eq!(
                reopened.read(&reference, case.artifact_max_bytes).unwrap(),
                bytes
            );
            assert!(matches!(
                reopened.remove(&reference),
                Err(ArtifactError::Required)
            ));
            references.push(
                json!({"event_id": row.event_id, "cursor": row.cursor, "artifact": reference}),
            );
        }
        assert!(row.trace().emit(&FailedTrace).is_err());
    }
    assert_eq!(references.len(), case.steps.len());
    write_jsonl(directory.join("response.artifacts.jsonl"), &references);
    assert_eq!(
        ledger.events_after(0).unwrap(),
        before,
        "exports, artifacts and failed diagnostics must not rewrite facts"
    );
}

fn pages(ledger: &SqliteLedger, case: &Case, policy: ContentPolicy) -> Vec<LinkedTrajectoryRecord> {
    let mut after = 0;
    let mut rows = Vec::new();
    loop {
        let page =
            LinkedTrajectory::load(ledger, &case.binding, after, case.page_limit, policy).unwrap();
        assert!(page.records.len() <= case.page_limit);
        for row in &page.records {
            assert!(row.cursor > after);
            assert_eq!(row.binding, case.binding);
            let stored = ledger.event_by_id(&row.event_id).unwrap().unwrap();
            assert_eq!(stored.cursor, row.cursor);
            assert_eq!(stored.execution_id, case.binding.execution_id);
            if policy == ContentPolicy::Full {
                assert_eq!(stored.payload, row.payload);
            }
            after = row.cursor;
        }
        let count = page.records.len();
        rows.extend(page.records);
        if count < case.page_limit {
            return rows;
        }
    }
}

fn compare(
    directory: &Path,
    case: &Case,
    full: &[LinkedTrajectoryRecord],
    metadata: &[LinkedTrajectoryRecord],
    requests: &[ModelRequest],
    initial: &ModelRequest,
) {
    let mut counts = BTreeMap::<String, usize>::new();
    for row in full {
        let kind = serde_json::to_value(row.kind)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        *counts.entry(kind).or_default() += 1;
    }
    let selected: BTreeMap<_, _> = case
        .expected
        .counts
        .keys()
        .map(|kind| (kind.clone(), counts.get(kind).copied().unwrap_or(0)))
        .collect();
    let stream_types: Vec<_> = full
        .iter()
        .filter(|row| row.kind == LedgerEventKind::ModelStreamEvent)
        .map(|row| row.payload["type"].as_str().unwrap().to_owned())
        .collect();
    let recorded: Vec<ModelRequest> = full
        .iter()
        .filter(|row| row.kind == LedgerEventKind::ModelRequested)
        .map(|row| serde_json::from_value(row.payload["request"].clone()).unwrap())
        .collect();
    write_jsonl(directory.join("requests.actual.jsonl"), requests);
    write_jsonl(
        directory.join("comparison.actual.jsonl"),
        &[json!({"counts":selected,"stream_types":stream_types})],
    );
    assert_eq!(selected, case.expected.counts);
    assert_eq!(stream_types, case.expected.stream_types);
    let terminal_keys: Vec<Vec<String>> = full
        .iter()
        .filter(|row| row.kind == LedgerEventKind::TurnCompleted)
        .map(|row| row.payload.as_object().unwrap().keys().cloned().collect())
        .collect();
    assert_eq!(terminal_keys, case.expected.terminal_payload_keys);
    assert_eq!(recorded, requests);
    let mut admitted = initial.clone();
    admitted.request_id = requests.first().unwrap().request_id.clone();
    assert_eq!(requests.first(), Some(&admitted));
    let responses: Vec<ModelResponse> = full
        .iter()
        .filter(|row| row.kind == LedgerEventKind::StepCompleted)
        .map(|row| serde_json::from_value(row.payload["step"]["response"].clone()).unwrap())
        .collect();
    let step_ids: Vec<_> = full
        .iter()
        .filter(|row| row.kind == LedgerEventKind::StepCompleted)
        .map(|row| row.step_id.as_deref().unwrap())
        .collect();
    assert_eq!(responses.len(), case.steps.len());
    assert_eq!(step_ids.len(), requests.len());
    for (step_id, request) in step_ids.iter().zip(requests) {
        assert_eq!(*step_id, request.request_id);
    }
    for (response, script) in responses.iter().zip(&case.steps) {
        assert_eq!(response.content, script.content);
        assert_eq!(response.stop_reason, script.stop_reason);
        assert_eq!(response.usage, script.usage);
    }
    let observations: Vec<ToolResult> = full
        .iter()
        .filter(|row| row.kind == LedgerEventKind::ToolExecutionCompleted)
        .map(|row| serde_json::from_value(row.payload["result"].clone()).unwrap())
        .collect();
    assert_eq!(observations, case.tool_results);
    let receipts: Vec<ToolResult> = full
        .iter()
        .filter(|row| row.kind == LedgerEventKind::EffectReceipt)
        .map(|row| serde_json::from_value(row.payload["output"].clone()).unwrap())
        .collect();
    assert_eq!(receipts, case.tool_results);
    for result in &case.tool_results {
        assert!(requests.iter().skip(1).any(|request| request.messages.iter().flat_map(|message| &message.content)
            .any(|block| matches!(block, ContentBlock::ToolResult { result: actual } if actual == result))));
    }
    let exported = serde_json::to_string(metadata).unwrap();
    let original = serde_json::to_string(full).unwrap();
    assert_eq!(metadata.len(), full.len());
    assert!(!original.contains(&case.expected.neighbor_marker));
    assert!(!exported.contains(&case.expected.neighbor_marker));
    for secret in &case.expected.redacted_strings {
        assert!(original.contains(secret));
        assert!(!exported.contains(secret));
    }
    for (redacted, actual) in metadata.iter().zip(full) {
        assert_eq!(redacted.event_id, actual.event_id);
        assert_eq!(redacted.cursor, actual.cursor);
        assert_eq!(redacted.step_id, actual.step_id);
        assert_eq!(redacted.kind, actual.kind);
        assert_eq!(redacted.payload["content_redacted"], true);
    }
}

fn write_jsonl(path: impl AsRef<Path>, rows: &[impl Serialize]) {
    let mut file = File::create(path).unwrap();
    for row in rows {
        serde_json::to_writer(&mut file, row).unwrap();
        writeln!(file).unwrap();
    }
}
