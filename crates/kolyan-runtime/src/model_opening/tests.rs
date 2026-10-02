//! Manual recorder/source fixtures exercise the production Runtime consumer, not
//! the mandatory Driver or real SDK HTTP acceptance owned by the integration batch.

mod provider;
mod stores;

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kolyan_core::{TurnControl, TurnDeadline};
use kolyan_ledger::{
    FactJournal, InMemoryLedger, LedgerEvent, LedgerEventKind as Kind, LedgerQuery, LedgerStore,
    MemoryFactJournal, SqliteFactJournal, SqliteLedger,
};
use kolyan_model::{ModelProvider, ModelRequest};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::{
    ExecutionKey, ModelOpeningEventRef, ModelOpeningInspectionLimits,
    ModelOpeningInspectionRequest, inspect_model_openings,
};

#[derive(Deserialize)]
struct Case {
    id: String,
    policy: String,
    mode: String,
    error: Option<String>,
    prepare: usize,
    count: usize,
    #[serde(rename = "gen")]
    generation: usize,
    state: String,
}

#[tokio::test]
async fn memory_consumer_matrix() {
    matrix(false).await;
}
#[tokio::test]
async fn reopened_sqlite_consumer_matrix() {
    matrix(true).await;
}

async fn matrix(sqlite: bool) {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-opening-consumer-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    for case in &cases {
        let (ledger, facts): (Arc<dyn LedgerStore>, Arc<dyn FactJournal>) = if sqlite {
            (
                Arc::new(
                    SqliteLedger::open(directory.join(format!("{}.ledger", case.id))).unwrap(),
                ),
                Arc::new(
                    SqliteFactJournal::open(directory.join(format!("{}.facts", case.id))).unwrap(),
                ),
            )
        } else {
            (
                Arc::new(InMemoryLedger::default()),
                Arc::new(MemoryFactJournal::default()),
            )
        };
        let base: Value = serde_json::from_str(include_str!("tests/model-input.json")).unwrap();
        let request: ModelRequest = serde_json::from_value(base["request"].clone()).unwrap();
        let execution: ExecutionKey = serde_json::from_value(base["execution"].clone()).unwrap();
        let deadline = TurnDeadline::capture(
            if case.mode == "zero_deadline" {
                Some(Duration::ZERO)
            } else {
                None
            },
            None,
        )
        .unwrap();
        ledger
            .append(event(
                "execution/execution-started",
                Kind::ExecutionStarted,
                base["execution"].clone(),
            ))
            .unwrap();
        let admission = ledger.append(event("execution/input-admitted",Kind::ExecutionInputAdmitted,json!({
            "schema_version":1,"opening_protocol":1,"key":execution,"model_request":request,
            "max_steps":2,"max_tool_calls":null,"deadline_at_ms":deadline.deadline_at_ms(),"tool_timeout_ms":null,
            "dispatch":{"mode":"Serial","on_error":"FailTurn"},"agent_snapshot_digest":null
        }))).unwrap();
        let control = TurnControl::default();
        let limits = ModelOpeningInspectionLimits {
            max_events: 64,
            max_payload_bytes: 1024 * 1024,
            max_total_bytes: 4 * 1024 * 1024,
        };
        let calls = Arc::new(Mutex::new(Vec::<Value>::new()));
        let actual_ledger = Arc::new(stores::Ledger {
            inner: ledger.clone(),
            mode: case.mode.clone(),
            calls: calls.clone(),
        });
        let actual_facts = Arc::new(stores::Facts {
            inner: facts.clone(),
            mode: case.mode.clone(),
            calls: calls.clone(),
        });
        let mut consumer_limits = limits.clone();
        if case.mode == "prefix_bound" {
            consumer_limits.max_events = 4;
        }
        let attempt = ModelOpeningAttempt::from_admission(
            actual_ledger,
            actual_facts,
            ModelOpeningAttemptConfig {
                execution: execution.clone(),
                input_admission: reference(&admission),
                deadline,
                control: control.clone(),
                limits: consumer_limits,
            },
        )
        .unwrap();
        ledger
            .append(event(
                "execution/step",
                Kind::StepStarted,
                json!({"step_id":request.request_id}),
            ))
            .unwrap();
        let requested = ledger
            .append(event(
                "execution/request",
                Kind::ModelRequested,
                json!({"request":request}),
            ))
            .unwrap();
        let publication_error = attempt
            .record_requested(&requested)
            .err()
            .map(|e| e.to_string());
        if case.mode == "ledger_cancel" {
            ledger
                .append(event(
                    "execution/arbitrary-cancel-id",
                    Kind::ExecutionCancelled,
                    Value::Null,
                ))
                .unwrap();
        }
        let counters = Arc::new(provider::Counters::default());
        let raw = provider::FixtureProvider {
            owner: Arc::new(()),
            mode: case.mode.clone(),
            control,
            counters: counters.clone(),
        };
        let policy = if case.policy.contains("bytes") {
            OpeningAccountingPolicy::WireBytes {
                policy_revision: "fixture-bytes-v1".into(),
                max_generation_wire_bytes: if case.policy == "zero_bytes" { 0 } else { 4096 },
            }
        } else {
            OpeningAccountingPolicy::ProviderReported {
                max_input_tokens: if case.policy == "zero_tokens" { 0 } else { 10 },
                count_timeout: Duration::from_millis(20),
            }
        };
        let provider = OpeningModelProvider::new(raw, attempt.clone(), policy).unwrap();
        let first = provider.stream(request.clone()).await;
        let second = if case.mode == "duplicate" {
            Some(provider.stream(request.clone()).await)
        } else {
            None
        };
        let completion_error = if case.mode == "completion" {
            let sources = attempt.completion_sources(&request.request_id).unwrap();
            let completed=ledger.append(event("execution/completed",Kind::StepCompleted,json!({
                "step_id":request.request_id,"outcome":"FinalAnswer",
                "step":{"step_id":request.request_id,"response":base["response"],"outcome":"FinalAnswer"},
                "opening_protocol":1,"model_requested":sources.model_requested(),"model_opening":sources.model_opening()
            }))).unwrap();
            attempt
                .record_completed(&completed)
                .err()
                .map(|e| e.to_string())
        } else {
            None
        };
        let result_error = second
            .as_ref()
            .and_then(|r| r.as_ref().err())
            .or_else(|| first.as_ref().err())
            .map(ToString::to_string);
        let typed = attempt.failure().unwrap();
        let error = typed.as_ref().map(|e| error_class(e.as_ref()));
        let events = ledger
            .query(&LedgerQuery {
                execution_id: Some("execution".into()),
                event_id: None,
                after: 0,
                through: None,
                limit: 64,
            })
            .unwrap();
        let mut records = Vec::new();
        for event in &events {
            if event.kind == Kind::ModelOpeningAdmitted {
                let fact: kolyan_ledger::FactRef =
                    serde_json::from_value(event.payload["preparation"].clone()).unwrap();
                records.extend(facts.read(&fact.stream_id, 0, 1).unwrap());
            }
        }
        // Also export preparation-only rows (cancelled before admission).
        let coordinate = serde_json::to_vec(&(&execution, "turn-step-0")).unwrap();
        use sha2::Digest;
        let stream = format!("model-preparation/{:x}", sha2::Sha256::digest(coordinate));
        let preparation_only = facts.read(&stream, 0, 1).unwrap();
        if records.is_empty() {
            records = preparation_only;
        }
        let (read_ledger, read_facts): (Arc<dyn LedgerStore>, Arc<dyn FactJournal>) = if sqlite {
            (
                Arc::new(
                    SqliteLedger::open(directory.join(format!("{}.ledger", case.id))).unwrap(),
                ),
                Arc::new(
                    SqliteFactJournal::open(directory.join(format!("{}.facts", case.id))).unwrap(),
                ),
            )
        } else {
            (ledger.clone(), facts.clone())
        };
        let proof = inspect_model_openings(
            read_ledger.as_ref(),
            read_facts.as_ref(),
            &ModelOpeningInspectionRequest {
                execution,
                through: reference(events.last().unwrap()),
                limits,
            },
        );
        let state = proof.as_ref().map(|p| {
            p.steps()
                .last()
                .map(|s| serde_json::to_value(s.state()).unwrap())
                .unwrap_or(Value::Null)
        });
        let actual = json!({"id":case.id,"backend":if sqlite {"reopened_sqlite"} else {"memory"},
            "origin":"manual recorder/source and Core completion fixtures; actual Runtime writer and fixture prepared provider; not SDK/Driver/EOF acceptance",
            "publication_error":publication_error,"completion_error":completion_error,
            "events":events,"facts":records,"store_calls":calls.lock().unwrap().clone(),"provider_events":counters.events.lock().unwrap().clone(),
            "error":error,"provider_error":result_error,"proof_error":proof.as_ref().err().map(ToString::to_string),
            "state":state.as_ref().ok(),"prepare":counters.prepare.load(Ordering::SeqCst),
            "count":counters.count.load(Ordering::SeqCst),"gen":counters.generation.load(Ordering::SeqCst)});
        serde_json::to_writer(&mut output, &actual).unwrap();
        writeln!(output).unwrap();
    }
    output.flush().unwrap();
    output.sync_all().unwrap();
    drop(output);
    let rows: Vec<Value> = BufReader::new(File::open(&path).unwrap())
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .collect();
    println!(
        "opening consumer artifact={} rows={}",
        path.display(),
        rows.len()
    );
    assert_eq!(rows.len(), cases.len());
    for (actual, case) in rows.iter().zip(cases) {
        assert_eq!(
            actual["error"],
            json!(case.error),
            "{}: {}",
            case.id,
            actual
        );
        assert_eq!(actual["prepare"], json!(case.prepare), "{}", case.id);
        assert_eq!(actual["count"], json!(case.count), "{}", case.id);
        assert_eq!(actual["gen"], json!(case.generation), "{}", case.id);
        assert_eq!(actual["completion_error"], Value::Null, "{}", case.id);
        assert_eq!(
            actual["state"],
            json!(case.state),
            "{}: {}",
            case.id,
            actual
        );
    }
}

fn reference(event: &LedgerEvent) -> ModelOpeningEventRef {
    ModelOpeningEventRef {
        event_id: event.event_id.clone(),
        cursor: event.cursor,
    }
}
fn event(id: &str, kind: Kind, payload: Value) -> LedgerEvent {
    LedgerEvent {
        event_id: id.into(),
        idempotency_key: id.into(),
        execution_id: "execution".into(),
        turn_id: "turn".into(),
        cursor: 0,
        kind,
        payload,
    }
}
fn error_class(error: &OpeningAdmissionError) -> &'static str {
    match error {
        OpeningAdmissionError::Cancelled => "cancelled",
        OpeningAdmissionError::DeadlineExceeded => "deadline",
        OpeningAdmissionError::CountTimedOut => "count_timeout",
        OpeningAdmissionError::AccountingRejected(_) => "accounting",
        OpeningAdmissionError::Provider(_) => "provider",
        OpeningAdmissionError::InvalidState(_) => "state",
        OpeningAdmissionError::BindingMismatch(_) => "binding",
        OpeningAdmissionError::InvalidConfiguration(_) => "configuration",
        OpeningAdmissionError::Ledger(_) => "ledger",
        OpeningAdmissionError::Fact(_) => "fact",
        OpeningAdmissionError::Proof(_) => "proof",
        OpeningAdmissionError::Worker(_) => "worker",
    }
}
