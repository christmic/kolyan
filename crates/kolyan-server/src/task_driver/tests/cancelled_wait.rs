//! Real durable external handoff; cancellation never supplies effect receipts.

use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, atomic::AtomicUsize};

use futures_util::FutureExt;
use kolyan_core::TurnExecutor;
use kolyan_ledger::{InMemoryLedger, LedgerStore};
use kolyan_storage::{FileSessionStore, SessionStore};
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use serde_json::{Value, json};

use super::super::*;
use super::support::*;
use crate::{ExecutionService, SessionService};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    signal_only: bool,
    mutation: Mutation,
    expected: String,
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    MissingWait,
    UnknownSchema,
    Resumed,
    NewEffect,
    LateStep,
    LateWait,
    ForeignGrant,
    ForeignScope,
}

async fn scenario(case: &Case) -> Value {
    let directory = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(directory.path()).unwrap();
    store.create("session-root").unwrap();
    let host = Host {
        coordinator: coordinator(),
        ledger: FaultLedger::default(),
    };
    let service = TaskExecutionService::new(
        host.coordinator.clone(),
        crate::SessionExecutionService::new(
            ExecutionService::new(host.ledger.clone(), NoopTraceSink)
                .with_external_wait_verifier(Arc::new(host.clone())),
            SessionService::new(store),
        ),
    );
    let executor = TurnExecutor::with_tools(
        Provider {
            delegate: true,
            calls: Arc::new(AtomicUsize::new(0)),
        },
        Delegate {
            coordinator: host.coordinator.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
        },
    )
    .with_policy_engine(policy());
    service
        .run("task", binding("root"), executor, request(&binding("root")))
        .await
        .unwrap();
    if case.signal_only {
        service
            .sessions()
            .execution()
            .cancel(&binding("root").execution)
            .unwrap();
    } else {
        service
            .sessions()
            .cancel(&binding("root").execution)
            .unwrap();
    }
    let original = host
        .ledger
        .execution_events_after("execution-root", 0)
        .unwrap();
    let ledger = InMemoryLedger::default();
    for mut event in original.clone() {
        if matches!(case.mutation, Mutation::MissingWait | Mutation::LateWait)
            && event.kind == LedgerEventKind::EffectAwaitingExternal
        {
            // Preserve cursor positions so refusal tests the absent handoff
            // proof, not a coincidentally invalid checkpoint publication cursor.
            event.kind = LedgerEventKind::ModelStreamEvent;
            event.event_id = "diagnostic-wait-gap".into();
            event.idempotency_key = event.event_id.clone();
            event.payload = Value::Null;
            ledger.append(event).unwrap();
            continue;
        }
        if case.mutation == Mutation::UnknownSchema
            && event.kind == LedgerEventKind::EffectAwaitingExternal
        {
            event.payload["schema_version"] = json!(2);
        }
        if case.mutation == Mutation::ForeignGrant
            && event.kind == LedgerEventKind::EffectAwaitingExternal
        {
            event.payload["prepared_grant"] = Value::Null;
        }
        if case.mutation == Mutation::ForeignScope
            && event.kind == LedgerEventKind::ExecutionSuspended
        {
            event.payload["suspension"]["checkpoint"]["scope"]["execution"]["execution_id"] =
                json!("foreign");
        }
        if event.kind == LedgerEventKind::ExecutionCancelled && case.mutation == Mutation::Resumed {
            let mut resumed = event.clone();
            resumed.event_id = "diagnostic-resumed".into();
            resumed.idempotency_key = resumed.event_id.clone();
            resumed.kind = LedgerEventKind::ExecutionStarted;
            ledger.append(resumed).unwrap();
        }
        if event.kind == LedgerEventKind::TurnCancelled && case.mutation == Mutation::NewEffect {
            let mut started = original
                .iter()
                .find(|e| e.kind == LedgerEventKind::EffectStarted)
                .unwrap()
                .clone();
            started.event_id = "diagnostic-late-effect".into();
            started.idempotency_key = started.event_id.clone();
            ledger.append(started).unwrap();
        }
        if event.kind == LedgerEventKind::TurnCancelled && case.mutation == Mutation::LateStep {
            let mut completed = original
                .iter()
                .find(|event| event.kind == LedgerEventKind::StepCompleted)
                .unwrap()
                .clone();
            completed.event_id = "diagnostic-late-step-completed".into();
            completed.idempotency_key = completed.event_id.clone();
            ledger.append(completed).unwrap();
        }
        ledger.append(event).unwrap();
    }
    if case.mutation == Mutation::LateWait {
        let waiting = original
            .iter()
            .find(|event| event.kind == LedgerEventKind::EffectAwaitingExternal)
            .unwrap()
            .clone();
        ledger.append(waiting).unwrap();
    }
    let before = ledger.events_after(0).unwrap();
    let active_wait = crate::suspension::current_suspension(&binding("root").execution, &before)
        .unwrap()
        .is_some();
    let observed = evidence::inspect(&ledger, &binding("root"), 0);
    let (outcome, source, error) = match observed {
        Ok(proof) => (
            match proof.stopped {
                evidence::StoppedOutcome::Cancelled => "cancelled",
                evidence::StoppedOutcome::RecoveryRequired => "recovery",
                _ => "other",
            },
            Some(proof.source),
            None,
        ),
        Err(error) => ("error", None, Some(error.to_string())),
    };
    json!({"id":case.id,"original":original,"before":before,"active_wait":active_wait,
        "events":ledger.events_after(0).unwrap(),
        "outcome":outcome,"source":source,"error":error})
}

#[tokio::test]
async fn cancellation_external_handoff_proofs_export_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("cancelled_wait/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-cancelled-wait-proof-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("CANCELLED_WAIT_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let row = match AssertUnwindSafe(scenario(case)).catch_unwind().await {
            Ok(row) => row,
            Err(panic) => json!({"id":case.id,"outcome":"scenario_failed","error":
                panic.downcast_ref::<String>().map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("scenario panic")}),
        };
        writeln!(output, "{}", row).unwrap();
        output.sync_all().unwrap();
    }
    let actual = std::fs::read_to_string(path).unwrap();
    assert_eq!(actual.lines().count(), cases.len());
    for (line, case) in actual.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["active_wait"], false, "{row}");
        assert_eq!(
            row["before"], row["events"],
            "inspection must not publish: {row}"
        );
        if case.expected == "refused" {
            assert!(
                matches!(row["outcome"].as_str(), Some("recovery" | "error")),
                "{row}"
            );
        } else {
            assert_eq!(row["outcome"], case.expected, "{row}");
        }
        if case.expected == "cancelled" {
            let events = row["events"].as_array().unwrap();
            let terminal = events
                .iter()
                .find(|e| e["kind"] == "turn_cancelled")
                .unwrap();
            assert_eq!(row["source"]["event_id"], terminal["event_id"]);
            assert_eq!(row["source"]["cursor"], terminal["cursor"]);
            assert!(!events.iter().any(|e| e["kind"] == "effect_receipt"));
        }
    }
}
