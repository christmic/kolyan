//! Independent memory/SQLite reconstruction matrix; export precedes comparison.

mod execution;
mod support;

use std::sync::atomic::Ordering;

use kolyan_ledger::{InMemoryLedger, LedgerStore, SqliteLedger};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use support::{ObservedLedger, fixture};

#[derive(Deserialize)]
struct Case {
    id: String,
    expected: String,
}

#[test]
fn memory_effect_proof_matrix() {
    run_matrix(false);
}

#[test]
fn sqlite_reconstructed_effect_proof_matrix() {
    run_matrix(true);
}

fn run_matrix(sqlite: bool) {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let artifact_dir = tempfile::Builder::new()
        .prefix("kolyan-effect-proof-")
        .tempdir()
        .unwrap()
        .keep();
    let backend = if sqlite { "sqlite-reopened" } else { "memory" };
    let mut rows = Vec::new();
    for case in &cases {
        let (request, events) = fixture(&case.id);
        let store: Box<dyn LedgerStore> = if sqlite {
            let path = artifact_dir.join(format!("{}.db", case.id));
            {
                let ledger = SqliteLedger::open(&path).unwrap();
                for event in events {
                    ledger.append(event).unwrap();
                }
            }
            Box::new(SqliteLedger::open(path).unwrap())
        } else {
            let ledger = InMemoryLedger::default();
            for event in events {
                ledger.append(event).unwrap();
            }
            Box::new(ledger)
        };
        let before = store.events_after(0).unwrap();
        let ledger = ObservedLedger::new(store, &case.id);
        let result = inspect_effect_proof(&ledger, &request);
        let (actual, proof) = observe(&result);
        let after = ledger.inner.events_after(0).unwrap();
        let reads = ledger.reads.lock().unwrap().clone();
        rows.push(json!({
            "case":case.id,"backend":backend,"request":json!({"execution":request.execution,
                "effect_id":request.effect_id,"terminal":{"event_id":request.terminal.event_id,
                "cursor":request.terminal.cursor},"max_input_bytes":request.max_input_bytes,
                "max_result_bytes":request.max_result_bytes}),
            "expected":case.expected,"actual":actual,"proof":proof,
            "error":result.as_ref().err().map(ToString::to_string),
            "events_before":before,"events_after":after,
            "append_calls":ledger.writes.load(Ordering::SeqCst),
            "unbounded_reads":ledger.unbounded.load(Ordering::SeqCst),
            "queries":reads,
        }));
    }
    let output = artifact_dir.join("actual.jsonl");
    let jsonl = rows
        .iter()
        .map(|row| serde_json::to_string(row).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(&output, jsonl).unwrap();
    println!(
        "effect proof {backend}: {} rows exported to {}",
        rows.len(),
        output.display()
    );
    // No result comparison occurs before the complete backend export exists.
    let exported: Vec<Value> = std::fs::read_to_string(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for row in &exported {
        assert_eq!(
            row["actual"], row["expected"],
            "{}: {}",
            row["case"], row["error"]
        );
        assert_eq!(row["events_before"], row["events_after"], "{}", row["case"]);
        assert_eq!(row["append_calls"], 0, "{}", row["case"]);
        assert_eq!(row["unbounded_reads"], 0, "{}", row["case"]);
        let queries = row["queries"].as_array().unwrap();
        assert!(queries.len() <= 6, "{}", row["case"]);
        for query in queries {
            assert_eq!(query["limit"], 1);
            assert!(query["event_id"].is_string());
        }
        if row["actual"] == "invalid" {
            assert!(queries.is_empty());
        }
        if !row["proof"].is_null() {
            assert_eq!(
                row["proof"]["scope"]["execution"],
                row["request"]["execution"]
            );
            assert_eq!(
                row["proof"]["sources"]["terminal"],
                row["request"]["terminal"]
            );
            let events = row["events_before"].as_array().unwrap();
            let sources = &row["proof"]["sources"];
            let mut previous = 0;
            for source in [
                "admission",
                "prepared",
                "authorized",
                "started",
                "receipt",
                "terminal",
            ] {
                let coordinate = &sources[source];
                let cursor = coordinate["cursor"].as_u64().unwrap();
                assert!(cursor > previous);
                previous = cursor;
                assert!(
                    events
                        .iter()
                        .any(|event| event["event_id"] == coordinate["event_id"]
                            && event["cursor"] == coordinate["cursor"])
                );
            }
        }
    }
}

pub(super) fn observe(
    result: &Result<VerifiedEffectProof, EffectProofError>,
) -> (&'static str, Value) {
    match result {
        Ok(proof) => {
            let class = match proof.result() {
                Ok(result) if result.is_error => "completed_error",
                Ok(_) => "completed",
                Err(ToolError::Cancelled) => "cancelled",
                Err(ToolError::TimedOut) => "timed_out",
                Err(_) => "failed",
            };
            (
                class,
                json!({"prepared":proof.prepared(),"scope":proof.scope(),
                "result":proof.result(),"receipt":proof.receipt(),
                "sources":source_value(proof.sources()),"terminal_kind":proof.terminal_kind()}),
            )
        }
        Err(error) => (
            match error {
                EffectProofError::InvalidRequest(_) => "invalid",
                EffectProofError::MissingEvidence { .. } => "missing",
                EffectProofError::BindingMismatch { .. } => "binding",
                EffectProofError::OrderingMismatch => "ordering",
                EffectProofError::BoundsExceeded { .. } => "bounds",
                EffectProofError::Indeterminate { .. } => "indeterminate",
                EffectProofError::Storage(_) => "storage",
            },
            Value::Null,
        ),
    }
}

fn source_value(sources: &EffectProofSources) -> Value {
    let coordinate =
        |source: &EffectProofCoordinate| json!({"event_id":source.event_id,"cursor":source.cursor});
    json!({"admission":coordinate(&sources.admission),"prepared":coordinate(&sources.prepared),
        "authorized":coordinate(&sources.authorized),"started":coordinate(&sources.started),
        "receipt":coordinate(&sources.receipt),"terminal":coordinate(&sources.terminal)})
}
