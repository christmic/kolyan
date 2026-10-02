//! Independent protocol fixtures, exported in full before verdict comparisons.

mod binding;
mod fixtures;
mod stores;

use std::fs::{self, File};
use std::io::Write;
use std::sync::atomic::Ordering;

use kolyan_ledger::{
    FactJournal, InMemoryLedger, LedgerStore, MemoryFactJournal, SqliteFactJournal, SqliteLedger,
};
use serde_json::{Value, json};

use super::*;
use fixtures::{Case, fixture};
use stores::{ObservedFacts, ObservedLedger, ledger_rows};

#[test]
fn memory_model_opening_protocol_matrix() {
    matrix(false);
}

#[test]
fn sqlite_reopened_model_opening_protocol_matrix() {
    matrix(true);
}

fn matrix(sqlite: bool) {
    let mut cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let required: Vec<Value> =
        serde_json::from_str(include_str!("tests/required_fields.json")).unwrap();
    for group in required {
        for field in group["fields"].as_array().unwrap() {
            cases.push(serde_json::from_value(json!({
                "id":format!("required-{}-{}",group["target"].as_str().unwrap(),field.as_str().unwrap().replace('/',"-")),
                "expected":group["expected"],
                "mutations":[{"target":group["target"],"pointer":format!("/{}",field.as_str().unwrap()),"remove":true}]
            })).unwrap());
        }
    }
    let artifacts = tempfile::Builder::new()
        .prefix("kolyan-model-opening-proof-")
        .tempdir()
        .unwrap()
        .keep();
    let backend = if sqlite { "sqlite-reopened" } else { "memory" };
    let mut rows = Vec::new();
    for case in cases {
        let mut fixture = fixture(&case);
        let (ledger, facts): (Box<dyn LedgerStore>, Box<dyn FactJournal>) = if sqlite {
            let database = artifacts.join(format!("{}.db", case.id));
            {
                let ledger = SqliteLedger::open(&database).unwrap();
                for event in fixture.events {
                    ledger.append(event).unwrap();
                }
                let facts = SqliteFactJournal::open(&database).unwrap();
                if !fixture.drafts.is_empty() {
                    facts.append("opening-fixtures", 0, fixture.drafts).unwrap();
                }
            }
            (
                Box::new(SqliteLedger::open(&database).unwrap()),
                Box::new(SqliteFactJournal::open(&database).unwrap()),
            )
        } else {
            let ledger = InMemoryLedger::default();
            for event in fixture.events {
                ledger.append(event).unwrap();
            }
            let facts = MemoryFactJournal::default();
            if !fixture.drafts.is_empty() {
                facts.append("opening-fixtures", 0, fixture.drafts).unwrap();
            }
            (Box::new(ledger), Box::new(facts))
        };
        let ledger_before = ledger_rows(&*ledger);
        let facts_before = facts.read("opening-fixtures", 0, 1024).unwrap();
        if matches!(
            case.mode.as_str(),
            "exact_limits" | "total_below_exact" | "payload_below_exact"
        ) {
            let own: Vec<_> = ledger_before
                .iter()
                .filter(|event| event.execution_id == "execution")
                .collect();
            fixture.request.limits.max_events = own.len();
            fixture.request.limits.max_total_bytes = own
                .iter()
                .map(|row| serde_json::to_vec(row).unwrap().len())
                .sum::<usize>()
                + facts_before
                    .iter()
                    .map(|row| serde_json::to_vec(row).unwrap().len())
                    .sum::<usize>();
            fixture.request.limits.max_payload_bytes = own
                .iter()
                .map(|row| serde_json::to_vec(&row.payload).unwrap().len())
                .chain(
                    facts_before
                        .iter()
                        .map(|row| serde_json::to_vec(&row.draft.payload).unwrap().len()),
                )
                .max()
                .unwrap();
            if case.mode == "total_below_exact" {
                fixture.request.limits.max_total_bytes -= 1;
            }
            if case.mode == "payload_below_exact" {
                fixture.request.limits.max_payload_bytes -= 1;
            }
        }
        let ledger = ObservedLedger::new(ledger, &case.mode);
        let facts = ObservedFacts::new(facts, &case.mode);
        let result = inspect_model_openings(&ledger, &facts, &fixture.request);
        rows.push(json!({
            "origin":"manual protocol fixture, not production writer/GEN acceptance", "case":case.id,"backend":backend,
            "expected":case.expected,"actual":classification(&result),"verified":observation(&result),
            "error":result.as_ref().err().map(ToString::to_string),
            "request":{"execution":fixture.request.execution,"through":fixture.request.through,
                "limits":{"max_events":fixture.request.limits.max_events,"max_total_bytes":fixture.request.limits.max_total_bytes,
                    "max_payload_bytes":fixture.request.limits.max_payload_bytes}},
            "ledger_before":ledger_before,"ledger_after":ledger_rows(&*ledger.inner),
            "facts_before":facts_before,"facts_after":facts.inner.read("opening-fixtures",0,1024).unwrap(),
            "ledger_reads":ledger.reads.lock().unwrap().clone(),"fact_reads":facts.reads.lock().unwrap().clone(),
            "ledger_writes":ledger.writes.load(Ordering::SeqCst),"fact_writes":facts.writes.load(Ordering::SeqCst),
            "unbounded_reads":ledger.unbounded.load(Ordering::SeqCst),
        }));
    }
    let path = artifacts.join("actual.jsonl");
    {
        let mut file = File::create(&path).unwrap();
        for row in &rows {
            serde_json::to_writer(&mut file, row).unwrap();
            file.write_all(b"\n").unwrap();
        }
        file.sync_all().unwrap();
    }
    let physical = fs::read_to_string(&path).unwrap();
    let actual: Vec<Value> = physical
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    println!(
        "opening-proof {backend}: {} rows, artifact {}",
        actual.len(),
        path.display()
    );
    assert_eq!(actual, rows, "physical export differs");
    for row in actual {
        assert_eq!(
            row["actual"], row["expected"],
            "{}: {}",
            row["case"], row["error"]
        );
        assert_eq!(row["ledger_before"], row["ledger_after"]);
        assert_eq!(row["facts_before"], row["facts_after"]);
        assert_eq!(row["ledger_writes"], 0);
        assert_eq!(row["fact_writes"], 0);
        assert_eq!(row["unbounded_reads"], 0);
        for read in row["ledger_reads"].as_array().unwrap() {
            assert_eq!(
                read["query"]["through"],
                row["request"]["through"]["cursor"]
            );
            assert_eq!(read["query"]["execution_id"], "execution");
            assert!(read["query"]["limit"].as_u64().unwrap() <= 512);
        }
    }
}

fn observation(result: &Result<VerifiedModelOpenings, ModelOpeningProofError>) -> Value {
    match result {
        Ok(proof) => {
            json!({"execution":proof.execution(),"input_admission":proof.input_admission(),
            "inspected_through":proof.inspected_through(),"has_uncertain":proof.has_uncertain(),
            "steps":proof.steps().iter().map(|step| json!({"step_id":step.step_id(),"state":step.state(),
                "started":step.started(),"requested":step.requested(),"preparation":step.preparation(),
                "opening":step.opening(),"completed":step.completed()})).collect::<Vec<_>>()})
        }
        Err(_) => Value::Null,
    }
}

fn classification(result: &Result<VerifiedModelOpenings, ModelOpeningProofError>) -> &'static str {
    match result {
        Ok(proof) if proof.has_uncertain() => "uncertain",
        Ok(proof) if proof.steps().is_empty() => "no_steps",
        Ok(proof)
            if proof
                .steps()
                .iter()
                .any(|step| step.state() == ModelOpeningState::NotAdmitted) =>
        {
            "not_admitted"
        }
        Ok(_) => "completed",
        Err(ModelOpeningProofError::InvalidRequest(_)) => "invalid_request",
        Err(ModelOpeningProofError::UnsupportedProtocol { .. }) => "unsupported",
        Err(ModelOpeningProofError::MissingEvidence { .. }) => "missing",
        Err(ModelOpeningProofError::BindingMismatch { .. }) => "binding",
        Err(ModelOpeningProofError::OrderingMismatch(_)) => "ordering",
        Err(ModelOpeningProofError::InvalidPayload { .. }) => "invalid_payload",
        Err(ModelOpeningProofError::BoundsExceeded { .. }) => "bounds",
        Err(ModelOpeningProofError::LedgerStorage(_)) => "ledger_storage",
        Err(ModelOpeningProofError::FactStorage(_)) => "fact_storage",
    }
}
