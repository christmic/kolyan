//! Fixture-driven coordination journal parity; no domain authority is inferred.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, FactRef, FactSubject, MemoryFactJournal,
    SqliteFactJournal,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    read_after: u64,
    page_limit: usize,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    drafts: BTreeMap<String, FactDraft>,
    operations: Vec<Append>,
    expected: BTreeMap<String, Vec<SemanticRecord>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Append {
    stream: String,
    expected_position: u64,
    batch: Vec<String>,
    outcome: Outcome,
    returned_positions: Vec<u64>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Committed,
    Conflict,
    MissingCause,
    StalePosition,
    Invalid,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SemanticRecord {
    position: u64,
    fact_id: String,
    kind: String,
    subject: FactSubject,
    schema_version: u32,
    critical: bool,
    causes: Vec<FactRef>,
}

impl From<&FactRecord> for SemanticRecord {
    fn from(record: &FactRecord) -> Self {
        Self {
            position: record.position,
            fact_id: record.draft.fact_id.clone(),
            kind: record.draft.kind.clone(),
            subject: record.draft.subject.clone(),
            schema_version: record.draft.schema_version,
            critical: record.draft.critical,
            causes: record.draft.causes.clone(),
        }
    }
}

#[test]
fn fixture_batches_match_both_journals_and_retained_jsonl() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("../fixtures/task_journal.json")).unwrap();
    assert!(!fixture.cases.is_empty());
    let mut names = BTreeSet::new();
    assert!(fixture.cases.iter().all(|case| names.insert(&case.name)));
    let root = tempfile::Builder::new()
        .prefix("kolyan-fact-journal-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("FactJournal evidence: {}", root.display());
    for case in &fixture.cases {
        assert_safe_component(&case.name);
        for backend in ["memory", "sqlite"] {
            let directory = root.join(&case.name).join(backend);
            fs::create_dir_all(&directory).unwrap();
            let database = directory.join("journal.sqlite");
            let journal: Box<dyn FactJournal> = match backend {
                "memory" => Box::new(MemoryFactJournal::default()),
                "sqlite" => Box::new(SqliteFactJournal::open(&database).unwrap()),
                _ => unreachable!(),
            };
            run_case(journal.as_ref(), &fixture, case, &directory);
            if backend == "sqlite" {
                drop(journal);
                let reopened = SqliteFactJournal::open(database).unwrap();
                compare_streams(&reopened, &fixture, case, &directory, "reopened");
            }
        }
    }
}

fn run_case(journal: &dyn FactJournal, fixture: &Fixture, case: &Case, directory: &Path) {
    assert!(!case.operations.is_empty());
    for (index, operation) in case.operations.iter().enumerate() {
        assert!(case.expected.contains_key(&operation.stream));
        let before = snapshot(journal, fixture, case);
        let batch: Vec<_> = operation
            .batch
            .iter()
            .map(|key| {
                case.drafts
                    .get(key)
                    .unwrap_or_else(|| panic!("unknown fixture draft {key}"))
                    .clone()
            })
            .collect();
        let result = journal.append(
            &operation.stream,
            operation.expected_position,
            batch.clone(),
        );
        match result {
            Ok(records) => {
                assert_eq!(
                    operation.outcome,
                    Outcome::Committed,
                    "{} operation {index}",
                    case.name
                );
                assert_eq!(
                    records.iter().map(|r| r.position).collect::<Vec<_>>(),
                    operation.returned_positions
                );
                assert_eq!(
                    records.iter().map(|r| r.draft.clone()).collect::<Vec<_>>(),
                    batch
                );
                assert!(records.iter().all(|r| r.stream_id == operation.stream));
            }
            Err(error) => {
                let outcome = match error {
                    FactError::Conflict(_) => Outcome::Conflict,
                    FactError::MissingCause(_) => Outcome::MissingCause,
                    FactError::StalePosition { .. } => Outcome::StalePosition,
                    FactError::Invalid(_) => Outcome::Invalid,
                    FactError::Storage(reason) => panic!("unexpected storage failure: {reason}"),
                };
                assert_eq!(
                    outcome, operation.outcome,
                    "{} operation {index}",
                    case.name
                );
                assert!(operation.returned_positions.is_empty());
                assert_eq!(
                    snapshot(journal, fixture, case),
                    before,
                    "failed append changed committed facts"
                );
            }
        }
        export_records(
            &directory.join(format!("operation-{index:03}.jsonl")),
            &snapshot(journal, fixture, case)
                .into_values()
                .flatten()
                .collect::<Vec<_>>(),
        );
    }
    compare_streams(journal, fixture, case, directory, "actual");
}

fn snapshot(
    journal: &dyn FactJournal,
    fixture: &Fixture,
    case: &Case,
) -> BTreeMap<String, Vec<FactRecord>> {
    case.expected
        .keys()
        .map(|stream| {
            let mut after = fixture.read_after;
            let mut records = Vec::new();
            loop {
                let page = journal.read(stream, after, fixture.page_limit).unwrap();
                assert!(page.len() <= fixture.page_limit);
                for record in &page {
                    assert_eq!(&record.stream_id, stream);
                    assert!(record.position > after);
                    after = record.position;
                }
                let count = page.len();
                records.extend(page);
                if count < fixture.page_limit {
                    break;
                }
            }
            (stream.clone(), records)
        })
        .collect()
}

fn compare_streams(
    journal: &dyn FactJournal,
    fixture: &Fixture,
    case: &Case,
    directory: &Path,
    label: &str,
) {
    let snapshot = snapshot(journal, fixture, case);
    let records: Vec<_> = snapshot.into_values().flatten().collect();
    let path = directory.join(format!("{label}-facts.jsonl"));
    export_records(&path, &records);
    let retained: Vec<FactRecord> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    assert_eq!(retained, records);
    let mut semantic: BTreeMap<String, Vec<SemanticRecord>> = case
        .expected
        .keys()
        .map(|stream| (stream.clone(), vec![]))
        .collect();
    for record in &retained {
        semantic
            .get_mut(&record.stream_id)
            .unwrap()
            .push(record.into());
    }
    fs::write(
        directory.join(format!("{label}-semantic.json")),
        serde_json::to_vec_pretty(&semantic).unwrap(),
    )
    .unwrap();
    assert_eq!(semantic, case.expected, "{} {label}", case.name);
}

fn export_records(path: &Path, records: &[FactRecord]) {
    let mut file = File::create(path).unwrap();
    for record in records {
        serde_json::to_writer(&mut file, record).unwrap();
        writeln!(file).unwrap();
    }
    file.sync_all().unwrap();
}

fn assert_safe_component(name: &str) {
    assert!(
        !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    );
}
