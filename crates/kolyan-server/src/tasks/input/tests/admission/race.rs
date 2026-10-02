//! Deterministic pre-append contention; the real SQLite CAS and retry rules remain intact.
use super::*;
use crate::tasks::reducer::reference;
use std::sync::{Arc, Barrier, Mutex};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum Operation {
    Publication,
    Admission,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RaceCase {
    id: String,
    operation: Operation,
    identical: bool,
    participants: usize,
    accepted: usize,
    refused: usize,
    committed: usize,
}

#[derive(Clone)]
struct AppendFence {
    journal: SqliteFactJournal,
    stream: String,
    expected: u64,
    barrier: Arc<Barrier>,
    candidates: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl FactJournal for AppendFence {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.journal.read(stream, after, limit)
    }

    fn append(
        &self,
        stream: &str,
        expected: u64,
        drafts: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        assert_eq!(stream, self.stream);
        assert_eq!(expected, self.expected);
        self.candidates.lock().unwrap().push(json!({
            "stream": stream, "expected_position": expected, "drafts": drafts,
        }));
        // Every caller has completed the production read/validate/candidate path.
        // No participant can commit before all candidates reach this boundary.
        self.barrier.wait();
        self.journal.append(stream, expected, drafts)
    }
}

fn run(case: &RaceCase, root: &std::path::Path) -> serde_json::Value {
    let path = root.join(format!("{}.sqlite", case.id));
    let journal = SqliteFactJournal::open(&path).unwrap();
    let coordinator = registered(journal.clone());
    let cause = FactRef {
        stream_id: "task".into(),
        position: 1,
        fact_id: "registered".into(),
    };
    let mut candidates = Vec::new();
    for index in 0..case.participants {
        let mut envelope = InvocationInputEnvelope {
            kind: InvocationInputKind::Standalone,
            scope: scope(),
            body: json!({"generic_fixture": true}),
        };
        if !case.identical {
            match case.operation {
                Operation::Publication => envelope.body = json!({"candidate": index}),
                Operation::Admission => envelope.scope.invocation_id = format!("root-{index}"),
            }
        }
        let source = match case.operation {
            Operation::Publication => None,
            Operation::Admission => Some(InvocationInputSource::Standalone {
                fact: coordinator
                    .publish_invocation_input_source(envelope.clone(), vec![cause.clone()])
                    .unwrap()
                    .reference,
            }),
        };
        candidates.push((envelope, source));
    }
    let (stream, expected) = match case.operation {
        Operation::Publication => (coordinate(&scope()).unwrap(), 0),
        Operation::Admission => ("task".into(), 1),
    };
    let before = journal.read(&stream, 0, 32).unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(case.participants));
    // Open all independent connections before starting the synchronized callers.
    let connections: Vec<_> = (0..case.participants)
        .map(|_| SqliteFactJournal::open(&path).unwrap())
        .collect();
    let results = std::thread::scope(|threads| {
        let handles: Vec<_> = connections
            .into_iter()
            .zip(candidates)
            .map(|(connection, (envelope, source))| {
                let fence = AppendFence {
                    journal: connection,
                    stream: stream.clone(),
                    expected,
                    barrier: barrier.clone(),
                    candidates: attempts.clone(),
                };
                let cause = cause.clone();
                threads.spawn(move || {
                    let coordinator = TaskCoordinator::new(fence);
                    match source {
                        None => coordinator
                            .publish_invocation_input_source(envelope, vec![cause])
                            .map(|value| serde_json::to_value(value).unwrap()),
                        Some(source) => {
                            let mut proposed = definition(source);
                            proposed.invocation_id = envelope.scope.invocation_id;
                            coordinator
                                .admit_invocation("task", "admitted", proposed)
                                .map(|value| serde_json::to_value(value).unwrap())
                        }
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    let after = journal.read(&stream, 0, 32).unwrap();
    let restored = TaskCoordinator::new(SqliteFactJournal::open(&path).unwrap());
    let recovered = match case.operation {
        Operation::Publication => restored
            .load_verified_invocation_input_source(
                &InvocationInputSource::Standalone {
                    fact: reference(after.last().unwrap()),
                },
                &scope(),
                MAX_BYTES,
            )
            .map(|value| serde_json::to_value(value).unwrap()),
        Operation::Admission => restored
            .snapshot("task")
            .map(|value| serde_json::to_value(value).unwrap()),
    };
    let classified: Vec<_> = results
        .iter()
        .map(|result| match result {
            Ok(value) => json!({"status":"accepted", "value":value}),
            Err(TaskError::Journal(FactError::Conflict(message))) => {
                json!({"status":"journal_conflict", "detail":message})
            }
            Err(TaskError::Invalid(message))
                if matches!(case.operation, Operation::Publication)
                    && message == "invocation input source content conflicts" =>
            {
                json!({"status":"source_content_conflict", "detail":message})
            }
            Err(error) => json!({"status":"unexpected_error", "detail":error.to_string()}),
        })
        .collect();
    json!({"case":case,"before":before,"candidates":*attempts.lock().unwrap(),
        "results":classified,"after":after,"recovered":recovered.as_ref().map_err(ToString::to_string)})
}

#[test]
fn invocation_input_source_deterministic_preappend_race_data() {
    let cases: Vec<RaceCase> = serde_json::from_str(include_str!("race_cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-input-preappend-race-")
        .tempdir()
        .unwrap()
        .keep();
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    println!(
        "INPUT_PREAPPEND_RACE_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let rows: Vec<_> = cases.iter().map(|case| run(case, &root)).collect();
    for row in &rows {
        writeln!(output, "{row}").unwrap();
    }
    output.sync_all().unwrap();
    for (case, row) in cases.iter().zip(rows) {
        let results = row["results"].as_array().unwrap();
        let accepted: Vec<_> = results
            .iter()
            .filter(|r| r["status"] == "accepted")
            .collect();
        assert_eq!(accepted.len(), case.accepted, "{}: {row}", case.id);
        assert_eq!(
            results.len() - accepted.len(),
            case.refused,
            "{}: {row}",
            case.id
        );
        assert!(
            !results.iter().any(|r| r["status"] == "unexpected_error"),
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["candidates"].as_array().unwrap().len(),
            case.participants
        );
        assert_eq!(
            row["after"].as_array().unwrap().len() - row["before"].as_array().unwrap().len(),
            case.committed
        );
        for winner in accepted {
            assert_eq!(
                winner["value"], row["recovered"]["Ok"],
                "{}: {row}",
                case.id
            );
        }
    }
}
