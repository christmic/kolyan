//! Deterministic pre-append rendezvous proves actual same-head CAS contention.

use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    sync::{Arc, Barrier, Mutex},
};

use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, MemoryFactJournal, SqliteFactJournal,
};
use kolyan_trace::ArtifactStore;
use serde::Deserialize;
use serde_json::{Value, json};

use super::super::*;

struct Rendezvous {
    inner: Arc<dyn FactJournal>,
    barrier: Barrier,
    candidates: Mutex<Vec<Value>>,
}

impl FactJournal for Rendezvous {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.inner.read(stream, after, limit)
    }
    fn append(
        &self,
        stream: &str,
        expected: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        self.candidates
            .lock()
            .unwrap()
            .push(json!({"stream":stream,"expected":expected,"batch":batch}));
        self.barrier.wait();
        self.inner.append(stream, expected, batch)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    same: bool,
    expected_ok: usize,
    expected_conflict: usize,
    expected_facts: usize,
}

#[test]
fn registration_same_head_cas_data_matrix() {
    let rows: Vec<Row> = serde_json::from_str(include_str!("race_cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-skills-cas-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut file = File::create(&path).unwrap();
    for backend in ["memory", "sqlite"] {
        for row in &rows {
            let database = root.join(format!("{backend}-{}.sqlite", row.id));
            let inner: Arc<dyn FactJournal> = if backend == "sqlite" {
                Arc::new(SqliteFactJournal::open(&database).unwrap())
            } else {
                Arc::new(MemoryFactJournal::default())
            };
            let journal = Arc::new(Rendezvous {
                inner,
                barrier: Barrier::new(2),
                candidates: Mutex::new(vec![]),
            });
            let artifacts = Arc::new(
                ArtifactStore::new(
                    root.join(format!("{backend}-{}-artifacts", row.id)),
                    MAX_BODY_BYTES as u64,
                )
                .unwrap(),
            );
            let catalog = SkillCatalog::new(
                journal.clone(),
                artifacts,
                "host.race".into(),
                SkillLimits::default(),
            )
            .unwrap();
            let before = journal.inner.read(catalog.stream_id(), 0, 1024).unwrap();
            let outcomes: Vec<_> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..2)
                    .map(|i| {
                        let catalog = catalog.clone();
                        scope.spawn(move || {
                            let body = if row.same || i == 0 {
                                "body-one"
                            } else {
                                "body-two"
                            };
                            let result = catalog.register(
                                SkillDescriptorInput {
                                    key: SkillKey::new("guide".into(), "1".into()).unwrap(),
                                    title: "guide".into(),
                                    description: "immutable".into(),
                                },
                                body,
                            );
                            match result {
                                Ok(value) => json!({"outcome":"ok","registered":value}),
                                Err(SkillError::Conflict) => json!({"outcome":"conflict"}),
                                Err(error) => {
                                    json!({"outcome":"unexpected","error":error.to_string()})
                                }
                            }
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect()
            });
            let after = journal.inner.read(catalog.stream_id(), 0, 1024).unwrap();
            let reopened: Arc<dyn FactJournal> = if backend == "sqlite" {
                Arc::new(SqliteFactJournal::open(&database).unwrap())
            } else {
                journal.inner.clone()
            };
            let rebuilt = reopened.read(catalog.stream_id(), 0, 1024).unwrap();
            serde_json::to_writer(&mut file, &json!({"id":row.id,"backend":backend,"before":before,"candidates":*journal.candidates.lock().unwrap(),"outcomes":outcomes,"after":after,"reconstructed":rebuilt})).unwrap();
            writeln!(file).unwrap();
        }
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    println!("SKILLS_CAS_ACTUAL={}", path.display());
    let actual: Vec<Value> = BufReader::new(File::open(&path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    assert_eq!(actual.len(), rows.len() * 2);
    for (index, observation) in actual.iter().enumerate() {
        let row = &rows[index % rows.len()];
        let candidates = observation["candidates"].as_array().unwrap();
        assert_eq!(candidates.len(), 2);
        assert!(candidates.iter().all(|c| c["expected"] == 0));
        let outcomes = observation["outcomes"].as_array().unwrap();
        assert_eq!(
            outcomes.iter().filter(|o| o["outcome"] == "ok").count(),
            row.expected_ok
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|o| o["outcome"] == "conflict")
                .count(),
            row.expected_conflict
        );
        assert_eq!(
            observation["after"].as_array().unwrap().len(),
            row.expected_facts
        );
        assert_eq!(observation["after"], observation["reconstructed"]);
        if row.same {
            assert_eq!(outcomes[0]["registered"], outcomes[1]["registered"]);
        }
    }
}
