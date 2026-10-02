use super::{
    super::*,
    foundation::{manifest, setup},
};
use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, MemoryFactJournal, SqliteFactJournal,
};
use kolyan_trace::ArtifactStore;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    sync::{Arc, Mutex},
};

#[derive(Deserialize)]
struct Case {
    id: String,
    action: String,
    expected: String,
}

// Explicit corrupted-read projections over a real Journal, never physical effects.
struct FaultRead {
    inner: Arc<dyn FactJournal>,
    action: Mutex<String>,
}
impl FactJournal for FaultRead {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        let mut rows = self.inner.read(stream, after, limit)?;
        let action = self.action.lock().unwrap();
        for row in &mut rows {
            let kind = row.draft.kind.as_str();
            if kind == "agent.hook.registered" {
                match action.as_str() {
                    "catalog-schema" => row.draft.schema_version = 2,
                    "catalog-subject" => row.draft.subject.id = "foreign".into(),
                    "catalog-critical" => row.draft.critical = false,
                    "catalog-storage" => {
                        return Err(FactError::Storage("injected catalog I/O".into()));
                    }
                    _ => {}
                }
            } else if kind == "agent.hook.bound" {
                match action.as_str() {
                    "binding-field" => row.draft.payload["unknown"] = json!(true),
                    "binding-causes" => row.draft.causes.clear(),
                    _ => {}
                }
            } else if kind == "agent.context.bound" {
                match action.as_str() {
                    "owner-field" => row.draft.payload["unknown"] = json!(true),
                    "owner-storage" => {
                        return Err(FactError::Storage("injected ownership I/O".into()));
                    }
                    _ => {}
                }
            }
        }
        Ok(rows)
    }
    fn append(
        &self,
        stream: &str,
        head: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        self.inner.append(stream, head, batch)
    }
}

#[test]
fn provenance_and_storage_fail_closed_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("provenance.json")).unwrap();
    let proof = tempfile::Builder::new()
        .prefix("kolyan-hook-provenance-")
        .tempdir()
        .unwrap()
        .keep();
    let path = proof.join("actual.jsonl");
    let mut file = File::create(&path).unwrap();
    for backend in ["memory", "sqlite"] {
        for case in &cases {
            let root = tempfile::tempdir().unwrap();
            let inner: Arc<dyn FactJournal> = if backend == "memory" {
                Arc::new(MemoryFactJournal::default())
            } else {
                Arc::new(SqliteFactJournal::open(root.path().join("journal.sqlite")).unwrap())
            };
            let journal = Arc::new(FaultRead {
                inner: inner.clone(),
                action: Mutex::new(String::new()),
            });
            let catalog = HookCatalog::new(
                journal.clone(),
                Arc::new(ArtifactStore::new(root.path().join("artifacts"), 1024 * 1024).unwrap()),
                "negative-fixture".into(),
            )
            .unwrap();
            let (scope, owner, policy) = setup(journal.clone());
            let registration = catalog.register(manifest(), "printf fixture").unwrap();
            let binding = catalog
                .bind(scope, owner, vec![manifest().key], &policy, "a".repeat(64))
                .unwrap();
            *journal.action.lock().unwrap() = case.action.clone();
            let result = catalog.validate_current(&binding, &policy, &"a".repeat(64));
            let actual = match &result {
                Err(HookError::Integrity(_)) => "integrity",
                Err(HookError::Journal(_)) => "journal",
                Ok(()) => "ok",
                _ => "other",
            };
            writeln!(file,"{}",json!({"id":case.id,"backend":backend,"projection_fault":case.action,"registration":registration,"binding":binding,"actual":actual,"expected":case.expected,"error":result.err().map(|e|e.to_string()),"original_catalog_facts":inner.read(catalog.stream_id(),0,257).unwrap(),"native_effects":0})).unwrap();
        }
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    println!("HOOK_PROVENANCE_ACTUAL {}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len() * 2);
    for row in rows {
        assert_eq!(row["actual"], row["expected"], "{row}");
        assert_eq!(row["native_effects"], 0);
    }
}
