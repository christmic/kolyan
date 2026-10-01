use super::*;

use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};

use kolyan_ledger::{FactRecord, MemoryFactJournal, SqliteFactJournal};
use serde_json::json;

fn owner(invocation: &str) -> InstanceOwner {
    InstanceOwner {
        logical_session_id: "logical-session".into(),
        task_id: "task".into(),
        invocation_id: invocation.into(),
    }
}

fn registry(journal: Arc<dyn FactJournal>, capacity: usize) -> InstanceRegistry {
    InstanceRegistry::new(journal, "persisted-host-a", capacity).unwrap()
}

fn backends() -> (tempfile::TempDir, Vec<Arc<dyn FactJournal>>) {
    let dir = tempfile::tempdir().unwrap();
    let journals: Vec<Arc<dyn FactJournal>> = vec![
        Arc::new(MemoryFactJournal::default()),
        Arc::new(SqliteFactJournal::open(dir.path().join("facts.db")).unwrap()),
    ];
    (dir, journals)
}

fn export(registry: &InstanceRegistry, label: &str) {
    let dir = tempfile::Builder::new()
        .prefix("kolyan-instance-registry-")
        .tempdir()
        .unwrap()
        .keep();
    let facts = registry.journal.read(&registry.stream, 0, 1024).unwrap();
    std::fs::write(
        dir.join("actual.json"),
        serde_json::to_vec_pretty(&json!({
            "case":label, "namespace":registry.host_namespace, "facts":facts,
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!(
        "registry observations: {}",
        dir.join("actual.json").display()
    );
}

#[test]
fn memory_and_sqlite_rebuild_retries_and_new_owners() {
    let (_dir, journals) = backends();
    for journal in journals {
        let first = registry(journal.clone(), 8);
        let original = first.reserve(owner("root")).unwrap();
        first.reserve(owner("later-child")).unwrap();
        drop(first);
        let rebuilt = registry(journal, 8);
        let retry = rebuilt.reserve(owner("root")).unwrap();
        let mut different = owner("root");
        different.logical_session_id = "other-session".into();
        let other_session = rebuilt.reserve(different).unwrap();
        let mut different = owner("root");
        different.task_id = "other-task".into();
        let other_task = rebuilt.reserve(different).unwrap();
        let child = rebuilt.reserve(owner("new-child")).unwrap();
        export(&rebuilt, "rebuild and permanent owners");
        assert_eq!(retry, original);
        for distinct in [other_session, other_task, child] {
            assert_ne!(distinct.instance_id, original.instance_id);
            assert_ne!(distinct.fact, original.fact);
        }
        assert_eq!(rebuilt.load().unwrap().owners.len(), 5);
    }
}

#[test]
fn sqlite_close_and_reopen_preserves_original_fact_reference() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reopen.db");
    let original = {
        let registry = registry(Arc::new(SqliteFactJournal::open(&path).unwrap()), 8);
        registry.reserve(owner("root")).unwrap()
    };
    let reopened = registry(Arc::new(SqliteFactJournal::open(&path).unwrap()), 8);
    let retry = reopened.reserve(owner("root")).unwrap();
    let child = reopened.reserve(owner("after-restart")).unwrap();
    export(&reopened, "sqlite reopened connection");
    assert_eq!(retry, original);
    assert_ne!(child.instance_id, original.instance_id);
    assert_eq!(child.fact.position, 2);
}

fn race(journals: Vec<Arc<dyn FactJournal>>, identical_owner: bool) {
    let barrier = Arc::new(Barrier::new(journals.len()));
    let verification = registry(journals[0].clone(), 32);
    let workers: Vec<_> = journals
        .into_iter()
        .enumerate()
        .map(|(index, journal)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let registry = registry(journal, 32);
                barrier.wait();
                registry.reserve(owner(&if identical_owner {
                    "same".into()
                } else {
                    format!("child-{index}")
                }))
            })
        })
        .collect();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().unwrap())
        .collect();
    export(&verification, "concurrent CAS reservations");
    let unique: std::collections::BTreeSet<_> =
        results.iter().map(|result| &result.instance_id).collect();
    assert_eq!(
        unique.len(),
        if identical_owner { 1 } else { results.len() }
    );
    if identical_owner {
        assert!(results.iter().all(|result| result == &results[0]));
    }
    assert_eq!(verification.load().unwrap().owners.len(), unique.len());
}

#[test]
fn memory_concurrent_same_and_different_owner_cas() {
    for identical_owner in [true, false] {
        let journal: Arc<dyn FactJournal> = Arc::new(MemoryFactJournal::default());
        race((0..12).map(|_| journal.clone()).collect(), identical_owner);
    }
}

#[test]
fn sqlite_independent_connections_concurrent_cas() {
    for identical_owner in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.db");
        let journals = (0..12)
            .map(|_| Arc::new(SqliteFactJournal::open(&path).unwrap()) as Arc<dyn FactJournal>)
            .collect();
        race(journals, identical_owner);
    }
}

#[test]
fn capacity_is_global_and_identical_retry_remains_valid_when_full() {
    let (_dir, journals) = backends();
    for journal in journals {
        let first = registry(journal.clone(), 1);
        let original = first.reserve(owner("root")).unwrap();
        let rebuilt = registry(journal, 1);
        let retry = rebuilt.reserve(owner("root")).unwrap();
        let refused = rebuilt.reserve(owner("child"));
        export(&rebuilt, "capacity and retry");
        assert_eq!(retry, original);
        assert!(matches!(refused, Err(InstanceRegistryError::Capacity)));
        assert_eq!(rebuilt.load().unwrap().head, 1);
    }
}

#[test]
fn namespace_and_owner_validation_cannot_accept_model_instance_selection() {
    let journal: Arc<dyn FactJournal> = Arc::new(MemoryFactJournal::default());
    for invalid in ["", "  ", "host\n", &"x".repeat(257)] {
        assert!(matches!(
            InstanceRegistry::new(journal.clone(), invalid, 8),
            Err(InstanceRegistryError::Invalid(_))
        ));
    }
    for capacity in [0, MAX_RESERVATIONS + 1] {
        assert!(matches!(
            InstanceRegistry::new(journal.clone(), "host", capacity),
            Err(InstanceRegistryError::Invalid(_))
        ));
    }
    let registry = registry(journal.clone(), 8);
    let mut invalid = owner("root");
    invalid.task_id = "\0".into();
    assert!(matches!(
        registry.reserve(invalid),
        Err(InstanceRegistryError::Invalid(_))
    ));
    assert!(serde_json::from_value::<InstanceOwner>(json!({
        "logical_session_id":"s", "task_id":"t", "invocation_id":"i", "instance_id":"model-selected"
    })).is_err());
    let first = registry.reserve(owner("root")).unwrap();
    let other_host = InstanceRegistry::new(journal, "persisted-host-b", 8).unwrap();
    assert_ne!(
        other_host.reserve(owner("root")).unwrap().instance_id,
        first.instance_id
    );
}

fn next_draft(registry: &InstanceRegistry, owner: InstanceOwner) -> FactDraft {
    let instance_id = registry.instance_id(2);
    FactDraft {
        fact_id: registry.fact_id(&owner).unwrap(),
        subject: FactSubject {
            kind: "agent.instance".into(),
            id: instance_id.clone(),
        },
        kind: KIND.into(),
        schema_version: 1,
        critical: true,
        causes: vec![],
        payload: serde_json::to_value(Reserved {
            host_namespace: registry.host_namespace.clone(),
            owner,
            instance_id,
        })
        .unwrap(),
    }
}

#[test]
fn memory_and_sqlite_unknown_schema_corruption_and_conflicting_owner_fail_closed() {
    for case in [
        "version",
        "payload",
        "namespace",
        "instance",
        "subject",
        "kind",
        "critical",
        "extra",
        "conflicting-owner",
    ] {
        let (_dir, journals) = backends();
        for journal in journals {
            let registry = registry(journal, 8);
            let original = registry.reserve(owner("root")).unwrap();
            let mut draft = next_draft(&registry, owner("child"));
            match case {
                "version" => draft.schema_version = 99,
                "payload" => draft.payload = json!("not a reservation"),
                "namespace" => draft.payload["host_namespace"] = json!("other-host"),
                "instance" => draft.payload["instance_id"] = json!("model-chosen"),
                "subject" => draft.subject.id = "wrong-instance".into(),
                "kind" => draft.kind = "unknown.reservation".into(),
                "critical" => draft.critical = false,
                "extra" => draft.payload["extra"] = json!(true),
                "conflicting-owner" => {
                    draft.payload["instance_id"] = json!(original.instance_id);
                    draft.subject.id = original.instance_id;
                }
                _ => unreachable!(),
            }
            registry
                .journal
                .append(&registry.stream, 1, vec![draft])
                .unwrap();
            export(&registry, case);
            // Validate the entire stream even when the requested owner was
            // already found; later corrupt facts cannot be silently ignored.
            let error = registry.reserve(owner("root")).unwrap_err();
            match case {
                "version" => assert!(matches!(error, InstanceRegistryError::UnknownSchema(99))),
                "conflicting-owner" => {
                    assert!(matches!(error, InstanceRegistryError::ConflictingOwner(_)))
                }
                _ => assert!(
                    matches!(error, InstanceRegistryError::Corrupt(_)),
                    "{case}: {error:?}"
                ),
            }
            assert_eq!(
                registry.journal.read(&registry.stream, 0, 8).unwrap().len(),
                2
            );
        }
    }
}

struct Contended {
    inner: Arc<MemoryFactJournal>,
    calls: AtomicUsize,
}

impl FactJournal for Contended {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.inner.read(stream, after, limit)
    }
    fn append(&self, _: &str, _: u64, _: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        registry(self.inner.clone(), 64)
            .reserve(owner(&format!("competitor-{index}")))
            .unwrap();
        Err(FactError::Conflict("occupied CAS batch".into()))
    }
}

#[test]
fn cas_retry_has_a_fixed_bound_under_real_journal_progress() {
    let journal = Arc::new(Contended {
        inner: Arc::new(MemoryFactJournal::default()),
        calls: AtomicUsize::new(0),
    });
    let registry = registry(journal.clone(), 64);
    let result = registry.reserve(owner("starved"));
    export(&registry, "bounded CAS contention");
    assert!(matches!(result, Err(InstanceRegistryError::Contention)));
    assert_eq!(journal.calls.load(Ordering::SeqCst), MAX_CAS_ATTEMPTS);
    assert_eq!(registry.load().unwrap().owners.len(), MAX_CAS_ATTEMPTS);
}

struct RefusesPersistence {
    inner: MemoryFactJournal,
    error: FactError,
    calls: AtomicUsize,
}

impl FactJournal for RefusesPersistence {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.inner.read(stream, after, limit)
    }
    fn append(&self, _: &str, _: u64, _: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(self.error.clone())
    }
}

#[test]
fn persistence_failure_or_identity_conflict_without_progress_never_allocates() {
    for error in [
        FactError::Storage("disk unavailable".into()),
        FactError::Conflict("foreign fact identity".into()),
    ] {
        let journal = Arc::new(RefusesPersistence {
            inner: MemoryFactJournal::default(),
            error: error.clone(),
            calls: AtomicUsize::new(0),
        });
        let registry = registry(journal.clone(), 8);
        assert!(
            matches!(registry.reserve(owner("root")), Err(InstanceRegistryError::Journal(actual)) if actual == error)
        );
        assert_eq!(journal.calls.load(Ordering::SeqCst), 1);
        assert!(registry.load().unwrap().owners.is_empty());
    }
}

#[test]
fn bounded_replay_pages_return_exact_original_proof() {
    let (_dir, journals) = backends();
    for journal in journals {
        let registry = registry(journal, PAGE_SIZE + 1);
        let original = registry.reserve(owner("root")).unwrap();
        // Populate through real journal CAS with exactly the canonical schema.
        for index in 1..=PAGE_SIZE {
            let mut draft = next_draft(&registry, owner(&format!("child-{index}")));
            let instance = registry.instance_id(index as u64 + 1);
            draft.subject.id = instance.clone();
            draft.payload["instance_id"] = json!(instance);
            registry
                .journal
                .append(&registry.stream, index as u64, vec![draft])
                .unwrap();
        }
        export(&registry, "multi-page bounded replay");
        assert_eq!(registry.reserve(owner("root")).unwrap(), original);
        assert!(matches!(
            registry.reserve(owner("over-capacity")),
            Err(InstanceRegistryError::Capacity)
        ));
    }
}
