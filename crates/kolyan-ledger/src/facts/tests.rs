use super::*;

fn draft(id: &str) -> FactDraft {
    FactDraft {
        fact_id: id.into(),
        subject: FactSubject {
            kind: "agent.invocation".into(),
            id: "invocation-1".into(),
        },
        kind: "future.observation".into(),
        schema_version: 7,
        critical: true,
        causes: vec![],
        payload: serde_json::json!({"unknown": true}),
    }
}

fn reference(stream: &str, position: u64, id: &str) -> FactRef {
    FactRef {
        stream_id: stream.into(),
        position,
        fact_id: id.into(),
    }
}

fn stores() -> (tempfile::TempDir, Vec<Box<dyn FactJournal>>) {
    let dir = tempfile::tempdir().unwrap();
    let stores: Vec<Box<dyn FactJournal>> = vec![
        Box::new(MemoryFactJournal::default()),
        Box::new(SqliteFactJournal::open(dir.path().join("facts.db")).unwrap()),
    ];
    (dir, stores)
}

#[test]
fn parity_exact_retry_after_head_advance_and_partial_conflict() {
    let (_dir, stores) = stores();
    for store in stores {
        let mut second = draft("two");
        second.causes.push(reference("a", 1, "one"));
        let batch = vec![draft("one"), second];
        let original = store.append("a", 0, batch.clone()).unwrap();
        store.append("a", 2, vec![draft("three")]).unwrap();
        assert_eq!(store.append("a", 0, batch.clone()).unwrap(), original);
        assert!(matches!(
            store.append("a", 0, vec![batch[0].clone()]),
            Err(FactError::Conflict(_))
        ));
        assert!(matches!(
            store.append("a", 1, vec![batch[1].clone()]),
            Err(FactError::Conflict(_))
        ));
        let mut changed = batch;
        changed[0].critical = false;
        assert!(matches!(
            store.append("a", 0, changed),
            Err(FactError::Conflict(_))
        ));
        assert_eq!(store.read("a", 1, 1).unwrap(), vec![original[1].clone()]);
        assert_eq!(store.read("missing", 0, 10).unwrap(), vec![]);
        assert!(store.read("a", u64::MAX, 10).unwrap().is_empty());
    }
}

#[test]
fn causal_identity_atomic_failure_and_cross_stream_parity() {
    let (_dir, stores) = stores();
    for store in stores {
        store.append("a", 0, vec![draft("one")]).unwrap();
        let mut dependent = draft("dependent");
        dependent.causes.push(reference("a", 1, "one"));
        store.append("b", 0, vec![dependent]).unwrap();
        for cause in [
            reference("a", 2, "one"),
            reference("b", 1, "one"),
            reference("a", 1, "absent"),
            reference("c", 2, "later"),
        ] {
            let mut bad = draft("bad");
            bad.causes.push(cause);
            assert!(matches!(
                store.append("c", 0, vec![draft("first"), bad, draft("later")]),
                Err(FactError::MissingCause(_))
            ));
            assert!(store.read("c", 0, 10).unwrap().is_empty());
        }
        store.append("c", 0, vec![draft("first")]).unwrap();
        assert!(matches!(
            store.append("c", 0, vec![draft("stale")]),
            Err(FactError::Conflict(_))
        ));
        assert!(matches!(
            store.append("empty", 1, vec![draft("stale")]),
            Err(FactError::StalePosition {
                expected: 1,
                actual: 0
            })
        ));
        assert!(store.read("empty", 0, 10).unwrap().is_empty());
    }
}

#[test]
fn native_bounds_and_unknown_semantics() {
    let (_dir, stores) = stores();
    for store in stores {
        for limit in [0, 1025] {
            assert!(store.read("a", 0, limit).is_err());
        }
        assert!(store.read(" ", 0, 1).is_err());
        assert!(store.append("a", 0, vec![]).is_err());
        assert!(
            store
                .append("a", u64::MAX, vec![draft("overflow")])
                .is_err()
        );
        let mut invalid = Vec::new();
        let mut d = draft("bad");
        d.kind = "undotted".into();
        invalid.push(d);
        let mut d = draft("bad");
        d.subject.kind = "agent..invocation".into();
        invalid.push(d);
        let mut d = draft("bad");
        d.subject.id.clear();
        invalid.push(d);
        let mut d = draft("bad");
        d.fact_id = "x".repeat(257);
        invalid.push(d);
        let mut d = draft("bad");
        d.schema_version = 0;
        invalid.push(d);
        let mut d = draft("bad");
        d.payload = Value::String("x".repeat(MAX_FACT_PAYLOAD_BYTES));
        invalid.push(d);
        let mut d = draft("bad");
        d.causes = vec![reference("a", 1, "one"); 33];
        invalid.push(d);
        let mut d = draft("bad");
        d.causes.push(reference("a", 0, "one"));
        invalid.push(d);
        for d in invalid {
            assert!(store.append("a", 0, vec![d]).is_err());
        }
        assert!(
            store
                .append("a", 0, (0..65).map(|i| draft(&i.to_string())).collect())
                .is_err()
        );
        assert!(
            store
                .append("a", 0, vec![draft("duplicate"), draft("duplicate")])
                .is_err()
        );
        assert!(store.read("a", 0, 10).unwrap().is_empty());
        assert_eq!(
            store.append("a", 0, vec![draft("unknown")]).unwrap()[0]
                .draft
                .schema_version,
            7
        );
        let mut allowed = draft("large");
        allowed.payload = Value::String("x".repeat(MAX_FACT_PAYLOAD_BYTES - 2));
        store.append("a", 1, vec![allowed]).unwrap();
    }
}

#[test]
fn sqlite_reopen_preserves_batch_identity_and_shares_execution_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.db");
    let ledger = crate::SqliteLedger::open(&path).unwrap();
    drop(ledger);
    let original = SqliteFactJournal::open(&path)
        .unwrap()
        .append("a", 0, vec![draft("one"), draft("two")])
        .unwrap();
    let reopened = SqliteFactJournal::open(&path).unwrap();
    assert_eq!(reopened.read("a", 0, 10).unwrap(), original);
    assert_eq!(
        reopened
            .append("a", 0, vec![draft("one"), draft("two")])
            .unwrap(),
        original
    );
    assert!(reopened.append("a", 0, vec![draft("one")]).is_err());
    let connection = ConnectionForTests::open(path).unwrap();
    assert_eq!(
        connection
            .pragma_query_value(None, "journal_mode", |r| r.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
}

type ConnectionForTests = rusqlite::Connection;

#[test]
fn sqlite_insertion_failure_rolls_back_records_and_batch_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollback.db");
    let store = SqliteFactJournal::open(&path).unwrap();
    let connection = ConnectionForTests::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_second BEFORE INSERT ON fact_records WHEN NEW.fact_id='two' BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    let batch = vec![draft("one"), draft("two")];
    assert!(matches!(
        store.append("a", 0, batch.clone()),
        Err(FactError::Storage(_))
    ));
    assert!(store.read("a", 0, 10).unwrap().is_empty());
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM fact_batches", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    connection
        .execute_batch("DROP TRIGGER reject_second;")
        .unwrap();
    assert_eq!(store.append("a", 0, batch).unwrap().len(), 2);
}

#[test]
fn identical_concurrent_retry_returns_one_original_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("retry.db");
    let stores = [
        SqliteFactJournal::open(&path).unwrap(),
        SqliteFactJournal::open(&path).unwrap(),
    ];
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = stores
        .into_iter()
        .map(|store| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .append("a", 0, vec![draft("one"), draft("two")])
                    .unwrap()
            })
        })
        .collect();
    let outcomes: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(
        SqliteFactJournal::open(&path)
            .unwrap()
            .read("a", 0, 10)
            .unwrap(),
        outcomes[0]
    );
}

#[test]
fn self_and_forward_causation_and_cross_stream_identity_collision_fail() {
    let (_dir, stores) = stores();
    for store in stores {
        let mut self_ref = draft("self");
        self_ref.causes.push(reference("a", 1, "self"));
        assert!(matches!(
            store.append("a", 0, vec![self_ref]),
            Err(FactError::MissingCause(_))
        ));
        let mut first = draft("first");
        first.causes.push(reference("a", 2, "second"));
        assert!(matches!(
            store.append("a", 0, vec![first, draft("second")]),
            Err(FactError::MissingCause(_))
        ));
        store.append("a", 0, vec![draft("one")]).unwrap();
        assert!(matches!(
            store.append("b", 0, vec![draft("one")]),
            Err(FactError::Conflict(_))
        ));
        assert!(store.read("b", 0, 10).unwrap().is_empty());
    }
}

#[test]
fn sqlite_read_uses_stream_position_index_and_corruption_is_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("indexed.db");
    let store = SqliteFactJournal::open(&path).unwrap();
    store.append("a", 0, vec![draft("one")]).unwrap();
    let connection = ConnectionForTests::open(path).unwrap();
    let detail: String = connection.query_row("EXPLAIN QUERY PLAN SELECT record_json FROM fact_records WHERE stream_id='a' AND position>0 ORDER BY position LIMIT 1", [], |r| r.get(3)).unwrap();
    assert!(detail.contains("INDEX"), "{detail}");
    assert!(detail.contains("stream_id=? AND position>?"), "{detail}");
    connection
        .execute(
            "UPDATE fact_records SET record_json='broken' WHERE fact_id='one'",
            [],
        )
        .unwrap();
    assert!(matches!(store.read("a", 0, 10), Err(FactError::Storage(_))));
}

#[test]
fn competing_writers_commit_one_complete_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.db");
    let sqlite_a = SqliteFactJournal::open(&path).unwrap();
    let sqlite_b = SqliteFactJournal::open(&path).unwrap();
    race(Arc::new(sqlite_a), Arc::new(sqlite_b));
    let memory = MemoryFactJournal::default();
    race(Arc::new(memory.clone()), Arc::new(memory));
}

#[test]
fn exact_batch_cause_and_identity_bounds_are_accepted() {
    let (_dir, stores) = stores();
    for store in stores {
        store
            .append("source", 0, vec![draft("source-fact")])
            .unwrap();
        let batch: Vec<_> = (0..MAX_FACT_BATCH)
            .map(|index| {
                let mut d = draft(&format!("{index:03}{}", "x".repeat(MAX_FACT_ID_BYTES - 3)));
                d.subject.id = "s".repeat(MAX_FACT_ID_BYTES);
                d.causes = vec![reference("source", 1, "source-fact"); MAX_FACT_CAUSES];
                d
            })
            .collect();
        let committed = store
            .append(&"s".repeat(MAX_FACT_ID_BYTES), 0, batch)
            .unwrap();
        assert_eq!(committed.len(), MAX_FACT_BATCH);
        let encoded = serde_json::to_string(&committed).unwrap();
        assert_eq!(
            serde_json::from_str::<Vec<FactRecord>>(&encoded).unwrap(),
            committed
        );
    }
}

fn race(a: Arc<dyn FactJournal>, b: Arc<dyn FactJournal>) {
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = [a.clone(), b]
        .into_iter()
        .enumerate()
        .map(|(index, store)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.append(
                    "race",
                    0,
                    vec![draft(&format!("{index}-a")), draft(&format!("{index}-b"))],
                )
            })
        })
        .collect();
    let outcomes: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(outcomes.iter().filter(|r| r.is_err()).count(), 1);
    let records = a.read("race", 0, 10).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].position, 1);
    assert_eq!(records[1].position, 2);
    assert_eq!(
        records[0].draft.fact_id.chars().next(),
        records[1].draft.fact_id.chars().next()
    );
}
