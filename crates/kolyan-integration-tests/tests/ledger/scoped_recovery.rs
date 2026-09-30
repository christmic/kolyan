//! SQLite reopen and Session projection repair with global auditing disabled.

use std::{
    fs,
    sync::{Arc, Mutex},
};

use kolyan_ledger::{
    LedgerError, LedgerEvent, LedgerEventKind, LedgerQuery, LedgerStore, SqliteLedger,
};
use kolyan_server::{ExecutionService, SessionExecutionService, SessionService};
use kolyan_storage::{FileSessionStore, SessionStore, SessionTurn, SessionTurnStatus};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};

#[derive(Clone)]
struct ScopedOnly {
    inner: SqliteLedger,
    reads: Arc<Mutex<Vec<LedgerQuery>>>,
}

impl LedgerStore for ScopedOnly {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.inner.append(event)
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.inner.append_unless_cancelled(event)
    }
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        assert!(query.execution_id.is_some() || query.event_id.is_some());
        self.reads.lock().unwrap().push(query.clone());
        self.inner.query(query)
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        Err(LedgerError::Storage(
            "global audit is forbidden in this scenario".into(),
        ))
    }
    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.inner.claim(key)
    }
}

#[test]
fn scoped_sqlite_recovery_matches_data_without_touching_neighbor() {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/ledger_scoped_recovery.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-ledger-recovery-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("Ledger recovery evidence: {}", root.display());
    for case in fixture["cases"].as_array().unwrap() {
        let directory = root.join(case["name"].as_str().unwrap());
        fs::create_dir_all(&directory).unwrap();
        let store = FileSessionStore::new(directory.join("sessions")).unwrap();
        let ledger_path = directory.join("ledger.sqlite");
        let ledger = SqliteLedger::open(&ledger_path).unwrap();
        for (name, kind) in [("target", "target_kind"), ("neighbor", "neighbor_kind")] {
            let id = case[name].as_str().unwrap();
            store.create(id).unwrap();
            store
                .begin_turn_with_input(
                    id,
                    SessionTurn {
                        turn_id: format!("turn-{id}"),
                        execution_id: id.into(),
                        status: SessionTurnStatus::Running,
                    },
                    0,
                    vec![],
                )
                .unwrap();
            ledger
                .append(LedgerEvent {
                    event_id: format!("{id}/terminal"),
                    turn_id: format!("turn-{id}"),
                    execution_id: id.into(),
                    cursor: 0,
                    kind: serde_json::from_value(case[kind].clone()).unwrap(),
                    idempotency_key: format!("{id}/terminal"),
                    payload: json!({"reason":case[kind]}),
                })
                .unwrap();
        }
        drop(ledger);
        let target = case["target"].as_str().unwrap();
        let neighbor = case["neighbor"].as_str().unwrap();
        let neighbor_before = store.load(neighbor).unwrap();
        let ledger = ScopedOnly {
            inner: SqliteLedger::open(&ledger_path).unwrap(),
            reads: Arc::default(),
        };
        let service = SessionExecutionService::new(
            ExecutionService::new(ledger.clone(), NoopTraceSink),
            SessionService::new(store.clone()),
        );
        let result = service.load_reconciled(target).unwrap();
        assert_eq!(
            serde_json::to_value(result.turns[0].status).unwrap(),
            case["expected_target"]
        );
        assert_eq!(store.load(neighbor).unwrap(), neighbor_before);
        assert_eq!(service.load_reconciled(target).unwrap(), result);
        let events = ledger.execution_events_after(target, 0).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == LedgerEventKind::SessionCommitted)
                .count() as u64,
            fixture["expected_repair_commits"].as_u64().unwrap()
        );
        let reads = ledger.reads.lock().unwrap();
        assert!(reads.iter().all(|query| {
            query.execution_id.as_deref() == Some(target)
                || query
                    .event_id
                    .as_deref()
                    .is_some_and(|id| id.starts_with(&format!("{target}/")))
        }));
        assert!(
            reads
                .iter()
                .filter(|query| query.execution_id.is_some())
                .all(
                    |query| query.limit as u64 == fixture["expected_query_limit"].as_u64().unwrap()
                )
        );
        let rows = events
            .iter()
            .map(|event| serde_json::to_string(event).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(directory.join("actual-ledger.jsonl"), format!("{rows}\n")).unwrap();
        fs::write(
            directory.join("result.json"),
            serde_json::to_vec_pretty(&result).unwrap(),
        )
        .unwrap();
    }
}
