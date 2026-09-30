use std::sync::Mutex;

use serde_json::json;
use tempfile::TempDir;

use super::*;
use crate::{InMemoryLedger, LedgerEventKind, LedgerStore};

fn event(number: u64, execution: &str) -> LedgerEvent {
    LedgerEvent {
        event_id: format!("event-{number}"),
        turn_id: format!("turn-{execution}"),
        execution_id: execution.into(),
        cursor: 0,
        kind: LedgerEventKind::StepStarted,
        idempotency_key: format!("key-{number}"),
        payload: json!({"number": number}),
    }
}

fn query() -> LedgerQuery {
    LedgerQuery {
        execution_id: None,
        event_id: None,
        after: 0,
        through: None,
        limit: 1024,
    }
}

fn cursors(store: &dyn LedgerStore, query: &LedgerQuery) -> Vec<u64> {
    store
        .query(query)
        .unwrap()
        .iter()
        .map(|event| event.cursor)
        .collect()
}

fn assert_query_contract(store: &dyn LedgerStore) {
    assert!(store.query(&query()).unwrap().is_empty());
    for number in 1..=8 {
        assert_eq!(
            store
                .append(event(
                    number,
                    if number % 2 == 0 { "target" } else { "other" }
                ))
                .unwrap()
                .cursor,
            number
        );
    }
    let cases = [
        (query(), vec![1, 2, 3, 4, 5, 6, 7, 8]),
        (
            LedgerQuery {
                execution_id: Some("target".into()),
                limit: 2,
                ..query()
            },
            vec![2, 4],
        ),
        (
            LedgerQuery {
                after: 2,
                through: Some(6),
                execution_id: Some("target".into()),
                ..query()
            },
            vec![4, 6],
        ),
        (
            LedgerQuery {
                after: 2,
                through: Some(2),
                ..query()
            },
            vec![],
        ),
        (
            LedgerQuery {
                after: 8,
                ..query()
            },
            vec![],
        ),
        (
            LedgerQuery {
                event_id: Some("event-4".into()),
                ..query()
            },
            vec![4],
        ),
        (
            LedgerQuery {
                execution_id: Some("target".into()),
                event_id: Some("event-4".into()),
                ..query()
            },
            vec![4],
        ),
        (
            LedgerQuery {
                execution_id: Some("other".into()),
                event_id: Some("event-4".into()),
                ..query()
            },
            vec![],
        ),
        (
            LedgerQuery {
                after: 4,
                event_id: Some("event-4".into()),
                ..query()
            },
            vec![],
        ),
        (
            LedgerQuery {
                through: Some(3),
                event_id: Some("event-4".into()),
                ..query()
            },
            vec![],
        ),
        (
            LedgerQuery {
                execution_id: Some("missing".into()),
                ..query()
            },
            vec![],
        ),
        (
            LedgerQuery {
                event_id: Some("missing".into()),
                ..query()
            },
            vec![],
        ),
        (
            LedgerQuery {
                limit: 1,
                ..query()
            },
            vec![1],
        ),
        (
            LedgerQuery {
                after: u64::MAX,
                ..query()
            },
            vec![],
        ),
        (
            LedgerQuery {
                through: Some(u64::MAX),
                ..query()
            },
            vec![1, 2, 3, 4, 5, 6, 7, 8],
        ),
    ];
    for (query, expected) in cases {
        assert_eq!(cursors(store, &query), expected, "{query:?}");
    }
    for invalid in [
        LedgerQuery {
            limit: 0,
            ..query()
        },
        LedgerQuery {
            limit: 1025,
            ..query()
        },
        LedgerQuery {
            limit: usize::MAX,
            ..query()
        },
        LedgerQuery {
            execution_id: Some(String::new()),
            ..query()
        },
        LedgerQuery {
            event_id: Some(String::new()),
            ..query()
        },
        LedgerQuery {
            after: 3,
            through: Some(2),
            ..query()
        },
    ] {
        assert!(
            matches!(store.query(&invalid), Err(LedgerError::InvalidQuery(_))),
            "{invalid:?}"
        );
    }
    assert!(matches!(
        store.event_by_id(""),
        Err(LedgerError::InvalidQuery(_))
    ));
    assert!(matches!(
        store.execution_events_after("", 0),
        Err(LedgerError::InvalidQuery(_))
    ));
    assert_eq!(
        store.event_by_id("event-4").unwrap(),
        Some(LedgerEvent {
            cursor: 4,
            ..event(4, "target")
        })
    );
    assert_eq!(store.event_by_id("missing").unwrap(), None);
    assert!(
        store
            .execution_events_after("missing", 0)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .execution_events_after("target", 2)
            .unwrap()
            .iter()
            .map(|event| event.cursor)
            .collect::<Vec<_>>(),
        vec![4, 6, 8]
    );
    // Reads neither consume claims nor advance the durable cursor.
    assert!(store.claim("read-only-check").unwrap());
    assert_eq!(store.append(event(9, "target")).unwrap().cursor, 9);
    assert_eq!(store.events_after(0).unwrap().len(), 9);
}

#[test]
fn memory_query_contract() {
    assert_query_contract(&InMemoryLedger::default());
}

#[test]
fn file_query_contract() {
    let temp = TempDir::new().unwrap();
    assert_query_contract(&FileLedger::open(temp.path().join("events.jsonl")).unwrap());
}

#[test]
fn fresh_file_query_creates_coordination_lock_and_returns_empty() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("events.jsonl");
    let lock_path = temp.path().join("events.jsonl.lock");
    let ledger = FileLedger::open(&path).unwrap();
    assert!(!lock_path.exists());
    assert!(ledger.query(&query()).unwrap().is_empty());
    assert!(lock_path.exists());
    assert!(std::fs::read(&path).unwrap().is_empty());
    assert!(
        ledger
            .execution_events_after("target", 0)
            .unwrap()
            .is_empty()
    );
    assert_eq!(ledger.event_by_id("missing").unwrap(), None);
}

#[test]
fn sqlite_query_contract() {
    let temp = TempDir::new().unwrap();
    assert_query_contract(&SqliteLedger::open(temp.path().join("events.sqlite")).unwrap());
}

fn assert_pagination(store: &dyn LedgerStore) {
    for number in 1..=2050 {
        store
            .append(event(
                number,
                if number % 2 == 0 { "target" } else { "other" },
            ))
            .unwrap();
    }
    let events = store.execution_events_after("target", 0).unwrap();
    assert_eq!(events.len(), 1025);
    assert_eq!(
        events.iter().map(|event| event.cursor).collect::<Vec<_>>(),
        (2..=2050).step_by(2).collect::<Vec<_>>()
    );
    assert_eq!(
        store.execution_events_after("target", 1026).unwrap().len(),
        512
    );
    let mut frozen = LedgerQuery {
        execution_id: Some("target".into()),
        through: Some(2050),
        limit: 512,
        ..query()
    };
    let mut observed = Vec::new();
    loop {
        let page = store.query(&frozen).unwrap();
        assert!(page.len() <= 512);
        if page.is_empty() {
            break;
        }
        frozen.after = page.last().unwrap().cursor;
        observed.extend(page);
        if observed.len() == 512 {
            store.append(event(2051, "target")).unwrap();
        }
    }
    assert_eq!(observed, events);
    assert_eq!(
        store.execution_events_after("target", 2050).unwrap()[0].cursor,
        2051
    );
}

#[test]
fn memory_pagination_interleaving_and_frozen_range() {
    assert_pagination(&InMemoryLedger::default());
}

#[test]
fn file_pagination_interleaving_and_frozen_range() {
    let temp = TempDir::new().unwrap();
    assert_pagination(&FileLedger::open(temp.path().join("events.jsonl")).unwrap());
}

#[test]
fn sqlite_pagination_interleaving_and_frozen_range() {
    let temp = TempDir::new().unwrap();
    assert_pagination(&SqliteLedger::open(temp.path().join("events.sqlite")).unwrap());
}

#[test]
fn file_and_sqlite_queries_survive_reopen_without_mutating_facts() {
    let temp = TempDir::new().unwrap();
    let file_path = temp.path().join("events.jsonl");
    let sqlite_path = temp.path().join("events.sqlite");
    {
        let stores: Vec<Box<dyn LedgerStore>> = vec![
            Box::new(FileLedger::open(&file_path).unwrap()),
            Box::new(SqliteLedger::open(&sqlite_path).unwrap()),
        ];
        for store in stores {
            store.append(event(1, "other")).unwrap();
            store.append(event(2, "target")).unwrap();
        }
    }
    let bytes = std::fs::read(&file_path).unwrap();
    let stores: Vec<Box<dyn LedgerStore>> = vec![
        Box::new(FileLedger::open(&file_path).unwrap()),
        Box::new(SqliteLedger::open(&sqlite_path).unwrap()),
    ];
    for store in stores {
        assert_eq!(
            store.execution_events_after("target", 0).unwrap(),
            vec![LedgerEvent {
                cursor: 2,
                ..event(2, "target")
            }]
        );
        assert_eq!(store.event_by_id("event-2").unwrap().unwrap().cursor, 2);
    }
    assert_eq!(std::fs::read(&file_path).unwrap(), bytes);
}

#[test]
fn file_query_reports_io_and_decode_errors() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("events.jsonl");
    let ledger = FileLedger::open(&path).unwrap();
    ledger.append(event(1, "target")).unwrap();
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    std::io::Write::write_all(&mut file, b"not-json\n").unwrap();
    assert_eq!(
        cursors(
            &ledger,
            &LedgerQuery {
                limit: 1,
                ..query()
            }
        ),
        vec![1]
    );
    assert!(matches!(
        ledger.query(&query()),
        Err(LedgerError::Storage(_))
    ));
    assert!(matches!(
        ledger.execution_events_after("missing", 0),
        Err(LedgerError::Storage(_))
    ));
    std::fs::remove_file(path).unwrap();
    assert!(matches!(
        ledger.event_by_id("event-1"),
        Err(LedgerError::Storage(_))
    ));
    assert!(matches!(
        ledger.query(&LedgerQuery {
            limit: 0,
            ..query()
        }),
        Err(LedgerError::InvalidQuery(_))
    ));
}

#[test]
fn sqlite_query_reports_storage_and_decode_errors() {
    let temp = TempDir::new().unwrap();
    let ledger = SqliteLedger::open(temp.path().join("events.sqlite")).unwrap();
    ledger.append(event(1, "target")).unwrap();
    ledger
        .connection
        .lock()
        .unwrap()
        .execute("UPDATE events SET payload = 'invalid'", [])
        .unwrap();
    assert!(matches!(
        ledger.event_by_id("event-1"),
        Err(LedgerError::Storage(_))
    ));
    ledger
        .connection
        .lock()
        .unwrap()
        .execute("DROP TABLE events", [])
        .unwrap();
    assert!(matches!(
        ledger.query(&query()),
        Err(LedgerError::Storage(_))
    ));
    assert!(matches!(
        ledger.query(&LedgerQuery {
            limit: 0,
            ..query()
        }),
        Err(LedgerError::InvalidQuery(_))
    ));
}

#[test]
fn sqlite_explain_uses_execution_cursor_and_unique_event_indexes() {
    let temp = TempDir::new().unwrap();
    let ledger = SqliteLedger::open(temp.path().join("events.sqlite")).unwrap();
    let connection = ledger.connection.lock().unwrap();
    for (name, query, expected) in [
        (
            "execution",
            LedgerQuery {
                execution_id: Some("target".into()),
                after: 2,
                through: Some(100),
                limit: 512,
                ..query()
            },
            "events_execution_cursor",
        ),
        (
            "event",
            LedgerQuery {
                event_id: Some("event-4".into()),
                limit: 1,
                ..query()
            },
            "sqlite_autoindex_events_1",
        ),
        (
            "both",
            LedgerQuery {
                execution_id: Some("target".into()),
                event_id: Some("event-4".into()),
                ..query()
            },
            "sqlite_autoindex_events_1",
        ),
    ] {
        let (sql, values) = query_sql(&query);
        let details: Vec<String> = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map(params_from_iter(values), |row| row.get(3))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        println!("{name}: {details:?}");
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("SEARCH events USING INDEX")
                    && detail.contains(expected)),
            "{details:?}"
        );
        assert!(
            !details
                .iter()
                .any(|detail| detail.contains("SCAN events") || detail.contains("TEMP B-TREE")),
            "{details:?}"
        );
    }
}

struct QueryOnly {
    inner: InMemoryLedger,
    calls: Mutex<Vec<LedgerQuery>>,
    fail_at: Option<usize>,
}

struct MalformedPage(Vec<LedgerEvent>);

impl LedgerStore for MalformedPage {
    fn query(&self, _: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        Ok(self.0.clone())
    }
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        panic!("helpers must only query")
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        panic!("audit fallback is forbidden")
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        panic!("helpers must only query")
    }
}

#[test]
fn helpers_reject_backend_scope_order_limit_and_nonprogress_violations() {
    let first = LedgerEvent {
        cursor: 1,
        ..event(1, "target")
    };
    let second = LedgerEvent {
        cursor: 2,
        ..event(2, "target")
    };
    for page in [
        vec![event(1, "target")],
        vec![LedgerEvent {
            cursor: 1,
            ..event(1, "other")
        }],
        vec![second.clone(), first.clone()],
        vec![first.clone(), first.clone()],
        (1..=513)
            .map(|number| LedgerEvent {
                cursor: number,
                ..event(number, "target")
            })
            .collect(),
    ] {
        assert!(matches!(
            MalformedPage(page).execution_events_after("target", 0),
            Err(LedgerError::Storage(_))
        ));
    }
    assert!(matches!(
        MalformedPage(vec![first.clone()]).execution_events_after("target", 1),
        Err(LedgerError::Storage(_))
    ));
    assert!(matches!(
        MalformedPage(vec![first.clone()]).event_by_id("wrong-id"),
        Err(LedgerError::Storage(_))
    ));
    assert!(matches!(
        MalformedPage(vec![first, second]).event_by_id("event-1"),
        Err(LedgerError::Storage(_))
    ));
    assert!(matches!(
        MalformedPage(Vec::new()).execution_events_after("", 0),
        Err(LedgerError::InvalidQuery(_))
    ));
}

impl LedgerStore for QueryOnly {
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push(query.clone());
        if self.fail_at == Some(calls.len()) {
            return Err(LedgerError::Storage("injected query failure".into()));
        }
        self.inner.query(query)
    }
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        panic!("helpers must only query")
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        panic!("audit fallback is forbidden")
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        panic!("helpers must only query")
    }
}

#[test]
fn helpers_use_query_port_with_512_pages_and_propagate_later_errors() {
    let store = QueryOnly {
        inner: InMemoryLedger::default(),
        calls: Mutex::new(Vec::new()),
        fail_at: None,
    };
    for number in 1..=1024 {
        store.inner.append(event(number, "target")).unwrap();
    }
    assert_eq!(
        store.execution_events_after("target", 0).unwrap().len(),
        1024
    );
    assert_eq!(
        store
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|q| (q.after, q.limit))
            .collect::<Vec<_>>(),
        vec![(0, 512), (512, 512), (1024, 512)]
    );
    assert_eq!(store.event_by_id("event-17").unwrap().unwrap().cursor, 17);
    let exact = store.calls.lock().unwrap().last().unwrap().clone();
    assert_eq!(exact.event_id.as_deref(), Some("event-17"));
    assert_eq!(exact.limit, 1);
    let failing = QueryOnly {
        fail_at: Some(2),
        calls: Mutex::new(Vec::new()),
        inner: store.inner.clone(),
    };
    assert!(matches!(
        failing.execution_events_after("target", 0),
        Err(LedgerError::Storage(_))
    ));
    let failing = QueryOnly {
        fail_at: Some(1),
        calls: Mutex::new(Vec::new()),
        inner: store.inner,
    };
    assert!(matches!(
        failing.event_by_id("event-1"),
        Err(LedgerError::Storage(_))
    ));
}
