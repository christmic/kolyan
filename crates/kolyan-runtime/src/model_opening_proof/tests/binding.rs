//! Physical Task producer capture and hostile mutations of its exact prefix.

use super::*;
use kolyan_ledger::{LedgerEvent, LedgerEventKind as Kind};

#[test]
fn memory_task_binding_prefix_matrix() {
    matrix(false);
}

#[test]
fn reopened_sqlite_task_binding_prefix_matrix() {
    matrix(true);
}

fn matrix(sqlite: bool) {
    let cases: Vec<Value> = serde_json::from_str(include_str!("binding_cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-opening-binding-")
        .tempdir()
        .unwrap()
        .keep();
    let mut output = Vec::new();
    for case in cases {
        let id = case["id"].as_str().unwrap();
        let base: Case = serde_json::from_value(json!({"id":id,"expected":"no_steps"})).unwrap();
        let mut fixture = fixture(&base);
        let capture: Value =
            serde_json::from_str(include_str!("task_producer_prefix.json")).unwrap();
        fixture.events = serde_json::from_value(capture["ledger"].clone()).unwrap();
        fixture.drafts.clear();
        let mut bound: LedgerEvent = fixture.events.remove(0);
        match id {
            "foreign-session" => bound.payload["binding"]["session_id"] = json!("foreign"),
            "foreign-turn" => bound.payload["binding"]["turn_id"] = json!("foreign"),
            "foreign-execution" => bound.payload["binding"]["execution_id"] = json!("foreign"),
            "empty-task" => bound.payload["binding"]["task_id"] = json!(" "),
            "control-invocation" => bound.payload["binding"]["invocation_id"] = json!("bad\n"),
            "oversized-attempt" => bound.payload["binding"]["attempt_id"] = json!("a".repeat(257)),
            "missing-attempt" => {
                bound.payload["binding"]
                    .as_object_mut()
                    .unwrap()
                    .remove("attempt_id");
            }
            "unknown-binding-field" => bound.payload["binding"]["extra"] = json!(true),
            "unknown-envelope-field" => bound.payload["extra"] = json!(true),
            "wrong-event-id" => bound.event_id = "execution/not-binding".into(),
            "wrong-idempotency-key" => bound.idempotency_key = "wrong".into(),
            "arbitrary-prestart" => bound.kind = Kind::ExecutionBoundaryAdmitted,
            _ => {}
        }
        let index = usize::from(id == "binding-after-start");
        fixture.events.insert(index, bound.clone());
        if id == "duplicate-binding" {
            bound.event_id = "execution/extra-binding".into();
            bound.idempotency_key = bound.event_id.clone();
            fixture.events.insert(1, bound);
        }
        let db = directory.join(format!("{id}.db"));
        let ledger: Box<dyn LedgerStore> = if sqlite {
            let store = SqliteLedger::open(&db).unwrap();
            for mut event in fixture.events {
                event.cursor = 0;
                store.append(event).unwrap();
            }
            drop(store);
            Box::new(SqliteLedger::open(&db).unwrap())
        } else {
            let store = InMemoryLedger::default();
            for mut event in fixture.events {
                event.cursor = 0;
                store.append(event).unwrap();
            }
            Box::new(store)
        };
        let before = ledger_rows(&*ledger);
        let head = before.last().unwrap();
        fixture.request.through = ModelOpeningEventRef {
            event_id: head.event_id.clone(),
            cursor: head.cursor,
        };
        let result =
            inspect_model_openings(&*ledger, &MemoryFactJournal::default(), &fixture.request);
        output.push(json!({"case":id,"backend":if sqlite {"reopened-sqlite"}else{"memory"},
            "origin":capture["origin"],"source":capture["source"],
            "expected":case["expected"],"actual":classification(&result),
            "error":result.err().map(|error|error.to_string()),"ledger_before":before,"ledger_after":ledger_rows(&*ledger)}));
    }
    let path = directory.join("actual.jsonl");
    {
        let mut file = File::create(&path).unwrap();
        for row in &output {
            serde_json::to_writer(&mut file, row).unwrap();
            file.write_all(b"\n").unwrap();
        }
        file.sync_all().unwrap();
    }
    let actual: Vec<Value> = fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    println!(
        "binding-prefix: {} rows, artifact {}",
        actual.len(),
        path.display()
    );
    assert_eq!(actual, output);
    for row in actual {
        assert_eq!(
            row["actual"], row["expected"],
            "{}: {}",
            row["case"], row["error"]
        );
        assert_eq!(row["ledger_before"], row["ledger_after"]);
    }
}
