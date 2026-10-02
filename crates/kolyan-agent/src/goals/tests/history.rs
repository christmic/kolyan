//! Rebuilt Memory/SQLite sources and deliberately corrupted bounded read views.
use super::support::*;
use kolyan_ledger::*;
use kolyan_runtime::VerifiedEffectProof;
use kolyan_server::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io::Write, sync::atomic::Ordering};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    base: Value,
    backends: Vec<String>,
    cases: Vec<Value>,
}
struct Frozen(Vec<LedgerEvent>);
impl LedgerStore for &Frozen {
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        panic!("read only")
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        panic!("read only")
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        panic!("unbounded read")
    }
    fn query(&self, q: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        q.validate()?;
        Ok(self
            .0
            .iter()
            .filter(|e| {
                e.cursor > q.after
                    && q.through.is_none_or(|c| e.cursor <= c)
                    && q.event_id.as_ref().is_none_or(|id| id == &e.event_id)
                    && q.execution_id
                        .as_ref()
                        .is_none_or(|id| id == &e.execution_id)
            })
            .take(q.limit)
            .cloned()
            .collect())
    }
}
fn proof_value(p: &VerifiedEffectProof) -> Value {
    let s = p.sources();
    let coords = [
        &s.admission,
        &s.prepared,
        &s.authorized,
        &s.started,
        &s.receipt,
        &s.terminal,
    ]
    .map(|c| json!({"event_id":c.event_id,"cursor":c.cursor}));
    json!({"prepared":p.prepared(),"scope":p.scope(),"result":p.result(),"receipt":p.receipt(),"coordinates":coords})
}

#[tokio::test]
async fn historical_file_write_checker_reconstruction_matrix() {
    let data: Dataset = serde_json::from_str(include_str!("history.json")).unwrap();
    let root = tempfile::tempdir().unwrap().keep();
    let path = root.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for backend in &data.backends {
        for overlay in &data.cases {
            let mut input = data.base.clone();
            for (key, value) in overlay.as_object().unwrap() {
                input[key] = value.clone();
            }
            let case: Case = serde_json::from_value(input.clone()).unwrap();
            let h = produce(&case, backend == "sqlite").await;
            let (ledger, journal) = if backend == "sqlite" {
                stores(true, h.root.path())
            } else {
                (h.ledger.clone(), h.journal.clone())
            };
            let rebuilt = TaskCoordinator::new(journal.clone());
            let prefix = rebuilt.snapshot("task").unwrap();
            let before_events = ledger.events_after(0).unwrap();
            let before_facts = journal.read("task", 0, 1024).unwrap();
            let before_requests = h.requests.lock().unwrap().clone();
            let before_calls = h.effects.load(Ordering::SeqCst);
            let mut events = before_events.clone();
            match case.source_mode.as_str() {
                "missing_receipt" => events.retain(|e| e.kind != LedgerEventKind::EffectReceipt),
                "last_receipt_missing" => {
                    let cursor = events
                        .iter()
                        .filter(|e| e.kind == LedgerEventKind::EffectReceipt)
                        .map(|e| e.cursor)
                        .max()
                        .unwrap();
                    events.retain(|e| e.cursor != cursor);
                }
                "foreign_snapshot" => {
                    for e in &mut events {
                        if e.kind == LedgerEventKind::EffectStarted {
                            e.payload["scope"]["agent_snapshot_digest"] = json!("b".repeat(64));
                        }
                    }
                }
                _ => {}
            }
            let observed = Frozen(events.clone());
            let terminal = &prefix.attempts["attempt"]
                .observation
                .as_ref()
                .unwrap()
                .source;
            let limits = if case.source_mode == "row_bound" {
                GoalSourceLimits {
                    max_rows: events.len() + 5,
                    ..Default::default()
                }
            } else {
                GoalSourceLimits::default()
            };
            let source = if case.source_mode == "missing_receipt"
                || case.source_mode == "last_receipt_missing"
                || case.source_mode == "foreign_snapshot"
            {
                GoalSourceReader::new(&observed)
                    .inspect_stopped(&prefix, &h.attempt, terminal, &limits)
            } else {
                GoalSourceReader::new(ledger.clone())
                    .inspect_stopped(&prefix, &h.attempt, terminal, &limits)
            };
            let mut p = predicate();
            match case.predicate_mode.as_str() {
                "leaf" => p.leaf = "other.txt".into(),
                "physical_alias" => {
                    p.workspace.physical_path = "/historical/alias".into();
                    p.parent.physical_path = p.workspace.physical_path.clone();
                }
                "empty" | "unicode" => {
                    p.expected_bytes = case.content.len() as u64;
                    p.expected_sha256 = hash(case.content.as_bytes());
                }
                _ => {}
            }
            let criterion = criterion(json!(p));
            let (actual, source_value) = match source {
                Err(error) => (
                    json!({"status":"SourceError","error":error.to_string()}),
                    Value::Null,
                ),
                Ok(source) => {
                    let result = checker().assess(&criterion, &source);
                    let repeat = checker().assess(&criterion, &source);
                    let actual = |r: Result<ComputedGoalDecision, TaskError>| match r {
                        Ok(d) => json!({"status":d.verdict,"decision":d}),
                        Err(e) => json!({"status":"OperationalError","error":e.to_string()}),
                    };
                    let first = actual(result);
                    let second = actual(repeat);
                    (
                        json!({"status":first["status"],"first":first,"repeat":second}),
                        json!({"binding":source.binding(),"terminal":source.terminal(),"response":source.response(),"coverage":source.coverage(),"through":source.through(),"effects":source.effects().iter().map(proof_value).collect::<Vec<_>>()}),
                    )
                }
            };
            writeln!(export,"{}",json!({"backend":backend,"case":case.id,"input":input,"criterion":criterion,"actual":actual,"source":source_value,"prefix":prefix,"original_prefix":h.prefix,
                "before_events":before_events,"events":ledger.events_after(0).unwrap(),"observed_events":events,"before_facts":before_facts,"facts":journal.read("task",0,1024).unwrap(),
                "before_requests":before_requests,"requests":*h.requests.lock().unwrap(),"before_tool_calls":before_calls,"tool_calls":h.effects.load(Ordering::SeqCst),
                "evidence_origin":"scripted governed tool port; physical bindings are fixture data, not native worker acceptance"})).unwrap();
        }
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("file_goal_history {}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), data.cases.len() * data.backends.len());
    for row in rows {
        let case: Case = serde_json::from_value(row["input"].clone()).unwrap();
        assert_eq!(row["actual"]["status"], case.expected, "{}: {row}", case.id);
        if case.expected != "SourceError" {
            assert_eq!(row["actual"]["first"], row["actual"]["repeat"]);
        }
        assert_eq!(row["prefix"], row["original_prefix"]);
        assert_eq!(row["events"], row["before_events"]);
        assert_eq!(row["facts"], row["before_facts"]);
        assert_eq!(row["requests"], row["before_requests"]);
        assert_eq!(row["tool_calls"], row["before_tool_calls"]);
        if case.expected == "Satisfied" {
            let decision = &row["actual"]["first"]["decision"];
            let proof = &decision["proof"];
            let witnesses = row["source"]["effects"].as_array().unwrap();
            let selected = witnesses
                .iter()
                .min_by_key(|w| {
                    (
                        w["coordinates"][4]["cursor"].as_u64().unwrap(),
                        w["coordinates"][4]["event_id"].as_str().unwrap(),
                    )
                })
                .unwrap();
            assert_eq!(proof["coordinates"], selected["coordinates"]);
            assert_eq!(proof["coordinates"].as_array().unwrap().len(), 6);
            let cursors: Vec<u64> = proof["coordinates"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c["cursor"].as_u64().unwrap())
                .collect();
            assert!(cursors.windows(2).all(|c| c[0] < c[1]));
            assert_eq!(proof["prepared_digest"], selected["prepared"]["digest"]);
            let result: kolyan_model::ToolResult =
                serde_json::from_value(selected["result"]["Ok"].clone()).unwrap();
            assert_eq!(
                proof["tool_result_digest"],
                hash(&serde_json::to_vec(&result).unwrap())
            );
            assert_eq!(
                proof["bytes"],
                row["criterion"]["predicate"]["expected_bytes"]
            );
            assert_eq!(
                proof["sha256"],
                row["criterion"]["predicate"]["expected_sha256"]
            );
        }
    }
}
