//! Full source outcomes are exported before the dataset is compared.
mod assessments;
mod backends;
mod reconstruction;
mod support;
use super::*;
use kolyan_ledger::LedgerError;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;
use std::sync::Mutex;
use support::harness;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: String,
    page_cap: usize,
    growing: bool,
    budget: String,
    expected: String,
}
struct Observed {
    inner: backends::BackendLedger,
    unchanged: bool,
    events: Mutex<Vec<LedgerEvent>>,
    cap: usize,
    growing: bool,
    fault: bool,
    reads: Mutex<Vec<Value>>,
}
impl LedgerStore for &Observed {
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        panic!("reader attempted write")
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        panic!("reader attempted claim")
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        panic!("reader attempted unbounded read")
    }
    fn query(&self, q: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.reads
            .lock()
            .unwrap()
            .push(json!({"after":q.after,"through":q.through,"limit":q.limit,
            "event_id":q.event_id,"execution_id":q.execution_id}));
        if self.fault {
            return Err(LedgerError::Storage("fixture unavailable".into()));
        }
        if self.unchanged && !self.growing {
            let mut bounded = q.clone();
            bounded.limit = bounded.limit.min(self.cap);
            return self.inner.query(&bounded);
        }
        if self.growing && q.event_id.is_none() {
            let id = format!("execution/observation/{}", self.reads.lock().unwrap().len());
            let mut events = self.events.lock().unwrap();
            let cursor = events.last().unwrap().cursor + 1;
            events.push(LedgerEvent {
                event_id: id.clone(),
                idempotency_key: id,
                execution_id: "execution".into(),
                turn_id: "turn".into(),
                cursor,
                kind: LedgerEventKind::ExecutionBoundaryAdmitted,
                payload: json!({"observed":"concurrent append"}),
            });
        }
        q.validate()?;
        Ok(self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| {
                e.cursor > q.after
                    && q.through.is_none_or(|c| e.cursor <= c)
                    && q.event_id.as_ref().is_none_or(|id| id == &e.event_id)
                    && q.execution_id
                        .as_ref()
                        .is_none_or(|id| id == &e.execution_id)
            })
            .take(q.limit.min(self.cap))
            .cloned()
            .collect())
    }
}

#[tokio::test]
async fn bounded_source_matrix_uses_actual_runtime_effects() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let directory = tempfile::tempdir().unwrap().keep();
    let path = directory.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    let backends = backends::inventory();
    for backend in &backends {
        for case in &cases {
            let h = harness(crate::GoalVerdict::Satisfied, true, *backend).await;
            let prefix = h.service.coordinator().snapshot("task").unwrap();
            let source = prefix.attempts["attempt"]
                .observation
                .as_ref()
                .unwrap()
                .source
                .clone();
            let mut events = h.ledger.events_after(0).unwrap();
            events.retain(|e| match case.mutation.as_str() {
                "prepared_only" => !matches!(
                    e.kind,
                    LedgerEventKind::EffectAuthorized
                        | LedgerEventKind::EffectStarted
                        | LedgerEventKind::EffectReceipt
                ),
                "authorized_only" => !matches!(
                    e.kind,
                    LedgerEventKind::EffectStarted | LedgerEventKind::EffectReceipt
                ),
                "missing_receipt" => e.kind != LedgerEventKind::EffectReceipt,
                _ => true,
            });
            if case.mutation == "foreign_snapshot" {
                for e in &mut events {
                    if e.kind == LedgerEventKind::EffectStarted {
                        e.payload["scope"]["agent_snapshot_digest"] = json!("b".repeat(64));
                    }
                }
            }
            for e in &mut events {
                if e.kind != LedgerEventKind::StepCompleted {
                    continue;
                }
                if case.mutation == "non_final_response"
                    && e.payload["step"]["outcome"] == "FinalAnswer"
                {
                    e.payload["step"]["response"]["stop_reason"] = json!("refusal");
                }
                if case.mutation == "tool_call_mismatch"
                    && e.payload["step"]["outcome"] == "ToolCalls"
                {
                    e.payload["step"]["response"]["content"][0]["call"]["arguments"] =
                        json!({"substituted":true});
                }
            }
            if case.mutation == "late_step" {
                let mut event = events
                    .iter()
                    .find(|e| e.kind == LedgerEventKind::StepCompleted)
                    .unwrap()
                    .clone();
                event.cursor = events.last().unwrap().cursor + 1;
                event.event_id = "execution/late-step".into();
                event.idempotency_key = event.event_id.clone();
                events.push(event);
            }
            let observed = Observed {
                inner: h.ledger.clone(),
                unchanged: case.mutation == "none",
                events: Mutex::new(events.clone()),
                cap: case.page_cap,
                growing: case.growing,
                fault: case.mutation == "storage_fault",
                reads: Mutex::new(vec![]),
            };
            let mut limits = GoalSourceLimits::default();
            if case.budget == "small" {
                limits.max_rows = 64;
            }
            if case.budget == "scan_plus_two" {
                limits.max_rows = events.len() + 5;
            }
            if case.budget == "tiny_response" {
                limits.max_response_bytes = 1;
            }
            let result = GoalSourceReader::new(&observed)
                .inspect_stopped(&prefix, &h.binding, &source, &limits);
            let actual = match &result {
                Ok(v) => {
                    json!({"status":v.coverage(),"binding":v.binding(),"terminal":v.terminal(),
            "response":v.response(),"effects":v.effects().len(),"proofs":v.effects().iter().map(proof_value).collect::<Vec<_>>(),"through":v.through()})
                }
                Err(GoalSourceError::Storage(e)) => {
                    json!({"status":"Storage","error":e.to_string()})
                }
                Err(e) => json!({"status":"Refused","error":e.to_string()}),
            };
            writeln!(export,"{}",json!({"backend":backend,"case":case.id,"actual":actual,"requests":*h.requests.lock().unwrap(),
            "prefix":prefix,"events":events,"observed_events":*observed.events.lock().unwrap(),"reads":*observed.reads.lock().unwrap(),"session_root":h.root.path(),
            "tool_calls":h.tool_calls.load(std::sync::atomic::Ordering::SeqCst)})).unwrap();
        }
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("goal_source matrix {}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len() * backends.len());
    for (index, row) in rows.iter().enumerate() {
        let case = &cases[index % cases.len()];
        assert_eq!(
            row["backend"],
            serde_json::to_value(backends[index / cases.len()]).unwrap()
        );
        assert_eq!(
            row["actual"]["status"], case.expected,
            "{}: {}",
            case.id, row
        );
        if case.expected == "Complete" {
            let proof = &row["actual"]["proofs"][0];
            let cursors: Vec<u64> = proof["coordinates"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c["cursor"].as_u64().unwrap())
                .collect();
            assert_eq!(cursors.len(), 6);
            assert!(cursors.windows(2).all(|pair| pair[0] < pair[1]));
            assert_eq!(proof["scope"]["agent_snapshot_digest"], "a".repeat(64));
            assert!(
                row["reads"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|q| q["after"].as_u64() == row["actual"]["through"].as_u64())
            );
        }
        if case.growing {
            assert!(row["reads"].as_array().unwrap().len() <= 64);
            assert!(
                row["observed_events"].as_array().unwrap().len()
                    > row["events"].as_array().unwrap().len()
            );
        }
    }
    assert_eq!(rows.len(), cases.len() * backends.len());
}

fn proof_value(proof: &VerifiedEffectProof) -> Value {
    let s = proof.sources();
    let coordinates = [
        &s.admission,
        &s.prepared,
        &s.authorized,
        &s.started,
        &s.receipt,
        &s.terminal,
    ]
    .map(|c| json!({"event_id":c.event_id,"cursor":c.cursor}));
    json!({"prepared":proof.prepared(),"scope":proof.scope(),"result":proof.result(),
        "receipt":proof.receipt(),"terminal_kind":proof.terminal_kind(),"coordinates":coordinates})
}
