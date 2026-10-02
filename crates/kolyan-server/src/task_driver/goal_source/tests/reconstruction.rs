//! Crash-boundary reconstruction of the actual Service, without model/tool reentry.
use super::backends::inventory;
use super::support::{Harness, harness_with_child, verifier};
use crate::*;
use kolyan_ledger::{FactJournal, LedgerStore};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;
use std::sync::atomic::Ordering;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    verdict: GoalVerdict,
    child: bool,
    effect: bool,
    before_state: String,
    expected: String,
    completion_rows: usize,
    tool_calls: usize,
}

#[tokio::test]
async fn persisted_assessment_rebuilt_service_finalization_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("reconstruction.json")).unwrap();
    let backends = inventory();
    let directory = tempfile::tempdir().unwrap().keep();
    let path = directory.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for backend in &backends {
        for case in &cases {
            let Harness {
                service,
                ledger,
                binding,
                requests,
                tool_calls,
                root,
                backend: storage_backend,
            } = harness_with_child(case.verdict, case.effect, case.child, *backend).await;
            let assessed = service.assess_goal("task", "assessment", "goal").unwrap();
            let persisted_facts = service
                .coordinator()
                .journal()
                .read("task", 0, 1024)
                .unwrap();
            let original_events = ledger.events_after(0).unwrap();
            let original_requests = requests.lock().unwrap().clone();
            let original_tool_calls = tool_calls.load(Ordering::SeqCst);
            let original_journal = service.coordinator().journal().clone();
            // End the original service lifetime between the durable assessment and
            // finalization. Memory retains its stores; SQLite opens new connections.
            drop(service);
            let (reopened_ledger, reopened_journal) =
                storage_backend.reopen(root.path(), &ledger, &original_journal);
            drop(ledger);
            drop(original_journal);
            let coordinator = TaskCoordinator::new(reopened_journal)
                .with_goal_verifier(verifier(reopened_ledger.clone(), case.verdict));
            let before_finalize = coordinator.snapshot("task").unwrap();
            // The original fixture has no ArtifactPort, preparation hook or
            // custom Session context policy. Rebuild that exact configuration.
            let sessions = FileSessionStore::new(root.path()).unwrap();
            let rebuilt = TaskExecutionService::new(
                coordinator,
                SessionExecutionService::new(
                    ExecutionService::new(reopened_ledger.clone(), NoopTraceSink),
                    SessionService::new(sessions),
                ),
            );
            let first = rebuilt.complete("task", "complete");
            let facts_after_first = rebuilt
                .coordinator()
                .journal()
                .read("task", 0, 1024)
                .unwrap();
            let second = rebuilt.complete("task", "complete");
            let facts_after_second = rebuilt
                .coordinator()
                .journal()
                .read("task", 0, 1024)
                .unwrap();
            let actual = |result: Result<TaskSnapshot, TaskExecutionError>| match result {
                Ok(snapshot) => json!({"status":snapshot.state,"snapshot":snapshot}),
                Err(error) => json!({"status":"Refused","error":error.to_string()}),
            };
            writeln!(export, "{}", json!({
                "backend":backend,"case":case.id,"binding":binding,
                "configuration":{"artifact_port":null,"session_root":root.path(),
                    "preparation_hook":null,"context_policy":"default"},
                "assessed":assessed,"persisted_facts":persisted_facts,
                "before_finalize":before_finalize,"first":actual(first),"second":actual(second),
                "facts_after_first":facts_after_first,"facts_after_second":facts_after_second,
                "original_events":original_events,"events":reopened_ledger.events_after(0).unwrap(),
                "original_requests":original_requests,"requests":*requests.lock().unwrap(),
                "original_tool_calls":original_tool_calls,"tool_calls":tool_calls.load(Ordering::SeqCst)
            })).unwrap();
        }
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("goal_service_reconstruction matrix {}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len() * backends.len());
    for (index, row) in rows.iter().enumerate() {
        let case = &cases[index % cases.len()];
        assert_eq!(row["case"], case.id);
        assert_eq!(
            row["backend"],
            serde_json::to_value(backends[index / cases.len()]).unwrap()
        );
        assert_eq!(row["first"]["status"], case.expected, "{}: {row}", case.id);
        assert_eq!(row["second"], row["first"], "repeat result {}", case.id);
        assert_eq!(row["before_finalize"], row["assessed"]);
        assert_eq!(row["before_finalize"]["state"], case.before_state);
        assert_eq!(
            row["before_finalize"]["goal_assessments"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            row["facts_after_second"], row["facts_after_first"],
            "repeat wrote new facts"
        );
        let facts = row["facts_after_first"].as_array().unwrap();
        assert_eq!(
            facts
                .iter()
                .filter(|fact| fact["draft"]["kind"] == "task.completed")
                .count(),
            case.completion_rows
        );
        assert_eq!(
            facts
                .iter()
                .filter(|fact| fact["draft"]["kind"] == "task.goal_assessed")
                .count(),
            1
        );
        assert_eq!(row["events"], row["original_events"], "no Runtime reentry");
        assert_eq!(
            row["requests"], row["original_requests"],
            "no model reentry"
        );
        assert_eq!(
            row["tool_calls"], row["original_tool_calls"],
            "no tool reentry"
        );
        assert_eq!(row["original_tool_calls"], case.tool_calls);
    }
}
