//! Pure Core rejection and real Session denial; no executable adapter exists.

use super::super::*;
use kolyan_ledger::InMemoryLedger;
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: String,
    success: bool,
}

#[tokio::test]
async fn pending_denial_exact_scope_and_durable_terminal() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("pending_denial.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-session-pending-deny-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let root = tempfile::tempdir().unwrap();
        let store = FileSessionStore::new(root.path()).unwrap();
        store.create("s").unwrap();
        store
            .begin_turn_with_input(
                "s",
                SessionTurn {
                    turn_id: "t".into(),
                    execution_id: "e".into(),
                    status: SessionTurnStatus::Running,
                },
                0,
                vec![],
            )
            .unwrap();
        store
            .update_turn("s", "t", SessionTurnStatus::Suspended, vec![])
            .unwrap();
        let ledger = InMemoryLedger::default();
        let suspension = crate::suspension::tests::persist_approval(ledger.clone()).await;
        let key = ExecutionRef {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        };
        let mut scope = suspension.checkpoint.scope.clone();
        let mut approval = suspension.waiting.approvals[0].approval_id.clone();
        match case.mutation.as_str() {
            "none" => {}
            "session" => scope.execution.session_id = "foreign".into(),
            "turn" => scope.execution.turn_id = "foreign".into(),
            "execution" => scope.execution.execution_id = "foreign".into(),
            "step" => scope.step_id = "foreign".into(),
            "agent" => scope.agent_snapshot_digest = Some("foreign".into()),
            "approval" => approval = "foreign".into(),
            other => panic!("unknown mutation {other}"),
        }
        let pure = kolyan_core::reject_pending_approval(
            suspension.clone(),
            &approval,
            &scope,
            "user denied",
        );
        let service = SessionExecutionService::new(
            ExecutionService::new(ledger.clone(), NoopTraceSink),
            SessionService::new(store.clone()),
        );
        let before = ledger.events_after(0).unwrap();
        let result = service.deny_pending(&key, &scope, &approval);
        let after = ledger.events_after(0).unwrap();
        let repeated = if result.is_ok() {
            Some(service.deny_pending(&key, &scope, &approval).is_ok())
        } else {
            None
        };
        let row = json!({"case_id":case.id,"scope":scope,"approval_id":approval,"suspension":suspension,
            "pure_success":pure.is_ok(),"pure_error":pure.err().map(|e|e.to_string()),
            "success":result.is_ok(),"error":result.err().map(|e|e.to_string()),
            "before":before,"after":after,"session":store.load("s").unwrap(),"repeat_success":repeated,
            "repeat_events":ledger.events_after(0).unwrap()});
        writeln!(export, "{row}").unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    eprintln!("SESSION_PENDING_DENY_EVIDENCE={}", path.display());
    let actual: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual.len(), cases.len());
    for (case, row) in cases.iter().zip(actual) {
        assert_eq!(row["pure_success"], case.success, "{row}");
        assert_eq!(row["success"], case.success, "{row}");
        assert_eq!(row["repeat_events"], row["after"], "{row}");
        if case.success {
            assert_eq!(row["session"]["turns"][0]["status"], "failed", "{row}");
            assert_eq!(row["repeat_success"], false, "{row}");
            let added =
                &row["after"].as_array().unwrap()[row["before"].as_array().unwrap().len()..];
            assert!(added.iter().any(|e| e["kind"] == "turn_failed"), "{row}");
            assert!(
                !added.iter().any(|e| matches!(
                    e["kind"].as_str(),
                    Some("step_started" | "effect_started" | "execution_cancelled")
                )),
                "{row}"
            );
        } else {
            assert_eq!(row["before"], row["after"], "{row}");
        }
    }
}
