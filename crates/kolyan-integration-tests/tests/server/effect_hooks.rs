//! Actual Server/Runtime/file effects; explicitly scripted model and hook ports.

#[path = "effect_hooks/support.rs"]
mod support;

use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, FileLedger, LedgerStore, SqliteFactJournal};
use kolyan_runtime::{DurableTurnResult, RuntimeError};
use kolyan_server::{ExecutionService, ServerError, SessionExecutionService, SessionService};
use kolyan_storage::{FileSessionStore, SessionStore};
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::{Hooks, Provider, policy};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    approval: bool,
    user_deny: bool,
    hook_mode: String,
    tool_window_ms: u64,
    request: kolyan_model::ModelRequest,
    turn_id: String,
    max_steps: usize,
    call: kolyan_model::ToolCall,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    id: String,
    outcome: String,
    before: usize,
    after: usize,
    receipts: usize,
    session_status: String,
    file_content: Option<String>,
    model_requests: usize,
}

fn outcome(result: &Result<DurableTurnResult, ServerError>) -> String {
    match result {
        Ok(DurableTurnResult::Completed(execution, _)) => {
            format!("{:?}", execution.result.end_reason)
        }
        Ok(DurableTurnResult::Suspended { .. }) => "Suspended".into(),
        Err(ServerError::Runtime(RuntimeError::Turn(error))) => {
            format!("{:?}", error.end_reason())
        }
        Err(error) => format!("Other:{error}"),
    }
}

fn file_content(path: &std::path::Path) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(content) => Some(content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("cannot inspect actual effect {}: {error}", path.display()),
    }
}

#[tokio::test]
async fn data_driven_effect_hooks_survive_real_session_approval_reconstruction() {
    let input: Value =
        serde_json::from_str(include_str!("../fixtures/server/effect_hooks.json")).unwrap();
    let plan: Plan = serde_json::from_value(input.clone()).unwrap();
    let expected: Vec<Expected> =
        serde_json::from_str(include_str!("../expected/server/effect_hooks.json")).unwrap();
    let proof = tempfile::Builder::new()
        .prefix("kolyan-server-effect-hooks-")
        .tempdir()
        .unwrap()
        .keep();
    let path = proof.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    writeln!(
        export,
        "{}",
        json!({"event":"plan","input":input,
        "model":"declared-scripted","hooks":"declared-scripted",
        "tool":"actual in-process RestrictedFileTool","attempts":1})
    )
    .unwrap();
    for case in &plan.cases {
        let root = proof.join(&case.id);
        std::fs::create_dir_all(root.join("safe")).unwrap();
        let ledger_path = root.join("ledger.jsonl");
        let session_path = root.join("sessions");
        let file = root.join(case.call.arguments["path"].as_str().unwrap());
        let store = FileSessionStore::new(&session_path).unwrap();
        store.create("s").unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let journal_path = root.join("observations.sqlite");
        let current_policy = policy(&root, case.approval);
        let executor = || {
            TurnExecutor::with_tools(
                Provider {
                    call: case.call.clone(),
                    requests: requests.clone(),
                },
                PolicyEnforcingTool::new(RestrictedFileTool::new(&root), current_policy.clone()),
            )
            .with_policy_engine(current_policy.clone())
            .with_tool_timeout(Duration::from_millis(case.tool_window_ms))
        };
        let service = || {
            SessionExecutionService::new(
                ExecutionService::new(FileLedger::open(&ledger_path).unwrap(), NoopTraceSink),
                SessionService::new(FileSessionStore::new(&session_path).unwrap()),
            )
            .with_effect_hooks(Arc::new(Hooks {
                mode: case.hook_mode.clone(),
                ledger: FileLedger::open(&ledger_path).unwrap(),
                file: file.clone(),
                calls: calls.clone(),
                journal: SqliteFactJournal::open(&journal_path).unwrap(),
            }))
        };
        let first = service();
        let started = first
            .start(
                executor(),
                TurnRequest {
                    turn_id: case.turn_id.clone(),
                    model_request: case.request.clone(),
                    config: TurnConfig {
                        max_steps: case.max_steps,
                        max_tool_calls: Some(3),
                        deadline: Some(Duration::from_secs(10)),
                    },
                },
                "s",
                "e",
            )
            .await;
        let paused = match &started {
            Ok(DurableTurnResult::Suspended { suspension, .. }) => Some(json!({
                "suspension":suspension,
                "ledger":FileLedger::open(&ledger_path).unwrap().execution_events_after("e",0).unwrap(),
                "hooks":calls.lock().unwrap().clone(),
                "file_exists":file.exists(),
                "session":store.load("s").unwrap(),
                "model_requests":requests.lock().unwrap().clone(),
            })),
            _ => None,
        };
        // Drop the complete service. The next service reads the saved approval
        // from the actual file stores and reinstalls the trusted host port.
        drop(first);
        let mut result = started;
        let mut denied = false;
        if let Ok(DurableTurnResult::Suspended { suspension, .. }) = &result {
            let approval = &suspension.waiting.approvals[0].approval_id;
            let rebuilt = service();
            if case.user_deny {
                let key = kolyan_server::ExecutionRef {
                    session_id: "s".into(),
                    turn_id: case.turn_id.clone(),
                    execution_id: "e".into(),
                };
                let denial = rebuilt.deny_pending(&key, &suspension.checkpoint.scope, approval);
                denied = denial.is_ok();
                if let Err(error) = denial {
                    result = Err(error);
                }
            } else {
                result = rebuilt
                    .resume_approval(executor(), "s", "e", approval)
                    .await;
            }
        }
        let ledger = FileLedger::open(&ledger_path).unwrap();
        let row = json!({"event":"actual","id":case.id,"input":case.request,"call":case.call,
            "outcome":if denied {"ApprovalRejected".into()} else {outcome(&result)},
            "error":result.as_ref().err().map(ToString::to_string),
            "paused":paused,"hooks":calls.lock().unwrap().clone(),
            "ledger":ledger.execution_events_after("e",0).unwrap(),
            "observation_facts":SqliteFactJournal::open(&journal_path).unwrap().read("fixture.observations",0,64).unwrap(),
            "session":FileSessionStore::new(&session_path).unwrap().load("s").unwrap(),
            "file_content":file_content(&file),
            "model_requests":requests.lock().unwrap().clone()});
        writeln!(export, "{row}").unwrap();
        export.flush().unwrap();
        export.sync_all().unwrap();
    }
    drop(export);
    eprintln!("SERVER_EFFECT_HOOK_ACTUAL={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), expected.len() + 1);
    assert_eq!(plan.cases.len(), expected.len());
    for ((case, oracle), row) in plan.cases.iter().zip(expected).zip(rows.iter().skip(1)) {
        assert_eq!(case.id, oracle.id);
        assert_eq!(row["id"], oracle.id);
        assert_eq!(row["outcome"], oracle.outcome, "{row}");
        assert_eq!(
            row["session"]["turns"][0]["status"], oracle.session_status,
            "{row}"
        );
        assert_eq!(row["file_content"], json!(oracle.file_content), "{row}");
        assert_eq!(
            row["model_requests"].as_array().unwrap().len(),
            oracle.model_requests,
            "{row}"
        );
        let hooks = row["hooks"].as_array().unwrap();
        if hooks.len() == 2 {
            for identity in ["issued", "effect_id", "input_digest", "deadline"] {
                assert_eq!(hooks[0][identity], hooks[1][identity], "{row}");
            }
        }
        assert_eq!(
            hooks.iter().filter(|h| h["phase"] == "before").count(),
            oracle.before,
            "{row}"
        );
        assert_eq!(
            hooks.iter().filter(|h| h["phase"] == "after").count(),
            oracle.after,
            "{row}"
        );
        assert_eq!(
            row["observation_facts"].as_array().unwrap().len(),
            if oracle.after == 0 {
                0
            } else if case.hook_mode == "after_host" {
                1
            } else {
                2
            },
            "{row}"
        );
        let events = row["ledger"].as_array().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e["kind"] == "effect_receipt")
                .count(),
            oracle.receipts,
            "{row}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e["kind"] == "effect_started")
                .count(),
            oracle.receipts,
            "{row}"
        );
        if case.approval {
            assert!(!row["paused"].is_null(), "{row}");
            assert!(
                row["paused"]["hooks"].as_array().unwrap().is_empty(),
                "{row}"
            );
            assert_eq!(row["paused"]["file_exists"], false, "{row}");
            assert_eq!(
                row["paused"]["session"]["turns"][0]["status"], "suspended",
                "{row}"
            );
        }
        for hook in hooks {
            assert_eq!(hook["issued"]["scope"]["execution"]["execution_id"], "e");
            assert_eq!(hook["issued"]["scope"]["execution"]["session_id"], "s");
            assert_eq!(hook["issued"]["prepared"]["call"], row["call"]);
            assert!(hook["remaining_ms"].as_u64().unwrap() <= case.tool_window_ms);
            let at = hook["ledger_at_call"].as_array().unwrap();
            if hook["phase"] == "before" {
                assert!(!at.iter().any(|e| matches!(
                    e["kind"].as_str(),
                    Some("effect_started" | "effect_authorized")
                )));
                assert_eq!(hook["file_exists"], false);
            } else {
                let receipt = at.iter().find(|e| e["kind"] == "effect_receipt").unwrap();
                assert_eq!(hook["receipt"], *receipt);
                assert_eq!(hook["file_exists"], true);
                assert_eq!(hook["result"], receipt["payload"]["output"]);
            }
        }
        if let Some(last) = row["model_requests"].as_array().unwrap().get(1) {
            assert!(
                last["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|m| m["content"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|b| b["type"] == "tool_result")),
                "{row}"
            );
        }
    }
}
