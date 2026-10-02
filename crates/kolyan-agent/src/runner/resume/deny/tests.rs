//! Task-aware denial through actual Runtime/Session facts; synthetic factories.

use std::{io::Write, sync::Arc};

use kolyan_ledger::LedgerStore;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::runner::*;

mod fixtures {
    pub(super) mod support {
        include!("../../tests/support.rs");
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    factory_refuses: bool,
    revoke: bool,
    wrong_owner: bool,
    expected_success: bool,
}

#[tokio::test]
async fn denial_rebuild_and_factory_refusal_export_before_compare() {
    use fixtures::support::*;
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-root-deny-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let mut harness = Harness::new();
        Arc::get_mut(&mut harness.runner)
            .unwrap()
            .providers
            .call_tool = true;
        Arc::get_mut(&mut harness.runner).unwrap().tools.1 = true;
        let mut input = harness.request(&case.id, true);
        input.limits.max_tokens = None;
        input.limits.max_steps_per_turn = 3;
        input.turn.config.max_steps = 3;
        input.turn.model_request.max_output_tokens = Some(50);
        let started = harness.runner.start(input).await.unwrap();
        let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = &started.execution
        else {
            panic!("fixed setup did not reach approval: {}", case.id);
        };
        let mut current = harness.runner.host.clone();
        if case.revoke {
            current.tools.clear();
        }
        let restored = Arc::new(
            AgentRunner::new(
                harness.service.clone(),
                harness.runner.instances.clone(),
                harness.bindings.clone(),
                crate::AgentCatalog::new(8).unwrap(),
                current,
                (
                    Providers {
                        observations: harness.observations.clone(),
                        fail: case.factory_refuses,
                        reject_context: false,
                        call_tool: true,
                    },
                    Tools(harness.observations.clone(), true),
                ),
                harness.runner.input_artifacts.clone(),
            )
            .unwrap(),
        );
        let request = RootApprovalResumeRequest {
            task_id: case.id.clone(),
            invocation_id: "root".into(),
            logical_session_id: if case.wrong_owner {
                "foreign"
            } else {
                "session"
            }
            .into(),
            attempt_id: "attempt".into(),
            approval_id: suspension.waiting.approvals[0].approval_id.clone(),
        };
        let before = harness.service.coordinator().snapshot(&case.id).unwrap();
        let result = restored.deny_approval(request.clone()).await;
        let after = harness.service.coordinator().snapshot(&case.id).unwrap();
        let repeated = if result.is_ok() {
            Some(restored.deny_approval(request.clone()).await)
        } else {
            None
        };
        let ledger = harness
            .service
            .sessions()
            .execution()
            .server()
            .coordinator()
            .ledger()
            .execution_events_after(&execution(&case.id).execution_id, 0)
            .unwrap();
        let row = json!({"case_id":case.id,"request":request,"before":before,"after":after,
            "success":result.is_ok(),"error":result.err().map(|e|e.to_string()),
            "repeat_success":repeated.map(|r|r.is_ok()),"ledger":ledger,
            "requests":*harness.observations.requests.lock().unwrap(),
            "effects":*harness.observations.effects.lock().unwrap()});
        writeln!(export, "{row}").unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    eprintln!("ROOT_DENY_EVIDENCE={}", path.display());
    let actual: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual.len(), cases.len());
    for (case, row) in cases.iter().zip(actual) {
        assert_eq!(row["success"], case.expected_success, "{}: {row}", case.id);
        assert_eq!(row["requests"].as_array().unwrap().len(), 1);
        assert!(row["effects"].as_array().unwrap().is_empty());
        if case.expected_success {
            assert_eq!(row["after"]["attempts"]["attempt"]["state"], "Failed");
            assert_eq!(row["repeat_success"], false);
        } else {
            assert_eq!(row["after"], row["before"]);
            assert_eq!(row["after"]["attempts"]["attempt"]["state"], "Suspended");
            if case.factory_refuses {
                assert!(row["error"].as_str().unwrap().contains("factory refused"));
            }
        }
    }
}
