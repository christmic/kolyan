//! Restored Runner acceptance uses real durable services, not process/network mocks.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use kolyan_ledger::LedgerStore;
use serde::Deserialize;
use serde_json::{Value, json};

use super::super::*;
use super::support::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    named: bool,
    current_read: bool,
    session_override: Option<String>,
    approval_override: Option<String>,
    expected_success: bool,
    expected_requests: usize,
    expected_effects: usize,
}

#[tokio::test]
async fn restored_root_approval_matrix_exports_then_compares_effects_and_no_model_replay() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("resume.json")).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        let mut harness = Harness::new();
        Arc::get_mut(&mut harness.runner)
            .unwrap()
            .providers
            .call_tool = true;
        Arc::get_mut(&mut harness.runner).unwrap().tools.1 = true;
        let mut input = harness.request(&case.id, case.named);
        input.limits.max_tokens = None;
        input.limits.max_steps_per_turn = 3;
        input.turn.config.max_steps = 3;
        input.turn.model_request.max_output_tokens = Some(50);
        let started = harness.runner.start(input).await.unwrap();
        let DurableTurnResult::Suspended { suspension, .. } = &started.execution else {
            panic!("{} must suspend before the effect", case.id);
        };
        assert_eq!(suspension.waiting.approvals.len(), 1);
        assert!(harness.observations.effects.lock().unwrap().is_empty());
        let mut host = harness.runner.host.clone();
        if !case.current_read {
            host.tools.clear();
        }
        // Restore without any registered definition: saved content must be enough.
        let restored = Arc::new(
            AgentRunner::new(
                harness.service.clone(),
                harness.runner.instances.clone(),
                harness.bindings.clone(),
                AgentCatalog::new(8).unwrap(),
                host,
                (
                    Providers {
                        observations: harness.observations.clone(),
                        fail: false,
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
            logical_session_id: case
                .session_override
                .clone()
                .unwrap_or_else(|| "session".into()),
            attempt_id: "attempt".into(),
            approval_id: case
                .approval_override
                .clone()
                .unwrap_or_else(|| suspension.waiting.approvals[0].approval_id.clone()),
        };
        let resumed = restored.resume_approval(request.clone()).await;
        let outcome = match &resumed {
            Ok(result) => json!({"success":true,"snapshot":result.snapshot,"task":result.task}),
            Err(error) => json!({"success":false,"error":error.to_string()}),
        };
        let repeated = if resumed.is_ok() {
            Some(
                restored
                    .resume_approval(request.clone())
                    .await
                    .err()
                    .expect("completed approval cannot replay")
                    .to_string(),
            )
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
        rows.push(json!({"fixture_id":case.id,"request":request,"original_snapshot":started.snapshot,"outcome":outcome,"repeat_refusal":repeated,"requests":*harness.observations.requests.lock().unwrap(),"effects":*harness.observations.effects.lock().unwrap(),"ledger":ledger}));
    }
    let root = tempfile::Builder::new()
        .prefix("kolyan-root-resume-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut output = BufWriter::new(File::create(&path).unwrap());
    for row in &rows {
        serde_json::to_writer(&mut output, row).unwrap();
        writeln!(output).unwrap();
    }
    output.flush().unwrap();
    println!("ROOT_APPROVAL_RESUME_TRACE={}", path.display());
    let actual: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual, rows);
    for (case, row) in cases.iter().zip(&actual) {
        assert_eq!(
            row["outcome"]["success"], case.expected_success,
            "{}: {}",
            case.id, row["outcome"]
        );
        assert_eq!(
            row["requests"].as_array().unwrap().len(),
            case.expected_requests,
            "{}",
            case.id
        );
        assert_eq!(
            row["effects"].as_array().unwrap().len(),
            case.expected_effects,
            "{}",
            case.id
        );
        if case.expected_success {
            assert_eq!(row["outcome"]["snapshot"], row["original_snapshot"]);
            assert_eq!(row["outcome"]["task"]["state"], "Completed");
            assert!(row["repeat_refusal"].is_string());
            assert_eq!(row["requests"][1]["messages"].as_array().unwrap().len(), 3);
        }
    }
}
