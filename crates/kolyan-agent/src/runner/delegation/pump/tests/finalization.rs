//! Historical proof reads and duplicate finalization of actual routed children.

use super::*;
use crate::{TaskFinalizationPolicy, TaskFinalizationRequest};
use kolyan_ledger::{LedgerStore, MemoryFactJournal};
use serde_json::Value;

type Runner = crate::AgentRunner<
    MemoryFactJournal,
    kolyan_ledger::InMemoryLedger,
    NoopTraceSink,
    kolyan_storage::FileSessionStore,
    provider::Factory,
    Tools,
>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Check {
    revoked: bool,
    expected_error: bool,
}

pub(super) async fn exercise(
    runner: &Arc<Runner>,
    service: &Arc<Service>,
    task_id: &str,
    check: &Check,
    row: &mut Value,
) {
    let before = service
        .coordinator()
        .journal()
        .read(task_id, 0, 1024)
        .unwrap();
    let requests = runner
        .providers
        .observations
        .requests
        .lock()
        .unwrap()
        .clone();
    let effects = runner
        .providers
        .observations
        .effects
        .lock()
        .unwrap()
        .clone();
    let task = service.coordinator().snapshot(task_id).unwrap();
    let root = &task.attempts["attempt"].binding;
    let request = TaskFinalizationRequest {
        task_id: task_id.into(),
        logical_session_id: root.execution.session_id.clone(),
        root_invocation_id: root.invocation_id.clone(),
        root_attempt_id: root.attempt_id.clone(),
        policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
    };
    let mut results = Vec::new();
    for _ in 0..2 {
        let mut host = runner.host.clone();
        if check.revoked {
            host.tools.clear();
        }
        let restored = Arc::new(
            crate::AgentRunner::new(
                service.clone(),
                runner.instances.clone(),
                runner.bindings.clone(),
                crate::AgentCatalog::new(8).unwrap(),
                host,
                (
                    runner.providers.clone(),
                    Tools(runner.providers.observations.clone(), false),
                ),
                runner.input_artifacts.clone(),
            )
            .unwrap(),
        );
        results.push(match restored.finalize_task(request.clone()).await {
            Ok(task) => json!({"task":task}),
            Err(error) => json!({"error":error.to_string()}),
        });
    }
    let mut proof_rows = Vec::new();
    let mut ledgers = Vec::new();
    for attempt in task.attempts.values() {
        let terminal =
            service.load_verified_historical_result(task_id, &attempt.binding, 1024 * 1024);
        proof_rows.push(match terminal {
            Ok(proof) => json!({"terminal":proof}),
            Err(error) => json!({"error":error.to_string()}),
        });
        ledgers.push(json!({"binding":attempt.binding,"events":service.sessions().execution().server().coordinator().ledger().execution_events_after(&attempt.binding.execution.execution_id,0).unwrap()}));
    }
    let mut consumption = Vec::new();
    for child in task
        .attempts
        .values()
        .filter(|child| child.binding != *root)
    {
        consumption.push(
            match service.load_verified_historical_consumed_result(
                task_id,
                root,
                &child.binding,
                1024 * 1024,
            ) {
                Ok(proof) => json!({"proof":proof}),
                Err(error) => json!({"error":error.to_string()}),
            },
        );
    }
    row["finalization"] = json!({
        "results":results,"facts_before":before,"facts_after":service.coordinator().journal().read(task_id,0,1024).unwrap(),
        "requests_before":requests,"requests_after":runner.providers.observations.requests.lock().unwrap().clone(),
        "effects_before":effects,"effects_after":runner.providers.observations.effects.lock().unwrap().clone(),
        "terminals":proof_rows,"consumptions":consumption,"ledgers":ledgers,
    });
}

pub(super) fn compare(row: &Value, check: &Check) {
    let captured = &row["finalization"];
    for result in captured["results"].as_array().unwrap() {
        assert_eq!(result["error"].is_string(), check.expected_error, "{row}");
        if !check.expected_error {
            assert_eq!(result["task"], row["resumed_task"]);
        }
    }
    assert_eq!(captured["facts_before"], captured["facts_after"]);
    assert_eq!(captured["requests_before"], captured["requests_after"]);
    assert_eq!(captured["effects_before"], captured["effects_after"]);
    for terminal in captured["terminals"].as_array().unwrap() {
        assert!(terminal["error"].is_null(), "{row}");
    }
    for consumption in captured["consumptions"].as_array().unwrap() {
        assert!(
            consumption["error"].is_null() && consumption["proof"].is_object(),
            "{row}"
        );
    }
    assert_eq!(
        captured["facts_after"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|fact| matches!(
                fact["draft"]["kind"].as_str(),
                Some("task.failed" | "task.completed")
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn historical_child_failures_and_root_success_finalize_without_reentry() {
    let cases = serde_json::from_str(include_str!("finalization.json")).unwrap();
    run_cases(cases).await;
}
