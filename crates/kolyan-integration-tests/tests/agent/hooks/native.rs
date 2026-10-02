#[path = "native/host.rs"]
mod host;

use host::Host;
use kolyan_agent::hooks::{
    HookDispatchResult, HookError, HookEvent, HookExecutionWindow, HookPayload, HookPhase,
    HookRuntime,
};
use kolyan_core::TurnControl;
use kolyan_policy::ToolExecutionScope;
use kolyan_runtime::ExecutionKey;
use kolyan_sandbox::{SandboxProcessEvent, sandbox_process_observation_channel};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    time::Duration,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    phase: HookPhase,
    script: String,
    action: String,
    timeout_ms: u64,
    max_output_bytes: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    id: String,
    result: String,
    spawned: usize,
    completed: usize,
    cause: Option<String>,
}

fn event(host: &Host, phase: HookPhase) -> HookEvent {
    let scope = ToolExecutionScope {
        execution: ExecutionKey {
            session_id: host.scope.private_session_id.clone(),
            execution_id: host.scope.execution_id.clone(),
            turn_id: host.scope.turn_id.clone(),
        },
        step_id: "step-0".into(),
        agent_snapshot_digest: Some(host.scope.agent_snapshot_digest.clone()),
    };
    let payload = match phase {
        HookPhase::BeforeModel => HookPayload::BeforeModel {
            opening: 1,
            source_digest: "a".repeat(64),
        },
        HookPhase::BeforeTool => HookPayload::BeforeTool {
            tool_name: "file.write".into(),
            prepared_digest: "b".repeat(64),
            argument_digest: "c".repeat(64),
            scope,
        },
        HookPhase::AfterTool => HookPayload::AfterTool {
            tool_name: "file.write".into(),
            prepared_digest: "b".repeat(64),
            scope,
            receipt_event_id: "fixture-committed-receipt".into(),
            receipt_cursor: 3,
            result_digest: "d".repeat(64),
        },
    };
    // Digest-only host event projection is synthetic declared fixture input,
    // not proof of a model request or an actual target-tool effect/receipt.
    HookEvent {
        schema_version: 1,
        scope: host.scope.clone(),
        payload,
    }
}
fn category(result: &Result<HookDispatchResult, HookError>) -> &'static str {
    match result {
        Ok(HookDispatchResult::Continued { .. }) => "continue",
        Ok(HookDispatchResult::Denied { .. }) => "deny",
        Err(HookError::Protocol(_)) => "protocol",
        Err(HookError::Native(_)) => "native",
        Err(HookError::Expired) => "expired",
        Err(HookError::Cancelled) => "cancelled",
        Err(HookError::Interrupted) => "interrupted",
        Err(HookError::Revoked) => "revoked",
        Err(HookError::Permission) => "permission",
        Err(HookError::Conflict) => "conflict",
        Err(HookError::Journal(_)) => "journal",
        _ => "other",
    }
}

#[tokio::test]
async fn native_script_data_matrix() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../../fixtures/agent/hooks/native.json")).unwrap();
    let expected: Vec<Expected> =
        serde_json::from_str(include_str!("../../expected/agent/hooks/native.json")).unwrap();
    let proof = tempfile::Builder::new()
        .prefix("kolyan-native-hooks-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    let path = proof.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    writeln!(output,"{}",json!({"kind":"plan","cases":serde_json::from_str::<Value>(include_str!("../../fixtures/agent/hooks/native.json")).unwrap(),"expected":serde_json::from_str::<Value>(include_str!("../../expected/agent/hooks/native.json")).unwrap(),"model_requests":[],"scope":"offline sandbox foundation only"})).unwrap();
    output.sync_all().unwrap();
    for case in &cases {
        let directory = proof.join(&case.id);
        std::fs::create_dir(&directory).unwrap();
        let host = Host::open(&directory, case.phase);
        let registration = host
            .catalog
            .register(
                Host::manifest(case.phase, case.timeout_ms, case.max_output_bytes),
                &case.script,
            )
            .unwrap();
        let binding = host
            .catalog
            .bind(
                host.scope.clone(),
                host.ownership.clone(),
                vec![Host::key()],
                &host.policy,
                host.native.digest().into(),
            )
            .unwrap();
        let (sender, mut receiver) = sandbox_process_observation_channel(64).unwrap();
        let native = host.native.clone().with_process_observer(sender.clone());
        let policy = if case.action == "deny-policy" {
            kolyan_agent::hooks::HookAccessPolicy::deny_all()
        } else {
            host.policy.clone()
        };
        let runtime = HookRuntime::new(host.catalog.clone(), policy.clone(), native.clone());
        if case.action == "record-failure" {
            host.journal
                .fail_completion
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if case.action == "revoke" {
            host.catalog
                .revoke(
                    &Host::key(),
                    &registration.reference,
                    "revoke-1".into(),
                    "removed by fixture".into(),
                )
                .unwrap();
        }
        let control = TurnControl::default();
        let request = event(&host, case.phase);
        let window = HookExecutionWindow::new(control.clone(), Duration::from_secs(10)).unwrap();
        let mut execution =
            Box::pin(runtime.dispatch(&binding, request.clone(), "operation-1".into(), window));
        let mut observations = Vec::new();
        let mut trigger_error = None;
        let mut first_result = None;
        let mut recovery_result = None;
        let result = if case.action == "cancel" || case.action == "drop" {
            let observed = tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    tokio::select! {
                        value=&mut execution=>break Err(value),
                        observation=receiver.recv()=>{
                            let observation=observation.expect("observer sender retained");
                            let spawned=matches!(observation.event,SandboxProcessEvent::Spawned);
                            observations.push(observation);
                            if spawned {break Ok(());}
                        }
                    }
                }
            })
            .await;
            match observed {
                Ok(Ok(())) => {
                    if case.action == "cancel" {
                        control.cancel();
                        execution.await
                    } else {
                        drop(execution);
                        let rebuilt = HookRuntime::new(host.catalog.clone(), policy, native);
                        rebuilt
                            .dispatch(
                                &binding,
                                request.clone(),
                                "operation-1".into(),
                                HookExecutionWindow::new(
                                    TurnControl::default(),
                                    Duration::from_secs(10),
                                )
                                .unwrap(),
                            )
                            .await
                    }
                }
                Ok(Err(result)) => {
                    trigger_error = Some("execution returned before Spawn trigger".to_owned());
                    result
                }
                Err(error) => {
                    drop(execution);
                    trigger_error = Some(error.to_string());
                    Err(HookError::Expired)
                }
            }
        } else {
            let first = execution.await;
            if case.action == "replay" {
                first_result = Some(
                    json!({"result":category(&first),"value":first.as_ref().ok(),"error":first.as_ref().err().map(ToString::to_string)}),
                );
                let rebuilt = HookRuntime::new(host.catalog.clone(), policy, native);
                rebuilt
                    .dispatch(
                        &binding,
                        request.clone(),
                        "operation-1".into(),
                        HookExecutionWindow::new(control, Duration::from_secs(10)).unwrap(),
                    )
                    .await
            } else {
                if case.action == "record-failure" {
                    let rebuilt = HookRuntime::new(host.catalog.clone(), policy, native);
                    let recovered = rebuilt
                        .dispatch(
                            &binding,
                            request.clone(),
                            "operation-1".into(),
                            HookExecutionWindow::new(
                                TurnControl::default(),
                                Duration::from_secs(10),
                            )
                            .unwrap(),
                        )
                        .await;
                    recovery_result = Some(
                        json!({"result":category(&recovered),"error":recovered.as_ref().err().map(ToString::to_string)}),
                    );
                }
                first
            }
        };
        // Observe actual cleanup without sleeping to manufacture a phase ordering.
        let requires_spawn = !matches!(case.action.as_str(), "revoke" | "deny-policy");
        let cleanup = if requires_spawn {
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    let complete = observations
                        .iter()
                        .filter(|o| matches!(o.event, SandboxProcessEvent::CaptureCompleted { .. }))
                        .count()
                        == 2;
                    let reaped = observations
                        .iter()
                        .any(|o| matches!(o.event, SandboxProcessEvent::Reaped { .. }));
                    let group = observations
                        .iter()
                        .any(|o| matches!(o.event, SandboxProcessEvent::GroupCleanup { .. }));
                    if complete && reaped && group {
                        break;
                    }
                    observations.push(receiver.recv().await.expect("observer sender retained"));
                }
            })
            .await
            .err()
            .map(|e| e.to_string())
        } else {
            None
        };
        while let Ok(observation) = receiver.try_recv() {
            observations.push(observation);
        }
        let records = host.journal.records();
        let artifacts:Vec<Value>=records.iter().filter_map(|r|r.draft.payload.get("input").or_else(||r.draft.payload.get("output"))).map(|value|{
            let reference:kolyan_trace::ArtifactRef=serde_json::from_value(value.clone()).unwrap();
            let bytes=host.artifacts.read(&reference,1024*1024).unwrap();
            json!({"reference":reference,"content":serde_json::from_slice::<Value>(&bytes).unwrap()})
        }).collect();
        writeln!(output,"{}",json!({"kind":"case","id":case.id,"input":request,"binding":binding,"registration":registration,"script":case.script,"result":category(&result),"value":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string),"first_result":first_result,"recovery_result":recovery_result,"trigger_error":trigger_error,"cleanup_error":cleanup,"observations":observations,"journal":records,"artifacts":artifacts,"lifecycle_dropped":sender.lifecycle_dropped(),"output_dropped":sender.output_dropped(),"forbidden_exists":directory.join("forbidden.txt").exists(),"attempts":1})).unwrap();
        output.sync_all().unwrap();
    }
    output.flush().unwrap();
    output.sync_all().unwrap();
    drop(output);
    println!("HOOK_NATIVE_ACTUAL {}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(&path).unwrap())
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .filter(|r: &Value| r["kind"] == "case")
        .collect();
    assert_eq!(rows.len(), cases.len());
    assert_eq!(expected.len(), cases.len());
    for expectation in expected {
        let row = rows.iter().find(|r| r["id"] == expectation.id).unwrap();
        assert_eq!(
            row["result"],
            expectation.result,
            "{} at {}",
            expectation.id,
            path.display()
        );
        assert!(
            row["trigger_error"].is_null() && row["cleanup_error"].is_null(),
            "{}",
            row["id"]
        );
        let events = row["observations"].as_array().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|o| o["event"]["kind"] == "spawned")
                .count(),
            expectation.spawned,
            "{}",
            row["id"]
        );
        assert_eq!(
            row["journal"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["draft"]["kind"] == "agent.hook.completed")
                .count(),
            expectation.completed,
            "{}",
            row["id"]
        );
        if expectation.spawned > 0 {
            assert_eq!(
                events
                    .iter()
                    .filter(|o| o["event"]["kind"] == "capture_completed")
                    .count(),
                2
            );
            assert!(events.iter().any(|o| o["event"]["kind"] == "reaped"));
            assert!(events.iter().any(|o|o["event"]["kind"]=="group_cleanup"&&o["event"]["error"].is_null()));
        }
        if let Some(cause) = expectation.cause {
            assert!(
                events
                    .iter()
                    .any(|o| o["event"]["kind"] == "cancellation_observed"
                        && o["event"]["cause"] == cause),
                "{}",
                row["id"]
            );
        }
        assert_eq!(row["lifecycle_dropped"], 0);
        assert_eq!(row["forbidden_exists"], false);
        assert_eq!(row["attempts"], 1);
        if row["id"] == "completed-not-replayed" {
            assert_eq!(row["first_result"]["result"], "continue");
        }
        if row["id"] == "completion-storage-fault" {
            assert_eq!(row["recovery_result"]["result"], "interrupted");
        }
    }
}
