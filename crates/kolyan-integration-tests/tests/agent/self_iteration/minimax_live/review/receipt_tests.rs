//! Real native read receipts; adversarial copies are explicitly non-authoritative.

use std::{
    collections::VecDeque,
    os::unix::fs::PermissionsExt,
    sync::{Arc, Mutex},
};

use futures_util::stream;
use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentSelector,
    EnvironmentTool, EnvironmentToolFactory,
};
use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{LedgerStore, SqliteLedger};
use kolyan_model::{ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ProviderFuture};
use kolyan_runtime::{DurableTurnDriver, DurableTurnResult};
use kolyan_trace::NoopTraceSink;

use super::super::super::driver;
use super::super::{RunConfig, tools::Factory};
use super::*;
use crate::evidence::Evidence;
use crate::tools::worker;

struct Script {
    frames: Mutex<VecDeque<Vec<ContentBlock>>>,
    evidence: Arc<Evidence>,
}
impl ModelProvider for Script {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            self.evidence.append(json!({"event":"offline_review_model_request","request":request,"provenance":"data-owned script; no network"})).unwrap();
            let content = self
                .frames
                .lock()
                .unwrap()
                .pop_front()
                .expect("bounded offline script");
            let stop_reason = if content
                .iter()
                .any(|b| matches!(b, ContentBlock::ToolCall { .. }))
            {
                kolyan_model::StopReason::ToolUse
            } else {
                kolyan_model::StopReason::EndTurn
            };
            let response = ModelResponse {
                id: request.request_id,
                model: request.model,
                content,
                structured_output: None,
                stop_reason,
                usage: Default::default(),
                metadata: json!({"provenance":"offline-data-owned-script"}),
            };
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

#[tokio::test]
async fn dataset_receipt_authority_failures_export_before_comparison() {
    let fixture = super::super::repair_fixture();
    let root = tempfile::Builder::new()
        .prefix("kolyan-self-review-native-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    let workspace = root.join("worktree");
    let control = root.join("control");
    for directory in [
        &workspace,
        &control,
        &control.join("state"),
        &control.join("staging"),
        &workspace.join(".git"),
    ] {
        std::fs::create_dir(directory).unwrap();
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    for path in &fixture.task.allowlist {
        let file = workspace.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, &fixture.read_fixture_content).unwrap();
    }
    let evidence = Arc::new(Evidence::new(&control.join("actual.jsonl")));
    let installation = worker::WorkerRun::prepare().await;
    worker::initialize_worker(&control, &evidence, &installation).unwrap();
    let plan = super::super::with_run(RunConfig {
        worktree: workspace,
        host_private: root.clone(),
        branch: "offline-native-review".into(),
        head: "0".repeat(40),
        source_snapshot_sha256: "0".repeat(64),
        baseline_manifest: "unused-offline-manifest".into(),
        baseline_manifest_sha256: "0".repeat(64),
        baseline_files: 0,
        baseline_server_tests: 0,
    });
    let permissions = AgentPermissions {
        tools: [EnvironmentTool::Read].into(),
        delegation: Default::default(),
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "native-review".into(),
        revision: "r1".into(),
        display_name: None,
        model: kolyan_model::ModelRef::new("fixture", "native-review"),
        instructions: "Offline data-owned read-only native proof.".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let key = definition.key();
    let mut catalog = AgentCatalog::new(1).unwrap();
    catalog.register(definition).unwrap();
    let snapshot = catalog
        .resolve(
            &AgentSelector::Named(key),
            "native-review-instance",
            &permissions,
            &permissions,
        )
        .unwrap();
    let bound = AttemptBinding {
        attempt_id: "native-review-attempt".into(),
        invocation_id: "native-review".into(),
        execution: kolyan_server::ExecutionRef {
            session_id: "native-review-session".into(),
            turn_id: "native-review-turn".into(),
            execution_id: "native-review-execution".into(),
        },
        agent: snapshot.identity().clone(),
        constraints_digest: snapshot.digest().into(),
        input_source: kolyan_server::InvocationInputSource::Standalone {
            fact: kolyan_ledger::FactRef {
                stream_id: "offline-audit-coordinate".into(),
                position: 1,
                fact_id: "not-a-task-admission".into(),
            },
        },
    };
    let set = Factory {
        plan: plan.base.clone(),
        control: control.clone(),
        evidence: evidence.clone(),
    }
    .build(&snapshot, &bound.execution)
    .unwrap();
    let mut request = driver::request(
        &plan,
        "native-review",
        "Use the fixture's declared four reads.".into(),
    );
    request.tools = set.definitions;
    let script = Script {
        frames: Mutex::new(VecDeque::from([
            fixture
                .read_script
                .iter()
                .cloned()
                .map(|call| ContentBlock::ToolCall { call })
                .collect(),
            vec![ContentBlock::Text {
                text: "Native offline fixture reads observed.".into(),
            }],
        ])),
        evidence: evidence.clone(),
    };
    let ledger = SqliteLedger::open(control.join("state/ledger.sqlite")).unwrap();
    let runtime = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    let result = runtime
        .start(
            TurnExecutor::with_tools(script, set.executor)
                .with_policy_engine(set.policy)
                .with_agent_snapshot_digest(snapshot.digest().into()),
            TurnRequest {
                turn_id: bound.execution.turn_id.clone(),
                config: TurnConfig {
                    max_steps: 3,
                    max_tool_calls: Some(4),
                    deadline: None,
                },
                model_request: request,
            },
            &bound.execution.session_id,
            &bound.execution.execution_id,
        )
        .await;
    let baseline = ledger.events_after(0).unwrap();
    for event in &baseline {
        evidence
            .append(json!({"event":"actual_native_read_ledger","value":event}))
            .unwrap();
    }
    evidence.append(json!({"event":"native_read_run_result","error":result.as_ref().err().map(ToString::to_string),"completed":matches!(result,Ok(DurableTurnResult::Completed(..))),"audit_binding":bound,"binding_provenance":"independent offline Runtime coordinates; not Agent Task admission","network_calls":0})).unwrap();
    let digest = super::super::baseline::digest(fixture.read_fixture_content.as_bytes());
    let expected = fixture
        .task
        .allowlist
        .iter()
        .map(|p| (p.clone(), digest.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::new();
    for case in fixture.receipt_cases {
        let mut events = baseline.clone();
        match case.mutation.as_str() {
            "success" => {}
            "missing_path" => events.retain(|e| {
                !(e.kind == LedgerEventKind::EffectReceipt
                    && e.payload["input"]["prepared"]["call"]["arguments"]["path"]
                        == fixture.task.allowlist[0])
            }),
            "missing_prepared" => events.retain(|e| e.kind != LedgerEventKind::EffectPrepared),
            "missing_started" => events.retain(|e| e.kind != LedgerEventKind::EffectStarted),
            "missing_authorization" => {
                events.retain(|e| e.kind != LedgerEventKind::EffectAuthorized)
            }
            "missing_completed" => events.retain(|e| e.kind != LedgerEventKind::EffectCompleted),
            "wrong_call" => {
                for e in &mut events {
                    if e.kind == LedgerEventKind::StepCompleted
                        && e.payload["step"]["response"]["content"][0]["type"] == "tool_call"
                    {
                        e.payload["step"]["response"]["content"][0]["call"]["id"] =
                            json!("not-issued");
                    }
                }
            }
            "wrong_step" => {
                for e in &mut events {
                    if e.kind == LedgerEventKind::StepCompleted {
                        e.payload["step"]["step_id"] = json!("foreign-step");
                    }
                }
            }
            "call_in_prose" => {
                for e in &mut events {
                    if e.kind == LedgerEventKind::StepCompleted {
                        let text = e.payload["step"]["response"]["content"].to_string();
                        e.payload["step"]["response"]["content"] =
                            serde_json::to_value(vec![ContentBlock::Text { text }]).unwrap();
                    }
                }
            }
            "foreign_authorization_effect" => {
                for e in &mut events {
                    if e.kind == LedgerEventKind::EffectAuthorized {
                        e.payload["effect_id"] = json!("foreign-effect");
                    }
                }
            }
            "started_wrong_input_digest" => {
                for e in &mut events {
                    if e.kind == LedgerEventKind::EffectStarted {
                        e.payload["input_digest"] = json!("f".repeat(64));
                    }
                }
            }
            "out_of_order" => {
                for e in &mut events {
                    if e.kind == LedgerEventKind::EffectStarted {
                        e.cursor = 1;
                    }
                }
            }
            "borrowed_stage_receipt" => {
                for e in &mut events {
                    if matches!(
                        e.kind,
                        LedgerEventKind::EffectPrepared
                            | LedgerEventKind::EffectAuthorized
                            | LedgerEventKind::EffectStarted
                            | LedgerEventKind::EffectReceipt
                            | LedgerEventKind::EffectCompleted
                    ) {
                        e.execution_id = "previous-stage-execution".into();
                    }
                }
            }
            mutation => {
                for e in &mut events {
                    if e.kind == LedgerEventKind::EffectReceipt {
                        match mutation {
                            "failed_result" => e.payload["output"]["is_error"] = json!(true),
                            "wrong_execution" => e.execution_id = "foreign-execution".into(),
                            "wrong_turn" => e.turn_id = "foreign-turn".into(),
                            "wrong_snapshot" => {
                                e.payload["input"]["scope"]["agent_snapshot_digest"] =
                                    json!("d".repeat(64))
                            }
                            "wrong_grant" => e.payload["prepared_grant"] = json!({}),
                            "digest_mismatch" => {
                                let mut output: Value = serde_json::from_str(
                                    e.payload["output"]["content"].as_str().unwrap(),
                                )
                                .unwrap();
                                output["sha256"] = json!("f".repeat(64));
                                e.payload["output"]["content"] = json!(output.to_string());
                            }
                            other => panic!("unknown receipt mutation {other}"),
                        }
                    }
                }
            }
        }
        let checks = receipt_checks(&events, &bound, &expected);
        let observed = require_receipts(&checks);
        let row = json!({"mutation":case.mutation,"negative_copy_not_authority":case.mutation!="success","source":"actual native Runtime read ledger","events":events,"checks":checks,"result":observed.as_ref().map_err(ToString::to_string),"valid":observed.is_ok(),"expected":case.expected_valid});
        evidence.append(row.clone()).unwrap();
        rows.push(row);
    }
    println!(
        "SELF_REVIEW_RECEIPTS_TRACE={}",
        control.join("actual.jsonl").display()
    );
    assert!(
        matches!(result, Ok(DurableTurnResult::Completed(..))),
        "actual native run: {result:?}"
    );
    drop(evidence);
    let rows = super::super::tests::read_rows(&control.join("actual.jsonl"));
    for row in rows.into_iter().filter(|r| r.get("mutation").is_some()) {
        assert_eq!(row["valid"], row["expected"], "{row}");
    }
}
