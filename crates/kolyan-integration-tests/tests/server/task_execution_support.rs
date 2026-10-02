//! Shared fixture-driven service reconstruction, evidence capture and comparison.
#[path = "task_input_source.rs"]
mod task_input_source;

use std::{fs, io::Write, path::Path, sync::Arc};

use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{ModelProvider, ModelRequest};
use kolyan_policy::{ApprovalMode, PathScope, PolicyEngine};
use kolyan_runtime::{ContentPolicy, DurableTurnResult, ExecutionBinding, LinkedTrajectory};
use kolyan_server::*;
use kolyan_storage::FileSessionStore;
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::{ArtifactStore, NoopTraceSink, Retention};
use serde_json::{Value, json};

type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;

/// Tests retain raw authoritative evidence even when an assertion unwinds.
struct EvidenceCapture<'a> {
    root: &'a Path,
    data: &'a Value,
}

impl Drop for EvidenceCapture<'_> {
    fn drop(&mut self) {
        if let Err(error) = capture_evidence(self.root, self.data) {
            eprintln!("Task evidence export failed: {error}");
        }
    }
}

fn capture_evidence(root: &Path, data: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let journal = SqliteFactJournal::open(root.join("ledger.sqlite"))?;
    task_input_source::export(
        &TaskCoordinator::new(journal.clone()),
        data["task_id"].as_str().unwrap(),
        &root.join("actual-input-sources.jsonl"),
    )?;
    let ledger = SqliteLedger::open(root.join("ledger.sqlite"))?;
    let mut file = fs::File::create(root.join("actual-task.jsonl"))?;
    let mut after = 0;
    loop {
        let page = journal.read(data["task_id"].as_str().unwrap(), after, 512)?;
        if page.is_empty() {
            break;
        }
        after = page.last().unwrap().position;
        for row in page {
            writeln!(file, "{}", serde_json::to_string(&row)?)?;
        }
    }
    for case in data["cases"].as_array().unwrap() {
        let mut file = fs::File::create(
            root.join(format!("{}-ledger.jsonl", case["name"].as_str().unwrap())),
        )?;
        for row in ledger.execution_events_after(case["execution_id"].as_str().unwrap(), 0)? {
            writeln!(file, "{}", serde_json::to_string(&row)?)?;
        }
    }
    Ok(())
}

fn service(root: &Path) -> Service {
    TaskExecutionService::new(
        TaskCoordinator::new(SqliteFactJournal::open(root.join("ledger.sqlite")).unwrap()),
        SessionExecutionService::new(
            ExecutionService::new(
                SqliteLedger::open(root.join("ledger.sqlite")).unwrap(),
                NoopTraceSink,
            ),
            SessionService::new(FileSessionStore::new(root.join("sessions")).unwrap()),
        )
        .with_context_policy(kolyan_storage::SessionContextPolicy::FullTrajectory),
    )
    .with_artifacts(ArtifactStore::new(root.join("artifacts"), 16 * 1024 * 1024).unwrap())
}

fn identity() -> AgentIdentity {
    AgentIdentity {
        definition_id: "personal-agent".into(),
        revision: "r1".into(),
        instance_id: "agent-instance".into(),
    }
}
fn constraints() -> String {
    "a".repeat(64)
}

fn binding(case: &Value, input_source: InvocationInputSource) -> AttemptBinding {
    AttemptBinding {
        attempt_id: case["attempt_id"].as_str().unwrap().into(),
        invocation_id: case["invocation_id"].as_str().unwrap().into(),
        execution: ExecutionRef {
            session_id: case["session_id"].as_str().unwrap().into(),
            turn_id: case["turn_id"].as_str().unwrap().into(),
            execution_id: case["execution_id"].as_str().unwrap().into(),
        },
        agent: identity(),
        constraints_digest: constraints(),
        input_source,
    }
}

fn model_request(
    data: &Value,
    case: &Value,
    family: &str,
    model: &str,
    max_output: Option<u32>,
) -> ModelRequest {
    serde_json::from_value(json!({"request_id":case["turn_id"],"model":{"provider":family,"model":model},
        "system":[{"text":data["system"],"cache":false}],"messages":[{"role":"user","content":[{"type":"text","text":case["input"]}]}],
        "tools":RestrictedFileTool::tool_definitions(),"tool_choice":"auto","output_format":null,"prompt_cache":null,"reasoning":null,"max_output_tokens":max_output,"extensions":{}})).unwrap()
}

/// This Server fixture publishes its actual host request, not Agent-owned input.
fn publish_source(
    service: &Service,
    task: &str,
    inv: &Value,
    request: ModelRequest,
) -> InvocationInputSource {
    let role: InvocationRole = serde_json::from_value(inv["role"].clone()).unwrap();
    let dependencies = if role == InvocationRole::Continuation {
        vec![inv["parent"].as_str().unwrap()]
    } else {
        vec![]
    };
    let definition = json!({
        "invocation_id":inv["id"],"role":inv["role"],"parent_invocation_id":inv["parent"],
        "dependencies":dependencies,"agent":identity(),"constraints_digest":constraints(),
    });
    task_input_source::publish(service.coordinator(), task, &definition, request)
}

fn executor<P: ModelProvider>(
    provider: P,
    root: &Path,
) -> TurnExecutor<P, PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>> {
    let mut policy = PolicyEngine::default();
    for mut manifest in RestrictedFileTool::tool_manifests() {
        manifest.path_scopes = vec![PathScope::new(root.join("safe").to_string_lossy())];
        if manifest.tool_name == "file.write" {
            manifest.approval = ApprovalMode::Always;
        }
        policy.register(manifest);
    }
    policy.restrict_workspace(root.join("safe").to_string_lossy());
    let policy = Arc::new(policy);
    TurnExecutor::with_tools(
        provider,
        PolicyEnforcingTool::new(RestrictedFileTool::new(root), policy.clone()),
    )
    .with_policy_engine(policy)
}

pub(super) async fn run_task<P: ModelProvider, F: Fn(&Value) -> P>(
    root: &Path,
    data: &Value,
    family: &str,
    model: &str,
    make: F,
    max_output: Option<u32>,
) {
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    let _capture = EvidenceCapture { root, data };
    let task = data["task_id"].as_str().unwrap();
    let initial = service(root);
    initial
        .coordinator()
        .register_task(
            "task-registration",
            TaskDefinition {
                task_id: task.into(),
                objective: data["objective"].as_str().unwrap().into(),
                criteria: data["invocations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|inv| CompletionCriterion::ExecutionCompleted {
                        id: format!("{}-complete", inv["id"].as_str().unwrap()),
                        invocation_id: inv["id"].as_str().unwrap().into(),
                    })
                    .collect(),
                agent: identity(),
                constraints_digest: constraints(),
                limits: serde_json::from_value(data["limits"].clone()).unwrap(),
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    for inv in data["invocations"].as_array().unwrap() {
        if inv["admit"] == "after_parent" {
            continue;
        }
        let id = inv["id"].as_str().unwrap();
        let case = data["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["invocation_id"] == id)
            .unwrap();
        let input_source = publish_source(
            &initial,
            task,
            inv,
            model_request(data, case, family, model, max_output),
        );
        initial
            .coordinator()
            .admit_invocation(
                task,
                &format!("admit-{id}"),
                InvocationDefinition {
                    invocation_id: id.into(),
                    agent: identity(),
                    constraints_digest: constraints(),
                    role: serde_json::from_value(inv["role"].clone()).unwrap(),
                    parent_invocation_id: inv["parent"].as_str().map(str::to_owned),
                    dependencies: vec![],
                    input_source,
                },
            )
            .unwrap();
        if let Some(parent) = inv["parent"].as_str() {
            initial
                .coordinator()
                .admit_dependency(task, &format!("depend-{id}"), parent, id)
                .unwrap();
        }
    }
    drop(initial);
    for case in data["cases"].as_array().unwrap() {
        let current = service(root);
        let input_source = if case["admit"] == "after_parent" {
            let inv = data["invocations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|inv| inv["id"] == case["invocation_id"])
                .unwrap();
            publish_source(
                &current,
                task,
                inv,
                model_request(data, case, family, model, max_output),
            )
        } else {
            current.coordinator().snapshot(task).unwrap().invocations
                [case["invocation_id"].as_str().unwrap()]
            .definition
            .input_source
            .clone()
        };
        let bound = binding(case, input_source);
        if case["admit"] == "after_parent" {
            let snapshot = current.coordinator().snapshot(task).unwrap();
            let parent = &snapshot.invocations["root"];
            assert_eq!(parent.state, InvocationState::Completed);
            current
                .coordinator()
                .admit_invocation(
                    task,
                    &format!("admit-{}", bound.invocation_id),
                    InvocationDefinition {
                        invocation_id: bound.invocation_id.clone(),
                        agent: identity(),
                        constraints_digest: constraints(),
                        role: InvocationRole::Continuation,
                        parent_invocation_id: Some("root".into()),
                        dependencies: vec!["root".into()],
                        input_source: bound.input_source.clone(),
                    },
                )
                .unwrap();
            let prior = snapshot.attempts[parent.attempts.last().unwrap()]
                .observation
                .as_ref()
                .unwrap();
            let AttemptOutcome::Completed { evidence } = &prior.outcome else {
                panic!("missing parent proof");
            };
            current
                .coordinator()
                .consume_child_result(
                    task,
                    &format!("consume-parent-{}", bound.invocation_id),
                    &bound.invocation_id,
                    ConsumedResult {
                        child_invocation_id: "root".into(),
                        completion_fact: parent.completion_fact.clone().unwrap(),
                        evidence: evidence.clone(),
                    },
                )
                .unwrap();
        }
        if case["reuse_session"] != true {
            current
                .sessions()
                .sessions()
                .create(&bound.execution.session_id)
                .unwrap();
        }
        let request = model_request(data, case, family, model, max_output);
        let (_, mut result) = current
            .run(
                task,
                bound.clone(),
                executor(make(case), &root.join("workspace")),
                TurnRequest {
                    turn_id: bound.execution.turn_id.clone(),
                    model_request: request,
                    config: TurnConfig {
                        max_steps: 12,
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap();
        if case["approval"] == true {
            let DurableTurnResult::Suspended { suspension, .. } = result else {
                panic!("expected task approval");
            };
            let approval = suspension.waiting.approvals.first().unwrap();
            assert!(
                !root
                    .join("workspace")
                    .join(case["path"].as_str().unwrap())
                    .exists()
            );
            let saved = current.coordinator().snapshot(task).unwrap();
            assert_eq!(
                saved.attempts[&bound.attempt_id].state,
                InvocationState::Suspended
            );
            drop(current);
            let rebuilt = service(root);
            assert_eq!(rebuilt.coordinator().snapshot(task).unwrap(), saved);
            for guard in data["approval_rejections"].as_array().unwrap() {
                let mut rejected = bound.clone();
                let mut rejected_approval = approval.approval_id.clone();
                match guard["mutation"].as_str().unwrap() {
                    "agent_revision" => rejected.agent.revision = "unadmitted-r2".into(),
                    "constraints" => rejected.constraints_digest = "b".repeat(64),
                    "approval_id" => rejected_approval = "foreign-approval".into(),
                    other => panic!("unknown approval rejection: {other}"),
                }
                assert!(
                    rebuilt
                        .resume_approval(
                            task,
                            rejected,
                            &rejected_approval,
                            executor(make(case), &root.join("workspace"))
                        )
                        .await
                        .is_err(),
                    "{}",
                    guard["name"]
                );
                assert_eq!(rebuilt.coordinator().snapshot(task).unwrap(), saved);
                assert!(
                    !root
                        .join("workspace")
                        .join(case["path"].as_str().unwrap())
                        .exists()
                );
            }
            let (_, resumed) = rebuilt
                .resume_approval(
                    task,
                    bound.clone(),
                    &approval.approval_id,
                    executor(make(case), &root.join("workspace")),
                )
                .await
                .unwrap();
            result = resumed;
        }
        let DurableTurnResult::Completed(execution, _) = result else {
            panic!("expected task attempt completion");
        };
        assert!(
            execution.result.steps.len() as u64
                >= case["expected"]["minimum_steps"].as_u64().unwrap()
        );
        verify_case(root, data, case, &bound);
        let rebuilt = service(root);
        let snapshot = rebuilt.coordinator().snapshot(task).unwrap();
        assert_eq!(
            serde_json::to_value(snapshot.attempts[&bound.attempt_id].state).unwrap(),
            case["expected"]["state"]
        );
        let child = &snapshot.invocations[&bound.invocation_id];
        if matches!(
            child.definition.role,
            InvocationRole::SelfCall | InvocationRole::Delegation
        ) {
            let AttemptOutcome::Completed { evidence } = snapshot.attempts[&bound.attempt_id]
                .observation
                .as_ref()
                .unwrap()
                .outcome
                .clone()
            else {
                panic!("missing child proof");
            };
            assert_ne!(
                snapshot.state,
                TaskState::Completed,
                "child completion cannot complete parent task"
            );
            rebuilt
                .coordinator()
                .consume_child_result(
                    task,
                    &format!("consume-{}", bound.invocation_id),
                    child.definition.parent_invocation_id.as_ref().unwrap(),
                    ConsumedResult {
                        child_invocation_id: bound.invocation_id.clone(),
                        completion_fact: child.completion_fact.clone().unwrap(),
                        evidence,
                    },
                )
                .unwrap();
        }
    }
    let finished = service(root);
    let snapshot = finished.complete(task, "task-completed").unwrap();
    let expected = &data["expected_task"];
    assert_eq!(
        serde_json::to_value(snapshot.state).unwrap(),
        expected["state"]
    );
    assert_eq!(snapshot.invocations.len() as u64, expected["invocations"]);
    assert_eq!(snapshot.attempts.len() as u64, expected["attempts"]);
    assert_eq!(
        snapshot.invocations["root"].consumed_results.len() as u64,
        expected["consumed_children"]
    );
    assert_eq!(snapshot.success_evidence.len() as u64, expected["evidence"]);
    assert_eq!(finished.complete(task, "task-completed").unwrap(), snapshot);
    assert_eq!(
        service(root).coordinator().snapshot(task).unwrap(),
        snapshot
    );
    let records = finished
        .coordinator()
        .journal()
        .read(task, 0, 1024)
        .unwrap();
    for forbidden in data["forbidden_journal_kinds"].as_array().unwrap() {
        assert!(
            !records
                .iter()
                .any(|r| r.draft.kind == forbidden.as_str().unwrap())
        );
    }
    fs::write(
        root.join("result.json"),
        serde_json::to_vec_pretty(&snapshot).unwrap(),
    )
    .unwrap();
}

fn verify_case(root: &Path, data: &Value, case: &Value, bound: &AttemptBinding) {
    let ledger = SqliteLedger::open(root.join("ledger.sqlite")).unwrap();
    let linked = ExecutionBinding {
        task_id: data["task_id"].as_str().unwrap().into(),
        invocation_id: bound.invocation_id.clone(),
        attempt_id: bound.attempt_id.clone(),
        session_id: bound.execution.session_id.clone(),
        turn_id: bound.execution.turn_id.clone(),
        execution_id: bound.execution.execution_id.clone(),
    };
    let mut rows = Vec::new();
    let mut after = 0;
    loop {
        let page =
            LinkedTrajectory::load(&ledger, &linked, after, 512, ContentPolicy::Full).unwrap();
        if page.records.is_empty() {
            break;
        }
        after = page.records.last().unwrap().cursor;
        rows.extend(page.records);
    }
    let mut file =
        fs::File::create(root.join(format!("{}.jsonl", case["name"].as_str().unwrap()))).unwrap();
    for row in &rows {
        writeln!(file, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    let calls = rows
        .iter()
        .filter(|row| row.kind == LedgerEventKind::ToolCallRequested)
        .collect::<Vec<_>>();
    let receipts = rows
        .iter()
        .filter(|row| row.kind == LedgerEventKind::EffectReceipt)
        .collect::<Vec<_>>();
    assert_eq!(calls.len() as u64, case["expected"]["calls"]);
    assert_eq!(receipts.len() as u64, case["expected"]["receipts"]);
    for call in calls {
        let receipt = receipts
            .iter()
            .find(|receipt| receipt.payload["output"]["call_id"] == call.payload["call_id"])
            .unwrap();
        assert_eq!(
            receipt.payload["input"]["prepared"]["call"]["name"],
            call.payload["name"]
        );
        assert_eq!(
            receipt.payload["input"]["prepared"]["call"]["arguments"],
            call.payload["arguments"]
        );
        assert_eq!(
            receipt.payload["input"]["prepared"]["call"]["id"],
            call.payload["call_id"]
        );
        assert_eq!(
            receipt.payload["input"]["scope"]["execution"]["execution_id"],
            receipt.binding.execution_id
        );
        assert_eq!(
            receipt.payload["input"]["scope"]["execution"]["turn_id"],
            receipt.binding.turn_id
        );
        assert_eq!(receipt.payload["output"]["is_error"], false);
        assert!(rows.iter().any(|row| {
            row.kind == LedgerEventKind::ModelRequested
                && row.cursor > receipt.cursor
                && row.payload["request"]["messages"]
                    .to_string()
                    .contains(&receipt.payload["output"].to_string())
        }));
    }
    if case["path"].is_string() {
        assert_eq!(
            fs::read_to_string(root.join("workspace").join(case["path"].as_str().unwrap()))
                .unwrap(),
            case["marker"].as_str().unwrap()
        );
    }
    let last = rows
        .iter()
        .rev()
        .find(|row| row.kind == LedgerEventKind::StepCompleted)
        .unwrap();
    let text = last.payload["step"]["response"]["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["type"] == "text")
        .map(|v| v["text"].as_str().unwrap())
        .collect::<String>();
    if let Some(markers) = case["expected"]["markers"].as_array() {
        for marker in markers {
            assert!(text.contains(marker.as_str().unwrap()));
        }
    }
    let bytes = serde_json::to_vec(&last.payload["step"]["response"]).unwrap();
    let store = ArtifactStore::new(root.join("artifacts"), 16 * 1024 * 1024).unwrap();
    let reference = store.put(&bytes, Retention::Required).unwrap();
    assert_eq!(
        store.read(&reference, reference.byte_length).unwrap(),
        bytes
    );
    let metadata =
        LinkedTrajectory::load(&ledger, &linked, 0, 512, ContentPolicy::MetadataOnly).unwrap();
    assert!(
        !serde_json::to_string(&metadata)
            .unwrap()
            .contains("TASK_ALPHA_7392")
    );
    assert!(
        !serde_json::to_string(&metadata)
            .unwrap()
            .contains("TASK_BETA_8463")
    );
}
