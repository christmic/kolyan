//! Ten independently admitted Turns with actual OS effects and durable recovery.
//! Offline frames are data, never injected into the live Provider. Session
//! continuation is not a Task InvocationRole::Continuation or context reduction.
#![cfg(target_os = "macos")]

#[path = "../common/mod.rs"]
mod common;
#[path = "long_task/continuation.rs"]
mod continuation;
#[allow(dead_code)]
mod data;
#[allow(dead_code)]
mod evidence;
#[path = "../common/matrix.rs"]
#[allow(dead_code)]
mod matrix;
#[path = "long_task/minimax_live.rs"]
mod minimax_live;
mod providers;
mod tools;
#[allow(dead_code)]
mod usage;

use evidence::Evidence;
use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentRunner,
    AgentSelector, EnvironmentTool, RootApprovalResumeRequest, RootRunRequest,
    binding::AgentInvocationBindingStore,
};
use kolyan_core::{TurnConfig, TurnOutcome, TurnRequest};
use kolyan_ledger::{
    FactDraft, FactJournal, FactRef, FactSubject, LedgerEventKind, LedgerStore, SqliteFactJournal,
    SqliteLedger,
};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelProvider, ModelRef, ModelRequest, SystemInstruction,
    ToolChoice,
};
use kolyan_policy::{ApprovalMode, ToolManifest};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{
    CancellationPolicy, ExecutionRef, ExecutionService, InstanceRegistry, SessionExecutionService,
    SessionService, TaskCoordinator, TaskExecutionService, TaskLimits, TaskState,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy, SessionStore};
use kolyan_trace::{ArtifactStore, NoopTraceSink, Retention};
use providers::Providers;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::Arc,
};
use tools::Tools;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    schema_version: u32,
    configured_combinations: usize,
    selectors: Vec<String>,
    instructions: String,
    artifact_path: String,
    history_marker: String,
    inspection_window_assumption_tokens: u32,
    output_reserve_tokens: u32,
    max_steps: usize,
    max_tool_calls: usize,
    max_suspensions: usize,
    approved_read_turns: Vec<String>,
    policy: Vec<ToolManifest>,
    turns: Vec<ScenarioTurn>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScenarioTurn {
    id: String,
    input: String,
    script: Vec<data::Frame>,
}

fn case() -> Case {
    serde_json::from_str(include_str!("../fixtures/agent/long_task.json")).unwrap()
}
fn expectations() -> Vec<Value> {
    include_str!("../expected/agent/long_task.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn dataset(case: &Case) -> data::Dataset {
    data::Dataset {
        schema_version: case.schema_version,
        configured_combinations: case.configured_combinations,
        selectors: case.selectors.clone(),
        instructions: case.instructions.clone(),
        inspection_window_assumption_tokens: case.inspection_window_assumption_tokens,
        output_reserve_tokens: case.output_reserve_tokens,
        max_steps: case.max_steps,
        max_tool_calls: case.max_tool_calls,
        policy_scope: "safe".into(),
        shell_policy_scope: ".".into(),
        policy: case.policy.clone(),
        turns: case
            .turns
            .iter()
            .map(|turn| data::Turn {
                id: turn.id.clone(),
                input: turn.input.clone(),
                script: turn.script.clone(),
                // Shared factories do not read the root harness's expectation field.
                // This target's semantic oracle is exclusively long_task.jsonl.
                expected: data::Expected {
                    file: BTreeMap::new(),
                    tool_minimums: BTreeMap::new(),
                    outcome: String::new(),
                    history_contains: vec![],
                    input_tokens: 0,
                    output_tokens: 0,
                },
            })
            .collect(),
    }
}

type Runner =
    AgentRunner<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore, Providers, Tools>;
struct Host {
    runner: Arc<Runner>,
    ledger: SqliteLedger,
    journal: SqliteFactJournal,
}
impl Host {
    fn open(
        root: &Path,
        source: &data::Dataset,
        definition: Option<&AgentDefinition>,
        permissions: &AgentPermissions,
        live: Option<Arc<dyn ModelProvider>>,
        evidence: Arc<Evidence>,
        offset: Option<(&str, usize)>,
    ) -> Self {
        let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
        let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
        let service = Arc::new(TaskExecutionService::new(
            TaskCoordinator::new(journal.clone()),
            SessionExecutionService::new(
                ExecutionService::new(ledger.clone(), NoopTraceSink),
                SessionService::new(FileSessionStore::new(root.join("state/sessions")).unwrap()),
            )
            .with_context_policy(SessionContextPolicy::FullTrajectory),
        ));
        let mut catalog = AgentCatalog::new(8).unwrap();
        if let Some(definition) = definition {
            catalog.register(definition.clone()).unwrap();
        }
        let mut script = source.clone();
        if live.is_none()
            && let Some((turn, count)) = offset
        {
            script
                .turns
                .iter_mut()
                .find(|item| item.id == turn)
                .unwrap()
                .script
                .drain(..count);
        }
        let port: Arc<dyn FactJournal> = Arc::new(journal.clone());
        let runner = Arc::new(
            AgentRunner::new(
                service,
                InstanceRegistry::new(port.clone(), "long-task-host", 128).unwrap(),
                AgentInvocationBindingStore::new(port),
                catalog,
                permissions.clone(),
                (
                    Providers {
                        live,
                        dataset: script,
                        evidence: evidence.clone(),
                    },
                    Tools {
                        root: root.into(),
                        dataset: source.clone(),
                        evidence,
                    },
                ),
                Arc::new(
                    ArtifactStore::new(root.join("state/artifacts"), 16 * 1024 * 1024).unwrap(),
                ),
            )
            .unwrap(),
        );
        Self {
            runner,
            ledger,
            journal,
        }
    }

    fn export(
        &self,
        execution: &ExecutionRef,
        task: &str,
        root: &Path,
        evidence: &Evidence,
    ) -> Vec<kolyan_ledger::LedgerEvent> {
        let events = self
            .ledger
            .execution_events_after(&execution.execution_id, 0)
            .unwrap();
        for event in &events {
            evidence
                .append(json!({"event":"ledger","value":event}))
                .unwrap();
        }
        let mut after = 0;
        loop {
            let page = self.journal.read(task, after, 512).unwrap();
            if page.is_empty() {
                break;
            }
            after = page.last().unwrap().position;
            for fact in page {
                evidence
                    .append(json!({"event":"journal","value":fact}))
                    .unwrap();
            }
        }
        let session = FileSessionStore::new(root.join("state/sessions"))
            .unwrap()
            .load(&execution.session_id)
            .unwrap();
        evidence
            .append(json!({"event":"session_snapshot","session":session}))
            .unwrap();
        events
    }
}

async fn run(
    case: Case,
    selector: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &tools::worker::WorkerRun,
) {
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-long-task-")
        .tempdir()
        .unwrap()
        .keep();
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join("staging"))
            .unwrap();
    }
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    tools::initialize_worker(&root, &evidence, installation).unwrap();
    println!("LONG_TASK_TRACE={}", root.join("actual.jsonl").display());
    evidence.append(json!({"event":"plan","selector":selector,"model":model,"mode":if live.is_some(){"actual_model"}else{"offline_scripted_real_os"},"budget_mode":"Inspect","counter":"Unsupported","strict_token_acceptance":false,"context_reduction_acceptance":false,"continuation":"new_admitted_turn_in_saved_logical_session_not_task_graph_continuation"})).unwrap();
    SessionService::new(FileSessionStore::new(root.join("state/sessions")).unwrap())
        .create("logical-session")
        .unwrap();
    let permissions = AgentPermissions {
        tools: [
            EnvironmentTool::Read,
            EnvironmentTool::Write,
            EnvironmentTool::Edit,
        ]
        .into_iter()
        .collect(),
        delegation: Default::default(),
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "long-root".into(),
        revision: "r1".into(),
        display_name: (selector == "named").then(|| "Long root".into()),
        model: model.clone(),
        instructions: case.instructions.clone(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let expected = expectations();
    let mut instances = BTreeSet::new();
    let mut all_events = vec![];
    let mut completion_facts = vec![];
    let mut retained_tool_results = vec![];
    let mut previous_messages = vec![];
    for (index, turn) in case.turns.iter().enumerate() {
        let mut source = dataset(&case);
        for manifest in &mut source.policy {
            if manifest.tool_name == "file.read" && case.approved_read_turns.contains(&turn.id) {
                manifest.approval = ApprovalMode::Always;
            }
        }
        let task = format!("task-{}", turn.id);
        let execution = ExecutionRef {
            session_id: "logical-session".into(),
            turn_id: format!("turn-{}", turn.id),
            execution_id: format!("execution-{}", turn.id),
        };
        let mut host = Host::open(
            &root,
            &source,
            Some(&definition),
            &permissions,
            live.clone(),
            evidence.clone(),
            None,
        );
        evidence
            .append(json!({"event":"host_rebuilt_for_turn","turn":turn.id,"index":index}))
            .unwrap();
        let request = ModelRequest {
            request_id: format!("input-{}", turn.id),
            model: model.clone(),
            system: vec![SystemInstruction {
                text:
                    "Execute the current operations with the admitted file tools; preserve history."
                        .into(),
                cache: false,
            }],
            messages: vec![Message {
                role: MessageRole::User,
                content: vec![ContentBlock::Text {
                    text: turn.input.clone(),
                }],
            }],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(case.output_reserve_tokens),
            extensions: Value::Null,
        };
        let mut outcome = host
            .runner
            .start(RootRunRequest {
                goals: vec![],
                task_id: task.clone(),
                invocation_id: "root".into(),
                attempt_id: "attempt-1".into(),
                execution: execution.clone(),
                selector: match selector {
                    "named" => AgentSelector::Named(definition.key()),
                    "inline" => AgentSelector::Inline(definition.clone()),
                    other => panic!("unknown selector {other}"),
                },
                requested_permissions: permissions.clone(),
                objective: turn.input.clone(),
                limits: TaskLimits {
                    max_depth: 1,
                    max_invocations: 1,
                    max_attempts: 1,
                    max_tokens: None,
                    max_steps_per_turn: u32::try_from(case.max_steps).unwrap(),
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
                turn: TurnRequest {
                    turn_id: execution.turn_id.clone(),
                    config: TurnConfig {
                        max_steps: case.max_steps,
                        max_tool_calls: Some(case.max_tool_calls),
                        deadline: None,
                    },
                    model_request: request,
                },
            })
            .await;
        let mut waits = 0;
        let mut snapshot = None;
        loop {
            let events = host.export(&execution, &task, &root, &evidence);
            evidence.append(json!({"event":"runner_observed","turn":turn.id,"error":outcome.as_ref().err().map(ToString::to_string),"task":outcome.as_ref().ok().map(|result|&result.task),"snapshot":outcome.as_ref().ok().map(|result|&result.snapshot)})).unwrap();
            let result = outcome.unwrap();
            if let Some(saved) = &snapshot {
                assert_eq!(saved, &result.snapshot);
            } else {
                snapshot = Some(result.snapshot.clone());
            }
            match result.execution {
                DurableTurnResult::Suspended { suspension, .. } => {
                    let receipts = events
                        .iter()
                        .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
                        .count();
                    evidence.append(json!({"event":"waiting_restart","turn":turn.id,"prior_receipts":receipts,"suspension":suspension})).unwrap();
                    assert!(waits < case.max_suspensions);
                    assert_eq!(suspension.waiting.approvals.len(), 1);
                    assert!(suspension.waiting.external_waits.is_empty());
                    let approval = &suspension.waiting.approvals[0];
                    let effect = format!(
                        "{}/{}",
                        suspension.checkpoint.scope.step_id, approval.call_id
                    );
                    assert!(
                        !events
                            .iter()
                            .any(|event| event.kind == LedgerEventKind::EffectStarted
                                && event.payload["effect_id"] == effect)
                    );
                    let confirmation = RootApprovalResumeRequest {
                        task_id: task.clone(),
                        invocation_id: "root".into(),
                        logical_session_id: execution.session_id.clone(),
                        attempt_id: "attempt-1".into(),
                        approval_id: approval.approval_id.clone(),
                    };
                    let consumed = events
                        .iter()
                        .filter(|event| event.kind == LedgerEventKind::ModelRequested)
                        .count();
                    let before = FileSessionStore::new(root.join("state/sessions"))
                        .unwrap()
                        .load(&execution.session_id)
                        .unwrap();
                    drop(suspension);
                    drop(host);
                    host = Host::open(
                        &root,
                        &source,
                        None,
                        &permissions,
                        live.clone(),
                        evidence.clone(),
                        Some((&turn.id, consumed)),
                    );
                    let after = FileSessionStore::new(root.join("state/sessions"))
                        .unwrap()
                        .load(&execution.session_id)
                        .unwrap();
                    evidence.append(json!({"event":"host_restored_at_wait","turn":turn.id,"before":before,"after":after,"catalog":"empty","script_offset_applies_only_offline":consumed})).unwrap();
                    assert_eq!(before, after);
                    outcome = host.runner.resume_approval(confirmation).await;
                    waits += 1;
                }
                DurableTurnResult::Completed(completed, _) => {
                    let physical = fs::read(root.join("workspace").join(&case.artifact_path));
                    evidence.append(json!({"event":"physical_file_observed","turn":turn.id,"path":case.artifact_path,"bytes":physical.as_ref().ok(),"error":physical.as_ref().err().map(ToString::to_string),"actual_turn":{"turn_id":completed.result.turn_id,"steps":completed.result.steps,"end_reason":format!("{:?}",completed.result.end_reason)}})).unwrap();
                    let bytes = physical.unwrap();
                    let artifacts =
                        ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap();
                    let artifact = artifacts.put(&bytes, Retention::Required).unwrap();
                    drop(artifacts);
                    let verified =
                        ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024)
                            .unwrap()
                            .read(&artifact, 4 * 1024 * 1024)
                            .unwrap();
                    let session = FileSessionStore::new(root.join("state/sessions"))
                        .unwrap()
                        .load(&execution.session_id)
                        .unwrap();
                    evidence.append(json!({"event":"turn_completed","turn":turn.id,"task":result.task,"steps":completed.result.steps,"artifact":artifact,"physical_utf8":String::from_utf8(bytes.clone()).unwrap(),"verified_artifact_bytes":verified,"session":session,"waits":waits})).unwrap();
                    assert_eq!(result.task.state, TaskState::Completed);
                    assert!(matches!(
                        completed.result.outcome,
                        TurnOutcome::FinalAnswer { .. }
                    ));
                    assert!(instances.insert(result.snapshot.identity().instance_id.clone()));
                    let oracle = expected.iter().find(|row| row["turn"] == turn.id).unwrap();
                    assert_eq!(String::from_utf8(bytes).unwrap(), oracle["file"]);
                    assert_eq!(session.turns.len(), index + 1);
                    assert!(session.context_messages.starts_with(&previous_messages));
                    let requests: Vec<ModelRequest> = events
                        .iter()
                        .filter(|event| event.kind == LedgerEventKind::ModelRequested)
                        .map(|event| {
                            serde_json::from_value(event.payload["request"].clone()).unwrap()
                        })
                        .collect();
                    assert!(requests[0].messages.starts_with(&previous_messages));
                    let initial = serde_json::to_value(&requests[0].messages).unwrap();
                    for retained in &retained_tool_results {
                        assert!(
                            initial
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|message| message["content"]
                                    .as_array()
                                    .unwrap()
                                    .contains(retained)),
                            "previous tool result lost"
                        );
                    }
                    if index > 0 {
                        assert!(
                            serde_json::to_string(&initial)
                                .unwrap()
                                .contains(&case.history_marker)
                        );
                    }
                    for (name, count) in oracle["receipts"].as_object().unwrap() {
                        assert_eq!(
                            events
                                .iter()
                                .filter(|event| event.kind == LedgerEventKind::EffectReceipt
                                    && event.payload["input"]["prepared"]["call"]["name"]
                                        == name.as_str())
                                .count() as u64,
                            count.as_u64().unwrap()
                        );
                    }
                    let receipts: Vec<_> = events
                        .iter()
                        .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
                        .collect();
                    let ids: BTreeSet<_> = receipts
                        .iter()
                        .map(|event| event.payload["effect_id"].as_str().unwrap())
                        .collect();
                    assert_eq!(ids.len(), receipts.len());
                    for receipt in &receipts {
                        let output: kolyan_model::ToolResult =
                            serde_json::from_value(receipt.payload["output"].clone()).unwrap();
                        let file: kolyan_tools::FileOperationResult =
                            serde_json::from_str(&output.content).unwrap();
                        evidence.append(json!({"event":"physical_receipt_comparison","turn":turn.id,"receipt":receipt.event_id,"output":output,"file":file,"artifact":artifact})).unwrap();
                        assert!(!output.is_error);
                        assert_eq!(file.path, case.artifact_path);
                        assert_eq!(file.sha256, artifact.digest);
                        assert_eq!(file.bytes as u64, artifact.byte_length);
                        if receipt.payload["input"]["prepared"]["call"]["name"] == "file.read" {
                            assert_eq!(file.content.as_deref(), oracle["file"].as_str());
                        }
                    }
                    let observed_usage = usage::observe(
                        completed
                            .result
                            .steps
                            .iter()
                            .map(|step| &step.response.usage),
                    );
                    evidence.append(json!({"event":"usage_comparison","turn":turn.id,"task":result.task.usage,"observed":observed_usage})).unwrap();
                    let observed_usage = observed_usage.expect("reported usage overflow must fail");
                    assert_eq!(
                        result.task.usage.input_tokens,
                        observed_usage.observed_input_tokens
                    );
                    assert_eq!(
                        result.task.usage.output_tokens,
                        observed_usage.observed_output_tokens
                    );
                    assert_eq!(
                        result.task.usage.unreported_steps,
                        observed_usage.unreported_steps
                    );
                    retained_tool_results = session
                        .context_messages
                        .iter()
                        .flat_map(|message| &message.content)
                        .filter(|block| matches!(block, ContentBlock::ToolResult { .. }))
                        .map(|block| serde_json::to_value(block).unwrap())
                        .collect();
                    previous_messages = session.context_messages;
                    let facts = host.journal.read(&task, 0, 512).unwrap();
                    let complete = facts
                        .iter()
                        .find(|fact| fact.draft.kind == "task.completed")
                        .unwrap();
                    completion_facts.push(FactRef {
                        stream_id: task.clone(),
                        position: complete.position,
                        fact_id: complete.draft.fact_id.clone(),
                    });
                    evidence.append(json!({"event":"session_continuation_boundary","turn":turn.id,"next":case.turns.get(index+1).map(|next|&next.id),"task_completion":completion_facts.last(),"artifact":artifact})).unwrap();
                    all_events.extend(events);
                    break;
                }
            }
        }
        drop(host);
    }
    let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
    let admitted = ledger
        .events_after(0)
        .unwrap()
        .iter()
        .filter(|event| event.kind == LedgerEventKind::ExecutionInputAdmitted)
        .count();
    let steps = all_events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::StepCompleted)
        .count();
    let rows = evidence.rows();
    let summary = expected
        .iter()
        .find(|row| row["event"] == "summary")
        .unwrap();
    let final_bytes = fs::read(root.join("workspace").join(&case.artifact_path)).unwrap();
    let artifact = ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024)
        .unwrap()
        .put(&final_bytes, Retention::Required)
        .unwrap();
    evidence.append(json!({"event":"long_task_summary","artifact":artifact,"admitted_turns":admitted,"actual_recorded_steps":steps,"task_completions":completion_facts})).unwrap();
    let recovery_nodes = json!({
        "waiting_before_effect": rows.iter().filter(|row|row["event"] == "waiting_restart" && row["prior_receipts"] == 0).count(),
        "waiting_with_prior_receipt": rows.iter().filter(|row|row["event"] == "waiting_restart" && row["prior_receipts"].as_u64().is_some_and(|count| count > 0)).count(),
        "completed_turn_to_next_admitted_turn": rows.iter().filter(|row|row["event"] == "session_continuation_boundary" && row["next"].is_string()).count(),
    });
    evidence
        .append(json!({"event":"recovery_nodes","counts":recovery_nodes}))
        .unwrap();
    assert_eq!(admitted as u64, summary["admitted_turns"].as_u64().unwrap());
    assert!(steps as u64 >= summary["minimum_steps"].as_u64().unwrap());
    if live.is_none() {
        assert_eq!(steps as u64, summary["offline_steps"].as_u64().unwrap());
    }
    for node in summary["recovery_nodes"].as_array().unwrap() {
        assert!(
            recovery_nodes[node.as_str().unwrap()].as_u64().unwrap() > 0,
            "missing actual recovery node {node}"
        );
    }
    for row in rows.iter().filter(|row| row["event"] == "context") {
        let source: ModelRequest = serde_json::from_value(row["source"].clone()).unwrap();
        let prepared: kolyan_agent::context::PreparedContext =
            serde_json::from_value(row["prepared"].clone()).unwrap();
        assert_eq!(source, prepared.request);
        assert!(matches!(
            prepared.budget,
            kolyan_agent::context::BudgetStatus::Unverified {
                estimated_input_tokens: None,
                ..
            }
        ));
        assert_eq!(
            serde_json::to_vec(&source).unwrap().len(),
            prepared.provenance.source_serialized_bytes
        );
        assert_eq!(
            prepared.provenance.source_serialized_bytes,
            prepared.provenance.prepared_serialized_bytes
        );
    }
    let recorded: Vec<Value> = all_events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::ModelRequested)
        .map(|event| event.payload["request"].clone())
        .collect();
    let dispatched: Vec<Value> = rows
        .iter()
        .filter(|row| row["event"] == "request")
        .map(|row| row["request"].clone())
        .collect();
    assert_eq!(
        recorded, dispatched,
        "Core input must equal actual Provider input after every rebuild"
    );
    let step_ids: BTreeSet<_> = all_events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::StepCompleted)
        .map(|event| {
            (
                &event.execution_id,
                event.payload["step_id"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(step_ids.len(), steps, "no replayed recorded Step");
    let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
    let completed = journal.append("long-task-verification",0,vec![FactDraft {fact_id:"long-task/artifact-backed-completion".into(),subject:FactSubject{kind:"integration.long-task".into(),id:"logical-session".into()},kind:"integration.long-task.artifact-backed-completion".into(),schema_version:1,critical:true,causes:completion_facts,payload:json!({"artifact":artifact,"path":case.artifact_path,"admitted_turns":admitted,"actual_recorded_steps":steps,"physical_receipts":all_events.iter().filter(|event|event.kind == LedgerEventKind::EffectReceipt).collect::<Vec<_>>(),"scope":"host_dataset_verification_not_a_new_task_criterion"})}]).unwrap();
    evidence.append(json!({"event":"artifact_backed_completion","facts":completed,"admitted_turns":admitted,"actual_recorded_steps":steps})).unwrap();
}

#[tokio::test]
async fn long_task_offline_real_os_ten_turns_and_wait_reconstruction() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let selectors = case().selectors;
    let mut report = matrix::Matrix::new(
        selectors
            .iter()
            .map(|selector| format!("long/offline/{selector}")),
    );
    for (index, selector) in selectors.iter().enumerate() {
        report
            .run(
                index,
                run(
                    case(),
                    selector,
                    ModelRef::new("fixture", "long-script"),
                    None,
                    &installation,
                ),
            )
            .await;
    }
    assert!(
        report.complete(),
        "Long task evidence: {}",
        report.directory.display()
    );
}

enum Deployment {
    OpenAi(&'static str, common::ProviderConfig, String),
    Anthropic(&'static str, common::AnthropicProviderConfig, String),
}
impl Deployment {
    fn label(&self) -> String {
        match self {
            Self::OpenAi(family, _, model) => format!("{family}/openai/{model}"),
            Self::Anthropic(family, _, model) => format!("{family}/anthropic/{model}"),
        }
    }
    fn build(&self) -> (ModelRef, Arc<dyn ModelProvider>) {
        match self {
            Self::OpenAi(family, config, model) => (
                ModelRef::new(*family, model),
                Arc::new(common::build_openai_provider(
                    config,
                    &common::require_api_key(config),
                )),
            ),
            Self::Anthropic(family, config, model) => (
                ModelRef::new(*family, model),
                Arc::new(common::build_anthropic_provider(
                    config,
                    &common::require_api_key_anthropic(config),
                )),
            ),
        }
    }
}
fn deployments() -> Vec<Deployment> {
    let config = common::load_config();
    let mut result = vec![];
    for (family, config) in [
        ("minimax", config.minimax_openai),
        ("qwen", config.qwen_openai),
    ] {
        for entry in &config.model_matrix {
            result.push(Deployment::OpenAi(
                family,
                config.clone(),
                entry.model.clone(),
            ));
        }
    }
    for (family, config) in [
        ("minimax", config.minimax_anthropic),
        ("qwen", config.qwen_anthropic),
    ] {
        for entry in &config.model_matrix {
            result.push(Deployment::Anthropic(
                family,
                config.clone(),
                entry.model.clone(),
            ));
        }
    }
    result
}

#[test]
fn long_task_real_entry_plans_all_nineteen_without_claiming_execution() {
    assert_eq!(case().schema_version, 1);
    assert_eq!(deployments().len(), case().configured_combinations);
    assert_eq!(case().turns.len(), 10);
}

#[tokio::test]
#[ignore = "actual ten-Turn long tasks across all nineteen combinations; requires every credential"]
async fn actual_model_long_task_matrix() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let deployments = deployments();
    let selectors = case().selectors;
    let mut report = matrix::Matrix::new(deployments.iter().flat_map(|deployment| {
        selectors
            .iter()
            .map(move |selector| format!("long/live/{}/{selector}", deployment.label()))
    }));
    let mut index = 0;
    for deployment in deployments {
        for selector in &selectors {
            report
                .run(index, async {
                    let (model, provider) = deployment.build();
                    run(case(), selector, model, Some(provider), &installation).await;
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "Long task live evidence: {}",
        report.directory.display()
    );
}
