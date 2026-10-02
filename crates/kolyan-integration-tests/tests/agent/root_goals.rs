//! Real Runner and native isolated file-worker goal acceptance, with local model scripts.
mod live;
mod live_approvals;
mod ports;
mod stores;
use kolyan_agent::*;
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore};
use kolyan_model::*;
use kolyan_runtime::DurableTurnResult;
use kolyan_server::*;
use kolyan_storage::FileSessionStore;
use kolyan_tools::{
    ExactFileBinding, FileOperationLimits, IsolatedFileConfig, IsolatedShellConfig,
    IsolatedToolSet, IsolatedToolSetConfig,
};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use ports::{Providers, Tools};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};
use stores::{Journal, Ledger};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    named: bool,
    content: String,
    final_text: String,
    write: bool,
    approval: bool,
    old_content: Option<String>,
    expected: String,
    verdict: String,
    receipts: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    case_revision: String,
    input: String,
    inspection_window_assumption_tokens: u32,
    output_limit_tokens: u32,
    backends: Vec<String>,
    cases: Vec<Case>,
    network_plan: Value,
}
type Service = TaskExecutionService<Journal, Ledger, NoopTraceSink, FileSessionStore>;
type Runner = AgentRunner<Journal, Ledger, NoopTraceSink, FileSessionStore, Providers, Tools>;
struct Host {
    runner: Arc<Runner>,
    service: Arc<Service>,
    ledger: Ledger,
    journal: Journal,
    definition: AgentDefinition,
    goal: GoalCriterion,
}

fn open(
    root: &std::path::Path,
    case: &Case,
    ledger: Ledger,
    journal: Journal,
    executor: IsolatedToolSet,
    providers: Providers,
) -> Result<Host, String> {
    // Obtain actual normalized adapter identity without issuing a preparation.
    let revision = executor
        .file_adapter_revision()
        .map_err(|e| e.to_string())?;
    let checker = Arc::new(
        FileWriteCommittedChecker::new([revision.clone()].into()).map_err(|e| e.to_string())?,
    );
    let workspace = root.join("workspace");
    let exact = ExactFileBinding::prepare(&workspace, &workspace.join("result.txt"), &[])
        .map_err(|e| e.to_string())?;
    let goal = GoalCriterion::new(
        "committed-write".into(),
        "root".into(),
        checker.key().clone(),
        json!(FileWriteCommittedPredicateV1 {
            schema_version: 1,
            tool_revision: revision,
            workspace: exact.workspace,
            parent: exact.parent,
            leaf: exact.leaf,
            expected_bytes: 5,
            expected_sha256: format!("{:x}", Sha256::digest(b"goal\n"))
        }),
    )
    .map_err(|e| e.to_string())?;
    let verifier = LedgerTaskGoalVerifier::new(
        ledger.clone(),
        GoalCheckerRegistry::new(vec![checker]).map_err(|e| e.to_string())?,
        GoalSourceLimits::default(),
    )
    .map_err(|e| e.to_string())?;
    let sessions = SessionService::new(
        FileSessionStore::new(root.join("state/sessions")).map_err(|e| e.to_string())?,
    );
    if !root.join("state/initialized").exists() {
        sessions.create("session").map_err(|e| e.to_string())?;
        std::fs::write(root.join("state/initialized"), b"initialized")
            .map_err(|e| e.to_string())?;
    }
    let artifacts = Arc::new(
        ArtifactStore::new(root.join("state/artifacts"), 16 * 1024 * 1024)
            .map_err(|e| e.to_string())?,
    );
    let service = Arc::new(
        Service::new(
            TaskCoordinator::new(journal.clone()).with_goal_verifier(Arc::new(verifier)),
            SessionExecutionService::new(
                ExecutionService::new(ledger.clone(), NoopTraceSink),
                sessions,
            ),
        )
        .with_artifacts(
            ArtifactStore::new(root.join("state/artifacts"), 16 * 1024 * 1024)
                .map_err(|e| e.to_string())?,
        ),
    );
    let permissions = AgentPermissions {
        tools: [EnvironmentTool::Write].into(),
        ..Default::default()
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "native-goal-root".into(),
        revision: "v1".into(),
        display_name: None,
        model: providers.model.clone(),
        instructions: "Follow the actual user input and tool results.".into(),
        permissions: permissions.clone(),
    })
    .map_err(|e| e.to_string())?;
    let mut catalog = AgentCatalog::new(8).map_err(|e| e.to_string())?;
    catalog
        .register(definition.clone())
        .map_err(|e| e.to_string())?;
    let port: Arc<dyn FactJournal> = Arc::new(journal.clone());
    let runner = Arc::new(
        AgentRunner::new(
            service.clone(),
            InstanceRegistry::new(port.clone(), "native-goal-host", 32)
                .map_err(|e| e.to_string())?,
            binding::AgentInvocationBindingStore::new(port),
            catalog,
            permissions,
            (
                providers,
                Tools {
                    executor,
                    workspace: workspace.to_str().ok_or("workspace utf8")?.into(),
                    approval: case.approval,
                },
            ),
            artifacts,
        )
        .map_err(|e| e.to_string())?,
    );
    Ok(Host {
        runner,
        service,
        ledger,
        journal,
        definition,
        goal,
    })
}

async fn scenario(
    case: &Case,
    backend: &str,
    installation: &super::tools::worker::WorkerRun,
    row: &mut Value,
    deployment: Option<&super::Deployment>,
) -> Result<(), String> {
    let dataset: Dataset =
        serde_json::from_str(include_str!("root_goals/cases.json")).map_err(|e| e.to_string())?;
    row["case_revision"] = json!(dataset.case_revision);
    let root = tempfile::Builder::new()
        .prefix("kolyan-runner-native-goals-")
        .tempdir()
        .map_err(|e| e.to_string())?;
    let path = root.path().canonicalize().map_err(|e| e.to_string())?;
    for directory in ["workspace", "staging", "state"] {
        std::fs::create_dir(path.join(directory)).map_err(|e| e.to_string())?;
    }
    let evidence = super::evidence::Evidence::new(&path.join("worker.jsonl"));
    super::tools::initialize_worker(&path, &evidence, installation)?;
    let worker = super::tools::worker::verified_worker(&path, &evidence)?;
    if let Some(content) = &case.old_content {
        std::fs::write(path.join("workspace/result.txt"), content).map_err(|e| e.to_string())?;
    }
    let executor = IsolatedToolSet::new(IsolatedToolSetConfig {
        files: IsolatedFileConfig {
            workspace: path.join("workspace"),
            staging_root: path.join("staging"),
            worker,
            protected_roots: vec![path.join("state"), path.join("trusted-worker")],
            file_limits: FileOperationLimits {
                max_read_bytes: 65536,
                max_write_bytes: 65536,
            },
            max_output_bytes: 1024 * 1024,
            timeout: Duration::from_secs(30),
        },
        shell: IsolatedShellConfig {
            workspace: path.join("workspace"),
            protected_roots: vec![path.join("state"), path.join("trusted-worker")],
            max_command_bytes: 4096,
            max_output_bytes: 4096,
            timeout: Duration::from_secs(30),
        },
    })
    .map_err(|e| e.to_string())?;
    let requests = Arc::new(Mutex::new(vec![]));
    let providers = Providers {
        output_limit_tokens: dataset.output_limit_tokens,
        inspection_window_assumption_tokens: dataset.inspection_window_assumption_tokens,
        case: case.clone(),
        requests: requests.clone(),
        model: deployment.map_or_else(
            || ModelRef::new("fixture", "native-goal-scripted"),
            |d| ModelRef::new(d.family, &d.model),
        ),
        live: deployment.map(super::Deployment::build),
    };
    let (ledger, journal) = stores::open(backend, &path);
    let host = open(
        &path,
        case,
        ledger.clone(),
        journal.clone(),
        executor.clone(),
        providers.clone(),
    )?;
    row["declared_goal"] = json!(host.goal);
    row["worker_evidence"] =
        json!(std::fs::read_to_string(path.join("worker.jsonl")).map_err(|e| e.to_string())?);
    let execution = ExecutionRef {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "execution".into(),
    };
    let request = RootRunRequest {
        goals: vec![host.goal.clone()],
        task_id: case.id.clone(),
        invocation_id: "root".into(),
        attempt_id: "attempt".into(),
        execution: execution.clone(),
        selector: if case.named {
            AgentSelector::Named(host.definition.key())
        } else {
            AgentSelector::Inline(host.definition.clone())
        },
        requested_permissions: host.definition.permissions().clone(),
        objective: "specified physical file receives goal plus LF through governed write".into(),
        limits: TaskLimits {
            max_depth: 1,
            max_invocations: 1,
            max_attempts: 1,
            max_tokens: None,
            max_steps_per_turn: 4,
        },
        cancellation_policy: CancellationPolicy::AllInvocations,
        turn: kolyan_core::TurnRequest {
            turn_id: "turn".into(),
            config: kolyan_core::TurnConfig {
                max_steps: 4,
                ..Default::default()
            },
            model_request: ModelRequest {
                request_id: "request".into(),
                model: host.definition.model().clone(),
                system: vec![],
                messages: vec![Message {
                    role: MessageRole::User,
                    content: vec![ContentBlock::Text {
                        text: dataset.input,
                    }],
                }],
                tools: vec![],
                tool_choice: ToolChoice::Auto,
                output_format: None,
                prompt_cache: None,
                reasoning: None,
                max_output_tokens: Some(dataset.output_limit_tokens),
                extensions: Value::Null,
            },
        },
    };
    row["planned_request"] = json!(request.turn.model_request);
    let started = host.runner.start(request).await;
    row["initial"] = match &started {
        Ok(r) => json!({"task":r.task}),
        Err(e) => json!({"error":e.to_string()}),
    };
    row["before_resume_events"] = json!(host.ledger.events_after(0).map_err(|e| e.to_string())?);
    row["before_resume_bytes"] = json!(std::fs::read(path.join("workspace/result.txt")).ok());
    let approval = match &started {
        Ok(r) => match &r.execution {
            DurableTurnResult::Suspended { suspension, .. } => suspension
                .waiting
                .approvals
                .first()
                .map(|a| a.approval_id.clone()),
            _ => None,
        },
        _ => None,
    };
    // Drop the original Runner/service, reopen both SQLite stores, and rebuild
    // adapters and the registry with the exact same host configuration.
    drop(host);
    let (reopened_ledger, reopened_journal) = if backend == "sqlite" {
        stores::open(backend, &path)
    } else {
        (ledger, journal)
    };
    let rebuilt = open(
        &path,
        case,
        reopened_ledger,
        reopened_journal,
        executor,
        providers,
    )?;
    if case.approval {
        let resumed = rebuilt
            .runner
            .resume_approval(RootApprovalResumeRequest {
                task_id: case.id.clone(),
                invocation_id: "root".into(),
                logical_session_id: "session".into(),
                attempt_id: "attempt".into(),
                approval_id: approval.ok_or("missing actual approval")?,
            })
            .await;
        row["resumed"] = match resumed {
            Ok(r) => json!({"task":r.task}),
            Err(e) => json!({"error":e.to_string()}),
        };
    }
    let finalization = TaskFinalizationRequest {
        task_id: case.id.clone(),
        logical_session_id: "session".into(),
        root_invocation_id: "root".into(),
        root_attempt_id: "attempt".into(),
        policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
    };
    let before = rebuilt
        .journal
        .read(&case.id, 0, 1024)
        .map_err(|e| e.to_string())?;
    let before_events = rebuilt.ledger.events_after(0).map_err(|e| e.to_string())?;
    let before_requests = requests.lock().unwrap().clone();
    let finalized = rebuilt.runner.finalize_task(finalization.clone()).await;
    row["final"] = match finalized {
        Ok(t) => json!({"status":t.state,"task":t}),
        Err(e) => json!({"status":"Refused","error":e.to_string()}),
    };
    let repeated = rebuilt.runner.finalize_task(finalization).await;
    row["repeat"] = match repeated {
        Ok(t) => json!({"status":t.state,"task":t}),
        Err(e) => json!({"status":"Refused","error":e.to_string()}),
    };
    row["before_finalize_facts"] = json!(before);
    row["before_finalize_events"] = json!(before_events);
    row["before_finalize_requests"] = json!(before_requests);
    row["facts"] = json!(
        rebuilt
            .journal
            .read(&case.id, 0, 1024)
            .map_err(|e| e.to_string())?
    );
    row["events"] = json!(rebuilt.ledger.events_after(0).map_err(|e| e.to_string())?);
    row["requests"] = json!(*requests.lock().unwrap());
    row["bytes"] = json!(std::fs::read(path.join("workspace/result.txt")).ok());
    row["task_after"] = json!(
        rebuilt
            .service
            .coordinator()
            .snapshot(&case.id)
            .map_err(|e| e.to_string())?
    );
    Ok(())
}

#[tokio::test]
async fn actual_runner_native_write_goals_rebuild_matrix() {
    let data: Dataset = serde_json::from_str(include_str!("root_goals/cases.json")).unwrap();
    let installation = super::tools::worker::WorkerRun::prepare().await;
    let root = tempfile::tempdir().unwrap().keep();
    let path = root.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for backend in &data.backends {
        for case in &data.cases {
            let mut row = json!({"backend":backend,"case":case.id,"fixture":case_to_value(case),"network_plan":data.network_plan});
            let result = scenario(case, backend, &installation, &mut row, None).await;
            row["scenario_error"] = json!(result.err());
            writeln!(export, "{}", row).unwrap();
        }
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("RUNNER_NATIVE_GOALS_TRACE={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), data.backends.len() * data.cases.len());
    for (index, row) in rows.iter().enumerate() {
        let case = &data.cases[index % data.cases.len()];
        assert!(row["scenario_error"].is_null(), "{}: {row}", case.id);
        assert_eq!(row["final"]["status"], case.expected);
        assert_eq!(row["repeat"], row["final"]);
        assert_eq!(row["facts"], row["before_finalize_facts"]);
        assert_eq!(row["events"], row["before_finalize_events"]);
        assert_eq!(row["requests"], row["before_finalize_requests"]);
        let task = &row["final"]["task"];
        assert_eq!(task["definition"]["criteria"].as_array().unwrap().len(), 2);
        assert_eq!(task["goal_assessments"].as_array().unwrap().len(), 1);
        assert_eq!(
            task["goal_assessments"][0]["assessment"]["verdict"],
            case.verdict
        );
        assert_eq!(
            row["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(
                    |e| e["kind"] == serde_json::to_value(LedgerEventKind::EffectReceipt).unwrap()
                )
                .count(),
            case.receipts
        );
        if case.write {
            assert_eq!(row["bytes"], json!(case.content.as_bytes()));
        } else {
            assert!(row["bytes"].is_null());
        }
        if case.approval {
            assert!(row["before_resume_bytes"].is_null());
            assert!(
                !row["before_resume_events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["kind"] == "effect_started" || e["kind"] == "effect_receipt")
            );
        }
    }
}
fn case_to_value(c: &Case) -> Value {
    json!({"id":c.id,"named":c.named,"content":c.content,"final_text":c.final_text,"write":c.write,"approval":c.approval,"old_content":c.old_content,"expected":c.expected,"verdict":c.verdict,"receipts":c.receipts})
}
