//! Assemble the existing root factories, not an alternate Agent or effect loop.

use std::{fs, os::unix::fs::DirBuilderExt, sync::Arc, time::Duration};

use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentRunner,
    AgentSelector, EnvironmentTool, RootRunRequest, binding::AgentInvocationBindingStore,
};
use kolyan_core::{TurnConfig, TurnOutcome, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{ModelProvider, ModelRef};
use kolyan_protocol_http::{HttpRetryPolicy, RetryProfile};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{
    CancellationPolicy, ExecutionRef, ExecutionService, InstanceRegistry, SessionExecutionService,
    SessionService, TaskCoordinator, TaskExecutionService, TaskLimits,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde_json::{Value, json};

use super::super::{
    data,
    evidence::Evidence,
    providers::Providers,
    tools::{Tools, worker::WorkerRun},
};
use super::{Case, Protocol, transport::Server};

pub(super) async fn run(case: &Case, installation: &WorkerRun) -> Value {
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-opening-retry-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("staging"))
        .unwrap();
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    println!("OPENING_RETRY_EVIDENCE={}", root.display());
    evidence.append(json!({"event":"plan","case_id":case.id,"source":"localhost_protocol_fixture_not_actual_llm","tool_source":"production_macos_isolated_worker","expected":{"requests":case.expected.requests,"receipts":case.expected.receipts}})).unwrap();
    super::super::tools::initialize_worker(&root, &evidence, installation).unwrap();
    let server = Server::start(case, root.clone(), evidence.clone()).unwrap();
    let provider = provider(case.protocol, &server.url);
    let mut dataset = data::dataset();
    dataset.max_steps = 3;
    dataset.max_tool_calls = 1;
    let model = ModelRef::new("fixture", "opening-retry");
    let permissions = AgentPermissions {
        tools: [
            EnvironmentTool::Read,
            EnvironmentTool::Write,
            EnvironmentTool::Edit,
            EnvironmentTool::Shell,
        ]
        .into(),
        delegation: Default::default(),
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "opening-root".into(),
        revision: "1".into(),
        display_name: Some("Loopback protocol root".into()),
        model: model.clone(),
        instructions: "Use the requested tool once, then report its result.".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let sessions = FileSessionStore::new(root.join("state/sessions")).unwrap();
    SessionService::new(sessions.clone())
        .create("logical-session")
        .unwrap();
    let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
    let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
    let service = Arc::new(
        TaskExecutionService::new(
            TaskCoordinator::new(journal.clone()),
            SessionExecutionService::new(
                ExecutionService::new(ledger.clone(), NoopTraceSink),
                SessionService::new(sessions),
            )
            .with_context_policy(SessionContextPolicy::FullTrajectory),
        )
        .with_artifacts(ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap()),
    );
    let journal_port: Arc<dyn FactJournal> = Arc::new(journal.clone());
    let mut catalog = AgentCatalog::new(8).unwrap();
    catalog.register(definition.clone()).unwrap();
    let runner = Arc::new(
        AgentRunner::new(
            service,
            InstanceRegistry::new(journal_port.clone(), "opening-fixture-host", 16).unwrap(),
            AgentInvocationBindingStore::new(journal_port),
            catalog,
            permissions.clone(),
            (
                Providers {
                    live: Some(provider),
                    dataset: dataset.clone(),
                    evidence: evidence.clone(),
                },
                Tools {
                    root: root.clone(),
                    dataset: dataset.clone(),
                    evidence: evidence.clone(),
                },
            ),
            Arc::new(ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap()),
        )
        .unwrap(),
    );
    let execution = ExecutionRef {
        session_id: "logical-session".into(),
        turn_id: "opening-turn".into(),
        execution_id: "opening-execution".into(),
    };
    let mut turn = dataset.turns[0].clone();
    turn.id = "opening".into();
    turn.input = format!(
        "Write {} to {} exactly once, then finish after the tool result.",
        case.content, case.path
    );
    let result = runner
        .start(RootRunRequest {
            task_id: "opening-task".into(),
            invocation_id: "root".into(),
            attempt_id: "attempt-1".into(),
            execution: execution.clone(),
            selector: AgentSelector::Named(definition.key()),
            requested_permissions: permissions,
            objective: turn.input.clone(),
            limits: TaskLimits {
                max_depth: 1,
                max_invocations: 1,
                max_attempts: 1,
                max_tokens: None,
                max_steps_per_turn: 3,
            },
            cancellation_policy: CancellationPolicy::AllInvocations,
            turn: TurnRequest {
                turn_id: execution.turn_id.clone(),
                config: TurnConfig {
                    max_steps: 3,
                    max_tool_calls: Some(1),
                    deadline: None,
                },
                model_request: super::super::harness::request(
                    &turn,
                    &model,
                    dataset.output_reserve_tokens,
                ),
            },
        })
        .await;
    let completed = matches!(&result, Ok(run) if matches!(&run.execution,
        DurableTurnResult::Completed(completed, _) if matches!(completed.result.outcome, TurnOutcome::FinalAnswer { .. })));
    let error = result.as_ref().err().map(ToString::to_string);
    evidence
        .append(json!({"event":"runner_result","completed":completed,"error":error}))
        .unwrap();
    let server_error = server.finish().await;
    let events = ledger
        .execution_events_after(&execution.execution_id, 0)
        .unwrap();
    let ledger_export = Evidence::new(&root.join("ledger.jsonl"));
    for event in &events {
        ledger_export
            .append(json!({"event":"ledger","value":event}))
            .unwrap();
    }
    let journal_export = Evidence::new(&root.join("journal.jsonl"));
    let mut after = 0;
    loop {
        let page = journal.read("opening-task", after, 256).unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().unwrap().position;
        for fact in page {
            journal_export
                .append(json!({"event":"journal","value":fact}))
                .unwrap();
        }
    }
    let receipt_export = Evidence::new(&root.join("receipts.jsonl"));
    let receipts: Vec<_> = events
        .iter()
        .filter(|e| e.kind == LedgerEventKind::EffectReceipt)
        .collect();
    for receipt in &receipts {
        receipt_export
            .append(json!({"event":"receipt","value":receipt}))
            .unwrap();
    }
    let tool_result = receipts
        .first()
        .map(|receipt| receipt.payload["output"].clone())
        .unwrap_or(Value::Null);
    let rows = evidence.rows();
    let requests: Vec<_> = rows
        .iter()
        .filter(|row| row["event"] == "http_request")
        .cloned()
        .collect();
    let retry_metadata: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            let raw = &row["content"]["Provider"]["raw"];
            (row["event"] == "model_event" && raw.is_object()).then(|| raw.clone())
        })
        .collect();
    let retry_reports: Vec<_> = retry_metadata
        .iter()
        .filter(|raw| raw["kind"] == "local_http_opening_retry")
        .map(|raw| raw["report"].clone())
        .collect();
    let file = fs::read(root.join("workspace").join(&case.path));
    let actual = json!({"evidence":root,"case_id":case.id,"completed":completed,"error":error,"server_error":server_error,"requests":requests,"file":file.as_ref().ok().and_then(|b|std::str::from_utf8(b).ok()),"file_bytes":file.as_ref().ok(),"file_error":file.as_ref().err().map(ToString::to_string),"effect_starts":events.iter().filter(|e|e.kind==LedgerEventKind::EffectStarted).count(),"receipts":receipts.len(),"receipt_events":receipts,"executions":rows.iter().filter(|r|r["event"]=="tool_adapter" && r["phase"]=="execute" && r["stage"]=="returned").count(),"model_requests":events.iter().filter(|e|e.kind==LedgerEventKind::ModelRequested).count(),"tool_result":tool_result,"retry_metadata":retry_metadata,"retry_reports":retry_reports});
    evidence
        .append(json!({"event":"actual","value":actual}))
        .unwrap();
    actual
}

fn provider(protocol: Protocol, url: &str) -> Arc<dyn ModelProvider> {
    match protocol {
        Protocol::Openai => {
            let mut config =
                kolyan_protocol_openai::OpenAiConfig::new("localhost-fixture-not-a-secret");
            config.base_url = url.into();
            config.timeout = Duration::from_secs(5);
            config.transport_retries = 0;
            config.http_retry =
                HttpRetryPolicy::new(2, 10_000, 1000, RetryProfile::OpenAi).unwrap();
            Arc::new(kolyan_provider_openai::OpenAiProvider::new(
                kolyan_protocol_openai::OpenAiClient::new(config).unwrap(),
            ))
        }
        Protocol::Anthropic => {
            let mut config =
                kolyan_protocol_anthropic::AnthropicConfig::new("localhost-fixture-not-a-secret");
            config.base_url = url.into();
            config.timeout = Duration::from_secs(5);
            config.transport_retries = 0;
            config.http_retry =
                HttpRetryPolicy::new(2, 10_000, 1000, RetryProfile::Anthropic).unwrap();
            Arc::new(kolyan_provider_anthropic::AnthropicProvider::new(
                kolyan_protocol_anthropic::AnthropicClient::new(config).unwrap(),
            ))
        }
    }
}
