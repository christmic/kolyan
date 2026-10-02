//! Real root admission, independent Turns and durable evidence, without a second loop.

use std::{fs, sync::Arc};

use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentRunner,
    AgentSelector, EnvironmentTool, RootRunRequest, binding::AgentInvocationBindingStore,
};
use kolyan_core::{TurnConfig, TurnOutcome, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelProvider, ModelRef, ModelRequest, SystemInstruction,
    ToolChoice,
};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{
    CancellationPolicy, ExecutionRef, ExecutionService, InstanceRegistry, SessionExecutionService,
    SessionService, TaskCoordinator, TaskExecutionService, TaskLimits,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy, SessionStore};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde_json::{Value, json};

use super::{
    data::{Dataset, Turn},
    evidence::{Evidence, compare_order},
    providers::Providers,
    tools::Tools,
};

pub async fn run(
    dataset: Dataset,
    selector: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &super::tools::worker::WorkerRun,
) {
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-root-")
        .tempdir()
        .unwrap()
        .keep();
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join("staging"))
            .unwrap();
    }
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    super::tools::initialize_worker(&root, &evidence, installation).unwrap();
    println!(
        "Agent root evidence: {}",
        root.join("actual.jsonl").display()
    );
    evidence.append(json!({"event":"plan","selector":selector,"model":model,"budget_mode":"Inspect","counter":"Unsupported","token_estimate":null,"inspection_window_assumption_tokens":dataset.inspection_window_assumption_tokens,"pending":serde_json::from_str::<Value>(include_str!("../fixtures/agent/pending.json")).unwrap()})).unwrap();
    let sessions = FileSessionStore::new(root.join("state/sessions")).unwrap();
    SessionService::new(sessions.clone())
        .create("logical-session")
        .unwrap();
    let permissions = AgentPermissions {
        tools: [
            EnvironmentTool::Read,
            EnvironmentTool::Write,
            EnvironmentTool::Edit,
            EnvironmentTool::Shell,
        ]
        .into_iter()
        .collect(),
        delegation: Default::default(),
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "root-definition".into(),
        revision: "r1".into(),
        display_name: if selector == "named" {
            Some("Named root".into())
        } else {
            None
        },
        model: model.clone(),
        instructions: dataset.instructions.clone(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let offline = live.is_none();
    let mut instance_ids = std::collections::BTreeSet::new();
    for turn in &dataset.turns {
        let task_id = format!("task-{}", turn.id);
        let execution = ExecutionRef {
            session_id: "logical-session".into(),
            turn_id: format!("turn-{}", turn.id),
            execution_id: format!("execution-{}", turn.id),
        };
        evidence
            .append(json!({"event":"root_started","execution":execution,"task_id":task_id}))
            .unwrap();
        // Each independent Turn reconstructs host stores and the real Runner.
        let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
        let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
        let service = Arc::new(
            TaskExecutionService::new(
                TaskCoordinator::new(journal.clone()),
                SessionExecutionService::new(
                    ExecutionService::new(ledger.clone(), NoopTraceSink),
                    SessionService::new(sessions.clone()),
                )
                .with_context_policy(SessionContextPolicy::FullTrajectory),
            )
            .with_artifacts(
                ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap(),
            ),
        );
        let journal_port: Arc<dyn FactJournal> = Arc::new(journal.clone());
        let mut catalog = AgentCatalog::new(8).unwrap();
        catalog.register(definition.clone()).unwrap();
        let runner = Arc::new(
            AgentRunner::new(
                service,
                InstanceRegistry::new(journal_port.clone(), "agent-test-host", 128).unwrap(),
                AgentInvocationBindingStore::new(journal_port),
                catalog,
                permissions.clone(),
                (
                    Providers {
                        live: live.clone(),
                        dataset: dataset.clone(),
                        evidence: evidence.clone(),
                    },
                    Tools {
                        root: root.clone(),
                        dataset: dataset.clone(),
                        evidence: evidence.clone(),
                    },
                ),
                Arc::new(
                    ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap(),
                ),
            )
            .unwrap(),
        );
        let result = runner
            .start(RootRunRequest {
                task_id: task_id.clone(),
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
                    max_steps_per_turn: u32::try_from(dataset.max_steps)
                        .expect("fixture step limit must fit the task contract"),
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
                turn: TurnRequest {
                    turn_id: execution.turn_id.clone(),
                    config: TurnConfig {
                        max_steps: dataset.max_steps,
                        max_tool_calls: Some(dataset.max_tool_calls),
                        deadline: None,
                    },
                    model_request: request(turn, &model, dataset.output_reserve_tokens),
                },
            })
            .await;
        // Export authoritative raw state before any result assertion can unwind.
        let events = ledger
            .execution_events_after(&execution.execution_id, 0)
            .unwrap();
        for event in &events {
            evidence
                .append(json!({"event":"ledger","value":event}))
                .unwrap();
        }
        let mut after = 0;
        loop {
            let page = journal.read(&task_id, after, 512).unwrap();
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
        match &result {
            Ok(result) => evidence
                .append(
                    json!({"event":"root_completed","snapshot":result.snapshot,"task":result.task}),
                )
                .unwrap(),
            Err(error) => evidence
                .append(json!({"event":"root_failed","error":error.to_string()}))
                .unwrap(),
        }
        let result = result.unwrap();
        assert!(instance_ids.insert(result.snapshot.identity().instance_id.clone()));
        let DurableTurnResult::Completed(completed, _) = result.execution else {
            panic!("unexpected suspension in non-approval baseline");
        };
        assert!(matches!(
            completed.result.outcome,
            TurnOutcome::FinalAnswer { .. }
        ));
        assert_eq!(turn.expected.outcome, "FinalAnswer");
        let observed = super::usage::observe(
            completed
                .result
                .steps
                .iter()
                .map(|step| &step.response.usage),
        );
        evidence
            .append(
                json!({"event":"usage_comparison","task":result.task.usage,"observed":observed}),
            )
            .unwrap();
        let observed = observed.expect("Task usage comparison must reject arithmetic overflow");
        assert_eq!(
            result.task.usage.input_tokens,
            observed.observed_input_tokens
        );
        assert_eq!(
            result.task.usage.output_tokens,
            observed.observed_output_tokens
        );
        assert_eq!(
            result.task.usage.unreported_steps,
            observed.unreported_steps
        );
        if offline {
            assert_eq!(result.task.usage.input_tokens, turn.expected.input_tokens);
            assert_eq!(result.task.usage.output_tokens, turn.expected.output_tokens);
        }
        for (path, expected) in &turn.expected.file {
            assert_eq!(
                fs::read_to_string(root.join("workspace").join(path))
                    .unwrap()
                    .trim_end_matches('\n'),
                expected
            );
        }
        for (name, minimum) in &turn.expected.tool_minimums {
            let count = events
                .iter()
                .filter(|event| {
                    event.kind == LedgerEventKind::EffectReceipt
                        && event.payload["input"]["prepared"]["call"]["name"] == *name
                })
                .count();
            assert!(count >= *minimum, "missing real tool receipt {name}");
        }
        let request = events
            .iter()
            .find(|event| event.kind == LedgerEventKind::ModelRequested)
            .unwrap();
        let request: ModelRequest =
            serde_json::from_value(request.payload["request"].clone()).unwrap();
        let history = serde_json::to_string(&request.messages).unwrap();
        for expected in &turn.expected.history_contains {
            assert!(
                history.contains(expected),
                "missing previous Turn history {expected}"
            );
        }
        for row in evidence
            .rows()
            .iter()
            .filter(|row| row["event"] == "context")
        {
            assert!(row["prepared"]["budget"].get("Unverified").is_some());
            assert!(row["prepared"]["budget"]["Unverified"]["estimated_input_tokens"].is_null());
            assert_eq!(row["source"], row["prepared"]["request"]);
        }
        drop(runner);
    }
    let session = sessions.load("logical-session").unwrap();
    evidence
        .append(json!({"event":"session","value":session}))
        .unwrap();
    assert_eq!(session.turns.len(), dataset.turns.len());
    compare_order(&evidence.rows());
}

pub(super) fn request(turn: &Turn, model: &ModelRef, reserve: u32) -> ModelRequest {
    ModelRequest {
        request_id: format!("input-{}", turn.id),
        model: model.clone(),
        system: vec![SystemInstruction {
            text: "Use only the trusted workspace tools.".into(),
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
        max_output_tokens: Some(reserve),
        extensions: Value::Null,
    }
}
