//! Data-driven nested approval cancellation, including every configured live row.

#[path = "../common/mod.rs"]
mod common;
#[path = "../common/matrix.rs"]
#[allow(dead_code)]
mod matrix;

use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture, StopReason, TokenUsage,
};
use kolyan_policy::{ApprovalMode, PathScope, PolicyEngine};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::*;
use kolyan_storage::FileSessionStore;
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};

type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;
type Combination = (
    &'static str,
    &'static str,
    common::ModelMatrixEntry,
    Option<common::ProviderConfig>,
    Option<common::AnthropicProviderConfig>,
);

#[derive(Clone)]
struct Shared(Arc<dyn ModelProvider>);
impl ModelProvider for Shared {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.0.stream(request)
    }
}

struct Scripted(Value);
impl ModelProvider for Scripted {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let observed = request.messages.iter().flat_map(|message| &message.content).any(|block|
            matches!(block, ContentBlock::ToolResult { result } if result.call_id == self.0["offline_call"]["id"]));
        let content = if observed {
            json!([{"type":"text","text":self.0["offline_answer"]}])
        } else {
            json!([{"type":"tool_call","call":self.0["offline_call"]}])
        };
        let response = ModelResponse {
            id: request.request_id,
            model: request.model,
            content: serde_json::from_value(content).unwrap(),
            structured_output: None,
            stop_reason: if observed {
                StopReason::EndTurn
            } else {
                StopReason::ToolUse
            },
            usage: TokenUsage {
                input_tokens: Some(12),
                output_tokens: Some(6),
                ..Default::default()
            },
            metadata: json!({"source":"deterministic_only"}),
        };
        Box::pin(async move {
            Ok(
                Box::pin(futures_util::stream::iter(vec![Ok(ModelEvent::Completed(
                    response,
                ))])) as ModelEventStream,
            )
        })
    }
}

fn fixture() -> Value {
    serde_json::from_str(include_str!("../fixtures/task_cancellation.json")).unwrap()
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
        ),
    )
}

fn executor(
    provider: Shared,
    root: &Path,
    data: &Value,
) -> TurnExecutor<Shared, PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>> {
    let scope = root
        .join("workspace")
        .join(data["workspace_scope"].as_str().unwrap())
        .to_string_lossy()
        .into_owned();
    let mut policy = PolicyEngine::default();
    for mut manifest in RestrictedFileTool::tool_manifests() {
        manifest.path_scopes = vec![PathScope::new(&scope)];
        if manifest.tool_name == "file.write" {
            manifest.approval = ApprovalMode::Always;
        }
        policy.register(manifest);
    }
    policy.restrict_workspace(scope);
    let policy = Arc::new(policy);
    TurnExecutor::with_tools(
        provider,
        PolicyEnforcingTool::new(
            RestrictedFileTool::new(root.join("workspace")),
            policy.clone(),
        ),
    )
    .with_policy_engine(policy)
}

/// Failure paths retain the same complete raw requests/outputs as passing paths.
struct Capture {
    root: PathBuf,
    task: String,
    execution: String,
}
impl Capture {
    fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        let journal = SqliteFactJournal::open(self.root.join("ledger.sqlite"))?;
        let ledger = SqliteLedger::open(self.root.join("ledger.sqlite"))?;
        let mut out = fs::File::create(self.root.join("actual-task.jsonl"))?;
        let mut after = 0;
        loop {
            let page = journal.read(&self.task, after, 512)?;
            if page.is_empty() {
                break;
            }
            after = page.last().unwrap().position;
            for row in page {
                writeln!(out, "{}", serde_json::to_string(&row)?)?;
            }
        }
        out.sync_all()?;
        let mut out = fs::File::create(self.root.join("actual-ledger.jsonl"))?;
        for row in ledger.execution_events_after(&self.execution, 0)? {
            writeln!(out, "{}", serde_json::to_string(&row)?)?;
        }
        out.sync_all()?;
        let snapshot = TaskCoordinator::new(journal).snapshot(&self.task)?;
        fs::write(
            self.root.join("actual-state.json"),
            serde_json::to_vec_pretty(&snapshot)?,
        )?;
        Ok(())
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        if let Err(error) = self.save() {
            eprintln!(
                "Cancellation evidence export failed at {}: {error}",
                self.root.display()
            );
        }
    }
}

async fn run_case(
    root: &Path,
    data: &Value,
    case: &Value,
    provider: Shared,
    family: &str,
    model: &str,
    max_output: Option<u32>,
) {
    fs::create_dir_all(
        root.join("workspace")
            .join(data["workspace_scope"].as_str().unwrap()),
    )
    .unwrap();
    let task = data["task_id"].as_str().unwrap();
    let binding: AttemptBinding = serde_json::from_value(json!({
        "attempt_id":data["attempt_id"],"invocation_id":"nested-delegate","execution":data["execution"],
        "agent":data["invocations"][2]["agent"],"constraints_digest":data["constraints_digest"]
    })).unwrap();
    let capture = Capture {
        root: root.into(),
        task: task.into(),
        execution: binding.execution.execution_id.clone(),
    };
    fs::write(
        root.join("expected.json"),
        serde_json::to_vec_pretty(&case["expected"]).unwrap(),
    )
    .unwrap();
    let initial = service(root);
    initial
        .coordinator()
        .register_task(
            "register-cancellation",
            TaskDefinition {
                task_id: task.into(),
                objective: data["objective"].as_str().unwrap().into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "nested-result".into(),
                    invocation_id: binding.invocation_id.clone(),
                }],
                agent: serde_json::from_value(data["agent"].clone()).unwrap(),
                constraints_digest: data["constraints_digest"].as_str().unwrap().into(),
                limits: serde_json::from_value(data["limits"].clone()).unwrap(),
                cancellation_policy: serde_json::from_value(case["policy"].clone()).unwrap(),
            },
        )
        .unwrap();
    for inv in data["invocations"].as_array().unwrap() {
        let mut inv = inv.clone();
        inv["constraints_digest"] = data["constraints_digest"].clone();
        initial
            .coordinator()
            .admit_invocation(
                task,
                &format!("admit-{}", inv["invocation_id"].as_str().unwrap()),
                serde_json::from_value(inv).unwrap(),
            )
            .unwrap();
    }
    initial
        .sessions()
        .sessions()
        .create(&binding.execution.session_id)
        .unwrap();
    let request: ModelRequest = serde_json::from_value(json!({
        "request_id":binding.execution.turn_id,"model":{"provider":family,"model":model},
        "system":[{"text":data["system"],"cache":false}],"messages":[{"role":"user","content":[{"type":"text","text":data["input"]}]}],
        "tools":RestrictedFileTool::tool_definitions(),"tool_choice":"auto","output_format":null,"prompt_cache":null,"reasoning":null,"max_output_tokens":max_output,"extensions":{}
    })).unwrap();
    let (_, paused) = initial
        .run(
            task,
            binding.clone(),
            executor(provider.clone(), root, data),
            TurnRequest {
                turn_id: binding.execution.turn_id.clone(),
                model_request: request,
                config: TurnConfig {
                    max_steps: data["limits"]["max_steps_per_turn"].as_u64().unwrap() as usize,
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    let DurableTurnResult::AwaitingApproval { approval, .. } = paused else {
        panic!("model must actually request the registered write and suspend for approval");
    };
    assert_eq!(approval.tool_name, data["offline_call"]["name"]);
    let before = initial.coordinator().snapshot(task).unwrap();
    assert_eq!(
        before.attempts[&binding.attempt_id].state,
        InvocationState::Suspended
    );
    assert!(
        !root
            .join("workspace")
            .join(data["path"].as_str().unwrap())
            .exists()
    );
    fs::write(
        root.join("pending-approval.json"),
        serde_json::to_vec_pretty(&approval).unwrap(),
    )
    .unwrap();
    drop(initial);
    let restarted = service(root);
    assert_eq!(restarted.coordinator().snapshot(task).unwrap(), before);
    let restored = restarted
        .sessions()
        .execution()
        .load_approval(&binding.execution.execution_id, &approval.approval_id)
        .unwrap();
    assert_eq!(restored, *approval);
    let cancelled = restarted
        .cancel(task, "cancel-task", data["cancel_reason"].as_str().unwrap())
        .unwrap();
    assert_eq!(cancelled.state, TaskState::Cancelled);
    assert!(cancelled.invocations["root"].cancellation_requested);
    assert_eq!(
        cancelled.attempts[&binding.attempt_id].cancellation_requested,
        !case["expected"]["resume_allowed"].as_bool().unwrap()
    );
    drop(restarted);
    let reopened = service(root);
    assert_eq!(reopened.coordinator().snapshot(task).unwrap(), cancelled);
    let resume = reopened
        .resume(
            task,
            binding.clone(),
            &approval.approval_id,
            executor(provider, root, data),
        )
        .await;
    if case["expected"]["resume_allowed"] == true {
        let (state, result) = resume.unwrap();
        assert_eq!(state.state, TaskState::Cancelled);
        let DurableTurnResult::Completed(execution, _) = result else {
            panic!("nested attempt must finish after its original approval resumes");
        };
        assert!(
            execution.result.steps.len() as u64
                >= case["expected"]["minimum_steps"].as_u64().unwrap()
        );
        let text: String = execution
            .result
            .steps
            .last()
            .unwrap()
            .response
            .content
            .iter()
            .filter_map(|block| {
                if let ContentBlock::Text { text } = block {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert!(
            text.contains(data["marker"].as_str().unwrap()),
            "actual final response lacks the fixture marker: {text}"
        );
    } else {
        assert!(
            resume.is_err(),
            "AllInvocations cancellation must refuse the original nested approval"
        );
        assert_eq!(reopened.coordinator().snapshot(task).unwrap(), cancelled);
    }
    let final_state = service(root).coordinator().snapshot(task).unwrap();
    assert_eq!(final_state.state, TaskState::Cancelled);
    assert!(final_state.success_evidence.is_empty());
    assert_eq!(final_state.invocations.len(), 3);
    assert_eq!(final_state.attempts.len(), 1);
    assert_eq!(
        serde_json::to_value(final_state.attempts[&binding.attempt_id].state).unwrap(),
        case["expected"]["nested_state"]
    );
    assert_eq!(
        root.join("workspace")
            .join(data["path"].as_str().unwrap())
            .exists(),
        case["expected"]["file_exists"].as_bool().unwrap()
    );
    if case["expected"]["file_exists"] == true {
        let bytes = fs::read(root.join("workspace").join(data["path"].as_str().unwrap())).unwrap();
        assert_eq!(bytes, data["marker"].as_str().unwrap().as_bytes());
        assert_eq!(bytes.len() as u64, data["byte_count"]);
    }
    let ledger = SqliteLedger::open(root.join("ledger.sqlite")).unwrap();
    let events = ledger
        .execution_events_after(&binding.execution.execution_id, 0)
        .unwrap();
    assert!(
        events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::StepCompleted)
            .count() as u64
            >= case["expected"]["minimum_steps"].as_u64().unwrap()
    );
    for (kind, key) in [
        (LedgerEventKind::ToolCallRequested, "calls"),
        (LedgerEventKind::EffectReceipt, "receipts"),
        (LedgerEventKind::ModelRequested, "model_requests"),
        (LedgerEventKind::ExecutionCancelled, "execution_cancelled"),
    ] {
        assert_eq!(
            events.iter().filter(|event| event.kind == kind).count() as u64,
            case["expected"][key],
            "{key}"
        );
    }
    let call = events
        .iter()
        .find(|event| event.kind == LedgerEventKind::ToolCallRequested)
        .unwrap();
    assert_eq!(call.payload["name"], data["offline_call"]["name"]);
    assert_eq!(call.payload["arguments"], data["offline_call"]["arguments"]);
    if let Some(receipt) = events
        .iter()
        .find(|event| event.kind == LedgerEventKind::EffectReceipt)
    {
        assert_eq!(receipt.payload["output"]["is_error"], false);
        assert_eq!(
            receipt.payload["output"]["call_id"],
            call.payload["call_id"]
        );
        assert!(events.iter().any(|event| {
            event.kind == LedgerEventKind::ModelRequested
                && event.cursor > receipt.cursor
                && event.payload["request"]["messages"]
                    .to_string()
                    .contains(&receipt.payload["output"].to_string())
        }));
    }
    let journal = SqliteFactJournal::open(root.join("ledger.sqlite"))
        .unwrap()
        .read(task, 0, 1024)
        .unwrap();
    assert!(
        !journal
            .iter()
            .any(|fact| fact.draft.kind == "task.completed")
    );
    assert_eq!(
        journal
            .iter()
            .filter(|fact| fact.draft.kind == "task.attempt_resumed")
            .count() as u64,
        case["expected"]["resume_facts"]
    );
    capture.save().unwrap();
}

fn combinations() -> Vec<Combination> {
    let config = common::load_config();
    let mut rows = Vec::new();
    for (family, cfg) in [
        ("minimax", config.minimax_openai),
        ("qwen", config.qwen_openai),
    ] {
        for model in cfg.model_matrix.clone() {
            rows.push((family, "openai_responses", model, Some(cfg.clone()), None));
        }
    }
    for (family, cfg) in [
        ("minimax", config.minimax_anthropic),
        ("qwen", config.qwen_anthropic),
    ] {
        for model in cfg.model_matrix.clone() {
            rows.push((family, "anthropic_messages", model, None, Some(cfg.clone())));
        }
    }
    rows
}

#[test]
fn cancellation_matrix_plans_every_configured_combination_and_policy() {
    let data = fixture();
    let rows = combinations();
    assert_eq!(rows.len() as u64, data["configured_combinations"]);
    assert_eq!(data["cases"].as_array().unwrap().len(), 2);
    assert_eq!(rows.len() as u64 * 2, data["planned_rows"]);
    let labels: BTreeSet<_> = rows
        .iter()
        .flat_map(|(family, protocol, model, _, _)| {
            data["cases"].as_array().unwrap().iter().map(move |case| {
                format!(
                    "{family}/{protocol}/{}/{}",
                    model.model,
                    case["name"].as_str().unwrap()
                )
            })
        })
        .collect();
    assert_eq!(labels.len() as u64, data["planned_rows"]);
}

#[tokio::test]
async fn deterministic_nested_approval_cancellation_from_fixture() {
    let data = fixture();
    let root = tempfile::Builder::new()
        .prefix("kolyan-task-cancellation-offline-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("Cancellation evidence: {}", root.display());
    for case in data["cases"].as_array().unwrap() {
        run_case(
            &root.join(case["name"].as_str().unwrap()),
            &data,
            case,
            Shared(Arc::new(Scripted(data.clone()))),
            "fixture",
            "offline",
            Some(1024),
        )
        .await;
    }
}

#[tokio::test]
#[ignore = "actual model-generated nested approvals for all 19 combinations and two cancel policies"]
async fn task_cancellation_live_matrix() {
    let data = fixture();
    let rows = combinations();
    assert_eq!(rows.len() as u64, data["configured_combinations"]);
    let labels: Vec<_> = rows
        .iter()
        .flat_map(|(family, protocol, model, _, _)| {
            data["cases"].as_array().unwrap().iter().map(move |case| {
                format!(
                    "{family}/{protocol}/{}/{}",
                    model.model,
                    case["name"].as_str().unwrap()
                )
            })
        })
        .collect();
    assert_eq!(labels.len() as u64, data["planned_rows"]);
    let mut report = matrix::Matrix::new(labels);
    let mut index = 0;
    for (family, protocol, model, openai, anthropic) in rows {
        for case in data["cases"].as_array().unwrap() {
            let root = report.directory.join(index.to_string());
            fs::create_dir_all(&root).unwrap();
            report
                .run(index, async {
                    let capture = Capture {
                        root: root.clone(),
                        task: data["task_id"].as_str().unwrap().into(),
                        execution: data["execution"]["execution_id"].as_str().unwrap().into(),
                    };
                    let table = serde_json::from_value(common::parameter_table(
                        family,
                        protocol,
                        &model.model,
                    ))
                    .unwrap();
                    let provider = if let Some(cfg) = &openai {
                        let key = common::require_api_key(cfg);
                        Shared(Arc::new(
                            common::build_openai_provider(cfg, &key)
                                .with_parameter_table(table)
                                .unwrap(),
                        ))
                    } else {
                        let cfg = anthropic.as_ref().unwrap();
                        let key = common::require_api_key_anthropic(cfg);
                        Shared(Arc::new(
                            common::build_anthropic_provider(cfg, &key)
                                .with_parameter_table(table)
                                .unwrap(),
                        ))
                    };
                    run_case(
                        &root,
                        &data,
                        case,
                        provider,
                        family,
                        &model.model,
                        model.max_output_tokens.or(Some(80960)),
                    )
                    .await;
                    capture.save().unwrap();
                })
                .await;
            index += 1;
        }
    }
    assert_eq!(index as u64, data["planned_rows"]);
    assert!(
        report
            .rows
            .iter()
            .all(|row| row.status == matrix::Status::Passed && row.attempts == 1),
        "38-row cancellation matrix failed: {}",
        report.directory.display()
    );
}
