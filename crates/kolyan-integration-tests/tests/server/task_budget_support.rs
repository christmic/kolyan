//! Fixture model only; journals, sessions, Runtime and governed filesystem effects are real.

use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::*;
use kolyan_policy::{ApprovalMode, PolicyEngine};
use kolyan_server::*;
use kolyan_storage::FileSessionStore;
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};

use super::{Case, task_input_source};

type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;

fn open(root: &Path) -> Service {
    TaskExecutionService::new(
        TaskCoordinator::new(SqliteFactJournal::open(root.join("facts.sqlite")).unwrap()),
        SessionExecutionService::new(
            ExecutionService::new(
                SqliteLedger::open(root.join("executions.sqlite")).unwrap(),
                NoopTraceSink,
            ),
            SessionService::new(FileSessionStore::new(root.join("sessions")).unwrap()),
        ),
    )
}

struct Fixture {
    approval: bool,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}
impl ModelProvider for Fixture {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.requests.lock().unwrap().push(request.clone());
        let loaded = request
            .messages
            .iter()
            .flat_map(|m| &m.content)
            .any(|b| matches!(b, ContentBlock::ToolResult { .. }));
        let call = self.approval && !loaded;
        let content = if call {
            vec![ContentBlock::ToolCall {
                call: ToolCall {
                    id: "write-call".into(),
                    name: "file.write".into(),
                    arguments: json!({"path":"result.txt","content":"budget\n"}),
                },
            }]
        } else {
            vec![ContentBlock::Text {
                text: "fixture finished".into(),
            }]
        };
        Box::pin(async move {
            Ok(
                Box::pin(futures_util::stream::iter([Ok(ModelEvent::Completed(
                    ModelResponse {
                        id: request.request_id,
                        model: request.model,
                        content,
                        structured_output: None,
                        stop_reason: if call {
                            StopReason::ToolUse
                        } else {
                            StopReason::EndTurn
                        },
                        usage: TokenUsage {
                            input_tokens: Some(1),
                            output_tokens: Some(1),
                            ..Default::default()
                        },
                        metadata: json!({"synthetic_model_not_vendor_acceptance":true}),
                    },
                ))])) as ModelEventStream,
            )
        })
    }
}

fn executor(
    root: &Path,
    approval: bool,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
) -> TurnExecutor<Fixture, PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>> {
    let mut policy = PolicyEngine::default();
    for mut manifest in RestrictedFileTool::tool_manifests() {
        manifest.approval = if approval && manifest.tool_name == "file.write" {
            ApprovalMode::Always
        } else {
            ApprovalMode::Never
        };
        policy.register(manifest);
    }
    let policy = Arc::new(policy);
    TurnExecutor::with_tools(
        Fixture { approval, requests },
        PolicyEnforcingTool::new(RestrictedFileTool::new(root), policy.clone()),
    )
    .with_policy_engine(policy)
}

fn request(id: &str, steps: usize) -> TurnRequest {
    TurnRequest {
        turn_id: format!("turn-{id}"),
        config: TurnConfig {
            max_steps: steps,
            ..Default::default()
        },
        model_request: ModelRequest {
            request_id: format!("request-{id}"),
            model: ModelRef::new("fixture", "budget"),
            system: vec![],
            messages: vec![Message {
                role: MessageRole::User,
                content: vec![ContentBlock::Text {
                    text: "Fixture budget execution".into(),
                }],
            }],
            tools: RestrictedFileTool::tool_definitions(),
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: json!({}),
        },
    }
}

fn now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn read_output(workspace: &Path) -> Option<String> {
    match fs::read_to_string(workspace.join("result.txt")) {
        Ok(value) => Some(value),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("cannot inspect actual file postcondition: {error}"),
    }
}

fn admit_successor(service: &Service, root: &AttemptBinding, steps: usize) -> AttemptBinding {
    let snapshot = service.coordinator().snapshot("task").unwrap();
    let definition = json!({"invocation_id":"child","role":InvocationRole::Continuation,
        "agent":root.agent,"constraints_digest":root.constraints_digest,"parent_invocation_id":"root"});
    let source = task_input_source::publish(
        service.coordinator(),
        "task",
        &definition,
        request("child", steps).model_request,
    );
    service
        .sessions()
        .sessions()
        .create("session-child")
        .unwrap();
    service
        .coordinator()
        .admit_invocation(
            "task",
            "admit-child",
            InvocationDefinition {
                invocation_id: "child".into(),
                agent: root.agent.clone(),
                constraints_digest: root.constraints_digest.clone(),
                role: InvocationRole::Continuation,
                parent_invocation_id: Some("root".into()),
                dependencies: vec!["root".into()],
                input_source: source.clone(),
            },
        )
        .unwrap();
    let AttemptOutcome::Completed { evidence } = &snapshot.attempts[&root.attempt_id]
        .observation
        .as_ref()
        .unwrap()
        .outcome
    else {
        panic!("predecessor must be completed")
    };
    service
        .coordinator()
        .consume_child_result(
            "task",
            "consume-root",
            "child",
            ConsumedResult {
                child_invocation_id: "root".into(),
                completion_fact: snapshot.invocations["root"]
                    .completion_fact
                    .clone()
                    .unwrap(),
                evidence: evidence.clone(),
            },
        )
        .unwrap();
    AttemptBinding {
        attempt_id: "attempt-child".into(),
        invocation_id: "child".into(),
        agent: root.agent.clone(),
        constraints_digest: root.constraints_digest.clone(),
        input_source: source,
        execution: ExecutionRef {
            session_id: "session-child".into(),
            turn_id: "turn-child".into(),
            execution_id: "exec-child".into(),
        },
    }
}

pub(super) async fn run(case: &Case, root: &Path) -> Value {
    fs::create_dir_all(root).unwrap();
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let current = open(root);
    let agent = AgentIdentity {
        definition_id: "fixture".into(),
        revision: "r1".into(),
        instance_id: "instance".into(),
    };
    let digest = "a".repeat(64);
    current
        .coordinator()
        .register_task(
            "register",
            TaskDefinition {
                task_id: "task".into(),
                objective: "Exercise the actual budget consumer".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent.clone(),
                constraints_digest: digest.clone(),
                limits: TaskLimits {
                    max_depth: 2,
                    max_invocations: 3,
                    max_attempts: 3,
                    max_tokens: None,
                    max_steps_per_turn: 8,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    let deadline = match case.mode.as_str() {
        "expired" => 1,
        "approval_expired" | "approval_deny_expired" => now() + 1000,
        "answer" | "approval" | "approval_child" => now() + 120000,
        other => panic!("unknown case mode {other}"),
    };
    current
        .coordinator()
        .configure_execution_budget(
            "task",
            "policy",
            TaskExecutionBudgetPolicy {
                id: "fixture-budget".into(),
                revision: "r1".into(),
                max_reserved_steps: case.total,
                deadline_at_ms: deadline,
            },
        )
        .unwrap();
    let mut bindings = Vec::new();
    let ids = if case.mode == "approval_child" {
        vec!["root", "child"]
    } else {
        vec!["root"]
    };
    for id in ids {
        current
            .sessions()
            .sessions()
            .create(&format!("session-{id}"))
            .unwrap();
        let role = if id == "root" {
            InvocationRole::Root
        } else {
            InvocationRole::SelfCall
        };
        let definition = json!({"invocation_id":id,"role":role,"agent":agent,"constraints_digest":digest,"parent_invocation_id":if id=="root"{None}else{Some("root")}});
        let source = task_input_source::publish(
            current.coordinator(),
            "task",
            &definition,
            request(id, case.request_steps).model_request,
        );
        current
            .coordinator()
            .admit_invocation(
                "task",
                &format!("admit-{id}"),
                InvocationDefinition {
                    invocation_id: id.into(),
                    agent: agent.clone(),
                    constraints_digest: digest.clone(),
                    role,
                    parent_invocation_id: (id != "root").then(|| "root".into()),
                    dependencies: vec![],
                    input_source: source.clone(),
                },
            )
            .unwrap();
        bindings.push(AttemptBinding {
            attempt_id: format!("attempt-{id}"),
            invocation_id: id.into(),
            agent: agent.clone(),
            constraints_digest: digest.clone(),
            input_source: source,
            execution: ExecutionRef {
                session_id: format!("session-{id}"),
                turn_id: format!("turn-{id}"),
                execution_id: format!("exec-{id}"),
            },
        });
    }
    let requests = Arc::new(Mutex::new(Vec::new()));
    let approval = case.mode.starts_with("approval");
    let first = current
        .run(
            "task",
            bindings[0].clone(),
            executor(&workspace, approval, requests.clone()),
            request("root", case.request_steps),
        )
        .await;
    let start_ok = first.is_ok();
    let start_error = first.as_ref().err().map(ToString::to_string);
    let mut checkpoint = Value::Null;
    let mut before_resume = Value::Null;
    let mut resume_ok = None;
    let mut resume_error = None;
    let mut deny_ok = None;
    let mut deny_error = None;
    let file_before_resume = read_output(&workspace);
    let mut child_ok = None;
    let mut child_error = None;
    if approval {
        let saved = current
            .sessions()
            .execution()
            .load_current_suspension("exec-root")
            .unwrap();
        checkpoint = serde_json::to_value(&saved.checkpoint).unwrap();
        let approval_id = saved.checkpoint.approvals[0].approval_id.clone();
        if case.mode == "approval_child" {
            let child = current
                .run(
                    "task",
                    bindings[1].clone(),
                    executor(&workspace, false, requests.clone()),
                    request("child", case.request_steps),
                )
                .await;
            child_ok = Some(child.is_ok());
            child_error = child.as_ref().err().map(ToString::to_string);
            let snapshot = current.coordinator().snapshot("task").unwrap();
            let AttemptOutcome::Completed { evidence } = &snapshot.attempts["attempt-child"]
                .observation
                .as_ref()
                .unwrap()
                .outcome
            else {
                panic!("child must complete before consumption")
            };
            current
                .coordinator()
                .consume_child_result(
                    "task",
                    "consume-child",
                    "root",
                    ConsumedResult {
                        child_invocation_id: "child".into(),
                        completion_fact: snapshot.invocations["child"]
                            .completion_fact
                            .clone()
                            .unwrap(),
                        evidence: evidence.clone(),
                    },
                )
                .unwrap();
        }
        before_resume =
            serde_json::to_value(current.coordinator().snapshot("task").unwrap()).unwrap();
        drop(current);
        if matches!(
            case.mode.as_str(),
            "approval_expired" | "approval_deny_expired"
        ) {
            tokio::time::sleep(Duration::from_millis(
                deadline.saturating_add(50).saturating_sub(now()),
            ))
            .await;
        }
        let rebuilt = open(root);
        if case.mode == "approval_deny_expired" {
            let denied = rebuilt.sessions().deny_pending(
                &bindings[0].execution,
                &saved.checkpoint.scope,
                &approval_id,
            );
            deny_ok = Some(denied.is_ok());
            deny_error = denied.as_ref().err().map(ToString::to_string);
            if denied.is_ok() {
                rebuilt.reconcile("task", "attempt-root").unwrap();
            }
        } else {
            let resumed = rebuilt
                .resume_approval(
                    "task",
                    bindings[0].clone(),
                    &approval_id,
                    executor(&workspace, true, requests.clone()),
                )
                .await;
            resume_ok = Some(resumed.is_ok());
            resume_error = resumed.as_ref().err().map(ToString::to_string);
        }
    } else {
        if case.child {
            let child = admit_successor(&current, &bindings[0], case.request_steps);
            let observed = current
                .run(
                    "task",
                    child,
                    executor(&workspace, false, requests.clone()),
                    request("child", case.request_steps),
                )
                .await;
            child_ok = Some(observed.is_ok());
            child_error = observed.as_ref().err().map(ToString::to_string);
        }
        drop(current);
    }
    let rebuilt = open(root);
    let after = rebuilt.coordinator().snapshot("task").unwrap();
    let budget = after.execution_budget.as_ref().unwrap();
    let ledger = rebuilt
        .sessions()
        .execution()
        .server()
        .coordinator()
        .ledger();
    let events = ledger.execution_events_after("exec-root", 0).unwrap();
    let admission = events
        .iter()
        .find(|e| e.kind == kolyan_ledger::LedgerEventKind::ExecutionInputAdmitted)
        .map(|e| e.payload.clone());
    task_input_source::export(rebuilt.coordinator(), "task", &root.join("sources.jsonl")).unwrap();
    json!({"id":case.id,"start_ok":start_ok,"start_error":start_error,"resume_ok":resume_ok,"resume_error":resume_error,"deny_ok":deny_ok,"deny_error":deny_error,"child_ok":child_ok,"child_error":child_error,
        "deadline":deadline,"requests":*requests.lock().unwrap(),"remaining":budget.remaining_steps().unwrap(),"root_reserved":budget.reservations.get("attempt-root").map(|s|s.max_steps).unwrap_or(0),"child_reserved":budget.reservations.get("attempt-child").map(|s|s.max_steps).unwrap_or(0),
        "root_admission":admission,"checkpoint":checkpoint,"before_resume":before_resume,"after":after,
        "file_before_resume":file_before_resume,"file":read_output(&workspace),"ledger":events,
        "child_ledger":ledger.execution_events_after("exec-child",0).unwrap(),"facts":rebuilt.coordinator().journal().read("task",0,100).unwrap(),
        "session":rebuilt.sessions().sessions().load("session-root").unwrap()})
}
