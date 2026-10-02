//! Portable loop regressions; deterministic model requests are actually driven
//! through Turn, not manufactured checkpoint-only completion evidence.

use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

mod checkpoint_recording;
mod limits;
mod parallel;
mod serialization;

#[derive(Clone, Copy)]
enum Behavior {
    Complete,
    Wait,
    Uncertain,
    InvalidWait,
    Oversized,
    Hang,
    PrepareFailure,
    Failure,
}

#[derive(Clone)]
struct ScenarioTools {
    behaviors: HashMap<String, Behavior>,
    prepared: Arc<Mutex<Vec<String>>>,
    executed: Arc<Mutex<Vec<String>>>,
    issued: Arc<Mutex<Vec<PreparedCall>>>,
    output_limit: u64,
    timeout_ms: u64,
    version: Arc<AtomicUsize>,
}

impl ScenarioTools {
    fn new(behaviors: &[(&str, Behavior)]) -> Self {
        Self {
            behaviors: behaviors
                .iter()
                .map(|(id, behavior)| ((*id).into(), *behavior))
                .collect(),
            prepared: Default::default(),
            executed: Default::default(),
            issued: Default::default(),
            output_limit: 4096,
            timeout_ms: 1000,
            version: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn executed(&self) -> Vec<String> {
        self.executed.lock().unwrap().clone()
    }
    fn prepare_count(&self, id: &str) -> usize {
        self.prepared
            .lock()
            .unwrap()
            .iter()
            .filter(|item| item.as_str() == id)
            .count()
    }
}

impl ToolExecutor for ScenarioTools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            self.prepared.lock().unwrap().push(call.id.clone());
            if matches!(self.behaviors.get(&call.id), Some(Behavior::PrepareFailure)) {
                return Err(ToolError::Failed {
                    message: "fixture preparation rejected".into(),
                });
            }
            let original = fixture_prepare(call)?;
            let mut requirements = original.requirements().clone();
            requirements.max_output_bytes = self.output_limit;
            requirements.timeout_ms = self.timeout_ms;
            PreparedCall::new(
                original.call().clone(),
                original.tool_revision().into(),
                original.claim().clone(),
                requirements,
            )
            .and_then(|prepared| {
                prepared.with_execution_binding(
                    serde_json::json!({"observed_version":self.version.load(Ordering::SeqCst)}),
                )
            })
            .map_err(|error| ToolError::Failed {
                message: error.to_string(),
            })
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &invocation.policy_revision,
                    &invocation.scope,
                )
                .map_err(|error| ToolError::PolicyDenied {
                    message: error.to_string(),
                })?;
            let current = self.prepare(invocation.prepared.call().clone()).await?;
            if current != invocation.prepared
                || invocation.scope.execution != fixture_key("suspension")
            {
                return Err(ToolError::PolicyDenied { message: "scenario authority differs from actual adapter preparation or admitted owner".into() });
            }
            let call = invocation.prepared.call();
            self.issued
                .lock()
                .unwrap()
                .push(invocation.prepared.clone());
            self.executed.lock().unwrap().push(call.id.clone());
            match self
                .behaviors
                .get(&call.id)
                .copied()
                .unwrap_or(Behavior::Complete)
            {
                Behavior::Wait => Ok(ToolOutcome::AwaitingExternal(external_wait(&call.id))),
                Behavior::InvalidWait => Ok(ToolOutcome::AwaitingExternal(ExternalWait {
                    schema_version: 0,
                    ..external_wait(&call.id)
                })),
                Behavior::Uncertain => Err(ToolError::Uncertain {
                    message: "Started has no verified receipt or wait".into(),
                }),
                Behavior::Failure => Err(ToolError::Failed {
                    message: "known adapter failure".into(),
                }),
                Behavior::Oversized => Ok(ToolOutcome::Completed(ToolResult {
                    content: "x".repeat(self.output_limit as usize),
                    ..tool_result(&call.id)
                })),
                Behavior::Hang => std::future::pending().await,
                _ => {
                    if call.id == "write" {
                        self.version.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(ToolOutcome::Completed(tool_result(&call.id)))
                }
            }
        })
    }
}

struct BatchProvider {
    batch: Vec<ToolCall>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    repeat: bool,
}

impl ModelProvider for BatchProvider {
    fn stream(&self, request: ModelRequest) -> kolyan_model::ProviderFuture<'_> {
        let mut requests = self.requests.lock().unwrap();
        let content = if requests.is_empty() || self.repeat {
            self.batch
                .iter()
                .map(|call| ContentBlock::ToolCall { call: call.clone() })
                .collect()
        } else {
            let results: Vec<_> = request
                .messages
                .iter()
                .flat_map(|message| &message.content)
                .filter_map(|block| {
                    if let ContentBlock::ToolResult { result } = block {
                        Some(result.call_id.clone())
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(
                results,
                self.batch
                    .iter()
                    .map(|call| call.id.clone())
                    .collect::<Vec<_>>()
            );
            vec![ContentBlock::Text {
                text: "all original calls have paired results".into(),
            }]
        };
        let first = requests.is_empty() || self.repeat;
        requests.push(request.clone());
        drop(requests);
        let response = kolyan_model::ModelResponse {
            id: request.request_id,
            model: request.model,
            content,
            structured_output: None,
            stop_reason: if first {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage {
                input_tokens: Some(11),
                output_tokens: Some(7),
                ..Default::default()
            },
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

type ScenarioExecutor = TurnExecutor<BatchProvider, ScenarioTools>;

fn call(id: &str, name: &str, path: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments: serde_json::json!({"path":path,"content":"fixture"}),
    }
}

fn external_wait(id: &str) -> ExternalWait {
    ExternalWait {
        wait_id: checkpoint::host_identity("wait", &id),
        kind: "child-v1".into(),
        schema_version: 1,
        binding: serde_json::json!({"admitted_call":id,"trusted_fact":"child-admitted"}),
    }
}
fn tool_result(id: &str) -> ToolResult {
    ToolResult {
        call_id: id.into(),
        content: "verified result".into(),
        is_error: false,
    }
}

fn executor(
    calls: Vec<ToolCall>,
    tools: ScenarioTools,
    mode: ToolDispatchMode,
    approve_write: bool,
) -> (ScenarioExecutor, Arc<Mutex<Vec<ModelRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut policy = PolicyEngine::default();
    for name in ["file.read", "file.write"] {
        let write = name == "file.write";
        policy.register(kolyan_policy::ToolManifest {
            tool_name: name.into(),
            capabilities: [if write {
                kolyan_policy::Capability::FilesystemWrite
            } else {
                kolyan_policy::Capability::FilesystemRead
            }]
            .into(),
            effects: [if write {
                kolyan_policy::Effect::Update
            } else {
                kolyan_policy::Effect::Read
            }]
            .into(),
            path_scopes: vec![],
            idempotency: if write {
                kolyan_policy::Idempotency::NonIdempotent
            } else {
                kolyan_policy::Idempotency::Idempotent
            },
            approval: if write && approve_write {
                kolyan_policy::ApprovalMode::Always
            } else {
                kolyan_policy::ApprovalMode::Never
            },
        });
    }
    let executor = TurnExecutor::with_tools(
        BatchProvider {
            batch: calls,
            requests: requests.clone(),
            repeat: false,
        },
        tools,
    )
    .with_execution_key(fixture_key("suspension"))
    .with_policy_engine(Arc::new(policy))
    .with_tool_dispatch_policy(ToolDispatchPolicy {
        mode,
        on_error: ToolErrorPolicy::ContinueBatch,
    });
    (executor, requests)
}

fn input(max_calls: usize) -> TurnRequest {
    TurnRequest {
        turn_id: "suspension".into(),
        model_request: request(),
        config: TurnConfig {
            max_steps: 3,
            max_tool_calls: Some(max_calls),
            deadline: None,
        },
    }
}
fn suspended(outcome: ResumableTurn) -> TurnSuspension {
    match outcome {
        ResumableTurn::Suspended(value) => *value,
        other => panic!("expected suspension: {other:?}"),
    }
}
fn resolve(
    executor: &ScenarioExecutor,
    suspension: TurnSuspension,
    ids: &[&str],
) -> TurnCheckpoint {
    let scope = suspension.checkpoint.scope.clone();
    let resolutions = ids
        .iter()
        .map(|id| ExternalResolution {
            call_id: (*id).into(),
            wait: external_wait(id),
            result: tool_result(id),
        })
        .collect();
    executor
        .merge_resume_with_control(
            suspension,
            ResumeInput::ExternalResolved(resolutions),
            scope,
            TurnControl::default(),
        )
        .unwrap()
}

#[tokio::test]
async fn serial_tail_reconstructs_from_bytes_and_never_reprepares_or_reexecutes_completed_head() {
    let tools = ScenarioTools::new(&[("head", Behavior::Wait)]);
    let (executor, requests) = executor(
        vec![
            call("head", "file.read", "a"),
            call("tail", "file.read", "b"),
        ],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let waiting = suspended(executor.start_resumable(input(2)).await.unwrap());
    assert_eq!(tools.executed(), ["head"]);
    assert!(matches!(
        waiting.checkpoint.calls[1].state,
        CheckpointCallState::Ready
    ));
    let head_preparations = tools.prepare_count("head");
    let bytes = serde_json::to_vec(&waiting).unwrap();
    let restored: TurnSuspension = serde_json::from_slice(&bytes).unwrap();
    let checkpoint = resolve(&executor, restored, &["head"]);
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert_eq!(checkpoint.budget.tool_calls_used, 1);
    let scope = checkpoint.scope.clone();
    let outcome = executor
        .resume_checkpoint_with_control(checkpoint, scope, TurnControl::default())
        .await
        .unwrap();
    assert!(matches!(outcome, ResumableTurn::Completed(_)));
    assert_eq!(tools.executed(), ["head", "tail"]);
    assert_eq!(tools.prepare_count("head"), head_preparations);
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn actual_loop_opaque_call_id_and_hashed_approval_survive_persisted_resume() {
    for id in [
        "调用/原生".to_owned(),
        "provider/id/path".to_owned(),
        format!("调用/{}", "x".repeat(1024 - "调用/".len())),
    ] {
        let tools = ScenarioTools::new(&[]);
        let (executor, requests) = executor(
            vec![call(&id, "file.write", "safe/a")],
            tools.clone(),
            ToolDispatchMode::Serial,
            true,
        );
        let waiting = suspended(executor.start_resumable(input(1)).await.unwrap());
        let approval = &waiting.waiting.approvals[0];
        assert!(approval.approval_id.starts_with("approval-"));
        assert_eq!(approval.approval_id.len(), 73);
        assert_eq!(approval.call_id, id);
        assert_eq!(tools.executed().len(), 0);
        let restored: TurnSuspension =
            serde_json::from_slice(&serde_json::to_vec(&waiting).unwrap()).unwrap();
        let outcome = resume_confirmed(&executor, restored, &approval.approval_id)
            .await
            .unwrap();
        assert!(matches!(outcome, ResumableTurn::Completed(_)));
        assert_eq!(tools.executed(), [id]);
        assert_eq!(requests.lock().unwrap().len(), 2);
    }
}
