//! Real Runtime producers with scripted local model output; no network/OS claim.
use super::backends::{Backend, BackendJournal, BackendLedger};
use crate::input_fixture::SourceFixtureAdmission;
use crate::*;
use kolyan_core::{
    ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture, TurnExecutor,
    TurnRequest,
};
use kolyan_model::*;
use kolyan_policy::*;
use kolyan_storage::{FileSessionStore, SessionStore};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(super) type Service =
    TaskExecutionService<BackendJournal, BackendLedger, NoopTraceSink, FileSessionStore>;
pub(super) struct Harness {
    pub service: Service,
    pub ledger: BackendLedger,
    pub backend: Backend,
    pub binding: AttemptBinding,
    pub requests: Arc<Mutex<Vec<ModelRequest>>>,
    pub tool_calls: Arc<AtomicUsize>,
    pub root: tempfile::TempDir,
}
pub(super) struct TextChecker {
    key: GoalCheckerKey,
    verdict: GoalVerdict,
}
impl GoalChecker for TextChecker {
    fn key(&self) -> &GoalCheckerKey {
        &self.key
    }
    fn validate_predicate(&self, criterion: &GoalCriterion) -> Result<(), TaskError> {
        if criterion.predicate != json!({"expected":"done"}) {
            return Err(TaskError::Invalid("predicate differs".into()));
        }
        Ok(())
    }
    fn assess(
        &self,
        _: &GoalCriterion,
        source: &VerifiedGoalSource,
    ) -> Result<ComputedGoalDecision, TaskError> {
        Ok(ComputedGoalDecision {
            verdict: self.verdict,
            reason: "actual scripted final response inspected".into(),
            proof: json!({"terminal":source.terminal(),"response":source.response(),"effects":source.effects().len()}),
        })
    }
}
pub(super) fn verifier(ledger: BackendLedger, verdict: GoalVerdict) -> Arc<dyn TaskGoalVerifier> {
    Arc::new(
        LedgerTaskGoalVerifier::new(ledger, registry(verdict), GoalSourceLimits::default())
            .unwrap(),
    )
}
pub(super) fn registry(verdict: GoalVerdict) -> GoalCheckerRegistry {
    GoalCheckerRegistry::new(vec![Arc::new(TextChecker {
        key: GoalCheckerKey {
            kind: "fixture.text".into(),
            revision: "v1".into(),
        },
        verdict,
    })])
    .unwrap()
}
pub(super) async fn harness(verdict: GoalVerdict, effect: bool, backend: Backend) -> Harness {
    harness_with_child(verdict, effect, false, backend).await
}
pub(super) async fn harness_with_child(
    verdict: GoalVerdict,
    effect: bool,
    child: bool,
    backend: Backend,
) -> Harness {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    store.create("session").unwrap();
    if child {
        store.create("session-child").unwrap();
    }
    let (ledger, journal) = backend.stores(root.path());
    let coordinator =
        TaskCoordinator::new(journal).with_goal_verifier(verifier(ledger.clone(), verdict));
    let agent = AgentIdentity {
        definition_id: "fixture".into(),
        revision: "r1".into(),
        instance_id: "fixture".into(),
    };
    let goal = GoalCriterion::new(
        "goal".into(),
        "root".into(),
        GoalCheckerKey {
            kind: "fixture.text".into(),
            revision: "v1".into(),
        },
        json!({"expected":"done"}),
    )
    .unwrap();
    coordinator
        .register_task(
            "register",
            TaskDefinition {
                task_id: "task".into(),
                objective: "verify actual final response".into(),
                criteria: vec![CompletionCriterion::Goal(goal)],
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                limits: TaskLimits {
                    max_depth: 2,
                    max_invocations: 4,
                    max_attempts: 4,
                    max_tokens: None,
                    max_steps_per_turn: 4,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    let admitted = coordinator
        .admit_fixture(
            "task",
            "admit",
            InvocationDefinition {
                invocation_id: "root".into(),
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                role: InvocationRole::Root,
                parent_invocation_id: None,
                dependencies: vec![],
                input_source: crate::input_fixture::fixture_source(
                    "task",
                    "root",
                    InvocationInputKind::Standalone,
                ),
            },
        )
        .unwrap();
    let binding = AttemptBinding {
        attempt_id: "attempt".into(),
        invocation_id: "root".into(),
        agent,
        constraints_digest: "a".repeat(64),
        input_source: admitted.invocations["root"].definition.input_source.clone(),
        execution: ExecutionRef {
            session_id: "session".into(),
            turn_id: "turn".into(),
            execution_id: "execution".into(),
        },
    };
    let service = TaskExecutionService::new(
        coordinator,
        SessionExecutionService::new(
            ExecutionService::new(ledger.clone(), NoopTraceSink),
            SessionService::new(store),
        ),
    );
    let requests = Arc::new(Mutex::new(Vec::new()));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "fixture.effect".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    let executor = TurnExecutor::with_tools(
        Provider {
            requests: requests.clone(),
            effect,
        },
        Tools(tool_calls.clone()),
    )
    .with_policy_engine(Arc::new(policy))
    .with_agent_snapshot_digest("a".repeat(64));
    let request = TurnRequest {
        turn_id: "turn".into(),
        model_request: ModelRequest {
            request_id: "request".into(),
            model: ModelRef::new("fixture", "model"),
            system: vec![],
            messages: vec![],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(64),
            extensions: Value::Null,
        },
        config: kolyan_core::TurnConfig {
            max_steps: 4,
            ..Default::default()
        },
    };
    if child {
        let admitted = service
            .coordinator()
            .admit_fixture(
                "task",
                "child-admit",
                InvocationDefinition {
                    invocation_id: "child".into(),
                    agent: binding.agent.clone(),
                    constraints_digest: binding.constraints_digest.clone(),
                    role: InvocationRole::Delegation,
                    parent_invocation_id: Some("root".into()),
                    dependencies: vec![],
                    input_source: crate::input_fixture::fixture_source(
                        "task",
                        "child",
                        InvocationInputKind::Derived,
                    ),
                },
            )
            .unwrap();
        let child_binding = AttemptBinding {
            attempt_id: "child-attempt".into(),
            invocation_id: "child".into(),
            execution: ExecutionRef {
                session_id: "session-child".into(),
                turn_id: "child-turn".into(),
                execution_id: "child-execution".into(),
            },
            agent: binding.agent.clone(),
            constraints_digest: binding.constraints_digest.clone(),
            input_source: admitted.invocations["child"]
                .definition
                .input_source
                .clone(),
        };
        let mut child_request = request.clone();
        child_request.turn_id = "child-turn".into();
        let (stopped, _) = service
            .run(
                "task",
                child_binding,
                TurnExecutor::new(Provider {
                    requests: requests.clone(),
                    effect: false,
                }),
                child_request,
            )
            .await
            .unwrap();
        service
            .coordinator()
            .consume_child_result(
                "task",
                "consume",
                "root",
                ConsumedResult {
                    child_invocation_id: "child".into(),
                    completion_fact: stopped.invocations["child"]
                        .completion_fact
                        .clone()
                        .unwrap(),
                    evidence: vec![],
                },
            )
            .unwrap();
    }
    service
        .run("task", binding.clone(), executor, request)
        .await
        .unwrap();
    Harness {
        service,
        ledger,
        backend,
        binding,
        requests,
        tool_calls,
        root,
    }
}
struct Provider {
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    effect: bool,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let mut requests = self.requests.lock().unwrap();
        let tool = self.effect && requests.is_empty();
        requests.push(request.clone());
        let response = ModelResponse {
            id: "response".into(),
            model: request.model,
            content: if tool {
                vec![ContentBlock::ToolCall {
                    call: ToolCall {
                        id: "call".into(),
                        name: "fixture.effect".into(),
                        arguments: json!({}),
                    },
                }]
            } else {
                vec![ContentBlock::Text {
                    text: "done".into(),
                }]
            },
            structured_output: None,
            stop_reason: if tool {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}
struct Tools(Arc<AtomicUsize>);
impl ToolExecutor for Tools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            PreparedCall::new(
                call,
                "v1".into(),
                InvocationClaim {
                    tool_name: "fixture.effect".into(),
                    capabilities: [Capability::FilesystemWrite].into(),
                    effects: [Effect::Update].into(),
                    resource: ResourceClaim { path: None },
                    idempotency: Idempotency::NonIdempotent,
                },
                ToolRequirements {
                    process_sandbox: false,
                    max_output_bytes: 1024,
                    timeout_ms: 1000,
                },
            )
            .map_err(|e| kolyan_core::ToolError::Failed {
                message: e.to_string(),
            })
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "effect observed".into(),
                is_error: false,
            }))
        })
    }
}
