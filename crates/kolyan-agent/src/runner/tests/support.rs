use std::sync::{Arc, Mutex};

use futures_util::stream;
use kolyan_core::{
    ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture,
    TurnConfig, TurnRequest,
};
use kolyan_ledger::{InMemoryLedger, MemoryFactJournal};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelDescriptor, ModelEvent, ModelEventStream,
    ModelProvider, ModelRef, ModelRequest, ModelResponse, ProviderFuture, StopReason,
    SystemInstruction, TokenUsage, ToolChoice, ToolDefinition,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PolicyEngine, PreparedCall,
    ResourceClaim, ToolManifest, ToolRequirements,
};
use kolyan_server::{ExecutionService, SessionExecutionService, SessionService, TaskCoordinator};
use kolyan_storage::FileSessionStore;
use kolyan_trace::NoopTraceSink;
use serde_json::json;

use super::super::*;
use crate::{
    AgentDefinition, AgentDefinitionInput, EnvironmentTool,
    context::{BudgetMode, ContextPolicy, ContextTokenCounter, TokenMeasurement},
    provider::{ContextRecord, ContextRecordError, ContextRecorder},
};

pub type Service =
    TaskExecutionService<MemoryFactJournal, InMemoryLedger, NoopTraceSink, FileSessionStore>;
type Runner = AgentRunner<
    MemoryFactJournal,
    InMemoryLedger,
    NoopTraceSink,
    FileSessionStore,
    Providers,
    Tools,
>;

#[derive(Default)]
pub struct Observations {
    pub requests: Mutex<Vec<ModelRequest>>,
    pub records: Mutex<Vec<ContextRecord>>,
    pub effects: Mutex<Vec<kolyan_policy::ToolExecutionScope>>,
}
impl ContextRecorder for Observations {
    fn record(&self, record: &ContextRecord) -> Result<(), ContextRecordError> {
        self.records.lock().unwrap().push(record.clone());
        Ok(())
    }
}
pub struct Provider(Arc<Observations>, bool);
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.0.requests.lock().unwrap().push(request.clone());
        let call_tool = self.1
            && !request.messages.iter().any(|message| {
                message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
            });
        Box::pin(async move {
            let response = ModelResponse {
                id: request.request_id,
                model: request.model,
                content: if call_tool {
                    vec![ContentBlock::ToolCall {
                        call: kolyan_model::ToolCall {
                            id: "read-call".into(),
                            name: "file.read".into(),
                            arguments: json!({"path":"input.txt"}),
                        },
                    }]
                } else {
                    vec![ContentBlock::Text {
                        text: "Actual root answer".into(),
                    }]
                },
                structured_output: None,
                stop_reason: if call_tool {
                    StopReason::ToolUse
                } else {
                    StopReason::EndTurn
                },
                usage: TokenUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(2),
                    ..Default::default()
                },
                metadata: json!({}),
            };
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}
struct Counter;
impl ContextTokenCounter for Counter {
    fn id(&self) -> &str {
        "unit-counter"
    }
    fn revision(&self) -> &str {
        "1"
    }
    fn count(&self, _: &ModelRequest, _: &[u8]) -> Result<TokenMeasurement, String> {
        Ok(TokenMeasurement::Trusted { input_tokens: 1 })
    }
}
pub struct Providers {
    pub observations: Arc<Observations>,
    pub fail: bool,
    pub reject_context: bool,
    pub call_tool: bool,
}
impl ProviderFactory for Providers {
    type Provider = Provider;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Provider>, RunnerError> {
        if self.fail {
            return Err(RunnerError::Host("factory refused".into()));
        }
        Ok(ContextPreparingProvider::new(
            Provider(self.observations.clone(), self.call_tool),
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(10000),
                max_output_tokens: Some(100),
                features: Default::default(),
            },
            ContextPolicy {
                id: "root-context".into(),
                revision: "1".into(),
                mode: BudgetMode::Strict,
                max_serialized_bytes: if self.reject_context { 1 } else { 65536 },
                max_messages: 100,
                max_content_blocks: 200,
                context_limit_tokens: None,
                output_reserve_tokens: 50,
            },
            Arc::new(Counter),
            self.observations.clone(),
        ))
    }
}
pub struct Tools(pub Arc<Observations>, pub bool);
pub struct TestExecutor(Arc<Observations>);
impl ToolExecutor for TestExecutor {
    fn prepare(&self, call: kolyan_model::ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            PreparedCall::new(
                call,
                "unit-read-v1".into(),
                InvocationClaim {
                    tool_name: "file.read".into(),
                    capabilities: [Capability::FilesystemRead].into_iter().collect(),
                    effects: [Effect::Read].into_iter().collect(),
                    resource: ResourceClaim {
                        path: Some("input.txt".into()),
                    },
                    idempotency: Idempotency::Idempotent,
                },
                ToolRequirements {
                    process_sandbox: false,
                    max_output_bytes: 4096,
                    timeout_ms: 1000,
                },
            )
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
            self.0.effects.lock().unwrap().push(invocation.scope);
            Ok(ToolOutcome::Completed(kolyan_model::ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "verified unit read".into(),
                is_error: false,
            }))
        })
    }
}
impl EnvironmentToolFactory for Tools {
    type Executor = TestExecutor;
    fn build(
        &self,
        _: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<RunnerToolSet<TestExecutor>, RunnerError> {
        let mut policy = PolicyEngine::default();
        policy.register(ToolManifest {
            tool_name: "file.read".into(),
            capabilities: [Capability::FilesystemRead].into_iter().collect(),
            effects: [Effect::Read].into_iter().collect(),
            path_scopes: vec![],
            idempotency: Idempotency::Idempotent,
            approval: if self.1 {
                ApprovalMode::Always
            } else {
                ApprovalMode::Never
            },
        });
        Ok(RunnerToolSet {
            executor: TestExecutor(self.0.clone()),
            definitions: definitions(),
            policy: Arc::new(policy),
        })
    }
}
pub fn definitions() -> Vec<ToolDefinition> {
    ["file.read", "file.write", "file.edit", "shell"]
        .into_iter()
        .map(|name| ToolDefinition {
            name: name.into(),
            description: None,
            input_schema: json!({"type":"object"}),
        })
        .collect()
}
pub fn execution(task: &str) -> ExecutionRef {
    ExecutionRef {
        session_id: "session".into(),
        turn_id: format!("turn-{task}"),
        execution_id: format!("execution-{task}"),
    }
}
pub struct Harness {
    pub _root: tempfile::TempDir,
    pub runner: Arc<Runner>,
    pub service: Arc<Service>,
    pub bindings: AgentInvocationBindingStore,
    pub observations: Arc<Observations>,
    definition: AgentDefinition,
}
impl Harness {
    pub fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let journal = MemoryFactJournal::default();
        let journal_port = Arc::new(journal.clone());
        let sessions =
            SessionService::new(FileSessionStore::new(root.path().join("sessions")).unwrap());
        sessions.create("session").unwrap();
        let service = Arc::new(TaskExecutionService::new(
            TaskCoordinator::new(journal),
            SessionExecutionService::new(
                ExecutionService::new(InMemoryLedger::default(), NoopTraceSink),
                sessions,
            ),
        ));
        let permissions = AgentPermissions {
            tools: [EnvironmentTool::Read].into_iter().collect(),
            delegation: Default::default(),
        };
        let definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "root-agent".into(),
            revision: "1".into(),
            display_name: None,
            model: ModelRef::new("test-provider", "selected-model"),
            instructions: "Agent instruction".into(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let mut catalog = AgentCatalog::new(8).unwrap();
        catalog.register(definition.clone()).unwrap();
        let observations = Arc::new(Observations::default());
        let bindings = AgentInvocationBindingStore::new(journal_port.clone());
        let runner = Arc::new(
            AgentRunner::new(
                service.clone(),
                InstanceRegistry::new(journal_port, "unit-host", 100).unwrap(),
                bindings.clone(),
                catalog,
                permissions,
                (
                    Providers {
                        observations: observations.clone(),
                        fail: false,
                        reject_context: false,
                        call_tool: false,
                    },
                    Tools(observations.clone(), false),
                ),
                Arc::new(
                    kolyan_trace::ArtifactStore::new(
                        root.path().join("input-artifacts"),
                        16 * 1024 * 1024,
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        );
        Self {
            _root: root,
            runner,
            service,
            bindings,
            observations,
            definition,
        }
    }
    pub fn request(&self, task: &str, named: bool) -> RootRunRequest {
        let execution = execution(task);
        RootRunRequest {
            goals: vec![],
            task_id: task.into(),
            invocation_id: "root".into(),
            attempt_id: "attempt".into(),
            execution: execution.clone(),
            selector: if named {
                AgentSelector::Named(self.definition.key())
            } else {
                AgentSelector::Inline(self.definition.clone())
            },
            requested_permissions: self.definition.permissions().clone(),
            objective: "Answer the user".into(),
            limits: TaskLimits {
                max_depth: 1,
                max_invocations: 1,
                max_attempts: 1,
                max_tokens: Some(50),
                max_steps_per_turn: 1,
            },
            cancellation_policy: CancellationPolicy::AllInvocations,
            turn: TurnRequest {
                turn_id: execution.turn_id,
                config: TurnConfig::default(),
                model_request: ModelRequest {
                    request_id: format!("request-{task}"),
                    model: ModelRef::new("ignored", "ignored"),
                    system: vec![SystemInstruction {
                        text: "Host instruction".into(),
                        cache: false,
                    }],
                    messages: vec![Message {
                        role: MessageRole::User,
                        content: vec![ContentBlock::Text {
                            text: format!("Input for {task}"),
                        }],
                    }],
                    tools: vec![],
                    tool_choice: ToolChoice::Auto,
                    output_format: None,
                    prompt_cache: None,
                    reasoning: None,
                    max_output_tokens: Some(100),
                    extensions: json!({}),
                },
            },
        }
    }
}
