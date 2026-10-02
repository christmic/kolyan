//! Scripted local Provider plus actual isolated OS tools; no network acceptance.
use super::Case;
use kolyan_agent::{
    AgentSnapshot, EnvironmentToolFactory, ProviderFactory, RunnerError, RunnerToolSet,
    context::{BudgetMode, ContextPolicy},
    provider::ContextPreparingProvider,
};
use kolyan_model::*;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyEngine, ToolManifest,
};
use kolyan_server::ExecutionRef;
use kolyan_tools::IsolatedToolSet;
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(super) struct Providers {
    pub inspection_window_assumption_tokens: u32,
    pub output_limit_tokens: u32,
    pub case: Case,
    pub requests: Arc<Mutex<Vec<ModelRequest>>>,
    pub model: ModelRef,
    pub live: Option<Arc<dyn ModelProvider>>,
}
#[derive(Clone)]
pub(super) struct Provider {
    case: Case,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    live: Option<Arc<dyn ModelProvider>>,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.requests.lock().unwrap().push(request.clone());
        if let Some(provider) = &self.live {
            let provider = provider.clone();
            return Box::pin(async move { provider.stream(request).await });
        }
        let write = self.case.write
            && !request
                .messages
                .iter()
                .flat_map(|m| &m.content)
                .any(|b| matches!(b, ContentBlock::ToolResult { .. }));
        let response = ModelResponse {
            id: request.request_id,
            model: request.model,
            content: if write {
                vec![ContentBlock::ToolCall {
                    call: ToolCall {
                        id: "write-call".into(),
                        name: "file.write".into(),
                        arguments: json!({"path":"result.txt","content":self.case.content}),
                    },
                }]
            } else {
                vec![ContentBlock::Text {
                    text: self.case.final_text.clone(),
                }]
            },
            structured_output: None,
            stop_reason: if write {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage {
                input_tokens: Some(1),
                output_tokens: Some(1),
                ..Default::default()
            },
            metadata: json!({"source":"local scripted model"}),
        };
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}
impl ProviderFactory for Providers {
    type Provider = Provider;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Provider>, RunnerError> {
        Ok(ContextPreparingProvider::new(
            Provider {
                case: self.case.clone(),
                requests: self.requests.clone(),
                live: self.live.clone(),
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                // Inspection-only host assumption; the counter remains untrusted.
                context_window: Some(self.inspection_window_assumption_tokens),
                max_output_tokens: None,
                features: Default::default(),
            },
            ContextPolicy {
                id: "native-goal-context".into(),
                revision: "v1".into(),
                mode: BudgetMode::Inspect,
                max_serialized_bytes: 1024 * 1024,
                max_messages: 128,
                max_content_blocks: 256,
                context_limit_tokens: None,
                output_reserve_tokens: self.output_limit_tokens,
            },
            Arc::new(super::super::providers::UnsupportedCounter),
            Arc::new(Recorder),
        ))
    }
}
struct Recorder;
impl kolyan_agent::provider::ContextRecorder for Recorder {
    fn record(
        &self,
        _: &kolyan_agent::provider::ContextRecord,
    ) -> Result<(), kolyan_agent::provider::ContextRecordError> {
        Ok(())
    }
}
pub(super) struct Tools {
    pub executor: IsolatedToolSet,
    pub workspace: String,
    pub approval: bool,
}
impl EnvironmentToolFactory for Tools {
    type Executor = IsolatedToolSet;
    fn build(
        &self,
        _: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<RunnerToolSet<IsolatedToolSet>, RunnerError> {
        let mut policy = PolicyEngine::default();
        policy.register(ToolManifest {
            tool_name: "file.write".into(),
            capabilities: [Capability::FilesystemWrite].into(),
            effects: [Effect::Create, Effect::Update].into(),
            path_scopes: vec![PathScope::new(self.workspace.clone())],
            idempotency: Idempotency::NonIdempotent,
            approval: if self.approval {
                ApprovalMode::Always
            } else {
                ApprovalMode::Never
            },
        });
        Ok(RunnerToolSet {
            executor: self.executor.clone(),
            definitions: IsolatedToolSet::tool_definitions(),
            policy: Arc::new(policy),
        })
    }
}
