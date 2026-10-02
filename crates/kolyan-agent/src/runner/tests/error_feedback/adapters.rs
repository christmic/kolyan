//! Dataset-driven synthetic responses/faults around the existing unit adapter.

use std::sync::{Arc, Mutex};

use futures_util::stream;
use kolyan_core::{ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::{
    ContentBlock, ModelDescriptor, ModelEvent, ModelEventStream, ModelProvider, ModelRequest,
    ModelResponse, ProviderFuture, StopReason, TokenUsage,
};
use kolyan_policy::{ApprovalMode, PathScope, PreparedCall};
use serde_json::{Value, json};

use super::super::super::*;
use super::super::support::{Observations, TestExecutor, Tools};
use super::{Case, Phase};
use crate::context::{BudgetMode, ContextPolicy, SerializedByteEstimator};

#[derive(Clone)]
pub(super) struct Factories {
    pub case: Case,
    pub observations: Arc<Observations>,
    pub dispatched: Arc<Mutex<Vec<Value>>>,
}

pub(super) struct Script(Factories);
impl ModelProvider for Script {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let mut observed = self.0.observations.requests.lock().unwrap();
        let index = observed.len();
        observed.push(request.clone());
        let calls = self.0.case.responses.get(index).cloned();
        Box::pin(async move {
            let calls = calls.ok_or_else(|| kolyan_model::ProviderError {
                kind: kolyan_model::ProviderErrorKind::Other,
                phase: kolyan_model::ProviderErrorPhase::Open,
                message: "synthetic response dataset exhausted".into(),
                provider: None,
                status: None,
                diagnostics: None,
            })?;
            let final_answer = calls.is_empty();
            let response = ModelResponse {
                id: request.request_id,
                model: request.model,
                content: if final_answer {
                    vec![ContentBlock::Text {
                        text: "Synthetic final answer".into(),
                    }]
                } else {
                    calls
                        .into_iter()
                        .map(|call| ContentBlock::ToolCall { call })
                        .collect()
                },
                structured_output: None,
                stop_reason: if final_answer {
                    StopReason::EndTurn
                } else {
                    StopReason::ToolUse
                },
                usage: TokenUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(2),
                    ..Default::default()
                },
                metadata: json!({"source":"synthetic_module_provider_not_actual_llm"}),
            };
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

impl ProviderFactory for Factories {
    type Provider = Script;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Script>, RunnerError> {
        Ok(ContextPreparingProvider::new(
            Script(self.clone()),
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(10000),
                max_output_tokens: Some(100),
                features: Default::default(),
            },
            ContextPolicy {
                id: "synthetic-error-context".into(),
                revision: "1".into(),
                mode: BudgetMode::Inspect,
                max_serialized_bytes: 65536,
                max_messages: 100,
                max_content_blocks: 200,
                context_limit_tokens: None,
                output_reserve_tokens: 50,
            },
            Arc::new(SerializedByteEstimator),
            self.observations.clone(),
        ))
    }
}

pub(super) struct Executor {
    inner: TestExecutor,
    factories: Factories,
}
impl ToolExecutor for Executor {
    fn prepare(&self, call: kolyan_model::ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            if let Some(fault) = &self.factories.case.fault
                && matches!(fault.phase, Phase::Prepare)
            {
                return Err(fault.error.error());
            }
            let prepared = self.inner.prepare(call.clone()).await?;
            // Real PolicyEngine evaluates the explicit synthetic resource claim.
            let mut claim = prepared.claim().clone();
            claim.resource.path = call.arguments["path"].as_str().map(str::to_owned);
            PreparedCall::new(
                call,
                prepared.tool_revision().into(),
                claim,
                prepared.requirements().clone(),
            )
            .map_err(|e| kolyan_core::ToolError::Failed {
                message: e.to_string(),
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
                .map_err(|e| kolyan_core::ToolError::PolicyDenied {
                    message: e.to_string(),
                })?;
            self.factories.dispatched.lock().unwrap().push(json!({"prepared":invocation.prepared,"grant":invocation.grant,"scope":invocation.scope,"policy_revision":invocation.policy_revision}));
            if let Some(fault) = &self.factories.case.fault
                && matches!(fault.phase, Phase::Execute)
            {
                return Err(fault.error.error());
            }
            self.inner.execute_invocation(invocation).await
        })
    }
}
impl EnvironmentToolFactory for Factories {
    type Executor = Executor;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<RunnerToolSet<Executor>, RunnerError> {
        let original =
            Tools(self.observations.clone(), self.case.approval).build(snapshot, execution)?;
        let mut policy = kolyan_policy::PolicyEngine::default();
        policy.register(kolyan_policy::ToolManifest {
            tool_name: "file.read".into(),
            capabilities: [kolyan_policy::Capability::FilesystemRead]
                .into_iter()
                .collect(),
            effects: [kolyan_policy::Effect::Read].into_iter().collect(),
            path_scopes: vec![PathScope::new("input.txt")],
            idempotency: kolyan_policy::Idempotency::Idempotent,
            approval: if self.case.approval {
                ApprovalMode::Always
            } else {
                ApprovalMode::Never
            },
        });
        Ok(RunnerToolSet {
            executor: Executor {
                inner: original.executor,
                factories: self.clone(),
            },
            definitions: original.definitions,
            policy: Arc::new(policy),
        })
    }
}
