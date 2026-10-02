//! Agent invocation routing is distinct from the four environment adapters.
//! Only admission runs in a Turn tool call; child driving is host-owned.

mod advertisement;
mod schema;

use std::sync::Arc;

use kolyan_core::{ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture};
use kolyan_model::{ToolCall, ToolDefinition};
use kolyan_policy::ApprovalMode;
use kolyan_server::AttemptBinding;

use super::delegation::denied;
use super::tools::SnapshotTools;
use super::*;
use crate::{
    AGENT_INVOKE_NAME, AgentInvocationBinding, InvokePrepareLimits, prepare_agent_invocation,
};

/// Host-selected preparation bounds and dynamic approval mode.
#[derive(Debug, Clone)]
pub struct AgentDelegationConfig {
    pub limits: InvokePrepareLimits,
    pub approval: ApprovalMode,
}

pub(super) struct RoutedTools<J, L, S, SS, P, T: EnvironmentToolFactory> {
    pub runner: Arc<AgentRunner<J, L, S, SS, P, T>>,
    pub saved: AgentInvocationBinding,
    pub parent: AttemptBinding,
    pub environment: SnapshotTools<T::Executor>,
}

impl<J, L, S, SS, P, T> ToolExecutor for RoutedTools<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            if call.name != AGENT_INVOKE_NAME {
                return self.environment.prepare(call).await;
            }
            let config = self
                .runner
                .delegation
                .as_ref()
                .ok_or_else(|| denied("delegation not configured"))?;
            prepare_agent_invocation(
                call,
                &self.saved,
                &self.parent.execution,
                &self.runner.catalog,
                &self.runner.host,
                &config.limits,
            )
            .map(|plan| plan.prepared().clone())
            .map_err(denied)
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            if invocation.prepared.call().name != AGENT_INVOKE_NAME {
                return self.environment.execute_invocation(invocation).await;
            }
            let config = self
                .runner
                .delegation
                .as_ref()
                .ok_or_else(|| denied("delegation not configured"))?;
            let key = &invocation.scope.execution;
            if key.session_id != self.parent.execution.session_id
                || key.turn_id != self.parent.execution.turn_id
                || key.execution_id != self.parent.execution.execution_id
                || invocation.scope.agent_snapshot_digest.as_deref()
                    != Some(self.saved.snapshot.digest())
            {
                return Err(denied(
                    "Core execution scope differs from bound host parent",
                ));
            }
            // Step identity is supplied by the running Core, never copied from
            // grant claims. Admission checks the full scope independently.
            let owner = DelegationOwner {
                task_id: self.saved.task_id.clone(),
                logical_session_id: self.saved.logical_session_id.clone(),
                parent: self.parent.clone(),
                scope: invocation.scope.clone(),
            };
            let wait = self
                .runner
                .admit_agent_children(
                    owner,
                    invocation,
                    config.limits.clone(),
                    self.environment.policy.clone(),
                )
                .await?;
            Ok(ToolOutcome::AwaitingExternal(wait))
        })
    }
}

pub(super) use advertisement::AdvertisementProvider;

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    pub(super) fn delegation_definition(
        &self,
        snapshot: &AgentSnapshot,
    ) -> Result<Option<ToolDefinition>, RunnerError> {
        let Some(config) = &self.delegation else {
            return Ok(None);
        };
        let mut definition = advertisement::definition(snapshot, &self.host, &self.catalog)?;
        if let Some(definition) = &mut definition {
            // The protocol's global maximum is not the host's admission bound.
            definition.input_schema["properties"]["children"]["maxItems"] =
                serde_json::json!(config.limits.max_children);
        }
        Ok(definition)
    }

    /// Every initial/resume/child/pump executor shares this exact advertisement.
    pub(super) fn routed_tool_set(
        &self,
        snapshot: &AgentSnapshot,
        execution: &kolyan_server::ExecutionRef,
    ) -> Result<RunnerToolSet<T::Executor>, RunnerError> {
        let mut set = self.tools.build(snapshot, execution)?;
        super::tools::validate_inventory(snapshot, &set.definitions)?;
        if let Some(definition) = self.delegation_definition(snapshot)? {
            let config = self
                .delegation
                .as_ref()
                .ok_or_else(|| RunnerError::Host("missing delegation configuration".into()))?;
            let mut policy = (*set.policy).clone();
            policy.register(crate::agent_invoke_manifest(config.approval));
            set.policy = Arc::new(policy);
            set.definitions.push(definition);
        }
        Ok(set)
    }

    pub(super) fn routed_provider(
        &self,
        snapshot: &AgentSnapshot,
        execution: &kolyan_server::ExecutionRef,
    ) -> Result<
        AdvertisementProvider<crate::provider::ContextPreparingProvider<P::Provider>>,
        RunnerError,
    > {
        Ok(AdvertisementProvider {
            inner: self.providers.build(snapshot, execution)?,
            expected: self.delegation_definition(snapshot)?,
        })
    }
}

#[cfg(test)]
mod tests;
