//! Static Agent authority narrows dynamic policy; schemas alone are not enforcement.

use std::collections::BTreeSet;
use std::sync::Arc;

use kolyan_core::{ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::{ToolCall, ToolDefinition};
use kolyan_policy::PolicyEngine;
use kolyan_server::ExecutionRef;

use super::RunnerError;
use crate::{AgentSnapshot, EnvironmentTool};

pub(super) struct SnapshotTools<T> {
    pub inner: T,
    pub snapshot: AgentSnapshot,
    pub execution: ExecutionRef,
    pub policy: Arc<PolicyEngine>,
}

impl<T: ToolExecutor> ToolExecutor for SnapshotTools<T> {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            if !permits(&self.snapshot, &call.name) {
                return Err(denied("tool exceeds the saved Agent ceiling"));
            }
            let prepared = self.inner.prepare(call.clone()).await?;
            if prepared.call() != &call {
                return Err(denied("adapter changed the original tool intent"));
            }
            Ok(prepared)
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            let key = &invocation.scope.execution;
            if !permits(&self.snapshot, &invocation.prepared.call().name)
                || key.session_id != self.execution.session_id
                || key.turn_id != self.execution.turn_id
                || key.execution_id != self.execution.execution_id
                || invocation.scope.agent_snapshot_digest.as_deref() != Some(self.snapshot.digest())
                || invocation.policy_revision != self.policy.revision()
            {
                return Err(denied(
                    "execution ownership, snapshot or current policy differs",
                ));
            }
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &self.policy.revision(),
                    &invocation.scope,
                )
                .map_err(|error| denied(&error.to_string()))?;
            self.inner.execute_invocation(invocation).await
        })
    }
}

pub(super) fn environment_name(name: &str) -> bool {
    [
        EnvironmentTool::Read,
        EnvironmentTool::Write,
        EnvironmentTool::Edit,
        EnvironmentTool::Shell,
    ]
    .iter()
    .any(|tool| tool.name() == name)
}

pub(super) fn validate_inventory(
    snapshot: &AgentSnapshot,
    definitions: &[ToolDefinition],
) -> Result<(), RunnerError> {
    let mut names = BTreeSet::new();
    for definition in definitions {
        if !environment_name(&definition.name) || !names.insert(definition.name.clone()) {
            return Err(RunnerError::Host(
                "environment inventory contains unknown or duplicate tools".into(),
            ));
        }
    }
    if snapshot
        .permissions()
        .tools
        .iter()
        .any(|tool| !names.contains(tool.name()))
    {
        return Err(RunnerError::Host(
            "environment inventory lacks an admitted tool".into(),
        ));
    }
    Ok(())
}

pub(super) fn permits(snapshot: &AgentSnapshot, name: &str) -> bool {
    snapshot
        .permissions()
        .tools
        .iter()
        .any(|tool| tool.name() == name)
}

fn denied(message: &str) -> ToolError {
    ToolError::PolicyDenied {
        message: message.into(),
    }
}
