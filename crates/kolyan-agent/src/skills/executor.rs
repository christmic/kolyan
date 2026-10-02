//! Real Core preparation/grant enforcement and blocking selected-artifact reading.

use std::sync::Arc;

use kolyan_core::{
    ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture,
};
use kolyan_model::{ToolCall, ToolResult};
use kolyan_policy::{
    Capability, Effect, Idempotency, InvocationClaim, PolicyEngine, PreparedCall, ResourceClaim,
    ToolRequirements,
};
use kolyan_server::ExecutionRef;
use serde_json::json;

use super::{
    MAX_TOOL_RESULT_BYTES, SKILL_LOAD_NAME, SkillError, SkillLoadInput, SkillRuntime,
    VerifiedSkillBinding,
};

/// Constructed only from a verified saved scope, never a model path or a reusable call ID.
#[derive(Clone)]
pub struct SkillExecutor {
    runtime: Arc<SkillRuntime>,
    binding: VerifiedSkillBinding,
    execution: ExecutionRef,
    policy: Arc<PolicyEngine>,
}

impl SkillExecutor {
    pub fn new(
        runtime: Arc<SkillRuntime>,
        binding: VerifiedSkillBinding,
        execution: ExecutionRef,
        policy: Arc<PolicyEngine>,
    ) -> Result<Self, SkillError> {
        if binding.advertisement().scope().private_session_id() != execution.session_id {
            return Err(SkillError::Provenance(
                "Skill execution uses a foreign physical Session".into(),
            ));
        }
        if runtime.restore_binding(binding.reference(), &binding.advertisement().scope())?
            != binding
        {
            return Err(SkillError::Integrity(
                "Skill historical binding changed".into(),
            ));
        }
        Ok(Self {
            runtime,
            binding,
            execution,
            policy,
        })
    }

    fn prepare_sync(&self, call: ToolCall) -> Result<PreparedCall, ToolError> {
        if call.name != SKILL_LOAD_NAME {
            return Err(ToolError::Unavailable { name: call.name });
        }
        super::encoding::encode(&call.arguments, 4096).map_err(failed)?;
        let input: SkillLoadInput =
            serde_json::from_value(call.arguments.clone()).map_err(failed)?;
        let key =
            super::SkillKey::new(input.skill_id.clone(), input.revision.clone()).map_err(failed)?;
        self.runtime
            .validate_current(&self.binding)
            .map_err(denied)?;
        if !self.binding.advertisement().skills().iter().any(|s| {
            s.metadata().descriptor().key == key
                && s.metadata().content_digest() == input.content_digest
        }) {
            return Err(denied("Skill is outside the saved exact selection"));
        }
        PreparedCall::new(
            call,
            "skill.load.v1".into(),
            InvocationClaim {
                tool_name: SKILL_LOAD_NAME.into(),
                capabilities: [Capability::SkillRead].into(),
                effects: [Effect::Read].into(),
                resource: ResourceClaim { path: None },
                idempotency: Idempotency::Idempotent,
            },
            ToolRequirements {
                process_sandbox: false,
                max_output_bytes: MAX_TOOL_RESULT_BYTES as u64,
                timeout_ms: 10_000,
            },
        )
        .and_then(|p| {
            p.with_execution_binding(
                json!({"schema_version":1,"binding":self.binding.reference(),
                "scope":self.binding.advertisement().scope(),"execution":self.execution}),
            )
        })
        .map_err(denied)
    }

    fn execute_sync(&self, invocation: ToolInvocation) -> Result<ToolOutcome, ToolError> {
        let scope = &invocation.scope;
        if scope.execution.session_id != self.execution.session_id
            || scope.execution.turn_id != self.execution.turn_id
            || scope.execution.execution_id != self.execution.execution_id
            || scope.agent_snapshot_digest.as_deref()
                != Some(self.binding.advertisement().scope().agent_snapshot_digest())
            || invocation.policy_revision != self.policy.revision()
        {
            return Err(denied("Skill scope, snapshot or current policy differs"));
        }
        invocation
            .grant
            .validate(&invocation.prepared, &self.policy.revision(), scope)
            .map_err(denied)?;
        if self.prepare_sync(invocation.prepared.call().clone())? != invocation.prepared {
            return Err(denied("Skill preparation changed"));
        }
        let input: SkillLoadInput =
            serde_json::from_value(invocation.prepared.call().arguments.clone()).map_err(failed)?;
        if invocation.control.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let loaded = self
            .runtime
            .read_selected(&self.binding, &input)
            .map_err(denied)?;
        let content = String::from_utf8(
            super::encoding::encode(&loaded, MAX_TOOL_RESULT_BYTES).map_err(failed)?,
        )
        .map_err(failed)?;
        let result = ToolResult {
            call_id: invocation.prepared.call().id.clone(),
            content,
            is_error: false,
        };
        let limit = invocation
            .grant
            .constraints()
            .max_output_bytes
            .ok_or_else(|| denied("missing Skill output ceiling"))?
            .min(MAX_TOOL_RESULT_BYTES as u64) as usize;
        super::encoding::encode(&result, limit).map_err(failed)?;
        Ok(ToolOutcome::Completed(result))
    }
}

impl ToolExecutor for SkillExecutor {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        let worker = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || worker.prepare_sync(call))
                .await
                .map_err(failed)?
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        let worker = self.clone();
        Box::pin(async move {
            let control = invocation.control.clone();
            if control.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
            let timeout = invocation
                .grant
                .constraints()
                .timeout_ms
                .ok_or_else(|| denied("missing Skill timeout"))?;
            let outcome = tokio::time::timeout(
                std::time::Duration::from_millis(timeout),
                tokio::task::spawn_blocking(move || worker.execute_sync(invocation)),
            )
            .await
            .map_err(|_| ToolError::TimedOut)?
            .map_err(failed)??;
            if control.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
            Ok(outcome)
        })
    }
}

fn denied(error: impl std::fmt::Display) -> ToolError {
    ToolError::PolicyDenied {
        message: error.to_string(),
    }
}
fn failed(error: impl std::fmt::Display) -> ToolError {
    ToolError::Failed {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests;
