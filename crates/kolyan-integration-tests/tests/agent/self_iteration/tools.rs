//! Exact host-selected write paths; Shell is unavailable, not labelled read-only.

use std::{path::PathBuf, sync::Arc, time::Duration};

use kolyan_agent::{AgentSnapshot, EnvironmentToolFactory, RunnerError, RunnerToolSet};
use kolyan_core::{ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyEngine, ToolManifest,
};
use kolyan_server::ExecutionRef;
use kolyan_tools::{FileOperationLimits, IsolatedFileConfig, IsolatedFileTools, IsolatedToolSet};

use super::super::{
    evidence::Evidence,
    tools::{ObservedTools, worker},
};
use super::{Plan, baseline};

pub(super) struct Factory {
    pub plan: Plan,
    pub control: PathBuf,
    pub evidence: Arc<Evidence>,
}
pub(super) struct ExactTools {
    inner: IsolatedFileTools,
    workspace: PathBuf,
    allowed: Vec<String>,
    writable: bool,
    execution: ExecutionRef,
    snapshot_digest: String,
}
impl EnvironmentToolFactory for Factory {
    type Executor = ObservedTools<ExactTools>;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<RunnerToolSet<Self::Executor>, RunnerError> {
        let worker =
            worker::verified_worker(&self.control, &self.evidence).map_err(RunnerError::Host)?;
        let workspace = self
            .plan
            .run
            .worktree
            .canonicalize()
            .map_err(|e| RunnerError::Host(e.to_string()))?;
        let writable = snapshot
            .permissions()
            .tools
            .contains(&kolyan_agent::EnvironmentTool::Write)
            || snapshot
                .permissions()
                .tools
                .contains(&kolyan_agent::EnvironmentTool::Edit);
        let inner = IsolatedFileTools::new(IsolatedFileConfig {
            workspace: workspace.clone(),
            staging_root: self.control.join("staging"),
            worker,
            protected_roots: vec![
                self.control.join("state"),
                self.plan.run.worktree.join(".git"),
            ],
            file_limits: FileOperationLimits {
                max_read_bytes: 262144,
                max_write_bytes: 262144,
            },
            max_output_bytes: 1048576,
            timeout: Duration::from_secs(30),
        })
        .map_err(|e| RunnerError::Host(e.to_string()))?;
        let policy = policy(&self.plan, &workspace);
        let definitions = IsolatedToolSet::tool_definitions()
            .into_iter()
            .filter(|tool| {
                snapshot
                    .permissions()
                    .tools
                    .iter()
                    .any(|allowed| allowed.name() == tool.name)
            })
            .collect();
        Ok(RunnerToolSet {
            executor: ObservedTools {
                inner: ExactTools {
                    inner,
                    workspace,
                    allowed: self.plan.allowlist.clone(),
                    writable,
                    execution: execution.clone(),
                    snapshot_digest: snapshot.digest().into(),
                },
                evidence: self.evidence.clone(),
            },
            definitions,
            policy: Arc::new(policy),
        })
    }
}
impl ExactTools {
    fn check(&self, call: &ToolCall) -> Result<(), ToolError> {
        permitted(&self.allowed, self.writable, call)?;
        if call.name != "file.read" {
            let name = call.arguments["path"].as_str().ok_or_else(denied)?;
            let candidate = self.workspace.join(name);
            let parent = candidate
                .parent()
                .ok_or_else(denied)?
                .canonicalize()
                .map_err(|e| ToolError::Failed {
                    message: e.to_string(),
                })?;
            if candidate != parent.join(candidate.file_name().ok_or_else(denied)?) {
                return Err(denied());
            }
            if std::fs::symlink_metadata(&candidate).is_ok_and(|meta| meta.file_type().is_symlink())
            {
                return Err(denied());
            }
        }
        Ok(())
    }
}
pub(super) fn permitted(
    allowlist: &[String],
    writable: bool,
    call: &ToolCall,
) -> Result<(), ToolError> {
    if call.name == "file.read" {
        return Ok(());
    }
    if !writable || !matches!(call.name.as_str(), "file.write" | "file.edit") {
        return Err(denied());
    }
    let path = call.arguments["path"].as_str().ok_or_else(denied)?;
    baseline::safe_relative(path).map_err(|_| denied())?;
    if !allowlist.iter().any(|allowed| allowed == path) {
        return Err(denied());
    }
    Ok(())
}
fn denied() -> ToolError {
    ToolError::PolicyDenied {
        message: "self-iteration exact candidate authority refuses this call".into(),
    }
}
impl ToolExecutor for ExactTools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            self.check(&call)?;
            ToolExecutor::prepare(&self.inner, call).await
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            self.check(invocation.prepared.call())?;
            let scope = &invocation.scope;
            if scope.execution.session_id != self.execution.session_id
                || scope.execution.execution_id != self.execution.execution_id
                || scope.execution.turn_id != self.execution.turn_id
                || scope.agent_snapshot_digest.as_deref() != Some(&self.snapshot_digest)
            {
                return Err(denied());
            }
            invocation
                .grant
                .validate(&invocation.prepared, &invocation.policy_revision, scope)
                .map_err(|_| denied())?;
            self.inner.execute_invocation(invocation).await
        })
    }
}

pub(super) fn policy(plan: &Plan, workspace: &std::path::Path) -> PolicyEngine {
    let mut policy = PolicyEngine::default();
    for (name, capabilities, effects) in [
        (
            "file.read",
            vec![Capability::FilesystemRead],
            vec![Effect::Read],
        ),
        (
            "file.write",
            vec![Capability::FilesystemWrite],
            vec![Effect::Create, Effect::Update],
        ),
        (
            "file.edit",
            vec![Capability::FilesystemRead, Capability::FilesystemWrite],
            vec![Effect::Read, Effect::Update],
        ),
    ] {
        policy.register(ToolManifest {
            tool_name: name.into(),
            capabilities: capabilities.into_iter().collect(),
            effects: effects.into_iter().collect(),
            path_scopes: if name == "file.read" {
                vec![PathScope::new(workspace.to_string_lossy())]
            } else {
                plan.allowlist
                    .iter()
                    .map(|name| PathScope::new(workspace.join(name).to_string_lossy()))
                    .collect()
            },
            idempotency: if name == "file.read" {
                Idempotency::Idempotent
            } else {
                Idempotency::NonIdempotent
            },
            approval: ApprovalMode::Never,
        });
    }
    policy.restrict_workspace(workspace.to_string_lossy());

    policy
}
