//! Approval resume rebuilds current adapters from immutable saved Agent ownership.
//! It never registers another Task, allocates an instance or reselects a catalog.

use std::sync::Arc;

use kolyan_core::TurnExecutor;
use kolyan_ledger::{FactJournal, LedgerStore};
use kolyan_storage::SessionStore;
use kolyan_trace::TraceSink;
use serde::{Deserialize, Serialize};

use super::{
    AgentRunner, EnvironmentToolFactory, ProviderFactory, RootRunResult, RunnerError,
    routing::RoutedTools, tools::SnapshotTools,
};
use crate::binding::BindingContextKind;

/// Trusted user confirmation of an exact pending root approval. Execution and
/// snapshot identities are loaded from host facts, never accepted from the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootApprovalResumeRequest {
    pub task_id: String,
    pub invocation_id: String,
    pub logical_session_id: String,
    pub attempt_id: String,
    pub approval_id: String,
}

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    /// Call only after authenticated user confirmation. Server persists the exact
    /// decision; Core rechecks current preparation/policy before any pending effect.
    /// Restart needs the original host stores, not the original Runner instance.
    pub async fn resume_approval(
        self: &Arc<Self>,
        request: RootApprovalResumeRequest,
    ) -> Result<RootRunResult, RunnerError> {
        let runner = self.clone();
        let approval_id = request.approval_id.clone();
        let (saved, binding, executor) = tokio::task::spawn_blocking(move || {
            for id in [
                &request.task_id,
                &request.invocation_id,
                &request.logical_session_id,
                &request.attempt_id,
                &request.approval_id,
            ] {
                crate::identity(id)?;
            }
            let saved = runner
                .bindings
                .load(
                    &request.task_id,
                    &request.invocation_id,
                    &request.logical_session_id,
                )?
                .ok_or_else(|| RunnerError::Host("saved root binding is absent".into()))?;
            if saved.context_kind != BindingContextKind::Root {
                return Err(RunnerError::Host(
                    "root resume cannot borrow a child context".into(),
                ));
            }
            let ceiling = runner
                .host
                .intersection(saved.snapshot.definition().permissions())?;
            saved.snapshot.permissions().require_subset_of(&ceiling)?;
            let task = runner
                .service
                .coordinator()
                .snapshot(&request.task_id)
                .map_err(kolyan_server::TaskExecutionError::from)?;
            let attempt = task
                .attempts
                .get(&request.attempt_id)
                .ok_or_else(|| RunnerError::Host("saved attempt is absent".into()))?;
            let binding = attempt.binding.clone();
            runner.verify_root_attempt_source(&saved, &binding)?;
            if task.state.is_terminal()
                || attempt.cancellation_requested
                || binding.invocation_id != saved.invocation_id
                || binding.execution.session_id != saved.private_session_id
                || binding.agent != *saved.snapshot.identity()
                || binding.constraints_digest != saved.snapshot.digest()
            {
                return Err(RunnerError::Host(
                    "saved attempt and Agent ownership differ or are terminal".into(),
                ));
            }
            let skill_binding = runner.restored_skills(&saved, &binding.input_source)?;
            let provider = runner.routed_provider(
                &saved.snapshot,
                &binding.execution,
                skill_binding.as_ref(),
            )?;
            let set = runner.routed_tool_set(
                &saved.snapshot,
                &binding.execution,
                skill_binding.as_ref(),
            )?;
            let executor = TurnExecutor::with_tools(
                provider,
                RoutedTools {
                    skill: runner.skill_executor(
                        skill_binding.as_ref(),
                        &binding.execution,
                        set.policy.clone(),
                    )?,
                    runner: runner.clone(),
                    saved: saved.clone(),
                    parent: binding.clone(),
                    environment: SnapshotTools {
                        inner: set.executor,
                        snapshot: saved.snapshot.clone(),
                        execution: binding.execution.clone(),
                        policy: set.policy.clone(),
                    },
                },
            )
            .with_tool_dispatch_policy(runner.tool_dispatch_policy())
            .with_policy_engine(set.policy)
            .with_agent_snapshot_digest(saved.snapshot.digest().into());
            Ok::<_, RunnerError>((saved, binding, executor))
        })
        .await
        .map_err(|error| {
            RunnerError::Host(format!("resume preparation worker failed: {error}"))
        })??;
        let permits = self
            .enter_execution(&saved.task_id, &saved.logical_session_id, &binding)
            .await
            .map_err(|error| RunnerError::Host(error.to_string()))?;
        let stopped = Box::pin(self.service.resume_approval(
            &saved.task_id,
            binding.clone(),
            &approval_id,
            executor,
        ))
        .await;
        drop(permits);
        self.finish_execution(saved.snapshot, &saved.task_id, &binding, stopped)
            .await
    }
}
