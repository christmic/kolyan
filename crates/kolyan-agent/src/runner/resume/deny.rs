//! Deny one verified pending root approval without invoking providers or tools.

use super::*;
use kolyan_server::TaskSnapshot;

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    /// Persist denial and reconcile the exact attempt. This is not Task cancellation.
    /// A storage failure remains recoverable; no provider stream or tool is opened.
    pub async fn deny_approval(
        self: &Arc<Self>,
        request: RootApprovalResumeRequest,
    ) -> Result<TaskSnapshot, RunnerError> {
        let runner = self.clone();
        tokio::task::spawn_blocking(move || {
            let (saved, binding) = runner.inspect_root_approval_owner(&request)?;
            let suspension = runner
                .service
                .sessions()
                .execution()
                .load_current_suspension(&binding.execution.execution_id)
                .map_err(kolyan_server::TaskExecutionError::from)?;
            let mut scope = suspension.checkpoint.scope.clone();
            scope.execution = kolyan_runtime::ExecutionKey {
                session_id: binding.execution.session_id.clone(),
                turn_id: binding.execution.turn_id.clone(),
                execution_id: binding.execution.execution_id.clone(),
            };
            scope.agent_snapshot_digest = Some(saved.snapshot.digest().into());
            runner
                .service
                .sessions()
                .deny_pending(&binding.execution, &scope, &request.approval_id)
                .map_err(kolyan_server::TaskExecutionError::from)?;
            let task = runner
                .service
                .reconcile(&request.task_id, &request.attempt_id)?;
            if task.invocations.values().all(|invocation| {
                matches!(
                    invocation.state,
                    kolyan_server::InvocationState::Completed
                        | kolyan_server::InvocationState::Failed
                        | kolyan_server::InvocationState::Cancelled
                )
            }) {
                runner.finalize_denied_root(
                    &super::super::TaskFinalizationRequest {
                        task_id: request.task_id,
                        logical_session_id: request.logical_session_id,
                        root_invocation_id: request.invocation_id,
                        root_attempt_id: request.attempt_id,
                        policy: super::super::TaskFinalizationPolicy::AllInvocationsSuccessful,
                    },
                    &request.approval_id,
                )
            } else {
                // Refusal stops this attempt, not unrelated in-flight children.
                // Their physical results remain a prerequisite for Task closure.
                Ok(task)
            }
        })
        .await
        .map_err(|error| RunnerError::Host(format!("approval denial worker failed: {error}")))?
    }
}

#[cfg(test)]
mod tests;
