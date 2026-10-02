//! Child approval continuation reconstructs saved ownership, not a retained future.

use super::*;
use crate::runner::{routing::RoutedTools, tools::SnapshotTools};
use kolyan_core::TurnExecutor;
use serde::{Deserialize, Serialize};

/// Authenticated host confirmation of a child under an exact parent admission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildApprovalResumeRequest {
    pub owner: DelegationOwner,
    pub issued: IssuedToolAuthority,
    pub wait: ExternalWait,
    pub child_invocation_id: String,
    pub checkpoint_id: String,
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
    /// Call only after authenticated approval. Server revalidates original scoped
    /// checkpoint and current adapters/policy. No original Runner future is needed.
    pub async fn resume_agent_child_approval(
        self: &Arc<Self>,
        request: ChildApprovalResumeRequest,
    ) -> Result<AgentChildDriveResult, ToolError> {
        let runner = self.clone();
        let inspection = request.clone();
        let children = tokio::task::spawn_blocking(move || {
            let coordinate = admission_coordinate(&inspection.owner, &inspection.issued)?;
            if inspection.wait != wait(coordinate.clone())? {
                return Err(denied("child wait differs from exact admission"));
            }
            let admission = runner
                .load_child_admission(&coordinate)?
                .ok_or_else(|| denied("child admission is absent"))?;
            runner.inspect_child_admission(&inspection.owner, &inspection.issued, &admission)?;
            Ok(admission.children)
        })
        .await
        .map_err(uncertain)??;
        let child = children
            .into_iter()
            .find(|child| child.attempt.invocation_id == request.child_invocation_id)
            .ok_or_else(|| denied("requested child is not in the exact parent admission"))?;
        let runner = self.clone();
        let loaded_child = child.clone();
        let loaded_request = request.clone();
        let executor = tokio::task::spawn_blocking(move || {
            // Scheduling below rechecks active-parent authority or the narrow
            // RootOnly entered-child exception before any service execution.
            runner.inspect_child_owner(&loaded_request.owner)?;
            crate::identity(&loaded_request.checkpoint_id).map_err(denied)?;
            crate::identity(&loaded_request.approval_id).map_err(denied)?;
            let current = runner
                .service
                .sessions()
                .execution()
                .load_current_suspension(&loaded_child.attempt.execution.execution_id)
                .map_err(denied)?;
            if current.checkpoint.checkpoint_id != loaded_request.checkpoint_id
                || !current.checkpoint.approvals.iter().any(|approval| {
                    approval.approval_id == loaded_request.approval_id
                        && approval.evidence_id.is_none()
                })
            {
                return Err(denied(
                    "child approval differs from current exact checkpoint",
                ));
            }
            let (saved, reference) = runner
                .bindings
                .load_with_reference(
                    &loaded_request.owner.task_id,
                    &loaded_child.attempt.invocation_id,
                    &loaded_request.owner.logical_session_id,
                )
                .map_err(denied)?
                .ok_or_else(|| denied("saved child binding is absent"))?;
            if saved.context_kind != crate::BindingContextKind::Child
                || reference != loaded_child.binding_fact
                || saved.snapshot.identity() != &loaded_child.attempt.agent
                || saved.snapshot.digest() != loaded_child.attempt.constraints_digest
            {
                return Err(denied("child approval ownership differs"));
            }
            let state = runner
                .service
                .coordinator()
                .snapshot(&loaded_request.owner.task_id)
                .map_err(denied)?;
            let attempt = state
                .attempts
                .get(&loaded_child.attempt.attempt_id)
                .ok_or_else(|| denied("child attempt is absent"))?;
            if attempt.binding != loaded_child.attempt
                || attempt.cancellation_requested
                || attempt.state != kolyan_server::InvocationState::Suspended
            {
                return Err(denied(
                    "child attempt is cancelled, changed or not suspended",
                ));
            }
            let ceiling = runner
                .host
                .intersection(saved.snapshot.definition().permissions())
                .map_err(denied)?;
            saved
                .snapshot
                .permissions()
                .require_subset_of(&ceiling)
                .map_err(denied)?;
            let provider = runner
                .routed_provider(&saved.snapshot, &loaded_child.attempt.execution)
                .map_err(denied)?;
            let set = runner
                .routed_tool_set(&saved.snapshot, &loaded_child.attempt.execution)
                .map_err(denied)?;
            Ok(TurnExecutor::with_tools(
                provider,
                RoutedTools {
                    runner: runner.clone(),
                    saved: saved.clone(),
                    parent: loaded_child.attempt.clone(),
                    environment: SnapshotTools {
                        inner: set.executor,
                        snapshot: saved.snapshot.clone(),
                        execution: loaded_child.attempt.execution,
                        policy: set.policy.clone(),
                    },
                },
            )
            .with_tool_dispatch_policy(runner.tool_dispatch_policy())
            .with_policy_engine(set.policy)
            .with_agent_snapshot_digest(saved.snapshot.digest().into()))
        })
        .await
        .map_err(uncertain)??;
        let permits = self
            .enter_execution(
                &request.owner.task_id,
                &request.owner.logical_session_id,
                &child.attempt,
            )
            .await?;
        let stopped = Box::pin(self.service.resume_approval(
            &request.owner.task_id,
            child.attempt.clone(),
            &request.approval_id,
            executor,
        ))
        .await;
        drop(permits);
        if let Ok((_, execution @ kolyan_runtime::DurableTurnResult::Suspended { .. })) = stopped {
            return Ok(AgentChildDriveResult::Waiting {
                child: Box::new(child),
                execution: Box::new(execution),
            });
        }
        let dispatch_error = stopped.err().map(|error| error.to_string());
        let service = self.service.clone();
        let task_id = request.owner.task_id;
        let binding = child.attempt;
        let result = tokio::task::spawn_blocking(move || {
            // Read this admitted continuation's terminal publication only.
            // Historical evidence never authorizes another child execution.
            service.load_verified_historical_result(&task_id, &binding, 1024 * 1024)
        })
        .await
        .map_err(uncertain)?
        .map_err(|error| {
            uncertain(format!(
                "child approval terminal proof unavailable: {error}; dispatch={dispatch_error:?}"
            ))
        })?;
        Ok(AgentChildDriveResult::Terminal {
            result: Box::new(result),
            dispatch_error,
        })
    }
}
