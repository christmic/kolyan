//! Child denial checks historical admission without opening executable adapters.

use super::*;

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    /// Deny only the admitted child's current approval, then load its verified
    /// physical failure. Parent consumption and continuation remain host-owned.
    pub async fn deny_agent_child_approval(
        self: &Arc<Self>,
        request: ChildApprovalResumeRequest,
    ) -> Result<AgentChildDriveResult, ToolError> {
        let runner = self.clone();
        tokio::task::spawn_blocking(move || {
            let coordinate = admission_coordinate(&request.owner, &request.issued)?;
            if request.wait != wait(coordinate.clone())? {
                return Err(denied("child wait differs from exact admission"));
            }
            let admission = runner
                .load_child_admission(&coordinate)?
                .ok_or_else(|| denied("child admission is absent"))?;
            runner.inspect_historical_child_admission(
                &request.owner,
                &request.issued,
                &admission,
            )?;
            let child = admission
                .children
                .into_iter()
                .find(|child| child.attempt.invocation_id == request.child_invocation_id)
                .ok_or_else(|| denied("requested child is not in the exact parent admission"))?;
            crate::identity(&request.checkpoint_id).map_err(denied)?;
            crate::identity(&request.approval_id).map_err(denied)?;
            let (saved, reference) = runner
                .bindings
                .load_with_reference(
                    &request.owner.task_id,
                    &child.attempt.invocation_id,
                    &request.owner.logical_session_id,
                )
                .map_err(denied)?
                .ok_or_else(|| denied("saved child binding is absent"))?;
            if saved.context_kind != crate::BindingContextKind::Child
                || reference != child.binding_fact
                || saved.snapshot.identity() != &child.attempt.agent
                || saved.snapshot.digest() != child.attempt.constraints_digest
            {
                return Err(denied("child approval ownership differs"));
            }
            let task = runner
                .service
                .coordinator()
                .snapshot(&request.owner.task_id)
                .map_err(denied)?;
            let attempt = task
                .attempts
                .get(&child.attempt.attempt_id)
                .ok_or_else(|| denied("child attempt is absent"))?;
            if attempt.binding != child.attempt
                || attempt.cancellation_requested
                || attempt.state != kolyan_server::InvocationState::Suspended
            {
                return Err(denied(
                    "child attempt is cancelled, changed or not suspended",
                ));
            }
            let current = runner
                .service
                .sessions()
                .execution()
                .load_current_suspension(&child.attempt.execution.execution_id)
                .map_err(denied)?;
            if current.checkpoint.checkpoint_id != request.checkpoint_id
                || !current.checkpoint.approvals.iter().any(|approval| {
                    approval.approval_id == request.approval_id && approval.evidence_id.is_none()
                })
            {
                return Err(denied(
                    "child approval differs from current exact checkpoint",
                ));
            }
            let mut scope = current.checkpoint.scope.clone();
            scope.execution = kolyan_runtime::ExecutionKey {
                session_id: child.attempt.execution.session_id.clone(),
                turn_id: child.attempt.execution.turn_id.clone(),
                execution_id: child.attempt.execution.execution_id.clone(),
            };
            scope.agent_snapshot_digest = Some(saved.snapshot.digest().into());
            runner
                .service
                .sessions()
                .deny_pending(&child.attempt.execution, &scope, &request.approval_id)
                .map_err(uncertain)?;
            runner
                .service
                .reconcile(&request.owner.task_id, &child.attempt.attempt_id)
                .map_err(uncertain)?;
            let result = runner
                .service
                .load_verified_historical_result(
                    &request.owner.task_id,
                    &child.attempt,
                    1024 * 1024,
                )
                .map_err(uncertain)?;
            Ok(AgentChildDriveResult::Terminal {
                result: Box::new(result),
                dispatch_error: None,
            })
        })
        .await
        .map_err(uncertain)?
    }
}
