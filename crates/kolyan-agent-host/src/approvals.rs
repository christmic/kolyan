//! Rebuild approval authority from actual Task and checkpoint facts, never DTO grants.

use std::sync::Arc;

use kolyan_agent::{
    AgentChildrenPumpResult, ChildApprovalResumeRequest, DelegationOwner,
    RootApprovalResumeRequest, RunnerError,
};
use kolyan_core::{CheckpointCallState, TurnRequest};
use kolyan_server::{AttemptBinding, InvocationRole, InvocationState};
use serde::{Deserialize, Serialize};

use super::{AgentHost, HostTaskView, host::host};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Accept,
    Deny,
}

impl AgentHost {
    /// Decide only a pending approval owned by this logical Session. Child
    /// completion is followed by the production pump, not a hidden replay loop.
    pub async fn decide_approval(
        self: &Arc<Self>,
        session_id: String,
        task_id: String,
        invocation_id: String,
        approval_id: String,
        decision: ApprovalDecision,
    ) -> Result<HostTaskView, RunnerError> {
        let view = self.query(session_id.clone(), task_id.clone()).await?;
        let pending = view
            .approvals
            .iter()
            .find(|p| p.invocation_id == invocation_id && p.approval_id == approval_id)
            .ok_or_else(|| host("pending approval is absent"))?;
        let invocation = view
            .task
            .invocations
            .get(&invocation_id)
            .ok_or_else(|| host("approval invocation is absent"))?;
        if invocation.definition.role == InvocationRole::Root {
            let request = RootApprovalResumeRequest {
                task_id: task_id.clone(),
                invocation_id: invocation_id.clone(),
                logical_session_id: session_id.clone(),
                attempt_id: pending.attempt_id.clone(),
                approval_id,
            };
            match decision {
                ApprovalDecision::Accept => {
                    self.runner.resume_approval(request).await?;
                }
                ApprovalDecision::Deny => {
                    self.runner.deny_approval(request).await?;
                }
            }
        } else {
            let child = view
                .task
                .attempts
                .get(&pending.attempt_id)
                .ok_or_else(|| host("child approval attempt is absent"))?
                .binding
                .clone();
            let child_suspension = self.load_suspension(child.clone()).await?;
            let mut admitted = None;
            for parent in view
                .task
                .attempts
                .values()
                .filter(|a| a.state == InvocationState::Suspended)
            {
                let suspension = self.load_suspension(parent.binding.clone()).await?;
                for call in &suspension.checkpoint.calls {
                    let CheckpointCallState::AwaitingExternal { issued, wait } = &call.state else {
                        continue;
                    };
                    if call.call.name != kolyan_agent::AGENT_INVOKE_NAME {
                        continue;
                    }
                    let owner = DelegationOwner {
                        task_id: task_id.clone(),
                        logical_session_id: session_id.clone(),
                        parent: parent.binding.clone(),
                        scope: suspension.checkpoint.scope.clone(),
                    };
                    let children = match decision {
                        ApprovalDecision::Accept => {
                            self.runner
                                .verify_agent_child_wait(
                                    owner.clone(),
                                    issued.clone(),
                                    wait.clone(),
                                )
                                .await
                        }
                        ApprovalDecision::Deny => {
                            self.runner
                                .inspect_agent_child_wait(
                                    owner.clone(),
                                    issued.clone(),
                                    wait.clone(),
                                )
                                .await
                        }
                    }
                    .map_err(host)?;
                    if children.iter().any(|c| c.attempt == child) {
                        if admitted.is_some() {
                            return Err(host("child has multiple parent admissions"));
                        }
                        admitted = Some(ChildApprovalResumeRequest {
                            owner,
                            issued: issued.clone(),
                            wait: wait.clone(),
                            child_invocation_id: child.invocation_id.clone(),
                            checkpoint_id: child_suspension.checkpoint.checkpoint_id.clone(),
                            approval_id: approval_id.clone(),
                        });
                    }
                }
            }
            let request =
                admitted.ok_or_else(|| host("child has no verified pending parent admission"))?;
            match decision {
                ApprovalDecision::Accept => {
                    self.runner
                        .resume_agent_child_approval(request)
                        .await
                        .map_err(host)?;
                }
                ApprovalDecision::Deny => {
                    self.runner
                        .deny_agent_child_approval(request)
                        .await
                        .map_err(host)?;
                }
            }
        }
        // Denial is a terminal decision, not authority to consume/resume the
        // parent or open another model stream. Pending joins remain visible.
        if matches!(decision, ApprovalDecision::Accept) {
            self.advance_task(&session_id, &task_id).await?;
        }
        self.query(session_id, task_id).await
    }

    pub(crate) async fn advance_task(
        self: &Arc<Self>,
        session_id: &str,
        task_id: &str,
    ) -> Result<(), RunnerError> {
        // This is bounded host orchestration through the existing Runner, not a
        // second model/tool loop. Each suspended pending approval ends this call.
        for _ in 0..self.task_limits.max_attempts {
            let view = self.query(session_id.into(), task_id.into()).await?;
            if view.task.state.is_terminal() || !view.approvals.is_empty() {
                return Ok(());
            }
            let mut work = Vec::new();
            for attempt in view
                .task
                .attempts
                .values()
                .filter(|a| a.state == InvocationState::Suspended)
            {
                let suspension = self.load_suspension(attempt.binding.clone()).await?;
                if let Some(call) = suspension.checkpoint.calls.iter().find(|c| {
                    c.call.name == kolyan_agent::AGENT_INVOKE_NAME
                        && matches!(c.state, CheckpointCallState::AwaitingExternal { .. })
                }) {
                    let mut depth = 0;
                    let mut invocation = &attempt.binding.invocation_id;
                    while let Some(parent) = &view.task.invocations[invocation]
                        .definition
                        .parent_invocation_id
                    {
                        depth += 1;
                        if depth > view.task.definition.limits.max_depth {
                            return Err(host(
                                "saved parent chain exceeds its admitted depth bound",
                            ));
                        }
                        invocation = parent;
                    }
                    work.push((
                        depth,
                        DelegationOwner {
                            task_id: task_id.into(),
                            logical_session_id: session_id.into(),
                            parent: attempt.binding.clone(),
                            scope: suspension.checkpoint.scope.clone(),
                        },
                        suspension.checkpoint.checkpoint_id.clone(),
                        call.call.id.clone(),
                    ));
                }
            }
            work.sort_by_key(|entry| std::cmp::Reverse(entry.0));
            let Some((_, owner, checkpoint, call)) = work.into_iter().next() else {
                return Ok(());
            };
            let mut request = self.template.clone();
            request.messages.clear();
            let result = self
                .runner
                .pump_agent_children(
                    owner,
                    checkpoint,
                    call,
                    TurnRequest {
                        turn_id: "host-child-template".into(),
                        model_request: request,
                        config: self.turn_config,
                    },
                )
                .await
                .map_err(host)?;
            if matches!(result, AgentChildrenPumpResult::Waiting(_)) {
                let after = self.query(session_id.into(), task_id.into()).await?;
                if !after.approvals.is_empty() || after.task.position == view.task.position {
                    return Ok(());
                }
            }
        }
        Err(host(
            "host orchestration exhausted the admitted Task attempt bound",
        ))
    }

    async fn load_suspension(
        self: &Arc<Self>,
        attempt: AttemptBinding,
    ) -> Result<kolyan_core::TurnSuspension, RunnerError> {
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || {
            service
                .sessions()
                .execution()
                .load_current_suspension(&attempt.execution.execution_id)
                .map_err(host)
        })
        .await
        .map_err(host)?
    }
}
