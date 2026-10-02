//! Host-driven child progress and exact parent checkpoint continuation.
//! No polling model requests or retained parent future are needed while waiting.

use kolyan_core::{
    CheckpointCallState, ExternalResolution, ResumeInput, TurnExecutor, TurnRequest,
};

use super::*;
use crate::runner::{RootRunResult, routing::RoutedTools, tools::SnapshotTools};

/// A child still needs external authority, or the original parent was resumed.
pub enum AgentChildrenPumpResult {
    Waiting(Vec<AgentChildDriveResult>),
    Resumed(Box<RootRunResult>),
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
    /// Load historical authority from the original physical checkpoint, drive
    /// its admitted children, consume verified typed terminals and resume that
    /// same attempt. Runtime must have a matching host ExternalWaitVerifier;
    /// without one the existing fail-closed rejection remains in force.
    pub async fn pump_agent_children(
        self: &Arc<Self>,
        owner: DelegationOwner,
        checkpoint_id: String,
        call_id: String,
        child_template: TurnRequest,
    ) -> Result<AgentChildrenPumpResult, ToolError> {
        let runner = self.clone();
        let loaded_owner = owner.clone();
        let checkpoint = checkpoint_id.clone();
        let (issued, wait) = tokio::task::spawn_blocking(move || {
            crate::identity(&checkpoint).map_err(denied)?;
            let suspension = runner
                .service
                .sessions()
                .execution()
                .load_suspension(&loaded_owner.parent.execution.execution_id, &checkpoint)
                .map_err(denied)?;
            suspension
                .checkpoint
                .validate(&loaded_owner.scope)
                .map_err(denied)?;
            let call = suspension
                .checkpoint
                .calls
                .iter()
                .find(|call| call.call.id == call_id)
                .ok_or_else(|| denied("parent checkpoint does not contain the requested call"))?;
            let CheckpointCallState::AwaitingExternal { issued, wait } = &call.state else {
                return Err(denied("parent call is not awaiting external children"));
            };
            if issued.prepared.call().name != crate::AGENT_INVOKE_NAME {
                return Err(denied("parent wait is not an Agent invocation"));
            }
            Ok((issued.clone(), wait.clone()))
        })
        .await
        .map_err(uncertain)??;
        let children = self
            .drive_agent_children(owner.clone(), issued.clone(), wait.clone(), child_template)
            .await?;
        if children
            .iter()
            .any(|child| matches!(child, AgentChildDriveResult::Waiting { .. }))
        {
            return Ok(AgentChildrenPumpResult::Waiting(children));
        }
        let result = self
            .consume_agent_children(owner.clone(), issued.clone(), wait.clone())
            .await?;
        let runner = self.clone();
        let loaded_owner = owner.clone();
        let (saved, executor) = tokio::task::spawn_blocking(move || {
            runner.validate_child_owner(&loaded_owner)?;
            let saved = runner
                .bindings
                .load(
                    &loaded_owner.task_id,
                    &loaded_owner.parent.invocation_id,
                    &loaded_owner.logical_session_id,
                )
                .map_err(denied)?
                .ok_or_else(|| denied("parent binding disappeared"))?;
            let provider = runner
                .routed_provider(&saved.snapshot, &loaded_owner.parent.execution)
                .map_err(denied)?;
            let set = runner
                .routed_tool_set(&saved.snapshot, &loaded_owner.parent.execution)
                .map_err(denied)?;
            let executor = TurnExecutor::with_tools(
                provider,
                RoutedTools {
                    runner: runner.clone(),
                    saved: saved.clone(),
                    parent: loaded_owner.parent,
                    environment: SnapshotTools {
                        inner: set.executor,
                        snapshot: saved.snapshot.clone(),
                        execution: saved_execution(&loaded_owner.scope),
                        policy: set.policy.clone(),
                    },
                },
            )
            .with_tool_dispatch_policy(runner.tool_dispatch_policy())
            .with_policy_engine(set.policy)
            .with_agent_snapshot_digest(saved.snapshot.digest().into());
            Ok((saved, executor))
        })
        .await
        .map_err(uncertain)??;
        let input = ResumeInput::ExternalResolved(vec![ExternalResolution {
            call_id: issued.prepared.call().id.clone(),
            wait,
            result,
        }]);
        let permits = self
            .enter_execution(&owner.task_id, &owner.logical_session_id, &owner.parent)
            .await?;
        let stopped = Box::pin(self.service.resume(
            &owner.task_id,
            owner.parent.clone(),
            &checkpoint_id,
            input,
            executor,
        ))
        .await;
        drop(permits);
        let resumed = if saved.context_kind == crate::BindingContextKind::Root {
            self.finish_execution(saved.snapshot, &owner.task_id, &owner.parent, stopped)
                .await
                .map_err(denied)?
        } else {
            let (task, execution) = stopped.map_err(denied)?;
            RootRunResult {
                snapshot: saved.snapshot,
                task,
                execution,
            }
        };
        Ok(AgentChildrenPumpResult::Resumed(Box::new(resumed)))
    }
}

fn saved_execution(scope: &ToolExecutionScope) -> kolyan_server::ExecutionRef {
    kolyan_server::ExecutionRef {
        session_id: scope.execution.session_id.clone(),
        turn_id: scope.execution.turn_id.clone(),
        execution_id: scope.execution.execution_id.clone(),
    }
}

#[cfg(test)]
mod tests;
