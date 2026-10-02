//! Conservative scheduling from host enforcement, never model path speculation.

mod link;

use kolyan_server::{AttemptBinding, InvocationState};
use std::sync::{Arc, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::*;

pub(in crate::runner) struct SharedGate {
    bound: usize,
    gate: Weak<Semaphore>,
}

pub(in crate::runner) struct ExecutionPermits {
    _task: Option<OwnedSemaphorePermit>,
    _invocation: Option<OwnedSemaphorePermit>,
    _host: OwnedSemaphorePermit,
}

struct ExecutionPlan {
    task: Option<Arc<Semaphore>>,
    invocation: Option<Arc<Semaphore>>,
    host_count: u32,
}

impl super::super::AgentExecutionBudget {
    fn gate(&self, id: String, bound: usize) -> Result<Arc<Semaphore>, ToolError> {
        let mut gates = self.gates.lock().map_err(uncertain)?;
        gates.retain(|_, entry| entry.gate.strong_count() > 0);
        if let Some(entry) = gates.get(&id) {
            if entry.bound != bound {
                return Err(denied("shared execution gate bound changed"));
            }
            if let Some(gate) = entry.gate.upgrade() {
                return Ok(gate);
            }
        }
        let gate = Arc::new(Semaphore::new(bound));
        gates.insert(
            id,
            SharedGate {
                bound,
                gate: Arc::downgrade(&gate),
            },
        );
        Ok(gate)
    }
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
    pub(in crate::runner) fn validate_current_permissions(
        &self,
        snapshot: &crate::AgentSnapshot,
    ) -> Result<(), ToolError> {
        let ceiling = self
            .host
            .intersection(snapshot.definition().permissions())
            .map_err(denied)?;
        snapshot
            .permissions()
            .require_subset_of(&ceiling)
            .map_err(denied)
    }

    fn execution_plan(
        &self,
        task_id: &str,
        logical_session: &str,
        binding: &AttemptBinding,
    ) -> Result<ExecutionPlan, ToolError> {
        let saved = self
            .bindings
            .load(task_id, &binding.invocation_id, logical_session)
            .map_err(denied)?
            .ok_or_else(|| denied("execution binding is absent"))?;
        self.validate_current_permissions(&saved.snapshot)?;
        let task = self
            .service
            .coordinator()
            .snapshot(task_id)
            .map_err(denied)?;
        let invocation = task
            .invocations
            .get(&binding.invocation_id)
            .ok_or_else(|| denied("execution invocation is absent"))?;
        if invocation.definition.input_source != binding.input_source {
            return Err(denied(
                "execution input source differs from admitted source",
            ));
        }
        match invocation.definition.role {
            kolyan_server::InvocationRole::Root => self
                .verify_root_attempt_source(&saved, binding)
                .map_err(denied)?,
            kolyan_server::InvocationRole::SelfCall | kolyan_server::InvocationRole::Delegation => {
                self.verify_child_input(&saved, &binding.input_source)
                    .map_err(denied)?;
            }
            kolyan_server::InvocationRole::Continuation => self
                .verify_continuation_origin(&saved, &invocation.definition)
                .map_err(denied)?,
        }
        let detached = task.state == kolyan_server::TaskState::Cancelled
            && task.definition.cancellation_policy == kolyan_server::CancellationPolicy::RootOnly
            && saved.context_kind == crate::BindingContextKind::Child
            && matches!(
                invocation.definition.role,
                kolyan_server::InvocationRole::SelfCall | kolyan_server::InvocationRole::Delegation
            )
            && invocation.attempts.last() == Some(&binding.attempt_id)
            && task
                .attempts
                .get(&binding.attempt_id)
                .is_some_and(|attempt| {
                    attempt.binding == *binding && !attempt.cancellation_requested
                });
        if (task.state.is_terminal() && !detached)
            || invocation.cancellation_requested
            || matches!(
                invocation.state,
                InvocationState::Completed | InvocationState::Failed | InvocationState::Cancelled
            )
            || invocation.definition.agent != binding.agent
            || invocation.definition.constraints_digest != binding.constraints_digest
            || saved.snapshot.identity() != &binding.agent
            || saved.snapshot.digest() != binding.constraints_digest
            || saved.private_session_id != binding.execution.session_id
            || task
                .attempts
                .get(&binding.attempt_id)
                .is_some_and(|attempt| {
                    attempt.binding != *binding || attempt.cancellation_requested
                })
        {
            return Err(denied(
                "execution admission is revoked, terminal or foreign",
            ));
        }
        if self
            .service
            .sessions()
            .execution()
            .server()
            .coordinator()
            .ledger()
            .execution_events_after(&binding.execution.execution_id, 0)
            .map_err(denied)?
            .iter()
            .any(|event| event.kind == kolyan_ledger::LedgerEventKind::ExecutionCancelled)
        {
            return Err(ToolError::Cancelled);
        }
        let invocation_gate = if saved.context_kind == crate::BindingContextKind::Child {
            let (coordinate, admission) = self.execution_link(task_id, binding)?;
            if !detached {
                self.validate_child_owner(&admission.owner)?;
            }
            let bound = admission.issued.prepared.execution_binding()["limits"]["max_parallel"]
                .as_u64()
                .filter(|bound| (1..=8).contains(bound))
                .ok_or_else(|| denied("invalid original invocation bound"))?
                as usize;
            Some(self.execution_budget.gate(
                format!("invocation:{}", coordinate.stream_id),
                if admission.serialized { 1 } else { bound },
            )?)
        } else {
            None
        };
        let readonly = saved
            .snapshot
            .permissions()
            .tools
            .iter()
            .all(|tool| *tool == crate::EnvironmentTool::Read)
            && self.tools.enforces_read_only_parallel(&saved.snapshot);
        let finite = task.definition.limits.max_tokens.is_some();
        Ok(ExecutionPlan {
            task: finite
                .then(|| self.execution_budget.gate(format!("task:{task_id}"), 1))
                .transpose()?,
            invocation: invocation_gate,
            host_count: if readonly {
                1
            } else {
                self.execution_budget.bound as u32
            },
        })
    }

    /// This gate covers every actual run/resume, not read-only wait inspection.
    /// The returned guard must span Server's stopped-evidence/usage publication.
    pub(in crate::runner) async fn enter_execution(
        self: &Arc<Self>,
        task_id: &str,
        logical_session: &str,
        binding: &AttemptBinding,
    ) -> Result<ExecutionPermits, ToolError> {
        let plan = self
            .check_execution(task_id, logical_session, binding)
            .await?;
        let task = if let Some(gate) = plan.task {
            Some(
                self.acquire_execution(gate, 1, task_id, logical_session, binding)
                    .await?,
            )
        } else {
            None
        };
        let invocation = if let Some(gate) = plan.invocation {
            Some(
                self.acquire_execution(gate, 1, task_id, logical_session, binding)
                    .await?,
            )
        } else {
            None
        };
        let host = self
            .acquire_execution(
                self.execution_budget.slots.clone(),
                plan.host_count,
                task_id,
                logical_session,
                binding,
            )
            .await?;
        self.check_execution(task_id, logical_session, binding)
            .await?;
        Ok(ExecutionPermits {
            _task: task,
            _invocation: invocation,
            _host: host,
        })
    }

    async fn check_execution(
        self: &Arc<Self>,
        task: &str,
        logical_session: &str,
        binding: &AttemptBinding,
    ) -> Result<ExecutionPlan, ToolError> {
        let runner = self.clone();
        let task = task.to_owned();
        let logical_session = logical_session.to_owned();
        let binding = binding.clone();
        tokio::task::spawn_blocking(move || {
            runner.execution_plan(&task, &logical_session, &binding)
        })
        .await
        .map_err(uncertain)?
    }

    async fn acquire_execution(
        self: &Arc<Self>,
        gate: Arc<Semaphore>,
        count: u32,
        task: &str,
        logical_session: &str,
        binding: &AttemptBinding,
    ) -> Result<OwnedSemaphorePermit, ToolError> {
        let acquire = gate.acquire_many_owned(count);
        tokio::pin!(acquire);
        let mut check = tokio::time::interval(std::time::Duration::from_millis(25));
        loop {
            tokio::select! {
                permit = &mut acquire => return permit.map_err(uncertain),
                _ = check.tick() => { self.check_execution(task, logical_session, binding).await?; }
            }
        }
    }
    pub(super) fn parallel_eligible(
        &self,
        owner: &DelegationOwner,
        children: &[AdmittedAgentChild],
        requested: bool,
    ) -> Result<bool, ToolError> {
        if !requested || self.execution_budget.bound == 1 {
            return Ok(false);
        }
        let task = self
            .service
            .coordinator()
            .snapshot(&owner.task_id)
            .map_err(denied)?;
        // No concurrent token reservation exists yet. Never remove a configured
        // ceiling or substitute unknown usage with zero to permit fanout.
        if task.definition.limits.max_tokens.is_some() {
            return Ok(false);
        }
        self.read_only_children(owner, children)
    }

    pub(super) fn read_only_children(
        &self,
        owner: &DelegationOwner,
        children: &[AdmittedAgentChild],
    ) -> Result<bool, ToolError> {
        for child in children {
            let saved = self
                .bindings
                .load(
                    &owner.task_id,
                    &child.attempt.invocation_id,
                    &owner.logical_session_id,
                )
                .map_err(denied)?
                .ok_or_else(|| denied("parallel child binding is absent"))?;
            if saved
                .snapshot
                .permissions()
                .tools
                .iter()
                .any(|tool| *tool != crate::EnvironmentTool::Read)
                || !self.tools.enforces_read_only_parallel(&saved.snapshot)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
