//! Exact root admission and request assembly; this synchronous path may block.

use std::sync::Arc;

use kolyan_core::{TurnExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerStore};
use kolyan_server::{
    AttemptBinding, CompletionCriterion, InvocationDefinition, InvocationRole, TaskDefinition,
};
use kolyan_storage::SessionStore;
use kolyan_trace::TraceSink;

use super::{
    AgentRunner, EnvironmentToolFactory, ProviderFactory, RootRunRequest, RunnerError,
    routing::{AdvertisementProvider, RoutedTools},
    tools::SnapshotTools,
};
use crate::{AgentSnapshot, provider::ContextPreparingProvider};

type Admission<J, L, S, SS, P, T> = (
    String,
    AgentSnapshot,
    AttemptBinding,
    TurnRequest,
    TurnExecutor<
        AdvertisementProvider<ContextPreparingProvider<<P as ProviderFactory>::Provider>>,
        RoutedTools<J, L, S, SS, P, T>,
    >,
);

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    pub(super) fn admit(
        self: &Arc<Self>,
        mut request: RootRunRequest,
    ) -> Result<Admission<J, L, S, SS, P, T>, RunnerError> {
        crate::identity(&request.attempt_id)?;
        let mut criteria = vec![CompletionCriterion::ExecutionCompleted {
            id: "root-final-answer".into(),
            invocation_id: request.invocation_id.clone(),
        }];
        if request.goals.len() > 127 {
            return Err(RunnerError::Host("root accepts at most 127 goals".into()));
        }
        let mut ids = std::collections::BTreeSet::from(["root-final-answer".to_owned()]);
        for goal in request.goals {
            goal.validate()?;
            if goal.invocation_id != request.invocation_id || !ids.insert(goal.id.clone()) {
                return Err(RunnerError::Host(
                    "duplicate goal or wrong root goal owner".into(),
                ));
            }
            criteria.push(CompletionCriterion::Goal(goal));
        }
        if request.turn.turn_id != request.execution.turn_id {
            return Err(RunnerError::Host(
                "Turn identity differs from execution".into(),
            ));
        }
        let assembled = self.assemble_root_input(super::RootInputPreparationRequest {
            task_id: request.task_id.clone(),
            invocation_id: request.invocation_id.clone(),
            execution: request.execution.clone(),
            selector: request.selector,
            requested_permissions: request.requested_permissions,
            model_request: request.turn.model_request,
        })?;
        // Preserve fail-before-owner admission when the Provider factory refuses.
        let provider = self.routed_provider(
            &assembled.saved.snapshot,
            &assembled.request.execution,
            assembled.skill_binding.as_ref(),
        )?;
        let (prepared, tool_set, saved) = self.retain_root_input(assembled)?;
        let snapshot = prepared.snapshot;
        request.turn.model_request = prepared.selected_input;
        let input_source = kolyan_server::InvocationInputSource::Standalone {
            fact: prepared.input_source.reference,
        };
        let coordinator = self.service.coordinator();
        coordinator
            .register_task(
                &format!("{}/agent-root/registered", request.task_id),
                TaskDefinition {
                    task_id: request.task_id.clone(),
                    objective: request.objective,
                    criteria,
                    agent: snapshot.identity().clone(),
                    constraints_digest: snapshot.digest().into(),
                    limits: request.limits,
                    cancellation_policy: request.cancellation_policy,
                },
            )
            .map_err(kolyan_server::TaskExecutionError::from)?;
        coordinator
            .admit_invocation(
                &request.task_id,
                &format!("{}/agent-root/admitted", request.task_id),
                InvocationDefinition {
                    invocation_id: request.invocation_id.clone(),
                    agent: snapshot.identity().clone(),
                    constraints_digest: snapshot.digest().into(),
                    role: InvocationRole::Root,
                    parent_invocation_id: None,
                    dependencies: vec![],
                    input_source: input_source.clone(),
                },
            )
            .map_err(kolyan_server::TaskExecutionError::from)?;
        let binding = AttemptBinding {
            attempt_id: request.attempt_id,
            invocation_id: request.invocation_id,
            execution: request.execution.clone(),
            agent: snapshot.identity().clone(),
            constraints_digest: snapshot.digest().into(),
            input_source,
        };
        let skill_binding = self.restored_skills(&saved, &binding.input_source)?;
        let skill = self.skill_executor(
            skill_binding.as_ref(),
            &binding.execution,
            tool_set.policy.clone(),
        )?;
        let executor = TurnExecutor::with_tools(
            provider,
            RoutedTools {
                skill,
                runner: self.clone(),
                saved,
                parent: binding.clone(),
                environment: SnapshotTools {
                    inner: tool_set.executor,
                    snapshot: snapshot.clone(),
                    execution: request.execution,
                    policy: Arc::clone(&tool_set.policy),
                },
            },
        )
        .with_tool_dispatch_policy(self.tool_dispatch_policy())
        .with_policy_engine(tool_set.policy)
        .with_agent_snapshot_digest(snapshot.digest().into());
        Ok((request.task_id, snapshot, binding, request.turn, executor))
    }
}
