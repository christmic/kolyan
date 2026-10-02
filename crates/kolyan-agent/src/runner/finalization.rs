//! Recover Task verdicts from durable proofs, without re-entering an execution.

pub(super) mod continuation;
pub use continuation::projection::{ContinuationProjectionConfig, ContinuationProjectionRequest};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use kolyan_server::{
    InvocationRole, InvocationState, TaskSnapshot, TaskState, VerifiedTaskOutcome,
    VerifiedTaskResult,
};
use serde::{Deserialize, Serialize};

use super::*;
use crate::BindingContextKind;

/// Explicit minimal policy: every admitted invocation must succeed. A verified
/// execution failure or execution-only cancellation fails the Task once every
/// admitted invocation has stopped; unfinished work refuses failure finalization.
/// Neither
/// authorizes a retry nor fabricates cancellation of the whole Task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskFinalizationPolicy {
    AllInvocationsSuccessful,
}

/// Authenticated host coordinates, not model arguments or a retained Turn result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskFinalizationRequest {
    pub task_id: String,
    pub logical_session_id: String,
    pub root_invocation_id: String,
    pub root_attempt_id: String,
    pub policy: TaskFinalizationPolicy,
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
    /// Rebuild ownership and historical terminal/consumption proofs, then issue
    /// one deterministic, idempotent Task command. Does not build adapters, poll
    /// a Provider, execute a tool, resume a checkpoint or authorize a retry.
    /// Missing proofs, changed scope and current host revocation refuse writes.
    /// Dropping this future does not undo a command already committed by the
    /// blocking worker; the exact same request can recover that verdict.
    pub async fn finalize_task(
        self: &Arc<Self>,
        request: TaskFinalizationRequest,
    ) -> Result<TaskSnapshot, RunnerError> {
        let runner = self.clone();
        tokio::task::spawn_blocking(move || runner.finalize_saved_task(&request))
            .await
            .map_err(|error| RunnerError::Host(format!("finalization worker failed: {error}")))?
    }

    fn finalize_saved_task(
        &self,
        request: &TaskFinalizationRequest,
    ) -> Result<TaskSnapshot, RunnerError> {
        match request.policy {
            TaskFinalizationPolicy::AllInvocationsSuccessful => {}
        }
        for id in [
            &request.task_id,
            &request.logical_session_id,
            &request.root_invocation_id,
            &request.root_attempt_id,
        ] {
            crate::identity(id)?;
        }
        let task = self.service.coordinator().snapshot(&request.task_id)?;
        let mut terminals = BTreeMap::new();
        let mut root_seen = false;
        for (id, invocation) in &task.invocations {
            let saved = self
                .bindings
                .load(&request.task_id, id, &request.logical_session_id)?;
            let saved = match (invocation.definition.role, saved) {
                (InvocationRole::Continuation, None) => {
                    // An externally admitted role without an Agent-owned context
                    // remains outside this Runner's authority. Never infer it.
                    return Err(RunnerError::UnsupportedFinalizationRole {
                        invocation_id: id.clone(),
                        role: InvocationRole::Continuation,
                    });
                }
                (_, Some(saved)) => saved,
                (_, None) => return Err(invalid("saved invocation owner is absent")),
            };
            self.validate_current_permissions(&saved.snapshot)
                .map_err(|error| invalid(error.to_string()))?;
            if invocation.definition.agent != *saved.snapshot.identity()
                || invocation.definition.constraints_digest != saved.snapshot.digest()
            {
                return Err(invalid("finalization invocation binding differs"));
            }
            match invocation.definition.role {
                InvocationRole::Root => {
                    self.verify_root_input(&saved, &invocation.definition.input_source)?
                }
                InvocationRole::SelfCall | InvocationRole::Delegation => {
                    self.verify_child_input(&saved, &invocation.definition.input_source)?;
                }
                InvocationRole::Continuation => {
                    self.verify_continuation_origin(&saved, &invocation.definition)?
                }
            }
            let is_root = id == &request.root_invocation_id;
            if (saved.context_kind == BindingContextKind::Root) != is_root
                || (invocation.definition.role == InvocationRole::Root) != is_root
            {
                return Err(invalid("finalization requires the unique saved root"));
            }
            let binding = invocation
                .attempts
                .last()
                .and_then(|attempt| task.attempts.get(attempt));
            let Some(binding) = binding else {
                if task.state == TaskState::Cancelled && !is_root {
                    continue;
                }
                return Err(invalid("invocation has no current attempt"));
            };
            let binding = &binding.binding;
            if binding.invocation_id != *id
                || binding.agent != *saved.snapshot.identity()
                || binding.constraints_digest != saved.snapshot.digest()
                || binding.execution.session_id != saved.private_session_id
                || binding.input_source != invocation.definition.input_source
            {
                return Err(invalid("finalization attempt binding differs"));
            }
            if is_root
                && (binding.attempt_id != request.root_attempt_id
                    || binding.agent != task.definition.agent
                    || binding.constraints_digest != task.definition.constraints_digest
                    || invocation.definition.parent_invocation_id.is_some())
            {
                return Err(invalid("finalization root attempt or Task owner differs"));
            }
            root_seen |= is_root;
            if is_root {
                self.verify_root_execution_input(&saved, binding)?;
            }
            // The durable user-cancellation verdict is not a claim that every
            // in-flight attempt has stopped. Never resume or consume to prove it.
            if task.state == TaskState::Cancelled {
                continue;
            }
            let terminal = self.service.load_verified_historical_result(
                &request.task_id,
                binding,
                1024 * 1024,
            )?;
            if !matches!(
                (&terminal.outcome, invocation.state),
                (
                    VerifiedTaskOutcome::Completed { .. },
                    InvocationState::Completed
                ) | (VerifiedTaskOutcome::Failed { .. }, InvocationState::Failed)
                    | (
                        VerifiedTaskOutcome::Cancelled { .. },
                        InvocationState::Cancelled
                    )
            ) {
                return Err(invalid(
                    "finalization state differs from physical terminal proof",
                ));
            }
            terminals.insert(id.clone(), terminal);
        }
        if !root_seen {
            return Err(invalid("finalization root is absent"));
        }
        if task.state == TaskState::Cancelled {
            return Ok(task);
        }
        for (id, invocation) in &task.invocations {
            match invocation.definition.role {
                InvocationRole::Root | InvocationRole::SelfCall | InvocationRole::Delegation => {}
                InvocationRole::Continuation => {
                    self.verify_continuation_context(request, &task, id, &terminals)?;
                }
            }
        }
        for (parent_id, parent) in &task.invocations {
            // A failed/cancelled parent cannot legally consume late results.
            // Preserve those unconsumed edges instead of resurrecting the parent.
            if !matches!(
                terminals[parent_id].outcome,
                VerifiedTaskOutcome::Completed { .. }
            ) {
                continue;
            }
            let mut required: BTreeSet<_> =
                parent.definition.dependencies.iter().cloned().collect();
            required.extend(
                task.invocations
                    .iter()
                    .filter(|(_, child)| {
                        child.definition.parent_invocation_id.as_ref() == Some(parent_id)
                            && child.definition.role != InvocationRole::Continuation
                    })
                    .map(|(id, _)| id.clone()),
            );
            for child_id in required {
                let child = terminals
                    .get(&child_id)
                    .ok_or_else(|| invalid("required child terminal is absent"))?;
                let consumed = self
                    .service
                    .load_verified_historical_consumed_result(
                        &request.task_id,
                        &terminals[parent_id].binding,
                        &child.binding,
                        1024 * 1024,
                    )?
                    .ok_or_else(|| invalid("required historical consumption proof is absent"))?;
                if consumed.child != *child {
                    return Err(invalid("historical consumption terminal differs"));
                }
            }
        }
        let failures = terminal_failures(&terminals);
        // A bounded Task may already derive failure from a verified stopped
        // observation with overspend or unknown usage. It is a durable verdict,
        // not permission to append a second terminal command or retry the root.
        let budget_failed = task.definition.limits.max_tokens.is_some_and(|limit| {
            task.usage.unreported_steps > 0 || task.usage.total().is_some_and(|total| total > limit)
        });
        if task.state == TaskState::Failed && (!failures.is_empty() || budget_failed) {
            return Ok(task);
        }
        if failures.is_empty() {
            // Immutable Task criteria are the only goal inventory. Historical
            // terminal and consumption checks above precede every assessment.
            for criterion in &task.definition.criteria {
                let kolyan_server::CompletionCriterion::Goal(goal) = criterion else {
                    continue;
                };
                if !task
                    .goal_assessments
                    .iter()
                    .any(|saved| saved.assessment.criterion_id == goal.id)
                {
                    self.service.assess_goal(
                        &request.task_id,
                        &format!(
                            "agent-goal-assessment-{}",
                            crate::digest(&(
                                "kolyan.agent.root-goal-assessment/v1",
                                &request.task_id,
                                &goal.id,
                                &request.root_attempt_id,
                            ))?
                        ),
                        &goal.id,
                    )?;
                }
            }
            Ok(self.service.complete(
                &request.task_id,
                &format!("{}/agent-root/completed", request.task_id),
            )?)
        } else {
            Ok(self.service.coordinator().fail_task(
                &request.task_id,
                &format!("{}/agent-root/failed", request.task_id),
                &format!("unsuccessful admitted invocations: {}", failures.join(",")),
            )?)
        }
    }
}

fn terminal_failures(terminals: &BTreeMap<String, VerifiedTaskResult>) -> Vec<String> {
    terminals
        .iter()
        .filter_map(|(id, terminal)| match &terminal.outcome {
            VerifiedTaskOutcome::Completed { .. } => None,
            VerifiedTaskOutcome::Failed { .. } => Some(format!("{id}:failed")),
            VerifiedTaskOutcome::Cancelled { .. } => Some(format!("{id}:cancelled")),
        })
        .collect()
}

fn invalid(reason: impl Into<String>) -> RunnerError {
    RunnerError::Host(reason.into())
}

#[cfg(test)]
mod tests;
