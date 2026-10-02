//! Host-driven durable task attempts; scheduling and tool authority stay explicit.

pub(crate) mod evidence;
mod goal_source;
pub use goal_source::{
    GoalSourceCoverage, GoalSourceError, GoalSourceLimits, GoalSourceReader, VerifiedGoalSource,
};
mod historical_context;
pub use historical_context::{HistoricalContextRequest, VerifiedHistoricalContext};
mod result;
mod retry;

pub use result::{VerifiedConsumedResult, VerifiedTaskOutcome, VerifiedTaskResult};

use kolyan_core::{ResumeInput, ToolExecutor, TurnExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ModelProvider;
use kolyan_runtime::{DurableTurnResult, ExecutionBinding};
use kolyan_storage::SessionStore;
use kolyan_trace::{ArtifactRef, ArtifactStore, Retention, TraceSink};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AttemptBinding, AttemptObservation, AttemptOutcome, CompletionCriterion, CompletionEvidence,
    InvocationState, ServerError, SessionExecutionService, TaskCoordinator, TaskError,
    TaskSnapshot, TaskUsage,
};
use evidence::{StoppedOutcome, inspect, response_digest};

#[derive(Debug, Error)]
pub enum TaskExecutionError {
    #[error("task coordination: {0}")]
    Task(#[from] TaskError),
    #[error("session execution: {0}")]
    Server(#[from] ServerError),
}

/// Runs one admitted attempt at a time; no automatic model-driven task scheduling.
/// All state and waiting are reconstructed from the journal and execution facts.
pub struct TaskExecutionService<J, L, S, SS> {
    coordinator: TaskCoordinator<J>,
    execution: SessionExecutionService<L, S, SS>,
    artifacts: Option<ArtifactStore>,
}

impl<J, L, S, SS> TaskExecutionService<J, L, S, SS>
where
    J: FactJournal,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
{
    pub fn new(
        coordinator: TaskCoordinator<J>,
        execution: SessionExecutionService<L, S, SS>,
    ) -> Self {
        Self {
            coordinator,
            execution,
            artifacts: None,
        }
    }

    pub fn with_artifacts(mut self, artifacts: ArtifactStore) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    pub fn coordinator(&self) -> &TaskCoordinator<J> {
        &self.coordinator
    }
    pub fn sessions(&self) -> &SessionExecutionService<L, S, SS> {
        &self.execution
    }

    /// Read-only exact terminal proof for a host delegation adapter. This does
    /// not reconcile a commit gap, consume a result, or authorize child work.
    pub fn load_verified_result(
        &self,
        task_id: &str,
        binding: &AttemptBinding,
        max_bytes: usize,
    ) -> Result<VerifiedTaskResult, TaskExecutionError> {
        if self
            .execution
            .execution()
            .server()
            .coordinator()
            .is_active(&binding.execution.execution_id)
        {
            return Err(invalid("active child has no consumable result").into());
        }
        Ok(result::load(
            &self.coordinator,
            self.ledger(),
            task_id,
            binding,
            max_bytes,
        )?)
    }

    /// Forensic/idempotent-finalization read, never active resume authority.
    /// Task failure/cancellation does not erase independently verified physical
    /// terminal history. No work, grant, result consumption or fact is created.
    pub fn load_verified_historical_result(
        &self,
        task_id: &str,
        binding: &AttemptBinding,
        max_bytes: usize,
    ) -> Result<VerifiedTaskResult, TaskExecutionError> {
        if self
            .execution
            .execution()
            .server()
            .coordinator()
            .is_active(&binding.execution.execution_id)
        {
            return Err(invalid("active execution has no historical terminal result").into());
        }
        Ok(result::load_for(
            &self.coordinator,
            self.ledger(),
            task_id,
            binding,
            max_bytes,
            result::ResultRead::Historical,
        )?)
    }

    /// Exact already-committed edge between physically terminal attempts.
    /// Terminal Task/parent history is readable, but this does not consume again
    /// or confer active recovery authority. Missing consumption returns None.
    pub fn load_verified_historical_consumed_result(
        &self,
        task_id: &str,
        parent: &AttemptBinding,
        child: &AttemptBinding,
        max_bytes: usize,
    ) -> Result<Option<VerifiedConsumedResult>, TaskExecutionError> {
        let coordinator = self.execution.execution().server().coordinator();
        if coordinator.is_active(&parent.execution.execution_id)
            || coordinator.is_active(&child.execution.execution_id)
        {
            return Err(invalid("active execution has no historical consumed result").into());
        }
        Ok(result::load_consumed_for(
            &self.coordinator,
            self.ledger(),
            task_id,
            parent,
            child,
            max_bytes,
            result::ResultRead::Historical,
        )?)
    }

    /// Read-only recovery of an already committed result edge. None means no
    /// consumption fact; it never admits, consumes, grants, or drives work.
    pub fn load_verified_consumed_result(
        &self,
        task_id: &str,
        parent: &AttemptBinding,
        child: &AttemptBinding,
        max_bytes: usize,
    ) -> Result<Option<VerifiedConsumedResult>, TaskExecutionError> {
        if self
            .execution
            .execution()
            .server()
            .coordinator()
            .is_active(&child.execution.execution_id)
        {
            return Err(invalid("active child has no consumable result").into());
        }
        Ok(result::load_consumed(
            &self.coordinator,
            self.ledger(),
            task_id,
            parent,
            child,
            max_bytes,
        )?)
    }

    /// A new attempt requires explicit immutable identities and host-supplied
    /// current executor/request. Durable admission never restores tool grants.
    pub async fn run<P: ModelProvider, T: ToolExecutor>(
        &self,
        task_id: &str,
        binding: AttemptBinding,
        executor: TurnExecutor<P, T>,
        mut request: TurnRequest,
    ) -> Result<(TaskSnapshot, DurableTurnResult), TaskExecutionError> {
        let before = self.coordinator.snapshot(task_id)?;
        if before.attempts.contains_key(&binding.attempt_id)
            || request.turn_id != binding.execution.turn_id
        {
            return Err(invalid("attempt already exists or request Turn differs").into());
        }
        request.config.max_steps = request
            .config
            .max_steps
            .min(before.definition.limits.max_steps_per_turn as usize);
        if request.config.max_steps == 0 {
            return Err(invalid("Turn requires a nonzero Step budget").into());
        }
        if let Some(limit) = before.definition.limits.max_tokens {
            if before.usage.unreported_steps > 0 {
                return Err(invalid("unknown usage blocks budgeted work").into());
            }
            let remaining = limit
                .checked_sub(
                    before
                        .usage
                        .total()
                        .ok_or_else(|| invalid("usage overflow"))?,
                )
                .filter(|remaining| *remaining > 0)
                .ok_or_else(|| invalid("token budget exhausted"))?;
            let cap = remaining.min(u32::MAX as u64) as u32;
            request.model_request.max_output_tokens = Some(
                request
                    .model_request
                    .max_output_tokens
                    .unwrap_or(cap)
                    .min(cap),
            );
        }
        self.coordinator.start_attempt(
            task_id,
            &format!("{task_id}/attempt/{}/start", binding.attempt_id),
            binding.clone(),
        )?;
        self.bind_execution(task_id, &binding)?;
        let result = self
            .execution
            .start(
                executor,
                request,
                &binding.execution.session_id,
                &binding.execution.execution_id,
            )
            .await;
        self.record_stopped(task_id, &binding)?;
        let stopped = result?;
        Ok((self.coordinator.snapshot(task_id)?, stopped))
    }

    /// Checkpoint resume retains the attempt, validates the current definition and
    /// constraints, and reconstructs the original Runtime checkpoint from storage.
    pub async fn resume<P: ModelProvider, T: ToolExecutor>(
        &self,
        task_id: &str,
        binding: AttemptBinding,
        checkpoint_id: &str,
        input: ResumeInput,
        executor: TurnExecutor<P, T>,
    ) -> Result<(TaskSnapshot, DurableTurnResult), TaskExecutionError> {
        let saved = self
            .execution
            .execution()
            .load_suspension(&binding.execution.execution_id, checkpoint_id)?;
        crate::suspension::verify_resume_input(self.ledger(), &binding.execution, &saved, &input)?;
        let coordinate = serde_json::to_vec(&(checkpoint_id, &input))
            .map_err(|error| invalid(error.to_string()))?;
        self.coordinator.resume_attempt(
            task_id,
            &format!(
                "{task_id}/attempt/{}/resume/{:x}",
                binding.attempt_id,
                Sha256::digest(coordinate)
            ),
            binding.clone(),
            checkpoint_id,
        )?;
        self.bind_execution(task_id, &binding)?;
        let result = self
            .execution
            .resume(
                executor,
                &binding.execution.session_id,
                &binding.execution.execution_id,
                checkpoint_id,
                input,
            )
            .await;
        self.record_stopped(task_id, &binding)?;
        let stopped = result?;
        Ok((self.coordinator.snapshot(task_id)?, stopped))
    }

    pub async fn resume_approval<P: ModelProvider, T: ToolExecutor>(
        &self,
        task_id: &str,
        binding: AttemptBinding,
        approval_id: &str,
        executor: TurnExecutor<P, T>,
    ) -> Result<(TaskSnapshot, DurableTurnResult), TaskExecutionError> {
        let saved = self
            .execution
            .execution()
            .load_current_suspension(&binding.execution.execution_id)?;
        if !saved
            .checkpoint
            .approvals
            .iter()
            .any(|approval| approval.approval_id == approval_id)
        {
            return Err(invalid("approval is not pending in current checkpoint").into());
        }
        let coordinate = serde_json::to_vec(&(&saved.checkpoint.checkpoint_id, approval_id))
            .map_err(|error| invalid(error.to_string()))?;
        self.coordinator.resume_attempt(
            task_id,
            &format!(
                "{task_id}/attempt/{}/resume/{:x}",
                binding.attempt_id,
                Sha256::digest(coordinate)
            ),
            binding.clone(),
            &saved.checkpoint.checkpoint_id,
        )?;
        self.bind_execution(task_id, &binding)?;
        let result = self
            .execution
            .resume_approval(
                executor,
                &binding.execution.session_id,
                &binding.execution.execution_id,
                approval_id,
            )
            .await;
        self.record_stopped(task_id, &binding)?;
        Ok((self.coordinator.snapshot(task_id)?, result?))
    }

    /// Observe a stopped attempt after reconstruction. This never executes it.
    /// Callers must not reconcile while an in-process worker still owns execution.
    pub fn reconcile(
        &self,
        task_id: &str,
        attempt_id: &str,
    ) -> Result<TaskSnapshot, TaskExecutionError> {
        let snapshot = self.coordinator.snapshot(task_id)?;
        let attempt = snapshot
            .attempts
            .get(attempt_id)
            .ok_or_else(|| invalid("unknown attempt"))?;
        if self
            .execution
            .execution()
            .server()
            .coordinator()
            .is_active(&attempt.binding.execution.execution_id)
        {
            return Err(invalid("active execution cannot be reconciled").into());
        }
        self.record_stopped(task_id, &attempt.binding)?;
        Ok(self.coordinator.snapshot(task_id)?)
    }

    /// Physically revalidate every bound success proof before committing task success.
    pub fn assess_goal(
        &self,
        task_id: &str,
        fact_id: &str,
        criterion_id: &str,
    ) -> Result<TaskSnapshot, TaskExecutionError> {
        let prefix = self.coordinator.snapshot(task_id)?;
        let assessment = self
            .coordinator
            .compute_goal_assessment(&prefix, criterion_id)?;
        Ok(self.coordinator.assess_goal(task_id, fact_id, assessment)?)
    }

    /// Goal waiting is durable business state, not permission to rerun work.
    pub fn complete(
        &self,
        task_id: &str,
        fact_id: &str,
    ) -> Result<TaskSnapshot, TaskExecutionError> {
        let snapshot = self.coordinator.snapshot(task_id)?;
        if !snapshot.state.is_terminal()
            && snapshot.waiting.iter().any(|waiting| {
                matches!(
                    waiting,
                    crate::WaitingReason::GoalAssessment { .. }
                        | crate::WaitingReason::GoalUnmet { .. }
                )
            })
        {
            return Ok(snapshot);
        }
        let has_goals = snapshot
            .definition
            .criteria
            .iter()
            .any(|criterion| matches!(criterion, CompletionCriterion::Goal(_)));
        let mut proofs = Vec::new();
        let latest: std::collections::HashSet<_> = snapshot
            .invocations
            .values()
            .filter_map(|invocation| invocation.attempts.last())
            .collect();
        for (id, attempt) in &snapshot.attempts {
            if has_goals && !latest.contains(id) {
                continue;
            }
            if let Some(AttemptObservation {
                outcome: AttemptOutcome::Completed { evidence },
                ..
            }) = &attempt.observation
            {
                if has_goals {
                    // The configured enforcing port revalidates every latest
                    // physical source through the bounded reader before CAS.
                    proofs.extend(evidence.iter().cloned());
                    continue;
                }
                let actual = inspect(self.ledger(), &attempt.binding, 0)?;
                if !matches!(actual.stopped, StoppedOutcome::Completed) {
                    return Err(invalid("completion source is not a final answer").into());
                }
                for proof in evidence {
                    if proof.source() != &actual.source {
                        return Err(invalid("completion source changed").into());
                    }
                    match proof {
                        CompletionEvidence::ExecutionResult { result_digest, .. } => {
                            let response = actual
                                .response
                                .as_ref()
                                .ok_or_else(|| invalid("missing actual model result"))?;
                            if result_digest != &response_digest(response)? {
                                return Err(invalid("result digest differs").into());
                            }
                        }
                        CompletionEvidence::VerifiedArtifact {
                            sha256, byte_len, ..
                        } => {
                            self.artifacts
                                .as_ref()
                                .ok_or_else(|| invalid("artifact verification unavailable"))?
                                .read(
                                    &ArtifactRef {
                                        digest: sha256.clone(),
                                        byte_length: *byte_len,
                                        retention: Retention::Required,
                                    },
                                    *byte_len,
                                )
                                .map_err(|error| invalid(error.to_string()))?;
                        }
                        CompletionEvidence::GoalSatisfied { .. } => {
                            return Err(
                                invalid("goal evidence cannot be a physical observation").into()
                            );
                        }
                    }
                    proofs.push(proof.clone());
                }
            }
        }
        for saved in &snapshot.goal_assessments {
            if saved.assessment.verdict == crate::GoalVerdict::Satisfied {
                proofs.push(CompletionEvidence::GoalSatisfied {
                    criterion_id: saved.assessment.criterion_id.clone(),
                    source: saved.assessment.source.clone(),
                    assessment: saved.reference.clone(),
                    assessment_digest: saved.assessment_digest.clone(),
                });
            }
        }
        if has_goals {
            self.coordinator
                .verify_goal_completion(&snapshot, &proofs)?;
            for proof in &proofs {
                if let CompletionEvidence::VerifiedArtifact {
                    sha256, byte_len, ..
                } = proof
                {
                    self.artifacts
                        .as_ref()
                        .ok_or_else(|| invalid("artifact verification unavailable"))?
                        .read(
                            &ArtifactRef {
                                digest: sha256.clone(),
                                byte_length: *byte_len,
                                retention: Retention::Required,
                            },
                            (*byte_len).min(16 * 1024 * 1024),
                        )
                        .map_err(|error| invalid(error.to_string()))?;
                }
            }
        }
        Ok(self.coordinator.complete_task(task_id, fact_id, proofs)?)
    }

    /// Persist cancellation policy, then deliver idempotent Runtime cancellation
    /// requests. This does not claim that external side effects have stopped.
    pub fn cancel(
        &self,
        task_id: &str,
        fact_id: &str,
        reason: &str,
    ) -> Result<TaskSnapshot, TaskExecutionError> {
        let snapshot = self.coordinator.cancel_task(task_id, fact_id, reason)?;
        for attempt in snapshot
            .attempts
            .values()
            .filter(|attempt| attempt.cancellation_requested)
        {
            self.execution.cancel(&attempt.binding.execution)?;
        }
        Ok(snapshot)
    }

    fn ledger(&self) -> &L {
        self.execution.execution().server().coordinator().ledger()
    }

    fn bind_execution(&self, task_id: &str, binding: &AttemptBinding) -> Result<(), TaskError> {
        let linked = ExecutionBinding {
            task_id: task_id.into(),
            invocation_id: binding.invocation_id.clone(),
            attempt_id: binding.attempt_id.clone(),
            session_id: binding.execution.session_id.clone(),
            turn_id: binding.execution.turn_id.clone(),
            execution_id: binding.execution.execution_id.clone(),
        };
        linked
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        let id = format!("{}/task-binding", binding.execution.execution_id);
        let payload = json!({"binding":linked});
        let candidate = LedgerEvent {
            event_id: id.clone(),
            turn_id: binding.execution.turn_id.clone(),
            execution_id: binding.execution.execution_id.clone(),
            cursor: 0,
            kind: LedgerEventKind::ExecutionBound,
            idempotency_key: id.clone(),
            payload,
        };
        if let Some(mut stored) = self
            .ledger()
            .event_by_id(&id)
            .map_err(|error| invalid(error.to_string()))?
        {
            stored.cursor = 0;
            if stored != candidate {
                return Err(invalid(
                    "execution belongs to another task/invocation/attempt",
                ));
            }
            return Ok(());
        }
        self.ledger()
            .append(candidate)
            .map_err(|error| invalid(error.to_string()))?;
        Ok(())
    }

    fn record_stopped(&self, task_id: &str, binding: &AttemptBinding) -> Result<(), TaskError> {
        let snapshot = self.coordinator.snapshot(task_id)?;
        let attempt = snapshot
            .attempts
            .get(&binding.attempt_id)
            .ok_or_else(|| invalid("unknown attempt"))?;
        if attempt.binding != *binding {
            return Err(invalid("attempt binding differs"));
        }
        if matches!(
            attempt.state,
            InvocationState::Completed | InvocationState::Failed | InvocationState::Cancelled
        ) {
            return Ok(());
        }
        if self
            .execution
            .execution()
            .server()
            .coordinator()
            .is_active(&binding.execution.execution_id)
        {
            return Err(invalid("worker has not released execution"));
        }
        let after = attempt
            .observation
            .as_ref()
            .map_or(0, |observation| observation.source.cursor);
        let actual = match inspect(self.ledger(), binding, after) {
            Ok(actual) => actual,
            Err(error) => {
                self.coordinator.mark_recovery(
                    task_id,
                    &format!("{task_id}/attempt/{}/missing-evidence", binding.attempt_id),
                    &binding.attempt_id,
                    &error.to_string(),
                )?;
                return Ok(());
            }
        };
        if actual.source.cursor <= after {
            return Ok(());
        }
        let outcome = match actual.stopped {
            StoppedOutcome::Completed => {
                let mut evidence = Vec::new();
                for criterion in snapshot
                    .definition
                    .criteria
                    .iter()
                    .filter(|criterion| criterion.invocation_id() == binding.invocation_id)
                {
                    match criterion {
                        CompletionCriterion::ExecutionCompleted { id, .. } => {
                            evidence.push(CompletionEvidence::ExecutionResult {
                                criterion_id: id.clone(),
                                source: actual.source.clone(),
                                result_digest: response_digest(
                                    actual
                                        .response
                                        .as_ref()
                                        .ok_or_else(|| invalid("missing actual model result"))?,
                                )?,
                            })
                        }
                        CompletionCriterion::ArtifactDigest { id, sha256, .. } => {
                            let response = actual
                                .response
                                .as_ref()
                                .ok_or_else(|| invalid("missing artifact source"))?;
                            let bytes = serde_json::to_vec(response)
                                .map_err(|error| invalid(error.to_string()))?;
                            let reference = self
                                .artifacts
                                .as_ref()
                                .ok_or_else(|| invalid("artifact verification unavailable"))?
                                .put(&bytes, Retention::Required)
                                .map_err(|error| invalid(error.to_string()))?;
                            if &reference.digest != sha256 {
                                return Err(invalid("artifact criterion digest differs"));
                            }
                            evidence.push(CompletionEvidence::VerifiedArtifact {
                                criterion_id: id.clone(),
                                source: actual.source.clone(),
                                sha256: reference.digest,
                                byte_len: reference.byte_length,
                            });
                        }
                        CompletionCriterion::Goal(_) => {}
                    }
                }
                AttemptOutcome::Completed { evidence }
            }
            StoppedOutcome::Suspended(waiting) => AttemptOutcome::Suspended { waiting },
            StoppedOutcome::Failed(reason) => AttemptOutcome::Failed {
                reason,
                safe_to_retry: false,
            },
            StoppedOutcome::Cancelled => AttemptOutcome::Cancelled {
                reason: "Runtime terminal cancellation".into(),
            },
            StoppedOutcome::RecoveryRequired => AttemptOutcome::RecoveryRequired {
                reason: "interrupted attempt or uncertain effect requires evidence".into(),
            },
        };
        self.coordinator.observe_attempt(
            task_id,
            &format!(
                "{task_id}/attempt/{}/observed/{}",
                binding.attempt_id, actual.source.cursor
            ),
            AttemptObservation {
                attempt_id: binding.attempt_id.clone(),
                execution: binding.execution.clone(),
                source: actual.source,
                usage: TaskUsage {
                    input_tokens: actual.input_tokens,
                    output_tokens: actual.output_tokens,
                    unreported_steps: actual.unreported_steps,
                },
                outcome,
            },
        )?;
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> TaskError {
    TaskError::Invalid(message.into())
}

#[cfg(test)]
mod tests;
