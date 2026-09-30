//! Host-driven durable task attempts; scheduling and tool authority stay explicit.

mod evidence;
mod retry;

use kolyan_core::{ToolExecutor, TurnExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ModelProvider;
use kolyan_runtime::{DurableTurnResult, ExecutionBinding};
use kolyan_storage::SessionStore;
use kolyan_trace::{ArtifactRef, ArtifactStore, Retention, TraceSink};
use serde_json::json;
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

    /// Approval resume retains the attempt, validates the current definition and
    /// constraints, and reconstructs the original Runtime checkpoint from storage.
    pub async fn resume<P: ModelProvider, T: ToolExecutor>(
        &self,
        task_id: &str,
        binding: AttemptBinding,
        approval_id: &str,
        executor: TurnExecutor<P, T>,
    ) -> Result<(TaskSnapshot, DurableTurnResult), TaskExecutionError> {
        self.coordinator.resume_attempt(
            task_id,
            &format!(
                "{task_id}/attempt/{}/resume/{approval_id}",
                binding.attempt_id
            ),
            binding.clone(),
            approval_id,
        )?;
        self.bind_execution(task_id, &binding)?;
        let result = self
            .execution
            .resume(
                executor,
                &binding.execution.session_id,
                &binding.execution.execution_id,
                approval_id,
            )
            .await;
        self.record_stopped(task_id, &binding)?;
        let stopped = result?;
        Ok((self.coordinator.snapshot(task_id)?, stopped))
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
    pub fn complete(
        &self,
        task_id: &str,
        fact_id: &str,
    ) -> Result<TaskSnapshot, TaskExecutionError> {
        let snapshot = self.coordinator.snapshot(task_id)?;
        let mut proofs = Vec::new();
        for attempt in snapshot.attempts.values() {
            if let Some(AttemptObservation {
                outcome: AttemptOutcome::Completed { evidence },
                ..
            }) = &attempt.observation
            {
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
                    }
                    proofs.push(proof.clone());
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
                    }
                }
                AttemptOutcome::Completed { evidence }
            }
            StoppedOutcome::Approval(approval_id) => AttemptOutcome::Suspended { approval_id },
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
