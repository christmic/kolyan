//! Journal-backed command admission. Replay and writes share the same reducer.

use kolyan_ledger::{FactDraft, FactJournal, FactRecord, FactSubject};

use super::reducer::{apply, identity, invalid, reference};
use super::types::*;

const PAGE_SIZE: usize = 512;
const VERSION: u32 = 1;

/// Stateless domain coordinator; durable facts, not this value, own task state.
#[derive(Debug, Clone)]
pub struct TaskCoordinator<J> {
    journal: J,
}

impl<J: FactJournal> TaskCoordinator<J> {
    pub fn new(journal: J) -> Self {
        Self { journal }
    }

    pub fn journal(&self) -> &J {
        &self.journal
    }

    /// Replay validates all critical semantics. Querying never executes work.
    pub fn snapshot(&self, task_id: &str) -> Result<TaskSnapshot, TaskError> {
        let records = self.records(task_id)?;
        replay(task_id, &records)?.ok_or_else(|| TaskError::NotFound(task_id.to_owned()))
    }

    /// Registers an immutable definition; identical fact retries are idempotent.
    pub fn register_task(
        &self,
        fact_id: &str,
        definition: TaskDefinition,
    ) -> Result<TaskSnapshot, TaskError> {
        let task_id = definition.task_id.clone();
        self.command(&task_id, fact_id, TaskEvent::Registered(definition))
    }

    /// Admits explicit revision/instance/constraints without deriving permissions
    /// from a parent. Continuations require a completed predecessor dependency.
    pub fn admit_invocation(
        &self,
        task_id: &str,
        fact_id: &str,
        definition: InvocationDefinition,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(task_id, fact_id, TaskEvent::InvocationAdmitted(definition))
    }

    /// Adds a dependency without conflating its DAG with call ancestry.
    pub fn admit_dependency(
        &self,
        task_id: &str,
        fact_id: &str,
        invocation_id: &str,
        dependency_id: &str,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(
            task_id,
            fact_id,
            TaskEvent::DependencyAdmitted {
                invocation_id: invocation_id.into(),
                dependency_id: dependency_id.into(),
            },
        )
    }

    /// A retry is a new attempt and requires prior explicit safe-retry evidence.
    pub fn start_attempt(
        &self,
        task_id: &str,
        fact_id: &str,
        binding: AttemptBinding,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(task_id, fact_id, TaskEvent::AttemptStarted(binding))
    }

    /// The host must verify source facts/content before submitting an observation.
    /// Actual usage is retained even when it exceeds the admitted token ceiling.
    pub fn observe_attempt(
        &self,
        task_id: &str,
        fact_id: &str,
        observation: AttemptObservation,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(task_id, fact_id, TaskEvent::AttemptObserved(observation))
    }

    /// Approval resumption retains the original attempt and exact admission.
    pub fn resume_attempt(
        &self,
        task_id: &str,
        fact_id: &str,
        binding: AttemptBinding,
        approval_id: &str,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(
            task_id,
            fact_id,
            TaskEvent::AttemptResumed {
                binding,
                approval_id: approval_id.into(),
            },
        )
    }

    /// An interrupted attempt cannot be retried until its effects are reconciled.
    pub fn mark_recovery(
        &self,
        task_id: &str,
        fact_id: &str,
        attempt_id: &str,
        reason: &str,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(
            task_id,
            fact_id,
            TaskEvent::RecoveryNeeded {
                attempt_id: attempt_id.into(),
                reason: reason.into(),
            },
        )
    }

    /// Records a host-verified safe retry decision separately from the original
    /// stopped observation. Only a fresh attempt can subsequently execute.
    pub fn authorize_retry(
        &self,
        task_id: &str,
        fact_id: &str,
        attempt_id: &str,
        source: ExecutionEvidence,
        reason: &str,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(
            task_id,
            fact_id,
            TaskEvent::RetryAuthorized {
                attempt_id: attempt_id.into(),
                source,
                reason: reason.into(),
            },
        )
    }

    /// Commits an admitted result edge referencing exact completion evidence.
    /// Same fact retries are idempotent; new facts cannot double-consume it.
    pub fn consume_child_result(
        &self,
        task_id: &str,
        fact_id: &str,
        invocation_id: &str,
        result: ConsumedResult,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(
            task_id,
            fact_id,
            TaskEvent::ResultConsumed {
                invocation_id: invocation_id.into(),
                result,
            },
        )
    }

    /// All criteria require exact previously observed evidence; model text alone
    /// cannot complete a task. Physical evidence verification belongs to the host.
    pub fn complete_task(
        &self,
        task_id: &str,
        fact_id: &str,
        evidence: Vec<CompletionEvidence>,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(task_id, fact_id, TaskEvent::Completed { evidence })
    }

    /// Stops work admission while preserving observations of in-flight attempts.
    pub fn fail_task(
        &self,
        task_id: &str,
        fact_id: &str,
        reason: &str,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(
            task_id,
            fact_id,
            TaskEvent::Failed {
                reason: reason.into(),
            },
        )
    }

    /// Cancellation records policy-selected requests. The host delivers actual
    /// execution cancellation; the reducer never fabricates stopped outcomes.
    pub fn cancel_task(
        &self,
        task_id: &str,
        fact_id: &str,
        reason: &str,
    ) -> Result<TaskSnapshot, TaskError> {
        self.command(
            task_id,
            fact_id,
            TaskEvent::Cancelled {
                reason: reason.into(),
            },
        )
    }

    fn records(&self, task_id: &str) -> Result<Vec<FactRecord>, TaskError> {
        identity(task_id)?;
        let mut records = Vec::new();
        let mut after = 0;
        loop {
            let page = self.journal.read(task_id, after, PAGE_SIZE)?;
            if page.len() > PAGE_SIZE {
                return Err(invalid("journal returned oversized page"));
            }
            for record in page {
                if record.stream_id != task_id
                    || record.position
                        != after
                            .checked_add(1)
                            .ok_or_else(|| invalid("position overflow"))?
                {
                    return Err(invalid("journal returned noncontiguous or foreign records"));
                }
                after = record.position;
                records.push(record);
            }
            // Read until empty, not until a short page: adapters may cap pages.
            let tail = self.journal.read(task_id, after, 1)?;
            if tail.is_empty() {
                break;
            }
            if tail.len() != 1 || tail[0].stream_id != task_id || tail[0].position != after + 1 {
                return Err(invalid("journal did not advance"));
            }
        }
        Ok(records)
    }

    fn command(
        &self,
        task_id: &str,
        fact_id: &str,
        event: TaskEvent,
    ) -> Result<TaskSnapshot, TaskError> {
        identity(fact_id)?;
        let records = self.records(task_id)?;
        let mut snapshot = replay(task_id, &records)?;
        let (kind, subject) = envelope(task_id, &event);
        let payload = serde_json::to_value(&event).map_err(|error| invalid(error.to_string()))?;
        if let Some(existing) = records
            .iter()
            .find(|record| record.draft.fact_id == fact_id)
        {
            if existing.draft.kind != kind
                || existing.draft.subject != subject
                || existing.draft.schema_version != VERSION
                || !existing.draft.critical
                || existing.draft.payload != payload
            {
                return Err(invalid("fact identity reused with a different command"));
            }
            return snapshot.ok_or_else(|| TaskError::NotFound(task_id.into()));
        }
        let expected = records.last().map_or(0, |record| record.position);
        let mut causes = records
            .last()
            .map(reference)
            .into_iter()
            .collect::<Vec<_>>();
        if let TaskEvent::ResultConsumed { result, .. } = &event
            && !causes.contains(&result.completion_fact)
        {
            causes.push(result.completion_fact.clone());
        }
        let draft = FactDraft {
            fact_id: fact_id.into(),
            subject,
            kind: kind.into(),
            schema_version: VERSION,
            critical: true,
            causes,
            payload,
        };
        let position = expected
            .checked_add(1)
            .ok_or_else(|| invalid("position overflow"))?;
        let candidate = FactRecord {
            stream_id: task_id.into(),
            position,
            draft: draft.clone(),
        };
        apply(&mut snapshot, event, &candidate)?;
        let committed = self.journal.append(task_id, expected, vec![draft])?;
        if committed != vec![candidate] {
            return Err(invalid("journal committed a different fact"));
        }
        snapshot.ok_or_else(|| TaskError::NotFound(task_id.into()))
    }
}

fn envelope(task_id: &str, event: &TaskEvent) -> (&'static str, FactSubject) {
    let (kind, subject_kind, id) = match event {
        TaskEvent::Registered(_) => ("task.registered", "task", task_id),
        TaskEvent::InvocationAdmitted(definition) => (
            "task.invocation_admitted",
            "invocation",
            definition.invocation_id.as_str(),
        ),
        TaskEvent::DependencyAdmitted { invocation_id, .. } => (
            "task.dependency_admitted",
            "invocation",
            invocation_id.as_str(),
        ),
        TaskEvent::AttemptStarted(binding) => (
            "task.attempt_started",
            "attempt",
            binding.attempt_id.as_str(),
        ),
        TaskEvent::AttemptObserved(observation) => (
            "task.attempt_observed",
            "attempt",
            observation.attempt_id.as_str(),
        ),
        TaskEvent::AttemptResumed { binding, .. } => (
            "task.attempt_resumed",
            "attempt",
            binding.attempt_id.as_str(),
        ),
        TaskEvent::RecoveryNeeded { attempt_id, .. } => {
            ("task.recovery_needed", "attempt", attempt_id.as_str())
        }
        TaskEvent::RetryAuthorized { attempt_id, .. } => {
            ("task.retry_authorized", "attempt", attempt_id.as_str())
        }
        TaskEvent::ResultConsumed { invocation_id, .. } => {
            ("task.result_consumed", "invocation", invocation_id.as_str())
        }
        TaskEvent::Completed { .. } => ("task.completed", "task", task_id),
        TaskEvent::Cancelled { .. } => ("task.cancelled", "task", task_id),
        TaskEvent::Failed { .. } => ("task.failed", "task", task_id),
    };
    (
        kind,
        FactSubject {
            kind: format!("task.{subject_kind}"),
            id: id.into(),
        },
    )
}

fn replay(task_id: &str, records: &[FactRecord]) -> Result<Option<TaskSnapshot>, TaskError> {
    let mut snapshot = None;
    let mut previous = None;
    for record in records {
        let known = matches!(
            record.draft.kind.as_str(),
            "task.registered"
                | "task.invocation_admitted"
                | "task.dependency_admitted"
                | "task.attempt_started"
                | "task.attempt_observed"
                | "task.attempt_resumed"
                | "task.recovery_needed"
                | "task.retry_authorized"
                | "task.result_consumed"
                | "task.completed"
                | "task.cancelled"
                | "task.failed"
        );
        if !known || record.draft.schema_version != VERSION {
            if record.draft.critical {
                return Err(invalid("unknown critical kind or version"));
            }
        } else {
            if !record.draft.critical {
                return Err(invalid("task transitions must be critical"));
            }
            let event: TaskEvent = serde_json::from_value(record.draft.payload.clone())
                .map_err(|error| invalid(error.to_string()))?;
            let (kind, subject) = envelope(task_id, &event);
            if kind != record.draft.kind
                || subject != record.draft.subject
                || previous
                    .as_ref()
                    .is_some_and(|cause| !record.draft.causes.contains(cause))
            {
                return Err(invalid("task envelope/subject/causal predecessor mismatch"));
            }
            if let TaskEvent::Registered(definition) = &event
                && definition.task_id != task_id
            {
                return Err(invalid("registration belongs to another task stream"));
            }
            apply(&mut snapshot, event, record)?;
        }
        if let Some(state) = &mut snapshot {
            state.position = record.position;
        }
        previous = Some(reference(record));
    }
    Ok(snapshot)
}
