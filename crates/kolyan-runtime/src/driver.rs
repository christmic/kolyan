mod admission;
mod verified_input;
pub use verified_input::{VerifiedExecutionInput, verified_execution_input};
mod recorder;
mod recovery;
mod resume;
mod suspension;
mod tools;
use admission::InputAdmission;
use recorder::LedgerRecorder;
use tools::DurableTools;

use crate::{ExecutionKey as RuntimeTurnKey, RuntimeError, Trajectory, TrajectoryRecord};
use kolyan_core::{
    ResumableTurn, TurnBoundary, TurnBoundaryControl, TurnBoundaryFuture, TurnBoundaryKind,
    TurnDeadline, TurnError, TurnExecution, TurnExecutor, TurnRequest, TurnSuspension,
};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ModelProvider;
use kolyan_trace::TraceSink;
use serde_json::{Value, json};
use std::sync::Arc;

/// Result of one Runtime-owned resumable Turn attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum DurableTurnResult {
    Completed(Box<TurnExecution>, Trajectory),
    Suspended {
        suspension: Box<TurnSuspension>,
        trajectory: Trajectory,
    },
}

/// Runtime adapter that puts Core Turn boundaries behind a durable Ledger.
pub struct DurableTurnDriver<L, S> {
    ledger: L,
    trace: S,
    verifier: Arc<dyn crate::ExternalWaitVerifier>,
}

impl<L, S> DurableTurnDriver<L, S>
where
    L: LedgerStore + Clone + 'static,
    S: TraceSink,
{
    pub fn new(ledger: L, trace: S) -> Self {
        Self {
            ledger,
            trace,
            verifier: Arc::new(crate::RefuseExternalWaits),
        }
    }

    pub fn with_external_wait_verifier(
        mut self,
        verifier: Arc<dyn crate::ExternalWaitVerifier>,
    ) -> Self {
        self.verifier = verifier;
        self
    }

    pub fn ledger(&self) -> &L {
        &self.ledger
    }

    pub fn cancel(&self, execution_id: &str, turn_id: &str) -> Result<(), RuntimeError> {
        append_once(
            &self.ledger,
            execution_id,
            turn_id,
            "execution-cancelled",
            LedgerEventKind::ExecutionCancelled,
            Value::Null,
        )?;
        Ok(())
    }

    pub async fn start<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        request: TurnRequest,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
    ) -> Result<DurableTurnResult, RuntimeError>
    where
        P: ModelProvider,
        T: kolyan_core::ToolExecutor,
    {
        let deadline =
            TurnDeadline::capture(request.config.deadline, executor.absolute_deadline_at_ms())?;
        self.start_with_deadline(executor, request, session_id, execution_id, deadline)
            .await
    }

    /// Admit and execute the same anchored window. Dropping does not renew it.
    pub async fn start_with_deadline<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        request: TurnRequest,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        deadline: TurnDeadline,
    ) -> Result<DurableTurnResult, RuntimeError>
    where
        P: ModelProvider,
        T: kolyan_core::ToolExecutor,
    {
        deadline.validate_duration(request.config.deadline)?;
        let deadline = deadline.tighten_absolute(executor.absolute_deadline_at_ms())?;
        let key = RuntimeTurnKey {
            session_id: session_id.into(),
            turn_id: request.turn_id.clone(),
            execution_id: execution_id.into(),
        };
        let admitted = InputAdmission::new(
            key.clone(),
            &request,
            *executor.tool_dispatch_policy(),
            executor
                .tool_timeout()
                .map(|duration| u64::try_from(duration.as_millis()))
                .transpose()
                .map_err(|error| RuntimeError::Driver(error.to_string()))?,
            executor.agent_snapshot_digest().map(str::to_owned),
            &deadline,
        )?;
        let executor = if let Some(deadline) = admitted.deadline_at_ms {
            executor.with_absolute_deadline_at_ms(deadline)
        } else {
            executor
        };
        append_once(
            &self.ledger,
            &key.execution_id,
            &key.turn_id,
            "execution-started",
            LedgerEventKind::ExecutionStarted,
            json!(key),
        )?;
        admitted.persist(&self.ledger)?;
        if !self
            .ledger
            .claim(&format!("{}/attempt/start", key.execution_id))?
        {
            return Err(RuntimeError::Driver(
                "execution attempt is already claimed; replay is forbidden".into(),
            ));
        }
        let control = Arc::new(LedgerBoundaryControl::new(self.ledger.clone(), key.clone()));
        let recorder = Arc::new(LedgerRecorder::new(
            self.ledger.clone(),
            key.clone(),
            "start".into(),
        ));
        let controlled = executor
            .with_execution_key(key.clone())
            .map_tool_executor(|inner| {
                DurableTools::new(self.ledger.clone(), key.clone(), inner)
                    .with_snapshot_digest(admitted.agent_snapshot_digest.clone())
                    .with_wait_verifier(self.verifier.clone())
            })
            .with_boundary_control(control)
            .with_event_recorder(recorder.clone())
            .with_step_event_recorder(recorder);
        match controlled
            .start_resumable_with_deadline(request, deadline)
            .await
        {
            Ok(ResumableTurn::Completed(execution)) => self.completed(&key, *execution),
            Ok(ResumableTurn::Suspended(suspension)) => self.suspended(&key, *suspension),
            Err(error) => {
                self.persist_error(&key, &error)?;
                Err(RuntimeError::Turn(error))
            }
        }
    }

    fn completed(
        &self,
        key: &RuntimeTurnKey,
        execution: TurnExecution,
    ) -> Result<DurableTurnResult, RuntimeError> {
        let trajectory = self.project_events(key)?;
        Ok(DurableTurnResult::Completed(
            Box::new(execution),
            trajectory,
        ))
    }

    fn persist_error(&self, key: &RuntimeTurnKey, error: &TurnError) -> Result<(), RuntimeError> {
        if self
            .ledger
            .execution_events_after(&key.execution_id, 0)?
            .iter()
            .any(|event| {
                matches!(
                    event.kind,
                    LedgerEventKind::TurnCompleted
                        | LedgerEventKind::TurnCancelled
                        | LedgerEventKind::TurnFailed
                        | LedgerEventKind::TurnTimedOut
                )
            })
        {
            return Ok(());
        }
        append_once(
            &self.ledger,
            &key.execution_id,
            &key.turn_id,
            "turn-error",
            match error.end_reason() {
                kolyan_core::TurnEndReason::Cancelled => LedgerEventKind::TurnCancelled,
                kolyan_core::TurnEndReason::TimedOut => LedgerEventKind::TurnTimedOut,
                _ => LedgerEventKind::TurnFailed,
            },
            json!({"error": error.to_string()}),
        )?;
        Ok(())
    }

    fn project_events(&self, key: &RuntimeTurnKey) -> Result<Trajectory, RuntimeError> {
        let mut trajectory = Trajectory {
            turn_id: key.turn_id.clone(),
            execution_id: key.execution_id.clone(),
            ..Default::default()
        };
        let prefix = format!("{}/turn-event/", key.execution_id);
        for event in self
            .ledger
            .execution_events_after(&key.execution_id, 0)?
            .into_iter()
            .filter(|event| event.event_id.starts_with(&prefix))
        {
            if let Err(error) = self.trace.record(kolyan_trace::TraceRecord {
                turn_id: key.turn_id.clone(),
                execution_id: key.execution_id.clone(),
                sequence: event.cursor,
                kind: if event.kind == LedgerEventKind::ModelStreamEvent {
                    kolyan_trace::TraceKind::ModelDelta
                } else {
                    kolyan_trace::TraceKind::TurnEvent
                },
                payload: event.payload.clone(),
            }) {
                trajectory.trace_errors.push(error.to_string());
            }
            trajectory.records.push(TrajectoryRecord {
                sequence: event.cursor,
                kind: event.kind,
                payload: event.payload,
            });
        }
        Ok(trajectory)
    }
}

struct LedgerBoundaryControl<L> {
    ledger: L,
    key: RuntimeTurnKey,
    resume_cursor: Option<u64>,
}

impl<L> LedgerBoundaryControl<L> {
    fn new(ledger: L, key: RuntimeTurnKey) -> Self {
        Self {
            ledger,
            key,
            resume_cursor: None,
        }
    }

    fn for_resume(ledger: L, key: RuntimeTurnKey, cursor: u64) -> Self {
        Self {
            ledger,
            key,
            resume_cursor: Some(cursor),
        }
    }
}

impl<L: LedgerStore + Clone + 'static> TurnBoundaryControl for LedgerBoundaryControl<L> {
    fn admit(&self, boundary: TurnBoundary) -> TurnBoundaryFuture<'_> {
        if boundary.turn_id != self.key.turn_id {
            return Box::pin(async {
                Err(TurnError::BoundaryControl {
                    message: "boundary belongs to a different turn".into(),
                })
            });
        }
        Box::pin(async move {
            let (mut suffix, kind, payload) = boundary_event(&boundary);
            // A waiting checkpoint may survive several resume attempts. Only
            // lifecycle admissions vary by attempt; Step/tool effects keep their
            // stable identities so recovery cannot enter a completed effect twice.
            if kind == LedgerEventKind::ExecutionBoundaryAdmitted
                && let Some(cursor) = self.resume_cursor
            {
                suffix = format!("{suffix}/attempt/{cursor}");
            }
            let event_id = format!("{}/{}", self.key.execution_id, suffix);
            let event = LedgerEvent {
                event_id: event_id.clone(),
                turn_id: self.key.turn_id.clone(),
                execution_id: self.key.execution_id.clone(),
                cursor: 0,
                kind,
                idempotency_key: event_id,
                payload,
            };
            let result = if matches!(
                boundary.kind,
                TurnBoundaryKind::Terminal {
                    reason: kolyan_core::TurnEndReason::Cancelled
                }
            ) {
                self.ledger.append(event)
            } else {
                self.ledger.append_unless_cancelled(event)
            };
            result.map_err(|error| match error {
                kolyan_ledger::LedgerError::Cancelled(_) => TurnError::Cancelled,
                error => TurnError::BoundaryControl {
                    message: error.to_string(),
                },
            })?;
            Ok(())
        })
    }
}

fn boundary_event(boundary: &TurnBoundary) -> (String, LedgerEventKind, Value) {
    match &boundary.kind {
        TurnBoundaryKind::Step { step_id } => (
            format!("step/{step_id}"),
            LedgerEventKind::StepStarted,
            json!({"step_id": step_id}),
        ),
        TurnBoundaryKind::Tool { step_id, call_id } => (
            format!("tool/{step_id}/{call_id}"),
            LedgerEventKind::ToolExecutionStarted,
            json!({"step_id": step_id, "call_id": call_id}),
        ),
        TurnBoundaryKind::AwaitingApproval { approval_id } => (
            format!("approval/{approval_id}/boundary"),
            LedgerEventKind::ExecutionBoundaryAdmitted,
            json!({"approval_id": approval_id}),
        ),
        TurnBoundaryKind::ResumeApproval { approval_id } => (
            format!("approval/{approval_id}/resume-boundary"),
            LedgerEventKind::ExecutionBoundaryAdmitted,
            json!({"approval_id": approval_id}),
        ),
        TurnBoundaryKind::AwaitingExternal {
            checkpoint_id,
            wait_ids,
        } => (
            format!("checkpoint/{checkpoint_id}/wait-boundary"),
            LedgerEventKind::ExecutionBoundaryAdmitted,
            json!({"checkpoint_id":checkpoint_id,"wait_ids":wait_ids}),
        ),
        TurnBoundaryKind::ResumeCheckpoint { checkpoint_id } => (
            format!("checkpoint/{checkpoint_id}/resume-boundary"),
            LedgerEventKind::ExecutionBoundaryAdmitted,
            json!({"checkpoint_id":checkpoint_id}),
        ),
        TurnBoundaryKind::Terminal { reason } => (
            format!("terminal/{reason:?}"),
            match reason {
                kolyan_core::TurnEndReason::Cancelled => LedgerEventKind::TurnCancelled,
                kolyan_core::TurnEndReason::TimedOut => LedgerEventKind::TurnTimedOut,
                kolyan_core::TurnEndReason::Failed
                | kolyan_core::TurnEndReason::ApprovalRejected
                | kolyan_core::TurnEndReason::ApprovalExpired => LedgerEventKind::TurnFailed,
                _ => LedgerEventKind::TurnCompleted,
            },
            json!({"reason": format!("{reason:?}")}),
        ),
    }
}

fn append_once<L: LedgerStore>(
    ledger: &L,
    execution_id: &str,
    turn_id: &str,
    suffix: &str,
    kind: LedgerEventKind,
    payload: Value,
) -> Result<LedgerEvent, RuntimeError> {
    let event_id = format!("{execution_id}/{suffix}");
    if let Some(event) = ledger.event_by_id(&event_id)? {
        if event.turn_id != turn_id
            || event.execution_id != execution_id
            || event.kind != kind
            || event.payload != payload
        {
            return Err(RuntimeError::Driver(format!(
                "conflicting event identity: {event_id}"
            )));
        }
        return Ok(event);
    }
    Ok(ledger.append(LedgerEvent {
        event_id: event_id.clone(),
        turn_id: turn_id.into(),
        execution_id: execution_id.into(),
        cursor: 0,
        kind,
        idempotency_key: event_id,
        payload,
    })?)
}

#[cfg(test)]
mod tests;
