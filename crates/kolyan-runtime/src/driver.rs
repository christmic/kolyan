mod recorder;
mod tools;
use recorder::LedgerRecorder;
use tools::DurableTools;

use crate::{RuntimeError, Trajectory, TrajectoryRecord};
use kolyan_core::{
    ApprovalRequest, ResumableTurn, TurnBoundary, TurnBoundaryControl, TurnBoundaryFuture,
    TurnBoundaryKind, TurnError, TurnExecution, TurnExecutor, TurnRequest,
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
    AwaitingApproval {
        approval: Box<ApprovalRequest>,
        trajectory: Trajectory,
    },
}

/// Runtime adapter that puts Core Turn boundaries behind a durable Ledger.
pub struct DurableTurnDriver<L, S> {
    ledger: L,
    trace: S,
}

impl<L, S> DurableTurnDriver<L, S>
where
    L: LedgerStore + Clone + 'static,
    S: TraceSink,
{
    pub fn new(ledger: L, trace: S) -> Self {
        Self { ledger, trace }
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

    pub fn load_approval(
        &self,
        execution_id: &str,
        approval_id: &str,
    ) -> Result<ApprovalRequest, RuntimeError> {
        let event = self
            .ledger
            .execution_events_after(execution_id, 0)?
            .into_iter()
            .find(|event| {
                event.kind == LedgerEventKind::ApprovalRequested
                    && event.payload["approval_id"] == approval_id
            })
            .ok_or_else(|| RuntimeError::Driver(format!("approval not found: {approval_id}")))?;
        serde_json::from_value(event.payload)
            .map_err(|error| RuntimeError::Driver(format!("invalid approval checkpoint: {error}")))
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
        let key = RuntimeTurnKey {
            session_id: session_id.into(),
            turn_id: request.turn_id.clone(),
            execution_id: execution_id.into(),
        };
        append_once(
            &self.ledger,
            &key.execution_id,
            &key.turn_id,
            "execution-started",
            LedgerEventKind::ExecutionStarted,
            json!(key),
        )?;
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
            .map_tool_executor(|inner| DurableTools::new(self.ledger.clone(), key.clone(), inner))
            .with_boundary_control(control)
            .with_event_recorder(recorder.clone())
            .with_step_event_recorder(recorder);
        match controlled.start_resumable(request).await {
            Ok(ResumableTurn::Completed(execution)) => self.completed(&key, *execution),
            Ok(ResumableTurn::AwaitingApproval(approval)) => self.suspended(&key, *approval),
            Err(error) => {
                self.persist_error(&key, &error)?;
                Err(RuntimeError::Turn(error))
            }
        }
    }

    pub async fn resume<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        approval_id: &str,
    ) -> Result<DurableTurnResult, RuntimeError>
    where
        P: ModelProvider,
        T: kolyan_core::ToolExecutor,
    {
        let execution_id = execution_id.into();
        let approval = self.load_approval(&execution_id, approval_id)?;
        let key = RuntimeTurnKey {
            session_id: session_id.into(),
            turn_id: approval.turn_id.clone(),
            execution_id,
        };
        let identity = self
            .ledger
            .event_by_id(&format!("{}/execution-started", key.execution_id))?
            .ok_or_else(|| RuntimeError::Driver("missing execution identity".into()))?;
        if identity.payload != json!(key) {
            return Err(RuntimeError::Driver(
                "approval execution identity mismatch".into(),
            ));
        }
        if !self.ledger.claim(&format!(
            "{}/attempt/resume/{approval_id}",
            key.execution_id
        ))? {
            return Err(RuntimeError::Driver(
                "approval attempt is already claimed; replay is forbidden".into(),
            ));
        }
        let control = Arc::new(LedgerBoundaryControl::new(self.ledger.clone(), key.clone()));
        let recorder = Arc::new(LedgerRecorder::new(
            self.ledger.clone(),
            key.clone(),
            format!("resume/{approval_id}"),
        ));
        let controlled = executor
            .map_tool_executor(|inner| DurableTools::new(self.ledger.clone(), key.clone(), inner))
            .with_boundary_control(control)
            .with_event_recorder(recorder.clone())
            .with_step_event_recorder(recorder);
        match controlled.resume_approval(approval, approval_id).await {
            Ok(ResumableTurn::Completed(execution)) => self.completed(&key, *execution),
            Ok(ResumableTurn::AwaitingApproval(next)) => self.suspended(&key, *next),
            Err(error) => {
                self.persist_error(&key, &error)?;
                Err(RuntimeError::Turn(error))
            }
        }
    }

    fn suspended(
        &self,
        key: &RuntimeTurnKey,
        approval: ApprovalRequest,
    ) -> Result<DurableTurnResult, RuntimeError> {
        append_once(
            &self.ledger,
            &key.execution_id,
            &key.turn_id,
            &format!("approval/{}/requested", approval.approval_id),
            LedgerEventKind::ApprovalRequested,
            serde_json::to_value(&approval)
                .map_err(|error| RuntimeError::Driver(error.to_string()))?,
        )?;
        append_once(
            &self.ledger,
            &key.execution_id,
            &key.turn_id,
            &format!("execution-suspended/{}", approval.approval_id),
            LedgerEventKind::ExecutionSuspended,
            json!({"approval_id": approval.approval_id}),
        )?;
        Ok(DurableTurnResult::AwaitingApproval {
            approval: Box::new(approval),
            trajectory: self.project_events(key)?,
        })
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

#[derive(Clone, Debug, serde::Serialize)]
struct RuntimeTurnKey {
    session_id: String,
    turn_id: String,
    execution_id: String,
}

struct LedgerBoundaryControl<L> {
    ledger: L,
    key: RuntimeTurnKey,
}

impl<L> LedgerBoundaryControl<L> {
    fn new(ledger: L, key: RuntimeTurnKey) -> Self {
        Self { ledger, key }
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
            let (suffix, kind, payload) = boundary_event(&boundary);
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
            LedgerEventKind::ExecutionSuspended,
            json!({"approval_id": approval_id}),
        ),
        TurnBoundaryKind::ResumeApproval { approval_id } => (
            format!("approval/{approval_id}/resolved"),
            LedgerEventKind::ApprovalResolved,
            json!({"approval_id": approval_id}),
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
