use crate::{RuntimeError, Trajectory, TrajectoryRecord, encode_turn_event};
use kolyan_core::{
    ApprovalRequest, ResumableTurn, TurnBoundary, TurnBoundaryControl, TurnBoundaryFuture,
    TurnBoundaryKind, TurnError, TurnEvent, TurnExecution, TurnExecutor, TurnRequest,
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
            .events_after(0)?
            .into_iter()
            .find(|event| {
                event.execution_id == execution_id
                    && event.kind == LedgerEventKind::ApprovalRequested
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
        let control = Arc::new(LedgerBoundaryControl::new(self.ledger.clone(), key.clone()));
        let controlled = executor.with_boundary_control(control);
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
        let control = Arc::new(LedgerBoundaryControl::new(self.ledger.clone(), key.clone()));
        let controlled = executor.with_boundary_control(control);
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
            "execution-suspended",
            LedgerEventKind::ExecutionSuspended,
            json!({"approval_id": approval.approval_id}),
        )?;
        Ok(DurableTurnResult::AwaitingApproval {
            approval: Box::new(approval),
            trajectory: Trajectory {
                turn_id: key.turn_id.clone(),
                execution_id: key.execution_id.clone(),
                records: Vec::new(),
            },
        })
    }

    fn completed(
        &self,
        key: &RuntimeTurnKey,
        execution: TurnExecution,
    ) -> Result<DurableTurnResult, RuntimeError> {
        let trajectory = self.persist_events(key, &execution.events)?;
        Ok(DurableTurnResult::Completed(
            Box::new(execution),
            trajectory,
        ))
    }

    fn persist_error(&self, key: &RuntimeTurnKey, error: &TurnError) -> Result<(), RuntimeError> {
        append_once(
            &self.ledger,
            &key.execution_id,
            &key.turn_id,
            "turn-error",
            LedgerEventKind::TurnFailed,
            json!({"error": error.to_string()}),
        )?;
        Ok(())
    }

    fn persist_events(
        &self,
        key: &RuntimeTurnKey,
        events: &[TurnEvent],
    ) -> Result<Trajectory, RuntimeError> {
        let mut trajectory = Trajectory {
            turn_id: key.turn_id.clone(),
            execution_id: key.execution_id.clone(),
            records: Vec::new(),
        };
        for (index, event) in events.iter().enumerate() {
            let (kind, payload) = encode_turn_event(event);
            let suffix = format!("turn-event-{index}");
            let ledger_event = append_once(
                &self.ledger,
                &key.execution_id,
                &key.turn_id,
                &suffix,
                kind,
                payload.clone(),
            )?;
            self.trace
                .record(kolyan_trace::TraceRecord {
                    turn_id: key.turn_id.clone(),
                    execution_id: key.execution_id.clone(),
                    sequence: ledger_event.cursor,
                    kind: kolyan_trace::TraceKind::TurnEvent,
                    payload: payload.clone(),
                })
                .map_err(|error| RuntimeError::Trace(error.to_string()))?;
            trajectory.records.push(TrajectoryRecord {
                sequence: ledger_event.cursor,
                kind,
                payload,
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
        let cancelled = self.ledger.events_after(0).ok().is_some_and(|events| {
            events.iter().any(|event| {
                event.execution_id == self.key.execution_id
                    && event.kind == LedgerEventKind::ExecutionCancelled
            })
        });
        let ledger = self.ledger.clone();
        let key = self.key.clone();
        Box::pin(async move {
            if cancelled && !matches!(boundary.kind, TurnBoundaryKind::Terminal { .. }) {
                return Err(TurnError::Cancelled);
            }
            let (suffix, kind, payload) = boundary_event(&boundary);
            append_once(
                &ledger,
                &key.execution_id,
                &key.turn_id,
                &suffix,
                kind,
                payload,
            )
            .map_err(|error| TurnError::BoundaryControl {
                message: error.to_string(),
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
            LedgerEventKind::TurnCompleted,
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
    if let Some(event) = ledger
        .events_after(0)?
        .into_iter()
        .find(|event| event.event_id == event_id)
    {
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
