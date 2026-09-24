use kolyan_core::{ToolExecutor, TurnEvent, TurnExecution, TurnExecutor, TurnRequest};
use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ModelProvider;
use kolyan_trace::{TraceKind, TraceRecord, TraceSink};
use serde_json::{Value, json};
use thiserror::Error;

mod driver;
mod execution;

pub use driver::{DurableTurnDriver, DurableTurnResult};
pub use execution::{
    AdmissionDecision, AdmissionPort, EffectDisposition, EffectExecutor, EffectGrant,
    EffectOutcome, EffectReceipt, EffectRequest, ExecutionKey, ExecutionRuntime, ExecutionStatus,
    ReceiptStatus, RuntimeExecutionError,
};

#[derive(Debug, Clone, PartialEq)]
pub struct TrajectoryRecord {
    pub sequence: u64,
    pub kind: LedgerEventKind,
    pub payload: Value,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Trajectory {
    pub turn_id: String,
    pub execution_id: String,
    pub records: Vec<TrajectoryRecord>,
    /// Observability failures never change the durable execution outcome.
    pub trace_errors: Vec<String>,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("turn execution failed: {0}")]
    Turn(#[from] kolyan_core::TurnError),
    #[error("ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("trace failed: {0}")]
    Trace(String),
    #[error("runtime driver failed: {0}")]
    Driver(String),
}

pub struct TurnDriver<L, S> {
    ledger: L,
    trace: S,
}

impl<L, S> TurnDriver<L, S>
where
    L: LedgerStore,
    S: TraceSink,
{
    pub fn new(ledger: L, trace: S) -> Self {
        Self { ledger, trace }
    }

    pub fn ledger(&self) -> &L {
        &self.ledger
    }

    pub async fn execute<P, T>(
        &self,
        executor: &TurnExecutor<P, T>,
        request: TurnRequest,
        execution_id: impl Into<String>,
    ) -> Result<(TurnExecution, Trajectory), RuntimeError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let execution_id = execution_id.into();
        let turn_id = request.turn_id.clone();
        let execution = executor
            .execute_with_events(request, Default::default())
            .await?;
        let mut trajectory = Trajectory {
            turn_id: turn_id.clone(),
            execution_id: execution_id.clone(),
            records: Vec::new(),
            trace_errors: Vec::new(),
        };
        for (index, event) in execution.events.iter().enumerate() {
            let (kind, payload) = encode_turn_event(event);
            let event_id = format!("{turn_id}/{execution_id}/{index}");
            let ledger_event = self.ledger.append(LedgerEvent {
                event_id: event_id.clone(),
                turn_id: turn_id.clone(),
                execution_id: execution_id.clone(),
                cursor: 0,
                kind,
                idempotency_key: event_id,
                payload: payload.clone(),
            })?;
            if let Err(error) = self.trace.record(TraceRecord {
                turn_id: turn_id.clone(),
                execution_id: execution_id.clone(),
                sequence: ledger_event.cursor,
                kind: TraceKind::TurnEvent,
                payload: payload.clone(),
            }) {
                trajectory.trace_errors.push(error.to_string());
            }
            trajectory.records.push(TrajectoryRecord {
                sequence: ledger_event.cursor,
                kind,
                payload,
            });
        }
        Ok((execution, trajectory))
    }

    pub fn into_parts(self) -> (L, S) {
        (self.ledger, self.trace)
    }
}

fn encode_turn_event(event: &TurnEvent) -> (LedgerEventKind, Value) {
    match event {
        TurnEvent::Started { turn_id } => {
            (LedgerEventKind::TurnStarted, json!({ "turn_id": turn_id }))
        }
        TurnEvent::StepStarted { step_id, .. } => {
            (LedgerEventKind::StepStarted, json!({ "step_id": step_id }))
        }
        TurnEvent::StepCompleted { step, .. } => (
            LedgerEventKind::StepCompleted,
            json!({ "step_id": step.step_id, "outcome": format!("{:?}", step.outcome) }),
        ),
        TurnEvent::ToolCallRequested { call, .. } => (
            LedgerEventKind::ToolCallRequested,
            json!({ "call_id": call.id, "name": call.name, "arguments": call.arguments }),
        ),
        TurnEvent::ApprovalRequested { call_id, name, .. } => (
            LedgerEventKind::ApprovalRequested,
            json!({ "call_id": call_id, "name": name }),
        ),
        TurnEvent::ToolExecutionStarted { call_id, name, .. } => (
            LedgerEventKind::ToolExecutionStarted,
            json!({ "call_id": call_id, "name": name }),
        ),
        TurnEvent::ToolResult { result, .. } => (
            LedgerEventKind::ToolExecutionCompleted,
            json!({ "call_id": result.call_id, "is_error": result.is_error }),
        ),
        TurnEvent::ToolExecutionFailed {
            call_id,
            name,
            error,
            ..
        } => (
            LedgerEventKind::ToolExecutionFailed,
            json!({ "call_id": call_id, "name": name, "error": error.to_string() }),
        ),
        TurnEvent::Failed { error, .. } => (LedgerEventKind::TurnFailed, json!({ "error": error })),
        TurnEvent::Cancelled { .. } => (LedgerEventKind::TurnCancelled, Value::Null),
        TurnEvent::TimedOut { .. } => (LedgerEventKind::TurnTimedOut, Value::Null),
        TurnEvent::Completed { outcome, .. } => (
            LedgerEventKind::TurnCompleted,
            json!({ "outcome": format!("{outcome:?}") }),
        ),
    }
}

#[cfg(test)]
mod tests;
