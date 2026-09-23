use kolyan_core::{ToolExecutor, TurnEvent, TurnExecution, TurnExecutor, TurnRequest};
use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ModelProvider;
use kolyan_trace::{TraceKind, TraceRecord, TraceSink};
use serde_json::{Value, json};
use thiserror::Error;

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
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("turn execution failed: {0}")]
    Turn(#[from] kolyan_core::TurnError),
    #[error("ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("trace failed: {0}")]
    Trace(String),
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
            self.trace
                .record(TraceRecord {
                    turn_id: turn_id.clone(),
                    execution_id: execution_id.clone(),
                    sequence: ledger_event.cursor,
                    kind: TraceKind::TurnEvent,
                    payload: payload.clone(),
                })
                .map_err(|error| RuntimeError::Trace(error.to_string()))?;
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
        TurnEvent::Completed { outcome, .. } => (
            LedgerEventKind::TurnCompleted,
            json!({ "outcome": format!("{outcome:?}") }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use kolyan_core::NoopToolExecutor;
    use kolyan_model::{
        ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRef, ModelRequest,
        ModelResponse, ProviderFuture, StopReason, TokenUsage, ToolChoice,
    };

    const CASES: &str = r#"
[
  {"turn_id":"data-final-1","execution_id":"exec-1","expected":["turn_started","step_started","step_completed","turn_completed"]},
  {"turn_id":"data-final-2","execution_id":"exec-2","expected":["turn_started","step_started","step_completed","turn_completed"]}
]
"#;

    #[derive(Clone)]
    struct FinalProvider;

    impl ModelProvider for FinalProvider {
        fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
            let response = ModelResponse {
                id: request.request_id.clone(),
                model: request.model,
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
                structured_output: None,
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
                metadata: Value::Null,
            };
            Box::pin(async move {
                Ok(Box::pin(stream::iter(vec![
                    Ok(ModelEvent::Started),
                    Ok(ModelEvent::Completed(response)),
                ])) as ModelEventStream)
            })
        }
    }

    fn request(turn_id: &str) -> TurnRequest {
        TurnRequest {
            turn_id: turn_id.into(),
            model_request: ModelRequest {
                request_id: format!("{turn_id}-request"),
                model: ModelRef::new("fixture", "final"),
                system: Vec::new(),
                messages: Vec::new(),
                tools: Vec::new(),
                tool_choice: ToolChoice::Auto,
                output_format: None,
                prompt_cache: None,
                reasoning: None,
                max_output_tokens: None,
                extensions: Value::Null,
            },
            config: Default::default(),
        }
    }

    #[tokio::test]
    async fn data_driven_turn_events_become_ledger_and_trace_records() {
        #[derive(serde::Deserialize)]
        struct Case {
            turn_id: String,
            execution_id: String,
            expected: Vec<String>,
        }
        let cases: Vec<Case> = serde_json::from_str(CASES).unwrap();
        for case in cases {
            let ledger = kolyan_ledger::InMemoryLedger::default();
            let trace = kolyan_trace::VecTraceSink::default();
            let driver = TurnDriver::new(ledger.clone(), trace.clone());
            let executor = TurnExecutor::new(FinalProvider);
            let (_, trajectory) = driver
                .execute(&executor, request(&case.turn_id), case.execution_id.clone())
                .await
                .unwrap();
            let kinds = trajectory
                .records
                .iter()
                .map(|record| format!("{:?}", record.kind).to_lowercase())
                .map(|kind| kind.replace('_', ""))
                .collect::<Vec<_>>();
            let expected = case
                .expected
                .iter()
                .map(|kind| kind.replace('_', ""))
                .collect::<Vec<_>>();
            assert_eq!(kinds, expected);
            assert_eq!(trace.records().len(), case.expected.len());
            assert_eq!(ledger.events_after(0).unwrap().len(), case.expected.len());
        }
    }

    #[tokio::test]
    async fn driver_keeps_toolless_execution_independent_of_storage_type() {
        let driver = TurnDriver::new(
            kolyan_ledger::InMemoryLedger::default(),
            kolyan_trace::VecTraceSink::default(),
        );
        let executor = TurnExecutor::new(FinalProvider);
        let (execution, trajectory) = driver
            .execute(&executor, request("storage-independent"), "execution-1")
            .await
            .unwrap();
        assert!(matches!(
            execution.result.outcome,
            kolyan_core::TurnOutcome::FinalAnswer { .. }
        ));
        assert_eq!(trajectory.records.len(), 4);
        let _: TurnExecutor<FinalProvider, NoopToolExecutor> = executor;
    }
}
