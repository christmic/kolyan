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

struct BrokenTrace;
impl TraceSink for BrokenTrace {
    fn record(&self, _: TraceRecord) -> Result<(), kolyan_trace::TraceError> {
        Err(kolyan_trace::TraceError {
            message: "observability unavailable".into(),
        })
    }
}

#[tokio::test]
async fn trace_failure_does_not_turn_completed_execution_into_retryable_failure() {
    let ledger = kolyan_ledger::InMemoryLedger::default();
    let driver = DurableTurnDriver::new(ledger.clone(), BrokenTrace);
    let result = driver
        .start(
            TurnExecutor::new(FinalProvider),
            request("trace-failure"),
            "session",
            "exec",
        )
        .await
        .unwrap();
    let DurableTurnResult::Completed(_, trajectory) = result else {
        panic!("must remain completed");
    };
    assert!(!trajectory.trace_errors.is_empty());
    assert!(
        ledger
            .events_after(0)
            .unwrap()
            .iter()
            .any(|event| event.kind == LedgerEventKind::TurnCompleted)
    );
    assert!(
        !ledger
            .events_after(0)
            .unwrap()
            .iter()
            .any(|event| event.kind == LedgerEventKind::TurnFailed)
    );
}

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
