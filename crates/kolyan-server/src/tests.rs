use super::*;
use futures_util::stream;
use kolyan_ledger::InMemoryLedger;
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRef, ModelRequest,
    ModelResponse, ProviderFuture, StopReason, TokenUsage, ToolChoice,
};
use kolyan_trace::VecTraceSink;

#[derive(Clone)]
struct FinalProvider;

impl ModelProvider for FinalProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let response = ModelResponse {
            id: request.request_id.clone(),
            model: request.model,
            content: vec![ContentBlock::Text {
                text: "service complete".into(),
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
            model: ModelRef::new("fixture", "server-service"),
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

fn execution(id: &str) -> ExecutionRef {
    ExecutionRef {
        session_id: "session-1".into(),
        turn_id: format!("turn-{id}"),
        execution_id: id.into(),
    }
}

#[test]
fn coordinator_prevents_duplicate_local_admission_and_releases_after_cancel() {
    let coordinator = ExecutionCoordinator::new(InMemoryLedger::default());
    let execution = execution("execution-1");
    assert_eq!(
        coordinator.state("execution-1").unwrap(),
        ExecutionState::New
    );
    assert_eq!(
        coordinator.start(execution.clone()).unwrap().kind,
        AdmissionKind::Start
    );
    assert!(matches!(
        coordinator.start(execution.clone()),
        Err(CoordinatorError::AlreadyActive { .. })
    ));
    coordinator.cancel(&execution).unwrap();
    assert_eq!(
        coordinator.state("execution-1").unwrap(),
        ExecutionState::Cancelled
    );
    coordinator.release("execution-1");
}

#[test]
fn recovery_is_explicit_and_uses_ledger_state() {
    let ledger = InMemoryLedger::default();
    let first = ExecutionCoordinator::new(ledger.clone());
    let execution = execution("execution-2");
    first.start(execution.clone()).unwrap();
    first.release("execution-2");

    let restarted = ExecutionCoordinator::new(ledger);
    assert!(matches!(
        restarted.recover(execution).unwrap().kind,
        AdmissionKind::Recover
    ));
}

#[tokio::test]
async fn execution_service_owns_runtime_admission_and_release() {
    let ledger = InMemoryLedger::default();
    let service = ExecutionService::new(ledger.clone(), VecTraceSink::default());
    let result = service
        .start(
            TurnExecutor::new(FinalProvider),
            request("service-turn"),
            "service-session",
            "service-execution",
        )
        .await
        .unwrap();
    assert!(matches!(result, DurableTurnResult::Completed(_, _)));
    assert_eq!(
        service.state("service-execution").unwrap(),
        ExecutionState::Completed
    );
    assert!(matches!(
        service.server().start(ExecutionRef {
            session_id: "service-session".into(),
            turn_id: "service-turn".into(),
            execution_id: "service-execution".into(),
        }),
        Err(CoordinatorError::Terminal { .. })
    ));
    let events = ledger.events_after(0).unwrap();
    assert!(events.len() >= 7);
    assert!(
        events
            .iter()
            .any(|event| event.kind == LedgerEventKind::TurnCompleted)
    );
}

#[test]
fn json_rpc_control_plane_routes_start_status_and_cancel() {
    let server = ExecutionServer::new(InMemoryLedger::default());
    let params = json!({
        "session_id": "rpc-session",
        "turn_id": "rpc-turn",
        "execution_id": "rpc-execution"
    });
    let start = server.handle_json_rpc(
        &json!({
            "jsonrpc":"2.0", "id":1, "method":"execution.start", "params":params
        })
        .to_string(),
    );
    let start: RpcResponse = serde_json::from_str(&start).unwrap();
    assert_eq!(start.error, None);
    assert_eq!(start.result.unwrap()["admission"], "Start");

    let status = server.handle_json_rpc(
        &json!({
            "jsonrpc":"2.0", "id":2, "method":"execution.status", "params":params
        })
        .to_string(),
    );
    let status: RpcResponse = serde_json::from_str(&status).unwrap();
    assert_eq!(status.result.unwrap()["state"], "Running");

    let cancel = server.handle_json_rpc(
        &json!({
            "jsonrpc":"2.0", "id":3, "method":"execution.cancel", "params":params
        })
        .to_string(),
    );
    let cancel: RpcResponse = serde_json::from_str(&cancel).unwrap();
    assert_eq!(cancel.result.unwrap()["state"], "cancelled");
    assert_eq!(
        server.state("rpc-execution").unwrap(),
        ExecutionState::Cancelled
    );
    assert!(server.handle_json_rpc("not-json").contains("-32700"));
}
