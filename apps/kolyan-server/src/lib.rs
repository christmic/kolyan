use kolyan_core::{ToolExecutor, TurnExecutor, TurnRequest};
use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{Message, ModelProvider};
use kolyan_runtime::{DurableTurnDriver, DurableTurnResult, RuntimeError};
use kolyan_storage::{SessionRecord, SessionStore, SessionTurn, StorageError};
use kolyan_trace::TraceSink;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Identity routed by Server and consumed by the Runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRef {
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionState {
    New,
    Running,
    Suspended,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionKind {
    Start,
    Resume,
    Recover,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionAdmission {
    pub execution: ExecutionRef,
    pub kind: AdmissionKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("execution {execution_id} is already active in this server")]
    AlreadyActive { execution_id: String },
    #[error("execution {execution_id} is not suspended")]
    NotSuspended { execution_id: String },
    #[error("execution {execution_id} is not recoverable from state {state:?}")]
    NotRecoverable {
        execution_id: String,
        state: ExecutionState,
    },
    #[error("execution {execution_id} is terminal in state {state:?}")]
    Terminal {
        execution_id: String,
        state: ExecutionState,
    },
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("coordinator failed: {0}")]
    Coordinator(#[from] CoordinatorError),
    #[error("runtime failed: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("session failed: {0}")]
    Session(#[from] StorageError),
}

/// Single-process execution orchestration owned by Server.
///
/// It deliberately has no lease API. The active set prevents duplicate local
/// work while the Ledger remains authoritative across a process restart.
#[derive(Clone)]
pub struct ExecutionCoordinator<L> {
    ledger: L,
    active: Arc<Mutex<HashSet<String>>>,
}

/// Thin Server facade. Transport adapters should depend on this type instead
/// of reaching into the Coordinator directly.
#[derive(Clone)]
pub struct ExecutionServer<L> {
    coordinator: ExecutionCoordinator<L>,
}

/// Server-owned adapter that performs the complete admission → Runtime →
/// release lifecycle. Transport handlers should call this service instead of
/// assembling a Coordinator and DurableTurnDriver themselves.
#[derive(Clone)]
pub struct ExecutionService<L, S> {
    server: ExecutionServer<L>,
    trace: S,
}

/// Server facade for the persistent multi-Turn Session boundary.
#[derive(Clone)]
pub struct SessionService<S> {
    store: S,
}

impl<S> SessionService<S>
where
    S: SessionStore + Clone,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn create(&self, session_id: &str) -> Result<SessionRecord, StorageError> {
        self.store.create(session_id)
    }

    pub fn load(&self, session_id: &str) -> Result<SessionRecord, StorageError> {
        self.store.load(session_id)
    }

    pub fn append_turn(
        &self,
        session_id: &str,
        turn: SessionTurn,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        self.store.append_turn(session_id, turn, messages)
    }
}

impl<L, S> ExecutionService<L, S>
where
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
{
    pub fn new(ledger: L, trace: S) -> Self {
        Self {
            server: ExecutionServer::new(ledger),
            trace,
        }
    }

    pub fn server(&self) -> &ExecutionServer<L> {
        &self.server
    }

    pub async fn start<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        request: TurnRequest,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
    ) -> Result<DurableTurnResult, ServerError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let execution = ExecutionRef {
            session_id: session_id.clone(),
            turn_id: request.turn_id.clone(),
            execution_id: execution_id.clone(),
        };
        self.server.start(execution)?;
        let driver = DurableTurnDriver::new(
            self.server.coordinator().ledger().clone(),
            self.trace.clone(),
        );
        let result = driver
            .start(executor, request, session_id, execution_id.clone())
            .await;
        self.server.release(&execution_id);
        Ok(result?)
    }

    pub async fn resume<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        approval_id: &str,
    ) -> Result<DurableTurnResult, ServerError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let driver = DurableTurnDriver::new(
            self.server.coordinator().ledger().clone(),
            self.trace.clone(),
        );
        let approval = driver.load_approval(&execution_id, approval_id)?;
        let execution = ExecutionRef {
            session_id: session_id.clone(),
            turn_id: approval.turn_id.clone(),
            execution_id: execution_id.clone(),
        };
        self.server.resume(execution)?;
        let result = driver
            .resume(executor, session_id, execution_id.clone(), approval_id)
            .await;
        self.server.release(&execution_id);
        Ok(result?)
    }

    pub fn cancel(&self, execution: &ExecutionRef) -> Result<(), ServerError> {
        self.server.cancel(execution)?;
        Ok(())
    }

    pub fn state(&self, execution_id: &str) -> Result<ExecutionState, ServerError> {
        Ok(self.server.state(execution_id)?)
    }
}

impl<L> ExecutionServer<L>
where
    L: LedgerStore + Clone,
{
    pub fn new(ledger: L) -> Self {
        Self {
            coordinator: ExecutionCoordinator::new(ledger),
        }
    }

    pub fn coordinator(&self) -> &ExecutionCoordinator<L> {
        &self.coordinator
    }

    pub fn start(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        self.coordinator.start(execution)
    }

    pub fn resume(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        self.coordinator.resume(execution)
    }

    pub fn recover(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        self.coordinator.recover(execution)
    }

    pub fn cancel(&self, execution: &ExecutionRef) -> Result<(), CoordinatorError> {
        self.coordinator.cancel(execution)
    }

    pub fn state(&self, execution_id: &str) -> Result<ExecutionState, CoordinatorError> {
        self.coordinator.state(execution_id)
    }

    pub fn release(&self, execution_id: &str) {
        self.coordinator.release(execution_id);
    }

    pub fn handle_json_rpc(&self, input: &str) -> String {
        let response = match serde_json::from_str::<RpcRequest>(input) {
            Ok(request) => self.handle_rpc(request),
            Err(error) => RpcResponse {
                jsonrpc: "2.0".into(),
                id: Value::Null,
                result: None,
                error: Some(RpcError {
                    code: -32700,
                    message: format!("parse error: {error}"),
                }),
            },
        };
        serde_json::to_string(&response).expect("RPC response must serialize")
    }

    pub fn handle_rpc(&self, request: RpcRequest) -> RpcResponse {
        if request.jsonrpc != "2.0" {
            return rpc_error(request.id, -32600, "jsonrpc must be 2.0");
        }
        let id = request.id.clone();
        let result = match request.method.as_str() {
            "execution.status" => self
                .rpc_execution_ref(&request.params)
                .and_then(|execution| {
                    self.state(&execution.execution_id)
                        .map(|state| json!({"state": state}))
                }),
            "execution.start" => self
                .rpc_execution_ref(&request.params)
                .and_then(|execution| {
                    self.start(execution)
                        .map(|admission| json!({"admission": admission.kind}))
                }),
            "execution.resume" => self
                .rpc_execution_ref(&request.params)
                .and_then(|execution| {
                    self.resume(execution)
                        .map(|admission| json!({"admission": admission.kind}))
                }),
            "execution.cancel" => self
                .rpc_execution_ref(&request.params)
                .and_then(|execution| {
                    self.cancel(&execution)
                        .map(|()| json!({"state": "cancelled"}))
                }),
            _ => Err(CoordinatorError::NotRecoverable {
                execution_id: request.method,
                state: ExecutionState::New,
            }),
        };
        match result {
            Ok(result) => RpcResponse {
                jsonrpc: "2.0".into(),
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => rpc_error(id, -32000, error.to_string()),
        }
    }

    fn rpc_execution_ref(&self, params: &Value) -> Result<ExecutionRef, CoordinatorError> {
        serde_json::from_value(params.clone()).map_err(|error| CoordinatorError::NotRecoverable {
            execution_id: format!("invalid params: {error}"),
            state: ExecutionState::New,
        })
    }
}

fn rpc_error(id: Value, code: i32, message: impl Into<String>) -> RpcResponse {
    RpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: None,
        error: Some(RpcError {
            code,
            message: message.into(),
        }),
    }
}

impl<L> ExecutionCoordinator<L>
where
    L: LedgerStore + Clone,
{
    pub fn new(ledger: L) -> Self {
        Self {
            ledger,
            active: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn ledger(&self) -> &L {
        &self.ledger
    }

    pub fn state(&self, execution_id: &str) -> Result<ExecutionState, CoordinatorError> {
        let mut state = ExecutionState::New;
        for event in self
            .ledger
            .events_after(0)?
            .into_iter()
            .filter(|event| event.execution_id == execution_id)
        {
            state = match event.kind {
                LedgerEventKind::ExecutionStarted => ExecutionState::Running,
                LedgerEventKind::ExecutionSuspended => ExecutionState::Suspended,
                LedgerEventKind::ExecutionCancelled | LedgerEventKind::TurnCancelled => {
                    ExecutionState::Cancelled
                }
                LedgerEventKind::TurnCompleted => ExecutionState::Completed,
                LedgerEventKind::TurnFailed | LedgerEventKind::EffectUncertain => {
                    ExecutionState::Failed
                }
                _ => state,
            };
        }
        Ok(state)
    }

    pub fn start(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        let state = self.state(&execution.execution_id)?;
        match state {
            ExecutionState::New => {
                self.append_once(
                    &execution,
                    "execution-started",
                    LedgerEventKind::ExecutionStarted,
                    serde_json::to_value(&execution).unwrap_or(Value::Null),
                )?;
                self.admit(execution, AdmissionKind::Start)
            }
            ExecutionState::Running if self.is_active(&execution.execution_id) => {
                Err(CoordinatorError::AlreadyActive {
                    execution_id: execution.execution_id,
                })
            }
            ExecutionState::Running => Err(CoordinatorError::NotRecoverable {
                execution_id: execution.execution_id,
                state,
            }),
            _ if is_terminal(state) => Err(CoordinatorError::Terminal {
                execution_id: execution.execution_id,
                state,
            }),
            _ => Err(CoordinatorError::NotRecoverable {
                execution_id: execution.execution_id,
                state,
            }),
        }
    }

    pub fn resume(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        let state = self.state(&execution.execution_id)?;
        if state != ExecutionState::Suspended {
            return if is_terminal(state) {
                Err(CoordinatorError::Terminal {
                    execution_id: execution.execution_id,
                    state,
                })
            } else {
                Err(CoordinatorError::NotSuspended {
                    execution_id: execution.execution_id,
                })
            };
        }
        self.append_once(
            &execution,
            "execution-resumed",
            LedgerEventKind::ExecutionStarted,
            Value::Null,
        )?;
        self.admit(execution, AdmissionKind::Resume)
    }

    pub fn recover(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        let state = self.state(&execution.execution_id)?;
        if !matches!(state, ExecutionState::Running | ExecutionState::Suspended) {
            return Err(CoordinatorError::NotRecoverable {
                execution_id: execution.execution_id,
                state,
            });
        }
        self.append_once(
            &execution,
            "execution-recovered",
            LedgerEventKind::ExecutionStarted,
            Value::Null,
        )?;
        self.admit(execution, AdmissionKind::Recover)
    }

    pub fn cancel(&self, execution: &ExecutionRef) -> Result<(), CoordinatorError> {
        let state = self.state(&execution.execution_id)?;
        if is_terminal(state) {
            return Ok(());
        }
        self.append_once(
            execution,
            "execution-cancelled",
            LedgerEventKind::ExecutionCancelled,
            Value::Null,
        )?;
        self.release(&execution.execution_id);
        Ok(())
    }

    pub fn release(&self, execution_id: &str) {
        self.active
            .lock()
            .expect("coordinator lock must not be poisoned")
            .remove(execution_id);
    }

    fn admit(
        &self,
        execution: ExecutionRef,
        kind: AdmissionKind,
    ) -> Result<ExecutionAdmission, CoordinatorError> {
        let mut active = self
            .active
            .lock()
            .expect("coordinator lock must not be poisoned");
        if !active.insert(execution.execution_id.clone()) {
            return Err(CoordinatorError::AlreadyActive {
                execution_id: execution.execution_id,
            });
        }
        drop(active);
        Ok(ExecutionAdmission { execution, kind })
    }

    fn is_active(&self, execution_id: &str) -> bool {
        self.active
            .lock()
            .expect("coordinator lock must not be poisoned")
            .contains(execution_id)
    }

    fn append_once(
        &self,
        execution: &ExecutionRef,
        suffix: &str,
        kind: LedgerEventKind,
        payload: Value,
    ) -> Result<(), CoordinatorError> {
        let event_id = format!("{}/{}", execution.execution_id, suffix);
        if self
            .ledger
            .events_after(0)?
            .iter()
            .any(|event| event.event_id == event_id)
        {
            return Ok(());
        }
        self.ledger.append(LedgerEvent {
            event_id: event_id.clone(),
            turn_id: execution.turn_id.clone(),
            execution_id: execution.execution_id.clone(),
            cursor: 0,
            kind,
            idempotency_key: event_id,
            payload,
        })?;
        Ok(())
    }
}

fn is_terminal(state: ExecutionState) -> bool {
    matches!(
        state,
        ExecutionState::Completed | ExecutionState::Cancelled | ExecutionState::Failed
    )
}

#[cfg(test)]
mod tests {
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
}
