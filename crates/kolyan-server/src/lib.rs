use kolyan_core::{
    ApprovalRequest, ToolExecutor, TurnExecution, TurnExecutor, TurnOutcome, TurnRequest,
};
use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{Message, MessageRole, ModelProvider};
use kolyan_runtime::{DurableTurnDriver, DurableTurnResult, RuntimeError};
use kolyan_storage::{SessionRecord, SessionStore, SessionTurn, SessionTurnStatus, StorageError};
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

    pub fn begin_turn(
        &self,
        session_id: &str,
        turn: SessionTurn,
    ) -> Result<SessionRecord, StorageError> {
        self.store.begin_turn(session_id, turn)
    }

    pub fn update_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        status: SessionTurnStatus,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        self.store
            .update_turn(session_id, turn_id, status, messages)
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

    pub fn load_approval(
        &self,
        execution_id: &str,
        approval_id: &str,
    ) -> Result<ApprovalRequest, ServerError> {
        let driver = DurableTurnDriver::new(
            self.server.coordinator().ledger().clone(),
            self.trace.clone(),
        );
        Ok(driver.load_approval(execution_id, approval_id)?)
    }

    pub fn cancel(&self, execution: &ExecutionRef) -> Result<(), ServerError> {
        self.server.cancel(execution)?;
        Ok(())
    }

    pub fn state(&self, execution_id: &str) -> Result<ExecutionState, ServerError> {
        Ok(self.server.state(execution_id)?)
    }
}

/// Server-owned composition of persistent Session context and one-turn
/// execution. Session data is committed around the Runtime call; Runtime and
/// Turn remain unaware of SessionStore.
#[derive(Clone)]
pub struct SessionExecutionService<L, S, SS> {
    execution: ExecutionService<L, S>,
    sessions: SessionService<SS>,
}

impl<L, S, SS> SessionExecutionService<L, S, SS>
where
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
{
    pub fn new(execution: ExecutionService<L, S>, sessions: SessionService<SS>) -> Self {
        Self {
            execution,
            sessions,
        }
    }

    pub fn execution(&self) -> &ExecutionService<L, S> {
        &self.execution
    }

    pub fn sessions(&self) -> &SessionService<SS> {
        &self.sessions
    }

    pub async fn start<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        mut request: TurnRequest,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
    ) -> Result<DurableTurnResult, ServerError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let turn_id = request.turn_id.clone();
        let current_messages = request.model_request.messages.clone();
        let session = self.sessions.load(&session_id)?;
        let mut contextual_messages = session.messages;
        contextual_messages.extend(current_messages.clone());
        request.model_request.messages = contextual_messages;
        self.sessions.begin_turn(
            &session_id,
            SessionTurn {
                turn_id: turn_id.clone(),
                execution_id: execution_id.clone(),
                status: SessionTurnStatus::Running,
            },
        )?;

        let result = self
            .execution
            .start(executor, request, session_id.clone(), execution_id)
            .await;
        self.commit_result(&session_id, &turn_id, current_messages, result)
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
        let approval = self.execution.load_approval(&execution_id, approval_id)?;
        let session = self.sessions.load(&session_id)?;
        let prefix_len = session.messages.len();
        let pending_messages = approval
            .continuation
            .model_request
            .messages
            .get(prefix_len..)
            .unwrap_or_default()
            .to_vec();
        let result = self
            .execution
            .resume(executor, session_id.clone(), execution_id, approval_id)
            .await;
        self.commit_result(&session_id, &approval.turn_id, pending_messages, result)
    }

    pub fn cancel(&self, execution: &ExecutionRef) -> Result<(), ServerError> {
        self.execution.cancel(execution)?;
        self.sessions.update_turn(
            &execution.session_id,
            &execution.turn_id,
            SessionTurnStatus::Cancelled,
            Vec::new(),
        )?;
        Ok(())
    }

    pub fn state(&self, execution_id: &str) -> Result<ExecutionState, ServerError> {
        self.execution.state(execution_id)
    }

    fn commit_result(
        &self,
        session_id: &str,
        turn_id: &str,
        input_messages: Vec<Message>,
        result: Result<DurableTurnResult, ServerError>,
    ) -> Result<DurableTurnResult, ServerError> {
        match result {
            Ok(DurableTurnResult::AwaitingApproval {
                approval,
                trajectory,
            }) => {
                self.sessions.update_turn(
                    session_id,
                    turn_id,
                    SessionTurnStatus::Suspended,
                    Vec::new(),
                )?;
                Ok(DurableTurnResult::AwaitingApproval {
                    approval,
                    trajectory,
                })
            }
            Ok(DurableTurnResult::Completed(execution, trajectory)) => {
                let messages = completed_messages(&execution, input_messages);
                self.sessions.update_turn(
                    session_id,
                    turn_id,
                    SessionTurnStatus::Completed,
                    messages,
                )?;
                Ok(DurableTurnResult::Completed(execution, trajectory))
            }
            Err(error) => {
                self.sessions.update_turn(
                    session_id,
                    turn_id,
                    SessionTurnStatus::Failed,
                    Vec::new(),
                )?;
                Err(error)
            }
        }
    }
}

fn completed_messages(execution: &TurnExecution, input: Vec<Message>) -> Vec<Message> {
    let mut messages = input;
    let response = match &execution.result.outcome {
        TurnOutcome::FinalAnswer { response }
        | TurnOutcome::Refused { response }
        | TurnOutcome::Incomplete { response } => Some(response),
        TurnOutcome::Rejected { .. } | TurnOutcome::Expired { .. } | TurnOutcome::MaxSteps => None,
    };
    if let Some(response) = response {
        messages.push(Message {
            role: MessageRole::Assistant,
            content: response.content.clone(),
        });
    }
    messages
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
            if is_terminal(state) {
                break;
            }
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
            &format!(
                "execution-resumed/{}",
                self.latest_cursor(&execution.execution_id)?
            ),
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
            &format!(
                "execution-recovered/{}",
                self.latest_cursor(&execution.execution_id)?
            ),
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
        if let Some(existing) = self
            .ledger
            .events_after(0)?
            .into_iter()
            .find(|event| event.event_id == event_id)
        {
            if existing.turn_id != execution.turn_id
                || existing.kind != kind
                || existing.payload != payload
            {
                return Err(LedgerError::Conflict(event_id).into());
            }
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

    fn latest_cursor(&self, execution_id: &str) -> Result<u64, CoordinatorError> {
        Ok(self
            .ledger
            .events_after(0)?
            .iter()
            .rev()
            .find(|event| event.execution_id == execution_id)
            .map_or(0, |event| event.cursor))
    }
}

fn is_terminal(state: ExecutionState) -> bool {
    matches!(
        state,
        ExecutionState::Completed | ExecutionState::Cancelled | ExecutionState::Failed
    )
}

#[cfg(test)]
mod tests;
