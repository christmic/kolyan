mod coordinator;
mod rpc_execution;
mod session_execution;
pub use rpc_execution::ExecutionRpc;

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

struct ExecutionGuard<L: LedgerStore + Clone> {
    server: ExecutionServer<L>,
    execution_id: String,
}

impl<L: LedgerStore + Clone> Drop for ExecutionGuard<L> {
    fn drop(&mut self) {
        self.server.release(&self.execution_id);
    }
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
        let _guard = ExecutionGuard {
            server: self.server.clone(),
            execution_id: execution_id.clone(),
        };
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
        let _guard = ExecutionGuard {
            server: self.server.clone(),
            execution_id: execution_id.clone(),
        };
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
    context_policy: kolyan_storage::SessionContextPolicy,
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

#[cfg(test)]
mod tests;
