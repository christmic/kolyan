mod coordinator;
mod instance_registry;
mod preparation;
mod private_context;
mod rpc_execution;
mod session_execution;
mod suspension;
mod task_driver;
mod tasks;
pub use instance_registry::{
    InstanceOwner, InstanceRegistry, InstanceRegistryError, InstanceReservation,
};
pub use preparation::PreparationFailure;
pub use private_context::{
    PrivateContextOwner, PrivateContextOwnershipVerifier, PrivateContextService,
    VerifiedPrivateContextInitialization, private_context_initialization,
};
pub use rpc_execution::ExecutionRpc;
pub use suspension::{current_suspension as execution_suspension, suspension_view};
pub use task_driver::{
    HistoricalContextRequest, TaskExecutionError, TaskExecutionService, VerifiedConsumedResult,
    VerifiedHistoricalContext, VerifiedTaskOutcome, VerifiedTaskResult,
};
pub use tasks::*;

#[cfg(test)]
#[path = "tests/input_source.rs"]
mod input_fixture;

use kolyan_core::{
    ResumeInput, ToolExecutor, TurnExecution, TurnExecutor, TurnOutcome, TurnRequest,
    TurnSuspension,
};
use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{Message, MessageRole, ModelProvider, ModelRequest};
use kolyan_runtime::{DurableTurnDriver, DurableTurnResult, RuntimeError};
use kolyan_storage::{SessionRecord, SessionStore, SessionTurn, SessionTurnStatus, StorageError};
use kolyan_trace::TraceSink;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
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
    #[error("Turn preparation failed: {0}")]
    Preparation(#[from] PreparationFailure),
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
    external_wait_verifier: Arc<dyn kolyan_runtime::ExternalWaitVerifier>,
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
            external_wait_verifier: Arc::new(kolyan_runtime::RefuseExternalWaits),
        }
    }

    pub fn server(&self) -> &ExecutionServer<L> {
        &self.server
    }

    /// Host-only verification port. Installing it confers no new tool grant;
    /// Runtime still validates original authority and bounded results. Until
    /// exact admission proofs are implemented, the default refuses all waits.
    pub fn with_external_wait_verifier(
        mut self,
        verifier: Arc<dyn kolyan_runtime::ExternalWaitVerifier>,
    ) -> Self {
        self.external_wait_verifier = verifier;
        self
    }

    fn driver(&self) -> DurableTurnDriver<L, S> {
        DurableTurnDriver::new(
            self.server.coordinator().ledger().clone(),
            self.trace.clone(),
        )
        .with_external_wait_verifier(self.external_wait_verifier.clone())
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
        let driver = self.driver();
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
        checkpoint_id: &str,
        input: ResumeInput,
    ) -> Result<DurableTurnResult, ServerError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let driver = self.driver();
        let suspension = driver.load_suspension(&execution_id, checkpoint_id)?;
        let execution = ExecutionRef {
            session_id: session_id.clone(),
            turn_id: suspension.checkpoint.scope.execution.turn_id.clone(),
            execution_id: execution_id.clone(),
        };
        suspension::verify_resume_input(
            self.server.coordinator().ledger(),
            &execution,
            &suspension,
            &input,
        )?;
        if self.server.state(&execution_id)? != ExecutionState::Suspended {
            return Err(CoordinatorError::NotSuspended {
                execution_id: execution_id.clone(),
            }
            .into());
        }
        self.server
            .coordinator()
            .admit(execution, AdmissionKind::Resume)?;
        let _guard = ExecutionGuard {
            server: self.server.clone(),
            execution_id: execution_id.clone(),
        };
        let result = driver
            .resume(
                executor,
                session_id,
                execution_id.clone(),
                checkpoint_id,
                input,
            )
            .await;
        self.server.release(&execution_id);
        Ok(result?)
    }

    /// Confirm only an exact pending approval. The persisted decision contains
    /// every authority coordinate; the Runtime revalidates it before merging.
    pub async fn resume_approval<P, T>(
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
        let suspension = self.load_current_suspension(&execution_id)?;
        let key = ExecutionRef {
            session_id,
            execution_id,
            turn_id: suspension.checkpoint.scope.execution.turn_id.clone(),
        };
        if self.server.state(&key.execution_id)? != ExecutionState::Suspended {
            return Err(CoordinatorError::NotSuspended {
                execution_id: key.execution_id.clone(),
            }
            .into());
        }
        self.server
            .coordinator()
            .admit(key.clone(), AdmissionKind::Resume)?;
        let _guard = ExecutionGuard {
            server: self.server.clone(),
            execution_id: key.execution_id.clone(),
        };
        let confirmation = suspension::confirm_approval(
            self.server.coordinator().ledger(),
            &key,
            &suspension,
            approval_id,
        )?;
        let input = ResumeInput::ApprovalConfirmed(confirmation);
        suspension::verify_resume_input(
            self.server.coordinator().ledger(),
            &key,
            &suspension,
            &input,
        )?;
        let driver = self.driver();
        if suspension
            .checkpoint
            .approvals
            .iter()
            .any(|approval| approval.approval_id == approval_id && approval.evidence_id.is_some())
        {
            return Ok(driver
                .resume_committed(
                    executor,
                    &key.session_id,
                    &key.execution_id,
                    &suspension.checkpoint.checkpoint_id,
                )
                .await?);
        }
        Ok(driver
            .resume(
                executor,
                &key.session_id,
                &key.execution_id,
                &suspension.checkpoint.checkpoint_id,
                input,
            )
            .await?)
    }

    /// Recover the drive gap after a durable preparation or pure merge, without reconsuming
    /// an approval or external result. This is host orchestration, not an HTTP
    /// approval shortcut. A genuine content-bound publication is mandatory.
    pub async fn resume_committed<P: ModelProvider, T: ToolExecutor>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        checkpoint_id: &str,
    ) -> Result<DurableTurnResult, ServerError> {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let events = self
            .server
            .coordinator()
            .ledger()
            .execution_events_after(&execution_id, 0)
            .map_err(CoordinatorError::from)?;
        if events
            .iter()
            .rev()
            .find(|event| {
                matches!(
                    event.kind,
                    LedgerEventKind::ExecutionSuspended
                        | LedgerEventKind::TurnCheckpointMerged
                        | LedgerEventKind::TurnCheckpointPrepared
                )
            })
            .is_none_or(|event| {
                !matches!(
                    event.kind,
                    LedgerEventKind::TurnCheckpointMerged | LedgerEventKind::TurnCheckpointPrepared
                )
            })
        {
            return Err(StorageError::Conflict(
                "no committed preparation or merge to recover".into(),
            )
            .into());
        }
        let saved = self.load_suspension(&execution_id, checkpoint_id)?;
        let key = ExecutionRef {
            session_id,
            execution_id,
            turn_id: saved.checkpoint.scope.execution.turn_id.clone(),
        };
        if saved.checkpoint.scope.execution.session_id != key.session_id {
            return Err(StorageError::Conflict("foreign committed resume Session".into()).into());
        }
        let state = self.server.state(&key.execution_id)?;
        if !matches!(state, ExecutionState::Running | ExecutionState::Suspended) {
            return Err(CoordinatorError::NotRecoverable {
                execution_id: key.execution_id.clone(),
                state,
            }
            .into());
        }
        self.server
            .coordinator()
            .admit(key.clone(), AdmissionKind::Recover)?;
        let _guard = ExecutionGuard {
            server: self.server.clone(),
            execution_id: key.execution_id.clone(),
        };
        Ok(self
            .driver()
            .resume_committed(executor, &key.session_id, &key.execution_id, checkpoint_id)
            .await?)
    }

    pub fn load_suspension(
        &self,
        execution_id: &str,
        checkpoint_id: &str,
    ) -> Result<TurnSuspension, ServerError> {
        let driver = self.driver();
        Ok(driver.load_suspension(execution_id, checkpoint_id)?)
    }

    pub fn load_current_suspension(
        &self,
        execution_id: &str,
    ) -> Result<TurnSuspension, ServerError> {
        Ok(self.driver().load_current_suspension(execution_id)?)
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
    preparation_hook: Option<Arc<dyn TurnPreparationHook>>,
}

/// Trusted host preparation, not a client-selected request replacement port.
/// Called only for new Turns, after history assembly with its exact base version.
/// The host must persist full source and selection evidence before returning.
/// Server independently permits only ordered history omission with the current
/// input tail intact. Resume/recovery never reselect. Dropping the future does
/// not prove remote counting or an already-started artifact write was rolled back.
pub trait TurnPreparationHook: Send + Sync {
    fn prepare<'a>(
        &'a self,
        execution: &'a ExecutionRef,
        session_version: u64,
        request: &'a TurnRequest,
    ) -> TurnPreparationFuture<'a>;
}

pub type TurnPreparationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ModelRequest, ServerError>> + Send + 'a>>;

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
