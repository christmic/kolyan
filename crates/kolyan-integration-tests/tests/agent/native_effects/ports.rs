//! Test-owned receipt faults and admission handshakes; real adapters own effects.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use kolyan_agent::{AgentSnapshot, EnvironmentToolFactory, RunnerError, RunnerToolSet};
use kolyan_core::{ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture};
use kolyan_ledger::{
    LedgerError, LedgerEvent, LedgerEventKind, LedgerQuery, LedgerStore, SqliteLedger,
};
use kolyan_model::ToolCall;
use kolyan_policy::ToolExecutionScope;
use kolyan_runtime::DurableTurnDriver;
use kolyan_server::ExecutionRef;
use kolyan_trace::NoopTraceSink;
use serde_json::json;
use tokio::sync::Notify;

use super::super::{evidence::Evidence, tools::Tools};

#[derive(Clone)]
pub(super) struct FaultLedger {
    pub inner: SqliteLedger,
    pub state: Arc<State>,
}

pub(super) struct State {
    pub fault: String,
    pub expected_write: kolyan_tools::WriteArguments,
    pub evidence: Arc<Evidence>,
    pub gate_timeout_ms: u64,
    pub entered: Notify,
    pub release: Notify,
    pub executions: AtomicUsize,
    pub completed: AtomicUsize,
    selection: Mutex<Option<Selection>>,
    faults: AtomicUsize,
    refused: AtomicUsize,
    admitted_execution: Mutex<Option<ExecutionRef>>,
}

struct Selection {
    call: ToolCall,
    receipt_id: String,
    scope: ToolExecutionScope,
    digest: String,
    grant: kolyan_policy::PreparedGrant,
}

impl State {
    pub fn new(
        fault: String,
        expected_write: kolyan_tools::WriteArguments,
        evidence: Arc<Evidence>,
        gate_timeout_ms: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            fault,
            expected_write,
            evidence,
            gate_timeout_ms,
            entered: Notify::new(),
            release: Notify::new(),
            executions: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            selection: Mutex::new(None),
            faults: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
            admitted_execution: Mutex::new(None),
        })
    }
    pub fn effects(&self) -> usize {
        self.executions.load(Ordering::SeqCst)
    }
    pub fn completions(&self) -> usize {
        self.completed.load(Ordering::SeqCst)
    }
    pub fn injected(&self) -> usize {
        self.faults.load(Ordering::SeqCst)
    }
    pub fn refusals(&self) -> usize {
        self.refused.load(Ordering::SeqCst)
    }
    pub fn selected_scope(&self) -> Option<ToolExecutionScope> {
        self.selection
            .lock()
            .unwrap()
            .as_ref()
            .map(|selection| selection.scope.clone())
    }
    pub fn admit(&self, execution: ExecutionRef) -> Result<(), String> {
        let mut admitted = self
            .admitted_execution
            .lock()
            .map_err(|error| error.to_string())?;
        if admitted.is_some() {
            return Err("native fault target already admitted".into());
        }
        *admitted = Some(execution);
        Ok(())
    }
    pub fn selected_call(&self) -> Option<ToolCall> {
        self.selection
            .lock()
            .unwrap()
            .as_ref()
            .map(|selection| selection.call.clone())
    }
}

impl LedgerStore for FaultLedger {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        let selected = self
            .state
            .selection
            .lock()
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        let matches = selected.as_ref().is_some_and(|selected| {
            event.kind == LedgerEventKind::EffectReceipt
                && event.event_id == selected.receipt_id
                && event.idempotency_key == selected.receipt_id
                && event.execution_id == selected.scope.execution.execution_id
                && event.turn_id == selected.scope.execution.turn_id
                && event.payload["input"]["scope"] == json!(selected.scope)
                && event.payload["input"]["prepared"]["digest"] == selected.digest
                && event.payload["input"]["prepared"]["call"] == json!(selected.call)
                && event.payload["prepared_grant"] == json!(selected.grant)
        });
        drop(selected);
        if matches && self.state.fault == "lose_receipt" {
            self.state.faults.fetch_add(1, Ordering::SeqCst);
            self.state.evidence.append(json!({"event":"receipt_publication_refused","candidate":event,"meaning":"actual native result exists, no receipt delegated"})).map_err(LedgerError::Storage)?;
            return Err(LedgerError::Storage(
                "dataset-selected receipt publication failure".into(),
            ));
        }
        let stored = self.inner.append(event)?;
        if matches && self.state.fault == "cancel_after_receipt" {
            self.state.faults.fetch_add(1, Ordering::SeqCst);
            self.state
                .evidence
                .append(json!({"event":"receipt_committed_before_cancel","value":stored}))
                .map_err(LedgerError::Storage)?;
            DurableTurnDriver::new(self.inner.clone(), NoopTraceSink)
                .cancel(&stored.execution_id, &stored.turn_id)
                .map_err(|error| LedgerError::Storage(error.to_string()))?;
            self.state
                .evidence
                .append(
                    json!({"event":"durable_cancel_intent","meaning":"not a physical stop claim"}),
                )
                .map_err(LedgerError::Storage)?;
        }
        Ok(stored)
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.inner.append_unless_cancelled(event)
    }
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.query(query)
    }
    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.events_after(cursor)
    }
    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.inner.claim(key)
    }
}

pub(super) struct Factory {
    pub inner: Tools,
    pub state: Arc<State>,
}

pub(super) struct Executor<T> {
    pub(super) inner: T,
    pub(super) state: Arc<State>,
    pub(super) execution: ExecutionRef,
    pub(super) snapshot_digest: String,
}

impl EnvironmentToolFactory for Factory {
    type Executor = Executor<<Tools as EnvironmentToolFactory>::Executor>;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<RunnerToolSet<Self::Executor>, RunnerError> {
        let built = self.inner.build(snapshot, execution)?;
        Ok(RunnerToolSet {
            executor: Executor {
                inner: built.executor,
                state: self.state.clone(),
                execution: execution.clone(),
                snapshot_digest: snapshot.digest().into(),
            },
            definitions: built.definitions,
            policy: built.policy,
        })
    }
}

impl<T: ToolExecutor> ToolExecutor for Executor<T> {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        self.inner.prepare(call)
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            let call = invocation.prepared.call();
            let arguments =
                serde_json::from_value::<kolyan_tools::WriteArguments>(call.arguments.clone());
            if call.name != "file.write"
                || arguments.as_ref().ok() != Some(&self.state.expected_write)
                || self.state.admitted_execution.lock().unwrap().as_ref() != Some(&self.execution)
                || invocation.scope.execution.execution_id != self.execution.execution_id
                || invocation.scope.execution.session_id != self.execution.session_id
                || invocation.scope.execution.turn_id != self.execution.turn_id
                || invocation.scope.agent_snapshot_digest.as_deref()
                    != Some(self.snapshot_digest.as_str())
            {
                self.state.refused.fetch_add(1, Ordering::SeqCst);
                self.state.evidence.append(json!({"event":"native_selection_refused","prepared":invocation.prepared,"scope":invocation.scope,"grant":invocation.grant,"reason":"foreign invocation/snapshot or strict write arguments"})).unwrap();
                return Err(kolyan_core::ToolError::Failed {
                    message: "native fixture received foreign invocation".into(),
                });
            }
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &invocation.policy_revision,
                    &invocation.scope,
                )
                .map_err(|error| kolyan_core::ToolError::PolicyDenied {
                    message: error.to_string(),
                })?;
            let receipt_id = format!(
                "{}/effect/{}/{}/receipt",
                self.execution.execution_id,
                invocation.scope.step_id,
                invocation.prepared.call().id
            );
            {
                let mut selection = self.state.selection.lock().unwrap();
                if selection.is_some() {
                    self.state.refused.fetch_add(1, Ordering::SeqCst);
                    return Err(kolyan_core::ToolError::Failed {
                        message: "extra native write is not an authorized fault boundary".into(),
                    });
                }
                *selection = Some(Selection {
                    call: call.clone(),
                    receipt_id,
                    scope: invocation.scope.clone(),
                    digest: invocation.prepared.digest().into(),
                    grant: invocation.grant.clone(),
                });
            }
            self.state.evidence.append(json!({"event":"native_invocation","prepared":invocation.prepared,"grant":invocation.grant,"scope":invocation.scope,"revision":invocation.policy_revision,"pid":null,"pid_meaning":"not exposed by this public adapter"})).unwrap();
            if self.state.fault == "late_child" {
                self.state.entered.notify_one();
                tokio::time::timeout(
                    Duration::from_millis(self.state.gate_timeout_ms),
                    self.state.release.notified(),
                )
                .await
                .map_err(|_| kolyan_core::ToolError::Failed {
                    message: "host admission gate expired".into(),
                })?;
            }
            self.state.executions.fetch_add(1, Ordering::SeqCst);
            let outcome = self.inner.execute_invocation(invocation).await;
            if matches!(&outcome, Ok(ToolOutcome::Completed(result)) if !result.is_error) {
                self.state.completed.fetch_add(1, Ordering::SeqCst);
            }
            self.state.evidence.append(json!({"event":"native_adapter_returned","actual_result":match &outcome { Ok(ToolOutcome::Completed(result))=>Some(result), _=>None },"error":outcome.as_ref().err().map(ToString::to_string)})).unwrap();
            outcome
        })
    }
}
