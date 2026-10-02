//! Single-root Agent execution through the production Task/Session/Runtime chain.
//! Factories are host ports, not fallback Providers or implicit tool authority.
//! Explicit delegation routes admit durable private children; host pumping drives
//! and consumes terminal proofs before resuming the original parent checkpoint.
//! Read-only fanout requires host proof and bounded slots; writable isolation and
//! full deeper-recursion acceptance remain incomplete.

mod admission;
mod delegation;
mod finalization;
mod input;
mod preparation;
mod resume;
mod routing;
mod tools;

pub use delegation::{
    AdmittedAgentChild, AgentChildDriveResult, AgentChildWaitVerifier, AgentChildrenPumpResult,
    ChildApprovalResumeRequest, DelegationOwner,
};
pub use finalization::{
    ContinuationProjectionConfig, ContinuationProjectionRequest, TaskFinalizationPolicy,
    TaskFinalizationRequest,
};
pub use preparation::{PreparedRootInput, RootInputPreparationRequest};
pub use resume::RootApprovalResumeRequest;
pub use routing::AgentDelegationConfig;

use std::sync::Arc;

use kolyan_core::{ToolDispatchPolicy, ToolErrorPolicy, ToolExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerStore};
use kolyan_model::{ModelProvider, ToolDefinition};
use kolyan_policy::PolicyEngine;
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{
    CancellationPolicy, ExecutionRef, InstanceRegistry, TaskExecutionService, TaskLimits,
    TaskSnapshot,
};
use kolyan_storage::SessionStore;
use kolyan_trace::TraceSink;
use thiserror::Error;

use crate::{
    AgentCatalog, AgentPermissions, AgentSelector, AgentSnapshot,
    binding::AgentInvocationBindingStore, provider::ContextPreparingProvider,
};

/// Host-supplied coordinates and current input, not raw `agent.invoke` arguments.
/// The logical Session must already exist. `turn.model_request.tools` must be empty:
/// exact schemas come exclusively from the trusted environment factory.
pub struct RootRunRequest {
    pub task_id: String,
    pub invocation_id: String,
    pub attempt_id: String,
    pub execution: ExecutionRef,
    pub selector: AgentSelector,
    pub requested_permissions: AgentPermissions,
    pub objective: String,
    pub limits: TaskLimits,
    pub cancellation_policy: CancellationPolicy,
    pub turn: TurnRequest,
}

/// A stopped real attempt. Waiting and failure are never reported as task success.
pub struct RootRunResult {
    pub snapshot: AgentSnapshot,
    pub task: TaskSnapshot,
    pub execution: DurableTurnResult,
}

/// Immutable host concurrency budget, shareable across Runners without exposing
/// a permit mutation API to adapters or model input.
#[derive(Clone)]
pub struct AgentExecutionBudget {
    slots: Arc<tokio::sync::Semaphore>,
    bound: usize,
    gates: Arc<
        std::sync::Mutex<std::collections::BTreeMap<String, delegation::scheduling::SharedGate>>,
    >,
}
impl AgentExecutionBudget {
    pub fn new(bound: usize) -> Result<Self, RunnerError> {
        if !matches!(bound, 1 | 2 | 4) {
            return Err(RunnerError::Host(
                "host parallel bound must be 1, 2 or 4".into(),
            ));
        }
        Ok(Self {
            slots: Arc::new(tokio::sync::Semaphore::new(bound)),
            bound,
            gates: Arc::new(std::sync::Mutex::new(Default::default())),
        })
    }
    pub fn bound(&self) -> usize {
        self.bound
    }
}

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("Agent admission: {0}")]
    Agent(#[from] crate::AgentError),
    #[error("invocation binding: {0}")]
    Binding(#[from] crate::binding::BindingError),
    #[error("instance admission: {0}")]
    Instance(#[from] kolyan_server::InstanceRegistryError),
    #[error("task execution: {0}")]
    Execution(#[from] kolyan_server::TaskExecutionError),
    #[error("task coordination: {0}")]
    Task(#[from] kolyan_server::TaskError),
    #[error("execution failed: {execution}; Task finalization refused: {finalization}")]
    ExecutionAndFinalization {
        #[source]
        execution: kolyan_server::TaskExecutionError,
        finalization: Box<RunnerError>,
    },
    #[error(
        "Agent finalization does not own {role:?} context/dependency proofs for {invocation_id}"
    )]
    UnsupportedFinalizationRole {
        invocation_id: String,
        role: kolyan_server::InvocationRole,
    },
    #[error("Runner host contract: {0}")]
    Host(String),
}

/// Construct the actual model adapter plus its lossless per-stream context guard.
/// Descriptor/counter/recorder trust remains the host's responsibility. There is
/// no default Provider or unverified token-count fallback in the Runner.
pub trait ProviderFactory: Send + Sync + 'static {
    type Provider: ModelProvider + Send + Sync + 'static;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Self::Provider>, RunnerError>;
}

/// Exact inventory, scoped effect adapter and current dynamic policy.
pub struct RunnerToolSet<T> {
    pub executor: T,
    pub definitions: Vec<ToolDefinition>,
    pub policy: Arc<PolicyEngine>,
}

/// Construct real environment adapters. This port never authorizes delegation.
/// The Runner independently enforces the snapshot ceiling on advertised AND
/// executed calls; adapters must additionally enforce preparation/grant/sandbox.
pub trait EnvironmentToolFactory: Send + Sync + 'static {
    type Executor: ToolExecutor + 'static;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<RunnerToolSet<Self::Executor>, RunnerError>;

    /// Trusted enforcement attestation, not an inference from model paths or
    /// tool names. True requires every constructed adapter to prevent writes and
    /// process execution for this snapshot. Unproved factories remain serial.
    fn enforces_read_only_parallel(&self, _snapshot: &AgentSnapshot) -> bool {
        false
    }
}

/// All host admission paths must share the instance namespace and journal.
/// Session lifecycle is host-owned. A stopped attempt stays durable if this future
/// is dropped; a repeated `start` cannot replay an already admitted attempt.
pub struct AgentRunner<J, L, S, SS, P, T> {
    service: Arc<TaskExecutionService<J, L, S, SS>>,
    instances: InstanceRegistry,
    bindings: AgentInvocationBindingStore,
    catalog: AgentCatalog,
    host: AgentPermissions,
    providers: P,
    tools: T,
    delegation: Option<AgentDelegationConfig>,
    tool_error_policy: ToolErrorPolicy,
    execution_budget: AgentExecutionBudget,
    continuation_projection: Option<ContinuationProjectionConfig>,
    input_artifacts: Arc<kolyan_trace::ArtifactStore>,
}

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    /// Construction does not create Sessions, instance facts or model requests.
    pub fn new(
        service: Arc<TaskExecutionService<J, L, S, SS>>,
        instances: InstanceRegistry,
        bindings: AgentInvocationBindingStore,
        catalog: AgentCatalog,
        host: AgentPermissions,
        factories: (P, T),
        input_artifacts: Arc<kolyan_trace::ArtifactStore>,
    ) -> Result<Self, RunnerError> {
        host.validate()?;
        Ok(Self {
            service,
            instances,
            bindings,
            catalog,
            host,
            providers: factories.0,
            tools: factories.1,
            delegation: None,
            tool_error_policy: ToolErrorPolicy::FailTurn,
            execution_budget: AgentExecutionBudget::new(1)?,
            continuation_projection: None,
            input_artifacts,
        })
    }

    /// Explicit host opt-in. Environment permissions never imply delegation.
    pub fn with_delegation(mut self, config: AgentDelegationConfig) -> Result<Self, RunnerError> {
        config
            .limits
            .validate()
            .map_err(|error| RunnerError::Host(error.to_string()))?;
        self.delegation = Some(config);
        Ok(self)
    }

    /// Select tool-error feedback for newly started Turns through this host.
    /// FailTurn is the default. ContinueBatch uses Core's existing bounded error
    /// feedback without granting permissions, retrying effects or suppressing
    /// fatal errors. Tool batches remain serial by default; child scheduling is
    /// independent. Resumed Turns retain their saved checkpoint dispatch policy
    /// and remaining budgets, even when this Runner has a different selection.
    pub fn with_tool_error_policy(mut self, policy: ToolErrorPolicy) -> Self {
        self.tool_error_policy = policy;
        self
    }

    fn tool_dispatch_policy(&self) -> ToolDispatchPolicy {
        ToolDispatchPolicy {
            on_error: self.tool_error_policy,
            ..ToolDispatchPolicy::default()
        }
    }

    /// One stable budget shared by every child and nested invocation of this
    /// Runner. A host assembling multiple Runners must share the same budget.
    pub fn with_execution_budget(mut self, budget: AgentExecutionBudget) -> Self {
        self.execution_budget = budget;
        self
    }

    /// Host-owned immutable source artifacts and the real context counter used
    /// to verify explicit projections. Absence never permits a lossy fallback.
    pub fn with_continuation_projection(mut self, config: ContinuationProjectionConfig) -> Self {
        self.continuation_projection = Some(config);
        self
    }

    /// Resolve named/inline content, save immutable ownership, admit a real root,
    /// and run the actual durable Task service. Blocking preparation/journal work
    /// is isolated from the async executor. Admission retries use exact facts;
    /// changed bindings fail, and existing attempts are never implicitly resumed.
    pub async fn start(
        self: &Arc<Self>,
        request: RootRunRequest,
    ) -> Result<RootRunResult, RunnerError> {
        let runner = Arc::clone(self);
        let (task_id, snapshot, binding, turn, executor) =
            tokio::task::spawn_blocking(move || runner.admit(request))
                .await
                .map_err(|error| {
                    RunnerError::Host(format!("admission worker failed: {error}"))
                })??;
        let permits = self
            .enter_execution(&task_id, &binding.execution.session_id, &binding)
            .await
            .map_err(|error| RunnerError::Host(error.to_string()))?;
        // Keep the complete Server/Runtime/Core execution future out of the
        // enclosing orchestration state, including serial and recursive paths.
        let stopped = Box::pin(self.service.run(&task_id, binding.clone(), executor, turn)).await;
        drop(permits);
        self.finish_execution(snapshot, &task_id, &binding, stopped)
            .await
    }

    async fn finish_execution(
        self: &Arc<Self>,
        snapshot: AgentSnapshot,
        task_id: &str,
        binding: &kolyan_server::AttemptBinding,
        stopped: Result<(TaskSnapshot, DurableTurnResult), kolyan_server::TaskExecutionError>,
    ) -> Result<RootRunResult, RunnerError> {
        match stopped {
            Ok((task, execution)) => {
                self.finish(snapshot, task, execution, &binding.invocation_id)
                    .await
            }
            Err(execution) => {
                // A returned execution error can follow a durable typed terminal.
                // Close that Task from proofs; never retry the failed execution.
                let finalization = self
                    .finalize_task(TaskFinalizationRequest {
                        task_id: task_id.into(),
                        logical_session_id: binding.execution.session_id.clone(),
                        root_invocation_id: binding.invocation_id.clone(),
                        root_attempt_id: binding.attempt_id.clone(),
                        policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
                    })
                    .await;
                match finalization {
                    Ok(_) => Err(execution.into()),
                    Err(finalization) => Err(RunnerError::ExecutionAndFinalization {
                        execution,
                        finalization: Box::new(finalization),
                    }),
                }
            }
        }
    }

    async fn finish(
        self: &Arc<Self>,
        snapshot: AgentSnapshot,
        mut task: TaskSnapshot,
        execution: DurableTurnResult,
        invocation_id: &str,
    ) -> Result<RootRunResult, RunnerError> {
        if task.invocations.values().all(|invocation| {
            matches!(
                invocation.state,
                kolyan_server::InvocationState::Completed
                    | kolyan_server::InvocationState::Failed
                    | kolyan_server::InvocationState::Cancelled
            )
        }) {
            let binding = task
                .invocations
                .get(invocation_id)
                .and_then(|invocation| invocation.attempts.last())
                .and_then(|attempt| task.attempts.get(attempt))
                .ok_or_else(|| RunnerError::Host("root finalization attempt is absent".into()))?;
            task = self
                .finalize_task(TaskFinalizationRequest {
                    task_id: task.definition.task_id.clone(),
                    logical_session_id: binding.binding.execution.session_id.clone(),
                    root_invocation_id: invocation_id.into(),
                    root_attempt_id: binding.binding.attempt_id.clone(),
                    policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
                })
                .await?;
        }
        Ok(RootRunResult {
            snapshot,
            task,
            execution,
        })
    }
}

#[cfg(test)]
mod tests;
