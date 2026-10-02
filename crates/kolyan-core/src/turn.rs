use crate::{
    StepControl, StepError, StepEventRecorder, StepExecutionOptions, StepExecutor, StepOutcome,
    StepRequest, StepResult,
};
use futures_core::Stream;
use futures_util::{future::join_all, stream};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelProvider, ModelRequest, ToolCall, ToolResult,
};
use kolyan_policy::{
    ApprovalEvidence, PolicyContext, PolicyDecisionKind, PolicyEngine, PreparedCall, PreparedGrant,
    ToolExecutionScope,
};
use kolyan_types::ExecutionKey;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};
use thiserror::Error;

mod boundary;
mod checkpoint;
mod control;
mod deadline;
mod dispatch;
mod engine;
mod outcome;
mod planning;
mod suspension;
pub use boundary::{TurnBoundary, TurnBoundaryControl, TurnBoundaryFuture, TurnBoundaryKind};
pub use checkpoint::{
    CheckpointApproval, CheckpointBudget, CheckpointCall, CheckpointCallState, CheckpointError,
    CheckpointReconstruction, IssuedToolAuthority, MAX_TURN_CHECKPOINT_BYTES,
    TURN_CHECKPOINT_SCHEMA, TurnCheckpoint,
};
pub use control::TurnControl;
pub use deadline::{TurnDeadline, TurnDeadlineError};
pub use outcome::{ExternalResolution, ExternalWait, MAX_EXTERNAL_BINDING_BYTES, ToolOutcome};
pub use suspension::{
    ApprovalConfirmation, ApprovalRequest, PendingExternalWait, ResumeInput, SuspensionSummary,
    TurnSuspension,
};

#[derive(Debug, Clone, PartialEq)]
pub enum ResumableTurn {
    Completed(Box<TurnExecution>),
    Suspended(Box<TurnSuspension>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TurnRequest {
    pub turn_id: String,
    pub model_request: ModelRequest,
    pub config: TurnConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnConfig {
    pub max_steps: usize,
    pub max_tool_calls: Option<usize>,
    pub deadline: Option<Duration>,
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self {
            max_steps: 1,
            max_tool_calls: None,
            deadline: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ToolDispatchMode {
    Serial,
    Parallel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ToolErrorPolicy {
    FailTurn,
    ContinueBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDispatchPolicy {
    pub mode: ToolDispatchMode,
    pub on_error: ToolErrorPolicy,
}

impl Default for ToolDispatchPolicy {
    fn default() -> Self {
        Self {
            mode: ToolDispatchMode::Serial,
            on_error: ToolErrorPolicy::FailTurn,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolDispatchResult {
    pub call_id: String,
    pub result: Result<ToolResult, ToolError>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCallBatch {
    calls: Vec<ToolCall>,
}

impl ToolCallBatch {
    pub fn calls(&self) -> &[ToolCall] {
        &self.calls
    }

    pub fn len(&self) -> usize {
        self.calls.len()
    }

    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    pub fn into_calls(self) -> Vec<ToolCall> {
        self.calls
    }

    fn call(&self, call_id: &str) -> Option<&ToolCall> {
        self.calls.iter().find(|call| call.id == call_id)
    }

    fn validate_results(&self, results: &[ToolDispatchResult]) -> Result<(), ToolError> {
        if results.len() != self.calls.len() {
            return Err(ToolError::InvalidBatch {
                message: format!(
                    "expected {} tool results, got {}",
                    self.calls.len(),
                    results.len()
                ),
            });
        }

        let mut result_ids = HashSet::with_capacity(results.len());
        for dispatch in results {
            let Some(call) = self.call(&dispatch.call_id) else {
                return Err(ToolError::InvalidBatch {
                    message: format!("unknown tool result call id: {}", dispatch.call_id),
                });
            };
            if !result_ids.insert(dispatch.call_id.clone()) {
                return Err(ToolError::InvalidBatch {
                    message: format!("duplicate tool result call id: {}", dispatch.call_id),
                });
            }
            if let Ok(result) = &dispatch.result
                && result.call_id != call.id
            {
                return Err(ToolError::InvalidBatch {
                    message: format!(
                        "tool result call id mismatch: expected {}, got {}",
                        call.id, result.call_id
                    ),
                });
            }
        }

        if self.calls.iter().any(|call| !result_ids.contains(&call.id)) {
            return Err(ToolError::InvalidBatch {
                message: "tool result batch is missing a call id".into(),
            });
        }

        Ok(())
    }
}

impl TryFrom<Vec<ToolCall>> for ToolCallBatch {
    type Error = ToolError;

    fn try_from(calls: Vec<ToolCall>) -> Result<Self, Self::Error> {
        if calls.is_empty() {
            return Err(ToolError::InvalidBatch {
                message: "tool call batch must not be empty".into(),
            });
        }

        let mut call_ids = HashSet::with_capacity(calls.len());
        for call in &calls {
            if !call_ids.insert(call.id.clone()) {
                return Err(ToolError::InvalidBatch {
                    message: format!("duplicate tool call id: {}", call.id),
                });
            }
        }

        Ok(Self { calls })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    Pending,
    Running,
    WaitingModel,
    WaitingTool,
    ExecutingTools,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    MaxSteps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnEndReason {
    FinalAnswer,
    Refused,
    Incomplete,
    MaxSteps,
    Failed,
    Cancelled,
    TimedOut,
    ApprovalRejected,
    ApprovalExpired,
    NoProgress,
}

impl TurnState {
    pub fn transition(self, next: Self) -> Result<Self, TurnError> {
        let allowed = matches!(
            (self, next),
            (Self::Pending, Self::Running)
                | (Self::Running, Self::WaitingModel)
                | (Self::WaitingModel, Self::WaitingTool)
                | (Self::WaitingTool, Self::ExecutingTools)
                | (Self::ExecutingTools, Self::Running)
                | (Self::WaitingModel, Self::Completed)
                | (Self::WaitingModel, Self::Failed)
                | (Self::WaitingModel, Self::Cancelled)
                | (Self::WaitingModel, Self::TimedOut)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Cancelled)
                | (Self::Running, Self::TimedOut)
                | (Self::Running, Self::MaxSteps)
                | (Self::WaitingTool, Self::Failed)
                | (Self::WaitingTool, Self::Cancelled)
                | (Self::WaitingTool, Self::TimedOut)
                | (Self::ExecutingTools, Self::Failed)
                | (Self::ExecutingTools, Self::Cancelled)
                | (Self::ExecutingTools, Self::TimedOut)
        );
        if allowed {
            Ok(next)
        } else {
            Err(TurnError::InvalidStateTransition {
                from: self,
                to: next,
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TurnResult {
    pub turn_id: String,
    pub outcome: TurnOutcome,
    pub end_reason: TurnEndReason,
    pub steps: Vec<StepResult>,
}

/// Turn-level observation events. Fine-grained model deltas remain owned by
/// the StepEvent stream; TurnEvent describes the lifecycle and orchestration
/// around those Steps.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEvent {
    Started {
        turn_id: String,
    },
    StepStarted {
        turn_id: String,
        step_id: String,
    },
    StepCompleted {
        turn_id: String,
        step: StepResult,
    },
    ToolCallRequested {
        turn_id: String,
        call: ToolCall,
    },
    ToolExecutionStarted {
        turn_id: String,
        call_id: String,
        name: String,
    },
    ToolResult {
        turn_id: String,
        result: ToolResult,
    },
    ToolExecutionFailed {
        turn_id: String,
        call_id: String,
        name: String,
        error: ToolError,
    },
    ApprovalRequested {
        turn_id: String,
        call_id: String,
        name: String,
    },
    ToolAwaitingExternal {
        turn_id: String,
        call_id: String,
        wait: ExternalWait,
    },
    Failed {
        turn_id: String,
        error: String,
    },
    Cancelled {
        turn_id: String,
    },
    TimedOut {
        turn_id: String,
    },
    Completed {
        turn_id: String,
        outcome: TurnOutcome,
    },
}

pub type TurnEventStream<'a> =
    Pin<Box<dyn Stream<Item = Result<TurnEvent, TurnError>> + Send + 'a>>;

type EventQueue = Arc<Mutex<VecDeque<Result<TurnEvent, TurnError>>>>;

/// Commits execution facts before the loop advances. Unlike an observer, a
/// recording failure stops execution; implementations must not execute tools.
pub trait TurnEventRecorder: Send + Sync {
    fn record(&self, event: &TurnEvent) -> Result<(), TurnError>;

    /// Capture the actual Step input before invoking the model. Credentials are
    /// owned by Provider configuration and are not part of this request.
    fn record_request(&self, _request: &ModelRequest) -> Result<(), TurnError> {
        Ok(())
    }

    /// Publish the validated loop state after fresh preparation and planning,
    /// before any effect in the next stage is admitted. Failure blocks effects.
    /// The default means Core provides no persistence; durable hosts must
    /// implement this barrier and bind the snapshot to trusted input admission.
    fn record_checkpoint(&self, _checkpoint: &TurnCheckpoint) -> Result<(), TurnError> {
        Ok(())
    }
}

struct EventEmitter {
    events: Mutex<Vec<TurnEvent>>,
    queue: Option<EventQueue>,
    recorder: Option<Arc<dyn TurnEventRecorder>>,
}

impl EventEmitter {
    fn new(queue: Option<EventQueue>, recorder: Option<Arc<dyn TurnEventRecorder>>) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            queue,
            recorder,
        }
    }

    fn emit(&self, event: TurnEvent) -> Result<(), TurnError> {
        if let Some(recorder) = &self.recorder {
            recorder.record(&event)?;
        }
        if let Some(queue) = &self.queue {
            queue
                .lock()
                .expect("turn event queue must not be poisoned")
                .push_back(Ok(event.clone()));
        }
        self.events.lock().expect("turn events lock").push(event);
        Ok(())
    }

    fn into_events(self) -> Vec<TurnEvent> {
        self.events.into_inner().expect("turn events lock")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TurnExecution {
    pub result: TurnResult,
    pub events: Vec<TurnEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TurnOutcome {
    FinalAnswer {
        response: kolyan_model::ModelResponse,
    },
    Refused {
        response: kolyan_model::ModelResponse,
    },
    Incomplete {
        response: kolyan_model::ModelResponse,
    },
    Rejected {
        reason: String,
    },
    Expired {
        reason: String,
    },
    MaxSteps,
}

#[derive(Debug, Error)]
pub enum TurnError {
    #[error("deadline failed: {0}")]
    Deadline(#[from] TurnDeadlineError),
    #[error("turn stopped for no progress: repeated tool {tool_name}")]
    NoProgress { tool_name: String },
    #[error("turn boundary control failed: {message}")]
    BoundaryControl { message: String },
    #[error("turn request is invalid: {message}")]
    InvalidRequest { message: String },
    #[error("turn step failed: {0}")]
    Step(#[from] StepError),
    #[error("turn tool execution failed: {0}")]
    Tool(#[from] ToolError),
    #[error("turn was cancelled")]
    Cancelled,
    #[error("turn timed out")]
    TimedOut,
    #[error("turn reached its maximum step count")]
    MaxSteps,
    #[error("turn reached its maximum tool call count")]
    ToolBudgetExceeded,
    #[error("invalid turn state transition: {from:?} -> {to:?}")]
    InvalidStateTransition { from: TurnState, to: TurnState },
}

impl TurnError {
    pub fn end_reason(&self) -> TurnEndReason {
        match self {
            Self::NoProgress { .. } => TurnEndReason::NoProgress,
            Self::Cancelled => TurnEndReason::Cancelled,
            Self::TimedOut => TurnEndReason::TimedOut,
            Self::MaxSteps => TurnEndReason::MaxSteps,
            Self::Tool(ToolError::Cancelled) => TurnEndReason::Cancelled,
            Self::Tool(ToolError::TimedOut) => TurnEndReason::TimedOut,
            Self::Deadline(_)
            | Self::InvalidRequest { .. }
            | Self::BoundaryControl { .. }
            | Self::Step(_)
            | Self::Tool(_)
            | Self::ToolBudgetExceeded
            | Self::InvalidStateTransition { .. } => TurnEndReason::Failed,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ToolError {
    #[error("tool outcome is uncertain and requires trusted recovery: {message}")]
    Uncertain { message: String },
    #[error("invalid tool call batch: {message}")]
    InvalidBatch { message: String },
    #[error("tool is unavailable: {name}")]
    Unavailable { name: String },
    #[error("tool execution failed: {message}")]
    Failed { message: String },
    #[error("tool execution was cancelled")]
    Cancelled,
    #[error("tool execution timed out")]
    TimedOut,
    #[error("tool execution denied by policy: {message}")]
    PolicyDenied { message: String },
}

pub type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<ToolOutcome, ToolError>> + Send + 'a>>;
pub type ToolPreparationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PreparedCall, ToolError>> + Send + 'a>>;

/// Exact host-admitted authority and cooperative control for one external effect.
/// The expected scope is independent of serialized grant data. Adapters must
/// revalidate preparation and enforce every granted requirement before effects.
pub struct ToolInvocation {
    pub prepared: PreparedCall,
    pub grant: PreparedGrant,
    pub scope: ToolExecutionScope,
    pub policy_revision: String,
    pub control: TurnControl,
}

pub trait ToolExecutor: Send + Sync {
    /// Preparation performs no external effects and derives trusted semantics.
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_>;
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopToolExecutor;

impl ToolExecutor for NoopToolExecutor {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move { Err(ToolError::Unavailable { name: call.name }) })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            Err(ToolError::Unavailable {
                name: invocation.prepared.call().name.clone(),
            })
        })
    }
}

pub struct TurnExecutor<P, T = NoopToolExecutor> {
    step_executor: StepExecutor<P>,
    tool_executor: T,
    tool_dispatch: ToolDispatchPolicy,
    tool_timeout: Option<Duration>,
    policy_engine: Option<Arc<PolicyEngine>>,
    boundary_control: Option<Arc<dyn TurnBoundaryControl>>,
    event_recorder: Option<Arc<dyn TurnEventRecorder>>,
    execution_key: Option<ExecutionKey>,
    agent_snapshot_digest: Option<String>,
    absolute_deadline_at_ms: Option<u64>,
}

impl<P> TurnExecutor<P, NoopToolExecutor> {
    pub fn new(provider: P) -> Self {
        Self {
            step_executor: StepExecutor::new(provider),
            tool_executor: NoopToolExecutor,
            tool_dispatch: ToolDispatchPolicy::default(),
            tool_timeout: None,
            policy_engine: None,
            boundary_control: None,
            event_recorder: None,
            execution_key: None,
            agent_snapshot_digest: None,
            absolute_deadline_at_ms: None,
        }
    }
}

impl<P, T: ToolExecutor> TurnExecutor<P, T> {
    /// Bind the model side while moving every existing execution guard unchanged.
    /// Rejection returns the original wrapper error and runs no model or tools.
    /// The wrapper must enforce its own opening contract; this issues no permit.
    pub fn try_map_model_provider<Q, E>(
        self,
        wrap: impl FnOnce(P) -> Result<Q, E>,
    ) -> Result<TurnExecutor<Q, T>, E> {
        Ok(TurnExecutor {
            step_executor: self.step_executor.try_map_provider(wrap)?,
            tool_executor: self.tool_executor,
            tool_dispatch: self.tool_dispatch,
            tool_timeout: self.tool_timeout,
            policy_engine: self.policy_engine,
            boundary_control: self.boundary_control,
            event_recorder: self.event_recorder,
            execution_key: self.execution_key,
            agent_snapshot_digest: self.agent_snapshot_digest,
            absolute_deadline_at_ms: self.absolute_deadline_at_ms,
        })
    }

    /// Wrap tool execution while retaining the configured model and policy.
    pub fn map_tool_executor<U: ToolExecutor>(
        self,
        wrap: impl FnOnce(T) -> U,
    ) -> TurnExecutor<P, U> {
        TurnExecutor {
            step_executor: self.step_executor,
            tool_executor: wrap(self.tool_executor),
            tool_dispatch: self.tool_dispatch,
            tool_timeout: self.tool_timeout,
            policy_engine: self.policy_engine,
            boundary_control: self.boundary_control,
            event_recorder: self.event_recorder,
            execution_key: self.execution_key,
            agent_snapshot_digest: self.agent_snapshot_digest,
            absolute_deadline_at_ms: self.absolute_deadline_at_ms,
        }
    }

    pub fn with_tools(provider: P, tool_executor: T) -> Self {
        Self {
            step_executor: StepExecutor::new(provider),
            tool_executor,
            tool_dispatch: ToolDispatchPolicy::default(),
            tool_timeout: None,
            policy_engine: None,
            boundary_control: None,
            event_recorder: None,
            execution_key: None,
            agent_snapshot_digest: None,
            absolute_deadline_at_ms: None,
        }
    }

    pub fn with_step_executor(step_executor: StepExecutor<P>, tool_executor: T) -> Self {
        Self {
            step_executor,
            tool_executor,
            tool_dispatch: ToolDispatchPolicy::default(),
            tool_timeout: None,
            policy_engine: None,
            boundary_control: None,
            event_recorder: None,
            execution_key: None,
            agent_snapshot_digest: None,
            absolute_deadline_at_ms: None,
        }
    }

    pub fn with_tool_dispatch_policy(mut self, policy: ToolDispatchPolicy) -> Self {
        self.tool_dispatch = policy;
        self
    }

    pub fn with_tool_timeout(mut self, timeout: Duration) -> Self {
        self.tool_timeout = Some(timeout);
        self
    }

    pub fn with_boundary_control(mut self, control: Arc<dyn TurnBoundaryControl>) -> Self {
        self.boundary_control = Some(control);
        self
    }

    pub fn with_event_recorder(mut self, recorder: Arc<dyn TurnEventRecorder>) -> Self {
        self.event_recorder = Some(recorder);
        self
    }

    pub fn with_step_event_recorder(mut self, recorder: Arc<dyn StepEventRecorder>) -> Self {
        self.step_executor = self.step_executor.with_event_recorder(recorder);
        self
    }

    pub fn with_policy_engine(mut self, policy_engine: Arc<PolicyEngine>) -> Self {
        self.policy_engine = Some(policy_engine);
        self
    }

    pub fn with_execution_key(mut self, key: ExecutionKey) -> Self {
        self.execution_key = Some(key);
        self
    }

    pub fn with_agent_snapshot_digest(mut self, digest: String) -> Self {
        self.agent_snapshot_digest = Some(digest);
        self
    }

    /// Inspect configured source facts; these getters do not issue authority.
    pub fn agent_snapshot_digest(&self) -> Option<&str> {
        self.agent_snapshot_digest.as_deref()
    }

    pub fn tool_dispatch_policy(&self) -> &ToolDispatchPolicy {
        &self.tool_dispatch
    }

    pub fn tool_timeout(&self) -> Option<Duration> {
        self.tool_timeout
    }

    /// Inspect the configured absolute ceiling without creating a new window.
    pub fn absolute_deadline_at_ms(&self) -> Option<u64> {
        self.absolute_deadline_at_ms
    }

    /// Tighten the Turn's duration-based deadline with an admitted absolute
    /// deadline. Repeated configuration cannot enlarge an existing ceiling.
    pub fn with_absolute_deadline_at_ms(mut self, deadline: u64) -> Self {
        self.absolute_deadline_at_ms = Some(
            self.absolute_deadline_at_ms
                .map_or(deadline, |saved| saved.min(deadline)),
        );
        self
    }
}

impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    /// Run until completion or a serializable mixed waiting boundary.
    pub async fn start_resumable(&self, request: TurnRequest) -> Result<ResumableTurn, TurnError> {
        self.start_resumable_with_control(request, TurnControl::default())
            .await
    }

    pub async fn start_resumable_with_control(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<ResumableTurn, TurnError> {
        let deadline =
            TurnDeadline::capture(request.config.deadline, self.absolute_deadline_at_ms)?;
        self.start_resumable_with_control_and_deadline(request, control, deadline)
            .await
    }

    /// Start with the caller's original anchor, never a fresh relative window.
    pub async fn start_resumable_with_deadline(
        &self,
        request: TurnRequest,
        deadline: TurnDeadline,
    ) -> Result<ResumableTurn, TurnError> {
        self.start_resumable_with_control_and_deadline(request, TurnControl::default(), deadline)
            .await
    }

    /// Validate the original duration and intersect this executor's ceiling.
    pub async fn start_resumable_with_control_and_deadline(
        &self,
        request: TurnRequest,
        control: TurnControl,
        deadline: TurnDeadline,
    ) -> Result<ResumableTurn, TurnError> {
        deadline.validate_duration(request.config.deadline)?;
        let deadline = deadline.tighten_absolute(self.absolute_deadline_at_ms)?;
        let state = engine::RunState::with_deadline(
            request,
            self.tool_dispatch,
            self.tool_timeout,
            deadline,
        )?;
        self.run(state, control, true, None).await
    }

    /// Merge verified host input without preparing, executing or calling a model.
    /// Persist the returned checkpoint before driving it. Expected scope must
    /// come from independently verified admission, not the supplied envelope.
    pub fn merge_resume_with_control(
        &self,
        suspension: TurnSuspension,
        input: ResumeInput,
        expected_scope: ToolExecutionScope,
        control: TurnControl,
    ) -> Result<TurnCheckpoint, TurnError> {
        self.validate_resume_scope(&expected_scope)?;
        suspension
            .validate(&expected_scope)
            .map_err(checkpoint_error)?;
        engine::RunState::restore(&suspension.checkpoint, &expected_scope)?.check(&control)?;
        match input {
            ResumeInput::ApprovalConfirmed(confirmation) => suspension
                .checkpoint
                .merge_approval(&confirmation, &expected_scope, now_ms())
                .map_err(checkpoint_error),
            ResumeInput::ExternalResolved(resolutions) => suspension
                .checkpoint
                .merge_external(&resolutions, &expected_scope)
                .map_err(checkpoint_error),
        }
    }

    pub async fn resume_checkpoint_with_control(
        &self,
        checkpoint: TurnCheckpoint,
        expected_scope: ToolExecutionScope,
        control: TurnControl,
    ) -> Result<ResumableTurn, TurnError> {
        self.validate_resume_scope(&expected_scope)?;
        if self.absolute_deadline_at_ms.is_some_and(|limit| {
            checkpoint
                .budget
                .deadline_at_ms
                .is_none_or(|saved| saved > limit)
        }) {
            return Err(TurnError::InvalidRequest {
                message: "checkpoint exceeds admitted absolute deadline".into(),
            });
        }
        let state = engine::RunState::restore(&checkpoint, &expected_scope)?;
        self.run(state, control, true, None).await
    }

    pub fn reject_approval(
        &self,
        suspension: TurnSuspension,
        approval_id: &str,
        reason: impl Into<String>,
    ) -> Result<TurnExecution, TurnError> {
        let scope = self.tool_scope(
            &suspension.checkpoint.scope.execution.turn_id,
            &suspension.checkpoint.scope.step_id,
        )?;
        validate_approval_transition(&suspension, approval_id, &scope)?;
        terminal_approval_execution(
            suspension,
            TurnOutcome::Rejected {
                reason: reason.into(),
            },
            TurnEndReason::ApprovalRejected,
        )
    }

    pub fn expire_approval(
        &self,
        suspension: TurnSuspension,
        approval_id: &str,
    ) -> Result<TurnExecution, TurnError> {
        let scope = self.tool_scope(
            &suspension.checkpoint.scope.execution.turn_id,
            &suspension.checkpoint.scope.step_id,
        )?;
        validate_approval_transition(&suspension, approval_id, &scope)?;
        terminal_approval_execution(
            suspension,
            TurnOutcome::Expired {
                reason: "approval expired".into(),
            },
            TurnEndReason::ApprovalExpired,
        )
    }

    pub async fn execute(&self, request: TurnRequest) -> Result<TurnResult, TurnError> {
        Ok(self
            .execute_with_events(request, TurnControl::default())
            .await?
            .result)
    }

    pub async fn execute_with_control(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<TurnResult, TurnError> {
        Ok(self.execute_with_events(request, control).await?.result)
    }

    pub async fn execute_with_events(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<TurnExecution, TurnError> {
        self.execute_internal(request, control, None).await
    }

    async fn execute_internal(
        &self,
        request: TurnRequest,
        control: TurnControl,
        queue: Option<EventQueue>,
    ) -> Result<TurnExecution, TurnError> {
        let state = self.new_run_state(request)?;
        match self.run(state, control, false, queue).await? {
            ResumableTurn::Completed(execution) => Ok(*execution),
            ResumableTurn::Suspended(_) => Err(TurnError::InvalidRequest {
                message: "external waits require the resumable Turn API".into(),
            }),
        }
    }

    pub async fn execute_event_stream(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<TurnEventStream<'_>, TurnError> {
        validate_request(&request)?;
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let output_queue = queue.clone();
        let mut execution = Box::pin(self.execute_internal(request, control, Some(queue)));
        let mut finished = false;
        let events = stream::poll_fn(move |cx| {
            loop {
                if let Some(event) = output_queue
                    .lock()
                    .expect("turn event queue must not be poisoned")
                    .pop_front()
                {
                    return Poll::Ready(Some(event));
                }
                if finished {
                    return Poll::Ready(None);
                }
                match execution.as_mut().poll(cx) {
                    Poll::Ready(Ok(_)) => finished = true,
                    Poll::Ready(Err(error)) => {
                        finished = true;
                        output_queue
                            .lock()
                            .expect("turn event queue must not be poisoned")
                            .push_back(Err(error));
                    }
                    Poll::Pending => {
                        if output_queue
                            .lock()
                            .expect("turn event queue must not be poisoned")
                            .is_empty()
                        {
                            return Poll::Pending;
                        }
                    }
                }
            }
        });
        Ok(Box::pin(events))
    }
}

fn validate_request(request: &TurnRequest) -> Result<(), TurnError> {
    if request.turn_id.is_empty() {
        return Err(TurnError::InvalidRequest {
            message: "turn_id must not be empty".into(),
        });
    }
    if request.config.max_steps == 0 {
        return Err(TurnError::InvalidRequest {
            message: "max_steps must be greater than zero".into(),
        });
    }
    Ok(())
}

fn validate_approval_transition(
    suspension: &TurnSuspension,
    approval_id: &str,
    scope: &ToolExecutionScope,
) -> Result<(), TurnError> {
    suspension.validate(scope).map_err(checkpoint_error)?;
    if !suspension
        .waiting
        .approvals
        .iter()
        .any(|approval| approval.approval_id == approval_id)
    {
        return Err(TurnError::InvalidRequest {
            message: "approval id does not match a pending checkpoint approval".into(),
        });
    }
    Ok(())
}

fn terminal_approval_execution(
    suspension: TurnSuspension,
    outcome: TurnOutcome,
    end_reason: TurnEndReason,
) -> Result<TurnExecution, TurnError> {
    let turn_id = suspension.checkpoint.scope.execution.turn_id;
    let events = vec![TurnEvent::Completed {
        turn_id: turn_id.clone(),
        outcome: outcome.clone(),
    }];
    Ok(TurnExecution {
        result: TurnResult {
            turn_id,
            outcome,
            end_reason,
            steps: suspension.checkpoint.steps,
        },
        events,
    })
}

fn checkpoint_error(error: CheckpointError) -> TurnError {
    TurnError::InvalidRequest {
        message: error.to_string(),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn outcome_from_step(step: &StepResult) -> (TurnOutcome, TurnEndReason) {
    match step.outcome {
        StepOutcome::FinalAnswer => (
            TurnOutcome::FinalAnswer {
                response: step.response.clone(),
            },
            TurnEndReason::FinalAnswer,
        ),
        StepOutcome::Refused => (
            TurnOutcome::Refused {
                response: step.response.clone(),
            },
            TurnEndReason::Refused,
        ),
        StepOutcome::Incomplete => (
            TurnOutcome::Incomplete {
                response: step.response.clone(),
            },
            TurnEndReason::Incomplete,
        ),
        StepOutcome::ToolCalls => (TurnOutcome::MaxSteps, TurnEndReason::MaxSteps),
    }
}

fn map_step_error(error: StepError) -> TurnError {
    match error {
        StepError::Cancelled => TurnError::Cancelled,
        StepError::TimedOut => TurnError::TimedOut,
        error => TurnError::Step(error),
    }
}

fn terminal_event(turn_id: &str, error: &TurnError) -> TurnEvent {
    match error {
        TurnError::Cancelled | TurnError::Tool(ToolError::Cancelled) => TurnEvent::Cancelled {
            turn_id: turn_id.to_string(),
        },
        TurnError::TimedOut | TurnError::Tool(ToolError::TimedOut) => TurnEvent::TimedOut {
            turn_id: turn_id.to_string(),
        },
        _ => TurnEvent::Failed {
            turn_id: turn_id.to_string(),
            error: error.to_string(),
        },
    }
}

fn tool_calls(content: &[ContentBlock]) -> Vec<ToolCall> {
    content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall { call } => Some(call.clone()),
            _ => None,
        })
        .collect()
}

fn append_tool_context(
    messages: &mut Vec<Message>,
    assistant_content: &[ContentBlock],
    results: Vec<ToolResult>,
) {
    messages.push(Message {
        role: MessageRole::Assistant,
        content: assistant_content.to_vec(),
    });
    messages.extend(results.into_iter().map(|result| Message {
        role: MessageRole::User,
        content: vec![ContentBlock::ToolResult { result }],
    }));
}

#[cfg(test)]
mod tests;
