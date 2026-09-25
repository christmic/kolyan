use crate::{
    StepControl, StepError, StepExecutionOptions, StepExecutor, StepOutcome, StepRequest,
    StepResult,
};
use futures_core::Stream;
use futures_util::task::AtomicWaker;
use futures_util::{future::join_all, stream};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelProvider, ModelRequest, ToolCall, ToolChoice,
    ToolResult,
};
use kolyan_policy::{ExecutionGrant, PolicyContext, PolicyDecisionKind, PolicyEngine};
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use thiserror::Error;

mod boundary;
mod dispatch;
mod engine;
pub use boundary::{TurnBoundary, TurnBoundaryControl, TurnBoundaryFuture, TurnBoundaryKind};

/// Durable checkpoint for a turn paused at an approval boundary.
///
/// This contains the exact pending call and the model context that led to it,
/// so resuming does not call the model again for the completed step.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TurnContinuation {
    pub continuation_id: String,
    pub approval_id: String,
    pub turn_id: String,
    pub model_request: ModelRequest,
    pub assistant_content: Vec<ContentBlock>,
    pub pending_calls: Vec<ToolCall>,
    pub steps: Vec<StepResult>,
    pub max_steps: usize,
    pub next_step_index: usize,
    pub call_id: String,
    pub tool_name: String,
    pub args_fingerprint: String,
    pub policy_version: String,
    #[serde(default)]
    pub approved_call_ids: Vec<String>,
    #[serde(default)]
    pub max_tool_calls: Option<usize>,
    #[serde(default)]
    pub tool_calls_used: usize,
    #[serde(default)]
    pub deadline_at_ms: Option<u64>,
    pub tool_dispatch: ToolDispatchPolicy,
    pub tool_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ApprovalRequest {
    pub approval_id: String,
    pub turn_id: String,
    pub call_id: String,
    pub tool_name: String,
    pub reason: String,
    #[serde(default)]
    pub state: ApprovalState,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
    pub continuation: TurnContinuation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    #[default]
    Pending,
    Approved,
    Rejected,
    Expired,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResumableTurn {
    Completed(Box<TurnExecution>),
    AwaitingApproval(Box<ApprovalRequest>),
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

#[derive(Debug, Clone, PartialEq)]
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
            Self::InvalidRequest { .. }
            | Self::BoundaryControl { .. }
            | Self::Step(_)
            | Self::Tool(_)
            | Self::ToolBudgetExceeded
            | Self::InvalidStateTransition { .. } => TurnEndReason::Failed,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ToolError {
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

pub type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<ToolResult, ToolError>> + Send + 'a>>;

pub trait ToolExecutor: Send + Sync {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_>;

    /// Invocation identity is stable across approval recovery. Adapters may use
    /// it for receipts without making Core depend on storage.
    fn execute_invocation(
        &self,
        _step_id: String,
        call: ToolCall,
        grant: Option<ExecutionGrant>,
    ) -> ToolFuture<'_> {
        match grant {
            Some(grant) => self.execute_with_grant(call, grant),
            None => self.execute(call),
        }
    }

    fn execute_with_grant(&self, call: ToolCall, _grant: ExecutionGrant) -> ToolFuture<'_> {
        self.execute(call)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopToolExecutor;

impl ToolExecutor for NoopToolExecutor {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        Box::pin(async move { Err(ToolError::Unavailable { name: call.name }) })
    }
}

#[derive(Clone, Default)]
pub struct TurnControl {
    state: Arc<TurnControlState>,
}

#[derive(Default)]
struct TurnControlState {
    cancelled: AtomicBool,
    approved_tools: Mutex<HashSet<String>>,
    waiting_for_approval: Mutex<HashSet<String>>,
    waker: AtomicWaker,
}

impl TurnControl {
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.waker.wake();
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    pub fn approve_tool(&self, name: impl Into<String>) {
        let name = name.into();
        self.state
            .approved_tools
            .lock()
            .expect("approval lock must not be poisoned")
            .insert(name.clone());
        self.state
            .waiting_for_approval
            .lock()
            .expect("approval lock must not be poisoned")
            .remove(&name);
        self.state.waker.wake();
    }

    pub fn is_waiting_for_approval(&self, name: &str) -> bool {
        self.state
            .waiting_for_approval
            .lock()
            .expect("approval lock must not be poisoned")
            .contains(name)
    }

    fn wait_cancelled(&self) -> CancellationFuture {
        CancellationFuture {
            state: Arc::clone(&self.state),
        }
    }

    fn wait_for_tool_approval(&self, name: impl Into<String>) -> ApprovalFuture {
        ApprovalFuture {
            state: Arc::clone(&self.state),
            tool_name: name.into(),
        }
    }
}

struct CancellationFuture {
    state: Arc<TurnControlState>,
}

struct ApprovalFuture {
    state: Arc<TurnControlState>,
    tool_name: String,
}

impl Future for ApprovalFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self
            .state
            .approved_tools
            .lock()
            .expect("approval lock must not be poisoned")
            .remove(&self.tool_name)
        {
            return Poll::Ready(());
        }
        self.state
            .waiting_for_approval
            .lock()
            .expect("approval lock must not be poisoned")
            .insert(self.tool_name.clone());
        self.state.waker.register(cx.waker());
        if self
            .state
            .approved_tools
            .lock()
            .expect("approval lock must not be poisoned")
            .remove(&self.tool_name)
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Future for CancellationFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.cancelled.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        self.state.waker.register(cx.waker());
        if self.state.cancelled.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
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
        }
    }
}

impl<P, T: ToolExecutor> TurnExecutor<P, T> {
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

    pub fn with_policy_engine(mut self, policy_engine: Arc<PolicyEngine>) -> Self {
        self.policy_engine = Some(policy_engine);
        self
    }
}

impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    /// Run until completion or a serializable approval boundary.
    pub async fn start_resumable(&self, request: TurnRequest) -> Result<ResumableTurn, TurnError> {
        self.start_resumable_with_control(request, TurnControl::default())
            .await
    }

    pub async fn start_resumable_with_control(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<ResumableTurn, TurnError> {
        let state = engine::RunState::new(request, self.tool_dispatch, self.tool_timeout)?;
        self.run(state, control, true, None).await
    }

    pub async fn resume_approval(
        &self,
        approval: ApprovalRequest,
        approved_approval_id: &str,
    ) -> Result<ResumableTurn, TurnError> {
        self.resume_approval_with_control(approval, approved_approval_id, TurnControl::default())
            .await
    }

    pub async fn resume_approval_with_control(
        &self,
        approval: ApprovalRequest,
        approved_approval_id: &str,
        control: TurnControl,
    ) -> Result<ResumableTurn, TurnError> {
        validate_approval_transition(&approval, approved_approval_id)?;
        let mut state = engine::RunState::restore(&approval)?;
        self.validate_approval_policy(&approval)?;
        state
            .pending
            .as_mut()
            .expect("validated pending batch")
            .approved
            .push(approval.call_id);
        self.run(state, control, true, None).await
    }

    pub fn reject_approval(
        &self,
        approval: ApprovalRequest,
        approval_id: &str,
        reason: impl Into<String>,
    ) -> Result<TurnExecution, TurnError> {
        validate_approval_transition(&approval, approval_id)?;
        terminal_approval_execution(
            approval,
            TurnOutcome::Rejected {
                reason: reason.into(),
            },
            TurnEndReason::ApprovalRejected,
        )
    }

    pub fn expire_approval(
        &self,
        approval: ApprovalRequest,
        approval_id: &str,
    ) -> Result<TurnExecution, TurnError> {
        validate_approval_transition(&approval, approval_id)?;
        terminal_approval_execution(
            approval,
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
        let state = engine::RunState::new(request, self.tool_dispatch, self.tool_timeout)?;
        match self.run(state, control, false, queue).await? {
            ResumableTurn::Completed(execution) => Ok(*execution),
            ResumableTurn::AwaitingApproval(_) => unreachable!("inline approval does not suspend"),
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
    approval: &ApprovalRequest,
    approval_id: &str,
) -> Result<(), TurnError> {
    if approval.approval_id != approval_id
        || approval.continuation.approval_id != approval.approval_id
    {
        return Err(TurnError::InvalidRequest {
            message: "approval id does not match checkpoint".into(),
        });
    }
    if approval.state != ApprovalState::Pending {
        return Err(TurnError::InvalidRequest {
            message: "approval checkpoint is already resolved".into(),
        });
    }
    Ok(())
}

fn terminal_approval_execution(
    approval: ApprovalRequest,
    outcome: TurnOutcome,
    end_reason: TurnEndReason,
) -> Result<TurnExecution, TurnError> {
    let turn_id = approval.turn_id;
    let events = vec![TurnEvent::Completed {
        turn_id: turn_id.clone(),
        outcome: outcome.clone(),
    }];
    Ok(TurnExecution {
        result: TurnResult {
            turn_id,
            outcome,
            end_reason,
            steps: approval.continuation.steps,
        },
        events,
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn deadline_instant(deadline_at_ms: Option<u64>) -> Option<Instant> {
    deadline_at_ms
        .map(|deadline| Instant::now() + Duration::from_millis(deadline.saturating_sub(now_ms())))
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
