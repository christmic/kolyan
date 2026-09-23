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

struct EventEmitter {
    events: Mutex<Vec<TurnEvent>>,
    queue: Option<EventQueue>,
}

impl EventEmitter {
    fn new(queue: Option<EventQueue>) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            queue,
        }
    }

    fn emit(&self, event: TurnEvent) {
        if let Some(queue) = &self.queue {
            queue
                .lock()
                .expect("turn event queue must not be poisoned")
                .push_back(Ok(event.clone()));
        }
        self.events.lock().expect("turn events lock").push(event);
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
        }
    }
}

impl<P, T: ToolExecutor> TurnExecutor<P, T> {
    pub fn with_tools(provider: P, tool_executor: T) -> Self {
        Self {
            step_executor: StepExecutor::new(provider),
            tool_executor,
            tool_dispatch: ToolDispatchPolicy::default(),
            tool_timeout: None,
            policy_engine: None,
            boundary_control: None,
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
mod tests {
    use super::*;
    use futures_util::{StreamExt, stream};
    use kolyan_model::{
        ContentBlock, ModelEvent, ModelEventStream, ModelRef, ProviderFuture, StopReason,
        TokenUsage, ToolChoice,
    };
    use serde_json::Value;

    struct MockProvider {
        stop_reason: StopReason,
        content: Vec<ContentBlock>,
    }

    struct SlowProvider;

    impl ModelProvider for SlowProvider {
        fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
            let response = kolyan_model::ModelResponse {
                id: "slow-response".into(),
                model: request.model,
                content: vec![ContentBlock::Text {
                    text: "slow answer".into(),
                }],
                structured_output: None,
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
                metadata: Value::Null,
            };
            Box::pin(async move {
                let events =
                    stream::iter(vec![Ok(ModelEvent::Started)]).chain(stream::once(async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok(ModelEvent::Completed(response))
                    }));
                Ok(Box::pin(events) as ModelEventStream)
            })
        }
    }

    impl ModelProvider for MockProvider {
        fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
            let response = kolyan_model::ModelResponse {
                id: "turn-response".into(),
                model: request.model,
                content: self.content.clone(),
                structured_output: None,
                stop_reason: self.stop_reason.clone(),
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

    fn request() -> ModelRequest {
        ModelRequest {
            request_id: "turn-request".into(),
            model: ModelRef::new("mock", "model"),
            system: Vec::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: Value::Null,
        }
    }

    #[tokio::test]
    async fn completes_turn_from_one_final_answer_step() {
        let executor = TurnExecutor::new(MockProvider {
            stop_reason: StopReason::EndTurn,
            content: vec![ContentBlock::Text {
                text: "answer".into(),
            }],
        });
        let result = executor
            .execute(TurnRequest {
                turn_id: "turn-1".into(),
                model_request: request(),
                config: TurnConfig::default(),
            })
            .await
            .expect("turn should complete");

        assert_eq!(result.turn_id, "turn-1");
        assert_eq!(result.steps.len(), 1);
        assert!(matches!(result.outcome, TurnOutcome::FinalAnswer { .. }));
    }

    #[tokio::test]
    async fn emits_turn_lifecycle_events_for_a_final_answer() {
        let executor = TurnExecutor::new(MockProvider {
            stop_reason: StopReason::EndTurn,
            content: vec![ContentBlock::Text {
                text: "answer".into(),
            }],
        });
        let execution = executor
            .execute_with_events(
                TurnRequest {
                    turn_id: "turn-events".into(),
                    model_request: request(),
                    config: TurnConfig::default(),
                },
                TurnControl::default(),
            )
            .await
            .expect("turn events should complete");

        assert!(matches!(execution.events[0], TurnEvent::Started { .. }));
        assert!(matches!(execution.events[1], TurnEvent::StepStarted { .. }));
        assert!(matches!(
            execution.events[2],
            TurnEvent::StepCompleted { .. }
        ));
        assert!(matches!(
            execution.events[3],
            TurnEvent::Completed {
                outcome: TurnOutcome::FinalAnswer { .. },
                ..
            }
        ));

        let mut stream = executor
            .execute_event_stream(
                TurnRequest {
                    turn_id: "turn-event-stream".into(),
                    model_request: request(),
                    config: TurnConfig::default(),
                },
                TurnControl::default(),
            )
            .await
            .expect("turn event stream should open");
        let mut count = 0;
        while stream.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 4);
    }

    #[tokio::test]
    async fn event_stream_emits_before_the_model_step_finishes() {
        let executor = TurnExecutor::new(SlowProvider);
        let mut events = executor
            .execute_event_stream(
                TurnRequest {
                    turn_id: "turn-live-events".into(),
                    model_request: request(),
                    config: TurnConfig::default(),
                },
                TurnControl::default(),
            )
            .await
            .expect("event stream should open");

        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(20), events.next())
                .await
                .expect("started event should be immediate")
                .expect("stream should contain started")
                .expect("started event should be successful"),
            TurnEvent::Started { .. }
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(20), events.next())
                .await
                .expect("step-started event should be immediate")
                .expect("stream should contain step started")
                .expect("step-started event should be successful"),
            TurnEvent::StepStarted { .. }
        ));

        let remaining = events.collect::<Vec<_>>().await;
        assert!(
            remaining
                .iter()
                .any(|event| matches!(event, Ok(TurnEvent::Completed { .. })))
        );
    }

    #[tokio::test]
    async fn max_steps_is_a_completed_turn_outcome() {
        let call = ToolCall {
            id: "call-max-steps".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let execution = TurnExecutor::with_tools(provider, MockTool)
            .execute_with_events(
                TurnRequest {
                    turn_id: "turn-max-steps".into(),
                    model_request: request(),
                    config: TurnConfig {
                        max_steps: 1,
                        ..TurnConfig::default()
                    },
                },
                TurnControl::default(),
            )
            .await
            .expect("max steps should be a normal turn result");

        assert!(matches!(execution.result.outcome, TurnOutcome::MaxSteps));
        assert!(matches!(
            execution.events.last(),
            Some(TurnEvent::Completed {
                outcome: TurnOutcome::MaxSteps,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn failed_turn_stream_contains_terminal_event_before_error() {
        let call = ToolCall {
            id: "call-stream-failed".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let executor = TurnExecutor::with_tools(provider, FailingTool);
        let events = executor
            .execute_event_stream(
                TurnRequest {
                    turn_id: "turn-stream-failed".into(),
                    model_request: request(),
                    config: TurnConfig {
                        max_steps: 2,
                        ..TurnConfig::default()
                    },
                },
                TurnControl::default(),
            )
            .await
            .expect("event stream should open");
        let events = events.collect::<Vec<_>>().await;

        assert!(
            events
                .iter()
                .any(|event| matches!(event, Ok(TurnEvent::Failed { .. })))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Err(TurnError::Tool(ToolError::Failed { .. }))))
        );
    }

    #[tokio::test]
    async fn returns_tool_unavailable_instead_of_dropping_tool_calls() {
        let call = ToolCall {
            id: "call-1".into(),
            name: "search".into(),
            arguments: serde_json::json!({"query": "kolyan"}),
        };
        let executor = TurnExecutor::new(MockProvider {
            stop_reason: StopReason::ToolUse,
            content: vec![ContentBlock::ToolCall { call: call.clone() }],
        });
        let error = executor
            .execute(TurnRequest {
                turn_id: "turn-tool".into(),
                model_request: request(),
                config: TurnConfig::default(),
            })
            .await
            .expect_err("v0 should not execute tools");

        assert!(matches!(
            error,
            TurnError::Tool(ToolError::Unavailable { name }) if name == "search"
        ));
    }

    #[derive(Clone)]
    struct ScriptedProvider {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        saw_tool_result: std::sync::Arc<std::sync::atomic::AtomicBool>,
        tool_call: ToolCall,
    }

    impl ModelProvider for ScriptedProvider {
        fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
            let index = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if index == 1 {
                let has_tool_result = request.messages.iter().any(|message| {
                    message
                        .content
                        .iter()
                        .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
                });
                self.saw_tool_result
                    .store(has_tool_result, std::sync::atomic::Ordering::SeqCst);
            }
            let (stop_reason, content) = if index == 0 {
                (
                    StopReason::ToolUse,
                    vec![ContentBlock::ToolCall {
                        call: self.tool_call.clone(),
                    }],
                )
            } else {
                (
                    StopReason::EndTurn,
                    vec![ContentBlock::Text {
                        text: "tool result consumed".into(),
                    }],
                )
            };
            let response = kolyan_model::ModelResponse {
                id: format!("response-{index}"),
                model: request.model,
                content,
                structured_output: None,
                stop_reason,
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

    struct MultiApprovalProvider {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ModelProvider for MultiApprovalProvider {
        fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
            let index = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let content = if index == 0 {
                vec![
                    ContentBlock::ToolCall {
                        call: ToolCall {
                            id: "call-approval-a".into(),
                            name: "shell.query".into(),
                            arguments: serde_json::json!({"command": "count_lines"}),
                        },
                    },
                    ContentBlock::ToolCall {
                        call: ToolCall {
                            id: "call-approval-b".into(),
                            name: "shell.query".into(),
                            arguments: serde_json::json!({"command": "count_entries"}),
                        },
                    },
                ]
            } else {
                vec![ContentBlock::Text {
                    text: "all approvals completed".into(),
                }]
            };
            let response = kolyan_model::ModelResponse {
                id: format!("multi-approval-response-{index}"),
                model: request.model,
                content,
                structured_output: None,
                stop_reason: if index == 0 {
                    StopReason::ToolUse
                } else {
                    StopReason::EndTurn
                },
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

    struct MockTool;

    impl ToolExecutor for MockTool {
        fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
            Box::pin(async move {
                Ok(ToolResult {
                    call_id: call.id,
                    content: "count=1".into(),
                    is_error: false,
                })
            })
        }
    }

    #[tokio::test]
    async fn executes_tool_result_and_runs_a_second_step() {
        let call = ToolCall {
            id: "call-1".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let executor = TurnExecutor::with_tools(provider.clone(), MockTool);
        let result = executor
            .execute(TurnRequest {
                turn_id: "turn-multi-step".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..TurnConfig::default()
                },
            })
            .await
            .expect("turn should consume the tool result");

        assert_eq!(result.steps.len(), 2);
        assert!(matches!(result.outcome, TurnOutcome::FinalAnswer { .. }));
        assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(
            provider
                .saw_tool_result
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn rejects_cancelled_turn_before_starting_a_step() {
        let executor = TurnExecutor::new(MockProvider {
            stop_reason: StopReason::EndTurn,
            content: Vec::new(),
        });
        let control = TurnControl::default();
        control.cancel();
        let error = executor
            .execute_with_control(
                TurnRequest {
                    turn_id: "turn-cancelled".into(),
                    model_request: request(),
                    config: TurnConfig::default(),
                },
                control,
            )
            .await
            .expect_err("cancelled turn should not start");

        assert!(matches!(error, TurnError::Cancelled));
    }

    struct FailingTool;

    impl ToolExecutor for FailingTool {
        fn execute(&self, _call: ToolCall) -> ToolFuture<'_> {
            Box::pin(async {
                Err(ToolError::Failed {
                    message: "synthetic tool failure".into(),
                })
            })
        }
    }

    #[tokio::test]
    async fn continue_batch_converts_tool_failure_to_error_result() {
        let call = ToolCall {
            id: "call-failed".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let executor = TurnExecutor::with_tools(provider, FailingTool).with_tool_dispatch_policy(
            ToolDispatchPolicy {
                mode: ToolDispatchMode::Serial,
                on_error: ToolErrorPolicy::ContinueBatch,
            },
        );
        let execution = executor
            .execute_with_events(
                TurnRequest {
                    turn_id: "turn-continue-batch".into(),
                    model_request: request(),
                    config: TurnConfig {
                        max_steps: 2,
                        ..TurnConfig::default()
                    },
                },
                TurnControl::default(),
            )
            .await
            .expect("continue-batch should let the model observe the tool error");

        assert_eq!(execution.result.steps.len(), 2);
        assert!(matches!(
            execution.result.outcome,
            TurnOutcome::FinalAnswer { .. }
        ));
        assert!(execution.events.iter().any(|event| matches!(
            event,
            TurnEvent::ToolExecutionFailed { call_id, .. } if call_id == "call-failed"
        )));
        assert!(execution.events.iter().any(|event| matches!(
            event,
            TurnEvent::ToolResult { result, .. } if result.call_id == "call-failed" && result.is_error
        )));
    }

    #[tokio::test]
    async fn policy_plan_blocks_denied_calls_before_tool_execution() {
        let call = ToolCall {
            id: "call-policy-denied".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let executed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut policy = PolicyEngine::default();
        policy.deny_tool("shell.query");
        let executor = TurnExecutor::with_tools(
            provider,
            CountingTool {
                executed: executed.clone(),
            },
        )
        .with_policy_engine(Arc::new(policy))
        .with_tool_dispatch_policy(ToolDispatchPolicy {
            mode: ToolDispatchMode::Serial,
            on_error: ToolErrorPolicy::ContinueBatch,
        });
        let execution = executor
            .execute_with_events(
                TurnRequest {
                    turn_id: "turn-policy-denied".into(),
                    model_request: request(),
                    config: TurnConfig {
                        max_steps: 2,
                        ..TurnConfig::default()
                    },
                },
                TurnControl::default(),
            )
            .await
            .expect("denied policy result should be returned to the model");

        assert_eq!(execution.result.steps.len(), 2);
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(execution.events.iter().any(|event| matches!(
            event,
            TurnEvent::ToolExecutionFailed {
                error: ToolError::PolicyDenied { .. },
                ..
            }
        )));
    }

    #[tokio::test]
    async fn approval_pauses_turn_until_control_approves_the_grant() {
        let call = ToolCall {
            id: "call-approval".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let mut policy = PolicyEngine::default();
        policy.register(kolyan_policy::ToolManifest {
            tool_name: "shell.query".into(),
            capabilities: [kolyan_policy::Capability::ProcessInspect]
                .into_iter()
                .collect(),
            effects: [kolyan_policy::Effect::Read].into_iter().collect(),
            path_scopes: Vec::new(),
            idempotency: kolyan_policy::Idempotency::Idempotent,
            approval: kolyan_policy::ApprovalMode::Always,
        });
        let control = TurnControl::default();
        let task_control = control.clone();
        let executor =
            TurnExecutor::with_tools(provider, MockTool).with_policy_engine(Arc::new(policy));
        let task = tokio::spawn(async move {
            executor
                .execute_with_events(
                    TurnRequest {
                        turn_id: "turn-approval".into(),
                        model_request: request(),
                        config: TurnConfig {
                            max_steps: 2,
                            ..TurnConfig::default()
                        },
                    },
                    task_control,
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!task.is_finished(), "turn should wait for approval");
        control.approve_tool("shell.query");
        let execution = task
            .await
            .expect("approval task should join")
            .expect("approved turn should complete");
        assert!(execution.events.iter().any(|event| matches!(
            event,
            TurnEvent::ApprovalRequested { name, .. } if name == "shell.query"
        )));
        assert!(matches!(
            execution.result.outcome,
            TurnOutcome::FinalAnswer { .. }
        ));
    }

    #[tokio::test]
    async fn durable_approval_resumes_without_replaying_the_first_model_step() {
        let call = ToolCall {
            id: "call-durable".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let model_calls = provider.calls.clone();
        let mut policy = PolicyEngine::default();
        policy.register(kolyan_policy::ToolManifest {
            tool_name: "shell.query".into(),
            capabilities: [kolyan_policy::Capability::ProcessInspect]
                .into_iter()
                .collect(),
            effects: [kolyan_policy::Effect::Read].into_iter().collect(),
            path_scopes: Vec::new(),
            idempotency: kolyan_policy::Idempotency::Idempotent,
            approval: kolyan_policy::ApprovalMode::Always,
        });
        let executed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executor = TurnExecutor::with_tools(
            provider,
            CountingTool {
                executed: executed.clone(),
            },
        )
        .with_policy_engine(Arc::new(policy));
        let awaiting = executor
            .start_resumable(TurnRequest {
                turn_id: "turn-durable".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..TurnConfig::default()
                },
            })
            .await
            .expect("start should return a durable boundary");
        let approval = match awaiting {
            ResumableTurn::AwaitingApproval(value) => *value,
            ResumableTurn::Completed(_) => panic!("expected approval"),
        };
        assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let persisted = serde_json::to_vec(&approval).expect("checkpoint serializes");
        let restored: ApprovalRequest =
            serde_json::from_slice(&persisted).expect("checkpoint restores");
        let completed = executor
            .resume_approval(restored, &approval.approval_id)
            .await
            .expect("resume should complete");
        let execution = match completed {
            ResumableTurn::Completed(value) => *value,
            ResumableTurn::AwaitingApproval(_) => panic!("expected final answer"),
        };
        assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(
            execution.result.outcome,
            TurnOutcome::FinalAnswer { .. }
        ));
        assert_eq!(execution.result.steps.len(), 2);
    }

    #[tokio::test]
    async fn durable_batch_waits_for_all_approvals_before_executing_tools() {
        let provider = MultiApprovalProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let model_calls = provider.calls.clone();
        let mut policy = PolicyEngine::default();
        policy.register(kolyan_policy::ToolManifest {
            tool_name: "shell.query".into(),
            capabilities: [kolyan_policy::Capability::ProcessInspect]
                .into_iter()
                .collect(),
            effects: [kolyan_policy::Effect::Read].into_iter().collect(),
            path_scopes: Vec::new(),
            idempotency: kolyan_policy::Idempotency::Idempotent,
            approval: kolyan_policy::ApprovalMode::Always,
        });
        let executed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executor = TurnExecutor::with_tools(
            provider,
            CountingTool {
                executed: executed.clone(),
            },
        )
        .with_policy_engine(Arc::new(policy));

        let first = executor
            .start_resumable(TurnRequest {
                turn_id: "turn-multi-approval".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..TurnConfig::default()
                },
            })
            .await
            .expect("multi-approval start should succeed");
        let first = match first {
            ResumableTurn::AwaitingApproval(value) => *value,
            ResumableTurn::Completed(_) => panic!("expected first approval"),
        };
        assert_eq!(first.call_id, "call-approval-a");
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);

        let second = executor
            .resume_approval(first, "turn-multi-approval-step-0-approval-call-approval-a")
            .await
            .expect("first approval should expose the second boundary");
        let second = match second {
            ResumableTurn::AwaitingApproval(value) => *value,
            ResumableTurn::Completed(_) => panic!("expected second approval"),
        };
        assert_eq!(second.call_id, "call-approval-b");
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);

        let completed = executor
            .resume_approval(
                second,
                "turn-multi-approval-step-0-approval-call-approval-b",
            )
            .await
            .expect("second approval should execute the batch");
        let execution = match completed {
            ResumableTurn::Completed(value) => *value,
            ResumableTurn::AwaitingApproval(_) => panic!("all approvals were granted"),
        };
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(model_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(matches!(
            execution.result.outcome,
            TurnOutcome::FinalAnswer { .. }
        ));
    }

    #[tokio::test]
    async fn turn_tool_budget_fails_closed_before_executing_the_batch() {
        let call = ToolCall {
            id: "call-budget".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let executed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let execution = TurnExecutor::with_tools(
            provider,
            CountingTool {
                executed: executed.clone(),
            },
        )
        .execute_with_events(
            TurnRequest {
                turn_id: "turn-tool-budget".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    max_tool_calls: Some(0),
                    ..TurnConfig::default()
                },
            },
            TurnControl::default(),
        )
        .await
        .expect_err("tool budget must fail closed");
        assert!(matches!(execution, TurnError::ToolBudgetExceeded));
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn turn_cancellation_propagates_to_a_running_step() {
        let executor = TurnExecutor::new(SlowProvider);
        let control = TurnControl::default();
        let task_control = control.clone();
        let task = tokio::spawn(async move {
            executor
                .execute_with_control(
                    TurnRequest {
                        turn_id: "turn-cancel-running-step".into(),
                        model_request: request(),
                        config: TurnConfig::default(),
                    },
                    task_control,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        control.cancel();
        let error = task
            .await
            .expect("cancelled turn should join")
            .expect_err("cancelled running step must fail");
        assert!(matches!(error, TurnError::Cancelled));
    }

    #[tokio::test]
    async fn turn_deadline_stops_a_slow_model() {
        let error = TurnExecutor::new(SlowProvider)
            .execute_with_control(
                TurnRequest {
                    turn_id: "turn-deadline".into(),
                    model_request: request(),
                    config: TurnConfig {
                        max_steps: 1,
                        deadline: Some(Duration::from_millis(5)),
                        ..TurnConfig::default()
                    },
                },
                TurnControl::default(),
            )
            .await
            .expect_err("turn deadline must stop a slow model");
        assert!(matches!(error, TurnError::TimedOut));
    }

    #[tokio::test]
    async fn durable_approval_rejects_a_tampered_checkpoint() {
        let call = ToolCall {
            id: "call-tampered".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let mut policy = PolicyEngine::default();
        policy.register(kolyan_policy::ToolManifest {
            tool_name: "shell.query".into(),
            capabilities: [kolyan_policy::Capability::ProcessInspect]
                .into_iter()
                .collect(),
            effects: [kolyan_policy::Effect::Read].into_iter().collect(),
            path_scopes: Vec::new(),
            idempotency: kolyan_policy::Idempotency::Idempotent,
            approval: kolyan_policy::ApprovalMode::Always,
        });
        let executor =
            TurnExecutor::with_tools(provider, MockTool).with_policy_engine(Arc::new(policy));
        let awaiting = executor
            .start_resumable(TurnRequest {
                turn_id: "turn-tampered".into(),
                model_request: request(),
                config: TurnConfig {
                    max_steps: 2,
                    ..TurnConfig::default()
                },
            })
            .await
            .expect("start should return a durable boundary");
        let mut approval = match awaiting {
            ResumableTurn::AwaitingApproval(value) => *value,
            ResumableTurn::Completed(_) => panic!("expected approval"),
        };
        approval.continuation.pending_calls[0].arguments =
            serde_json::json!({"command": "delete_everything"});
        let error = executor
            .resume_approval(approval, "turn-tampered-step-0-approval-call-tampered")
            .await
            .expect_err("tampered checkpoint must fail closed");
        assert!(matches!(error, TurnError::InvalidRequest { .. }));
    }

    struct CountingTool {
        executed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ToolExecutor for CountingTool {
        fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
            self.executed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                Ok(ToolResult {
                    call_id: call.id,
                    content: "unexpected execution".into(),
                    is_error: false,
                })
            })
        }
    }

    #[tokio::test]
    async fn parallel_dispatch_preserves_tool_result_order() {
        let call = ToolCall {
            id: "call-parallel".into(),
            name: "shell.query".into(),
            arguments: serde_json::json!({"command": "count_lines"}),
        };
        let provider = ScriptedProvider {
            calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            saw_tool_result: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tool_call: call,
        };
        let executor = TurnExecutor::with_tools(provider, MockTool).with_tool_dispatch_policy(
            ToolDispatchPolicy {
                mode: ToolDispatchMode::Parallel,
                on_error: ToolErrorPolicy::FailTurn,
            },
        );
        let execution = executor
            .execute_with_events(
                TurnRequest {
                    turn_id: "turn-parallel".into(),
                    model_request: request(),
                    config: TurnConfig {
                        max_steps: 2,
                        ..TurnConfig::default()
                    },
                },
                TurnControl::default(),
            )
            .await
            .expect("parallel mode should execute tool calls");

        assert!(matches!(
            execution.result.outcome,
            TurnOutcome::FinalAnswer { .. }
        ));
        assert!(execution.events.iter().any(|event| matches!(
            event,
            TurnEvent::ToolResult { result, .. } if result.call_id == "call-parallel"
        )));
    }

    #[test]
    fn tool_call_batch_rejects_empty_calls() {
        let error = ToolCallBatch::try_from(Vec::new()).expect_err("empty batch must fail");

        assert!(matches!(
            error,
            ToolError::InvalidBatch { message } if message == "tool call batch must not be empty"
        ));
    }

    #[test]
    fn tool_call_batch_rejects_duplicate_call_ids() {
        let calls = vec![
            ToolCall {
                id: "duplicate".into(),
                name: "file.read".into(),
                arguments: serde_json::json!({"path": "a.txt"}),
            },
            ToolCall {
                id: "duplicate".into(),
                name: "file.read".into(),
                arguments: serde_json::json!({"path": "b.txt"}),
            },
        ];

        let error = ToolCallBatch::try_from(calls).expect_err("duplicate ids must fail");

        assert!(matches!(
            error,
            ToolError::InvalidBatch { message } if message == "duplicate tool call id: duplicate"
        ));
    }

    #[test]
    fn tool_call_batch_requires_matching_result_ids() {
        let batch = ToolCallBatch::try_from(vec![ToolCall {
            id: "call-1".into(),
            name: "file.read".into(),
            arguments: serde_json::json!({"path": "a.txt"}),
        }])
        .expect("batch should be valid");
        let result = ToolDispatchResult {
            call_id: "unknown".into(),
            result: Ok(ToolResult {
                call_id: "unknown".into(),
                content: "content".into(),
                is_error: false,
            }),
        };

        let error = batch
            .validate_results(&[result])
            .expect_err("unknown result id must fail");

        assert!(matches!(
            error,
            ToolError::InvalidBatch { message } if message == "unknown tool result call id: unknown"
        ));
    }

    #[test]
    fn tool_call_batch_rejects_result_payload_id_mismatch() {
        let batch = ToolCallBatch::try_from(vec![ToolCall {
            id: "call-1".into(),
            name: "file.read".into(),
            arguments: serde_json::json!({"path": "a.txt"}),
        }])
        .expect("batch should be valid");
        let result = ToolDispatchResult {
            call_id: "call-1".into(),
            result: Ok(ToolResult {
                call_id: "call-2".into(),
                content: "content".into(),
                is_error: false,
            }),
        };

        let error = batch
            .validate_results(&[result])
            .expect_err("payload id mismatch must fail");

        assert!(matches!(
            error,
            ToolError::InvalidBatch { message } if message == "tool result call id mismatch: expected call-1, got call-2"
        ));
    }

    #[test]
    fn turn_state_accepts_tool_loop_transitions() {
        let state = TurnState::Pending
            .transition(TurnState::Running)
            .and_then(|state| state.transition(TurnState::WaitingModel))
            .and_then(|state| state.transition(TurnState::WaitingTool))
            .and_then(|state| state.transition(TurnState::ExecutingTools))
            .and_then(|state| state.transition(TurnState::Running))
            .and_then(|state| state.transition(TurnState::WaitingModel))
            .and_then(|state| state.transition(TurnState::Completed))
            .expect("valid turn tool loop should transition");

        assert_eq!(state, TurnState::Completed);
    }

    #[test]
    fn turn_state_rejects_skipping_model_and_tool_phases() {
        let error = TurnState::Running
            .transition(TurnState::ExecutingTools)
            .expect_err("running must not skip waiting phases");

        assert!(matches!(
            error,
            TurnError::InvalidStateTransition {
                from: TurnState::Running,
                to: TurnState::ExecutingTools
            }
        ));
    }

    #[test]
    fn turn_errors_expose_terminal_reason() {
        assert_eq!(TurnError::Cancelled.end_reason(), TurnEndReason::Cancelled);
        assert_eq!(TurnError::TimedOut.end_reason(), TurnEndReason::TimedOut);
        assert_eq!(TurnError::MaxSteps.end_reason(), TurnEndReason::MaxSteps);
        assert_eq!(
            TurnError::Tool(ToolError::Failed {
                message: "failed".into(),
            })
            .end_reason(),
            TurnEndReason::Failed
        );
        assert_eq!(
            TurnError::Tool(ToolError::Cancelled).end_reason(),
            TurnEndReason::Cancelled
        );
        assert_eq!(
            TurnError::Tool(ToolError::TimedOut).end_reason(),
            TurnEndReason::TimedOut
        );
        assert_eq!(
            TurnError::ToolBudgetExceeded.end_reason(),
            TurnEndReason::Failed
        );
    }
}
