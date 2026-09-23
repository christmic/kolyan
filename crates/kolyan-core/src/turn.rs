use crate::{StepError, StepExecutionOptions, StepExecutor, StepOutcome, StepRequest, StepResult};
use futures_core::Stream;
use futures_util::task::AtomicWaker;
use futures_util::{future::join_all, stream};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelProvider, ModelRequest, ToolCall, ToolChoice,
    ToolResult,
};
use kolyan_policy::{ExecutionGrant, PolicyContext, PolicyDecisionKind, PolicyEngine};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;

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
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self { max_steps: 1 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolDispatchMode {
    Serial,
    Parallel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolErrorPolicy {
    FailTurn,
    ContinueBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    Completed {
        turn_id: String,
        outcome: TurnOutcome,
    },
}

pub type TurnEventStream = Pin<Box<dyn Stream<Item = Result<TurnEvent, TurnError>> + Send>>;

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
            | Self::Step(_)
            | Self::Tool(_)
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
            .contains(&self.tool_name)
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
            .contains(&self.tool_name)
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
}

impl<P> TurnExecutor<P, NoopToolExecutor> {
    pub fn new(provider: P) -> Self {
        Self {
            step_executor: StepExecutor::new(provider),
            tool_executor: NoopToolExecutor,
            tool_dispatch: ToolDispatchPolicy::default(),
            tool_timeout: None,
            policy_engine: None,
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
        }
    }

    pub fn with_step_executor(step_executor: StepExecutor<P>, tool_executor: T) -> Self {
        Self {
            step_executor,
            tool_executor,
            tool_dispatch: ToolDispatchPolicy::default(),
            tool_timeout: None,
            policy_engine: None,
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

    pub fn with_policy_engine(mut self, policy_engine: Arc<PolicyEngine>) -> Self {
        self.policy_engine = Some(policy_engine);
        self
    }
}

impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    /// Starts an execution that can cross a process boundary at approval.
    ///
    /// Unlike execute_with_events, this method never waits on TurnControl.
    /// It returns a serializable checkpoint when policy requires approval.
    pub async fn start_resumable(&self, request: TurnRequest) -> Result<ResumableTurn, TurnError> {
        validate_request(&request)?;
        let turn_id = request.turn_id.clone();
        let mut model_request = request.model_request.clone();
        let step_id = format!("{}-step-0", request.turn_id);
        model_request.request_id = step_id.clone();
        let step = self
            .step_executor
            .execute(StepRequest {
                step_id: step_id.clone(),
                model_request: model_request.clone(),
                options: StepExecutionOptions::default(),
            })
            .await
            .map_err(map_step_error)?;
        let mut events = vec![
            TurnEvent::Started {
                turn_id: turn_id.clone(),
            },
            TurnEvent::StepStarted {
                turn_id: turn_id.clone(),
                step_id,
            },
            TurnEvent::StepCompleted {
                turn_id: turn_id.clone(),
                step: step.clone(),
            },
        ];
        match step.outcome {
            StepOutcome::FinalAnswer | StepOutcome::Refused | StepOutcome::Incomplete => {
                let (outcome, end_reason) = outcome_from_step(&step);
                events.push(TurnEvent::Completed {
                    turn_id: turn_id.clone(),
                    outcome: outcome.clone(),
                });
                return Ok(ResumableTurn::Completed(Box::new(TurnExecution {
                    result: TurnResult {
                        turn_id,
                        outcome,
                        end_reason,
                        steps: vec![step],
                    },
                    events,
                })));
            }
            StepOutcome::ToolCalls => {}
        }

        let batch = ToolCallBatch::try_from(tool_calls(&step.response.content))?;
        let Some(policy) = &self.policy_engine else {
            return Err(TurnError::InvalidRequest {
                message: "resumable approval requires a policy engine".into(),
            });
        };
        let plan = policy.resolve_batch(
            &PolicyContext {
                turn_id: Some(request.turn_id.clone()),
                ..PolicyContext::default()
            },
            batch.calls(),
        );
        let Some(decision) = plan
            .decisions
            .iter()
            .find(|item| item.decision.kind == PolicyDecisionKind::RequireApproval)
        else {
            return Err(TurnError::InvalidRequest {
                message: "resumable start requires an approval boundary".into(),
            });
        };
        let call = batch
            .call(&decision.call_id)
            .expect("planned call exists")
            .clone();
        let pending_calls = batch.into_calls();
        events.push(TurnEvent::ToolCallRequested {
            turn_id: request.turn_id.clone(),
            call: call.clone(),
        });
        events.push(TurnEvent::ApprovalRequested {
            turn_id: request.turn_id.clone(),
            call_id: call.id.clone(),
            name: call.name.clone(),
        });
        Ok(ResumableTurn::AwaitingApproval(Box::new(
            make_approval_request(
                &request,
                model_request,
                vec![step],
                pending_calls,
                &call,
                decision.decision.reason.clone(),
                decision.decision.policy_version.clone(),
            ),
        )))
    }

    /// Resumes a persisted approval without relying on the original task or
    /// process. The approval id is supplied separately to prevent accidental
    /// acceptance of a checkpoint for another user decision.
    pub async fn resume_approval(
        &self,
        approval: ApprovalRequest,
        approved_approval_id: &str,
    ) -> Result<ResumableTurn, TurnError> {
        if approval.approval_id != approved_approval_id
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
        if approval
            .expires_at_ms
            .is_some_and(|deadline| deadline <= now_ms())
        {
            return Err(TurnError::InvalidRequest {
                message: "approval checkpoint has expired".into(),
            });
        }
        let continuation = approval.continuation;
        let Some(call) = continuation
            .pending_calls
            .iter()
            .find(|call| call.id == continuation.call_id)
            .cloned()
        else {
            return Err(TurnError::InvalidRequest {
                message: "checkpoint is missing its pending call".into(),
            });
        };
        if call.name != continuation.tool_name
            || serde_json::to_string(&call.arguments).map_err(|error| {
                TurnError::InvalidRequest {
                    message: format!("cannot fingerprint tool arguments: {error}"),
                }
            })? != continuation.args_fingerprint
        {
            return Err(TurnError::InvalidRequest {
                message: "checkpoint tool call was modified".into(),
            });
        }
        let Some(policy) = &self.policy_engine else {
            return Err(TurnError::InvalidRequest {
                message: "resumable approval requires a policy engine".into(),
            });
        };
        let plan = policy.resolve_batch(
            &PolicyContext {
                turn_id: Some(continuation.turn_id.clone()),
                ..PolicyContext::default()
            },
            &continuation.pending_calls,
        );
        let Some(decision) = plan
            .decisions
            .iter()
            .find(|item| item.call_id == continuation.call_id)
        else {
            return Err(TurnError::InvalidRequest {
                message: "checkpoint call is absent from the current policy plan".into(),
            });
        };
        if decision.decision.kind != PolicyDecisionKind::RequireApproval
            || decision.decision.policy_version != continuation.policy_version
        {
            return Err(TurnError::InvalidRequest {
                message: "approval is stale under the current policy".into(),
            });
        }
        let grant = decision
            .decision
            .clone()
            .into_approved_grant(&call)
            .map_err(|error| TurnError::InvalidRequest {
                message: error.to_string(),
            })?;
        let control = TurnControl::default();
        let tool_result = self
            .execute_tool(call.clone(), &control, Some(grant))
            .await?;
        let mut events = vec![
            TurnEvent::Started {
                turn_id: continuation.turn_id.clone(),
            },
            TurnEvent::ToolExecutionStarted {
                turn_id: continuation.turn_id.clone(),
                call_id: call.id.clone(),
                name: call.name.clone(),
            },
            TurnEvent::ToolResult {
                turn_id: continuation.turn_id.clone(),
                result: tool_result.clone(),
            },
        ];
        let mut model_request = continuation.model_request;
        append_tool_context(
            &mut model_request.messages,
            &continuation.assistant_content,
            vec![tool_result],
        );
        model_request.tool_choice = ToolChoice::Auto;
        let step_id = format!(
            "{}-step-{}",
            continuation.turn_id, continuation.next_step_index
        );
        model_request.request_id = step_id.clone();
        let step = self
            .step_executor
            .execute(StepRequest {
                step_id: step_id.clone(),
                model_request: model_request.clone(),
                options: StepExecutionOptions::default(),
            })
            .await
            .map_err(map_step_error)?;
        events.push(TurnEvent::StepStarted {
            turn_id: continuation.turn_id.clone(),
            step_id,
        });
        events.push(TurnEvent::StepCompleted {
            turn_id: continuation.turn_id.clone(),
            step: step.clone(),
        });
        if step.outcome != StepOutcome::ToolCalls {
            let (outcome, end_reason) = outcome_from_step(&step);
            events.push(TurnEvent::Completed {
                turn_id: continuation.turn_id.clone(),
                outcome: outcome.clone(),
            });
            return Ok(ResumableTurn::Completed(Box::new(TurnExecution {
                result: TurnResult {
                    turn_id: continuation.turn_id,
                    outcome,
                    end_reason,
                    steps: continuation
                        .steps
                        .into_iter()
                        .chain(std::iter::once(step))
                        .collect(),
                },
                events,
            })));
        }
        let next_batch = ToolCallBatch::try_from(tool_calls(&step.response.content))?;
        let next_plan = policy.resolve_batch(
            &PolicyContext {
                turn_id: Some(continuation.turn_id.clone()),
                ..PolicyContext::default()
            },
            next_batch.calls(),
        );
        let Some(next_decision) = next_plan
            .decisions
            .iter()
            .find(|item| item.decision.kind == PolicyDecisionKind::RequireApproval)
        else {
            return Err(TurnError::InvalidRequest {
                message: "resumable v1 requires the next step to end or request approval".into(),
            });
        };
        let next_call = next_batch
            .call(&next_decision.call_id)
            .expect("planned call exists")
            .clone();
        let next_pending_calls = next_batch.into_calls();
        let mut steps = continuation.steps;
        steps.push(step);
        let next_request = TurnRequest {
            turn_id: continuation.turn_id.clone(),
            model_request: model_request.clone(),
            config: TurnConfig {
                max_steps: continuation.max_steps,
            },
        };
        Ok(ResumableTurn::AwaitingApproval(Box::new(
            make_approval_request(
                &next_request,
                model_request,
                steps,
                next_pending_calls,
                &next_call,
                next_decision.decision.reason.clone(),
                next_decision.decision.policy_version.clone(),
            ),
        )))
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

    async fn execute_tool(
        &self,
        call: ToolCall,
        control: &TurnControl,
        grant: Option<ExecutionGrant>,
    ) -> Result<ToolResult, ToolError> {
        let execute = match grant {
            Some(grant) => self.tool_executor.execute_with_grant(call, grant),
            None => self.tool_executor.execute(call),
        };
        let execute_with_cancel = async {
            tokio::select! {
                result = execute => result,
                _ = control.wait_cancelled() => Err(ToolError::Cancelled),
            }
        };
        if let Some(timeout) = self.tool_timeout {
            tokio::time::timeout(timeout, execute_with_cancel)
                .await
                .unwrap_or(Err(ToolError::TimedOut))
        } else {
            execute_with_cancel.await
        }
    }

    async fn execute_batch(
        &self,
        batch: &ToolCallBatch,
        turn_id: &str,
        control: &TurnControl,
        events: &mut Vec<TurnEvent>,
    ) -> Vec<ToolDispatchResult> {
        let Some(policy) = &self.policy_engine else {
            return match self.tool_dispatch.mode {
                ToolDispatchMode::Serial => {
                    let mut results = Vec::with_capacity(batch.len());
                    for call in batch.calls() {
                        results.push(ToolDispatchResult {
                            call_id: call.id.clone(),
                            result: self.execute_tool(call.clone(), control, None).await,
                        });
                    }
                    results
                }
                ToolDispatchMode::Parallel => {
                    let futures = batch
                        .calls()
                        .iter()
                        .cloned()
                        .map(|call| self.execute_tool(call, control, None));
                    join_all(futures)
                        .await
                        .into_iter()
                        .zip(batch.calls())
                        .map(|(result, call)| ToolDispatchResult {
                            call_id: call.id.clone(),
                            result,
                        })
                        .collect()
                }
            };
        };

        let plan = policy.resolve_batch(
            &PolicyContext {
                turn_id: Some(turn_id.to_string()),
                ..PolicyContext::default()
            },
            batch.calls(),
        );
        let mut grants = std::collections::HashMap::new();
        let mut results = batch
            .calls()
            .iter()
            .filter_map(|call| {
                let decision = plan.decisions.iter().find(|item| item.call_id == call.id)?;
                match decision.decision.kind {
                    PolicyDecisionKind::Allow | PolicyDecisionKind::AllowWithConstraints => {
                        let grant = decision
                            .decision
                            .clone()
                            .into_grant(call)
                            .expect("allow decision must produce a grant");
                        grants.insert(call.id.clone(), grant);
                        None
                    }
                    PolicyDecisionKind::RequireApproval => None,
                    PolicyDecisionKind::Deny => Some(ToolDispatchResult {
                        call_id: call.id.clone(),
                        result: Err(ToolError::PolicyDenied {
                            message: decision.decision.reason.clone(),
                        }),
                    }),
                }
            })
            .collect::<Vec<_>>();

        let mut stages = plan.stages;
        for decision in &plan.decisions {
            if decision.decision.kind != PolicyDecisionKind::RequireApproval {
                continue;
            }
            let call = batch
                .call(&decision.call_id)
                .expect("policy call must be in batch");
            events.push(TurnEvent::ApprovalRequested {
                turn_id: turn_id.to_string(),
                call_id: call.id.clone(),
                name: call.name.clone(),
            });
            control.wait_for_tool_approval(&call.name).await;
            let grant = decision
                .decision
                .clone()
                .into_approved_grant(call)
                .expect("approved call must produce a grant");
            grants.insert(call.id.clone(), grant);
            stages.push(vec![call.id.clone()]);
        }

        for stage in stages {
            let calls = stage
                .iter()
                .filter_map(|call_id| batch.call(call_id).cloned())
                .collect::<Vec<_>>();
            let stage_results = match self.tool_dispatch.mode {
                ToolDispatchMode::Serial => {
                    let mut stage_results = Vec::with_capacity(calls.len());
                    for call in calls {
                        stage_results.push(ToolDispatchResult {
                            call_id: call.id.clone(),
                            result: self
                                .execute_tool(call.clone(), control, grants.remove(&call.id))
                                .await,
                        });
                    }
                    stage_results
                }
                ToolDispatchMode::Parallel => {
                    let futures = calls.into_iter().map(|call| {
                        let grant = grants.get(&call.id).cloned();
                        async move {
                            let call_id = call.id.clone();
                            ToolDispatchResult {
                                call_id,
                                result: self.execute_tool(call, control, grant).await,
                            }
                        }
                    });
                    join_all(futures).await
                }
            };
            results.extend(stage_results);
        }
        results.sort_by_key(|result| {
            batch
                .calls()
                .iter()
                .position(|call| call.id == result.call_id)
                .unwrap_or(usize::MAX)
        });
        results
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
        validate_request(&request)?;
        if control.is_cancelled() {
            return Err(TurnError::Cancelled);
        }

        let turn_id = request.turn_id.clone();
        let mut model_request = request.model_request;
        let mut steps = Vec::new();
        let mut events = vec![TurnEvent::Started {
            turn_id: turn_id.clone(),
        }];
        let mut state = TurnState::Pending.transition(TurnState::Running)?;

        for step_index in 0..request.config.max_steps {
            if control.is_cancelled() {
                return Err(TurnError::Cancelled);
            }

            state = state.transition(TurnState::WaitingModel)?;
            let step_id = format!("{}-step-{step_index}", request.turn_id);
            events.push(TurnEvent::StepStarted {
                turn_id: turn_id.clone(),
                step_id: step_id.clone(),
            });
            model_request.request_id = step_id.clone();
            let step = self
                .step_executor
                .execute(StepRequest {
                    step_id,
                    model_request: model_request.clone(),
                    options: StepExecutionOptions::default(),
                })
                .await
                .map_err(map_step_error)?;
            steps.push(step.clone());
            events.push(TurnEvent::StepCompleted {
                turn_id: turn_id.clone(),
                step: step.clone(),
            });

            match step.outcome {
                StepOutcome::FinalAnswer => {
                    let result = TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::FinalAnswer {
                            response: step.response,
                        },
                        end_reason: TurnEndReason::FinalAnswer,
                        steps,
                    };
                    let _ = state.transition(TurnState::Completed)?;
                    events.push(TurnEvent::Completed {
                        turn_id: turn_id.clone(),
                        outcome: result.outcome.clone(),
                    });
                    return Ok(TurnExecution { result, events });
                }
                StepOutcome::Refused => {
                    let result = TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::Refused {
                            response: step.response,
                        },
                        end_reason: TurnEndReason::Refused,
                        steps,
                    };
                    let _ = state.transition(TurnState::Completed)?;
                    events.push(TurnEvent::Completed {
                        turn_id: turn_id.clone(),
                        outcome: result.outcome.clone(),
                    });
                    return Ok(TurnExecution { result, events });
                }
                StepOutcome::Incomplete => {
                    let result = TurnResult {
                        turn_id: request.turn_id,
                        outcome: TurnOutcome::Incomplete {
                            response: step.response,
                        },
                        end_reason: TurnEndReason::Incomplete,
                        steps,
                    };
                    let _ = state.transition(TurnState::Completed)?;
                    events.push(TurnEvent::Completed {
                        turn_id: turn_id.clone(),
                        outcome: result.outcome.clone(),
                    });
                    return Ok(TurnExecution { result, events });
                }
                StepOutcome::ToolCalls => {
                    state = state.transition(TurnState::WaitingTool)?;
                    let batch = ToolCallBatch::try_from(tool_calls(&step.response.content))?;
                    state = state.transition(TurnState::ExecutingTools)?;
                    for call in batch.calls() {
                        events.push(TurnEvent::ToolCallRequested {
                            turn_id: turn_id.clone(),
                            call: call.clone(),
                        });
                        events.push(TurnEvent::ToolExecutionStarted {
                            turn_id: turn_id.clone(),
                            call_id: call.id.clone(),
                            name: call.name.clone(),
                        });
                    }
                    let results = self
                        .execute_batch(&batch, &turn_id, &control, &mut events)
                        .await;
                    batch.validate_results(&results)?;
                    let mut tool_results = Vec::with_capacity(results.len());
                    for dispatch in results {
                        let call = batch.call(&dispatch.call_id).expect("validated call id");
                        match dispatch.result {
                            Ok(result) => {
                                events.push(TurnEvent::ToolResult {
                                    turn_id: turn_id.clone(),
                                    result: result.clone(),
                                });
                                tool_results.push(result);
                            }
                            Err(error) => {
                                events.push(TurnEvent::ToolExecutionFailed {
                                    turn_id: turn_id.clone(),
                                    call_id: call.id.clone(),
                                    name: call.name.clone(),
                                    error: error.clone(),
                                });
                                match self.tool_dispatch.on_error {
                                    ToolErrorPolicy::FailTurn => {
                                        let _ = state.transition(TurnState::Failed)?;
                                        return Err(TurnError::Tool(error));
                                    }
                                    ToolErrorPolicy::ContinueBatch => {
                                        let result = ToolResult {
                                            call_id: call.id.clone(),
                                            content: error.to_string(),
                                            is_error: true,
                                        };
                                        events.push(TurnEvent::ToolResult {
                                            turn_id: turn_id.clone(),
                                            result: result.clone(),
                                        });
                                        tool_results.push(result);
                                    }
                                }
                            }
                        }
                    }
                    append_tool_context(
                        &mut model_request.messages,
                        &step.response.content,
                        tool_results,
                    );
                    state = state.transition(TurnState::Running)?;
                    model_request.tool_choice = ToolChoice::Auto;
                }
            }
        }

        let _ = state.transition(TurnState::MaxSteps)?;
        Err(TurnError::MaxSteps)
    }

    pub async fn execute_event_stream(
        &self,
        request: TurnRequest,
        control: TurnControl,
    ) -> Result<TurnEventStream, TurnError> {
        let execution = self.execute_with_events(request, control).await?;
        let events = execution.events.into_iter().map(Ok);
        Ok(Box::pin(stream::iter(events)))
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
    let events = vec![
        TurnEvent::Started {
            turn_id: turn_id.clone(),
        },
        TurnEvent::Completed {
            turn_id: turn_id.clone(),
            outcome: outcome.clone(),
        },
    ];
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

fn make_approval_request(
    request: &TurnRequest,
    model_request: ModelRequest,
    steps: Vec<StepResult>,
    pending_calls: Vec<ToolCall>,
    call: &ToolCall,
    reason: String,
    policy_version: String,
) -> ApprovalRequest {
    let args_fingerprint =
        serde_json::to_string(&call.arguments).expect("JSON tool arguments must be serializable");
    let approval_id = format!("{}-{}", request.turn_id, call.id);
    ApprovalRequest {
        approval_id: approval_id.clone(),
        turn_id: request.turn_id.clone(),
        call_id: call.id.clone(),
        tool_name: call.name.clone(),
        reason,
        state: ApprovalState::Pending,
        expires_at_ms: None,
        continuation: TurnContinuation {
            continuation_id: format!("continuation-{}", approval_id),
            approval_id,
            turn_id: request.turn_id.clone(),
            model_request,
            assistant_content: steps
                .last()
                .map(|step| step.response.content.clone())
                .unwrap_or_default(),
            pending_calls,
            steps,
            max_steps: request.config.max_steps,
            next_step_index: request
                .config
                .max_steps
                .saturating_sub(request.config.max_steps.saturating_sub(1)),
            call_id: call.id.clone(),
            tool_name: call.name.clone(),
            args_fingerprint,
            policy_version,
        },
    }
}

fn map_step_error(error: StepError) -> TurnError {
    match error {
        StepError::Cancelled => TurnError::Cancelled,
        StepError::TimedOut => TurnError::TimedOut,
        error => TurnError::Step(error),
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
                config: TurnConfig { max_steps: 2 },
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
                    config: TurnConfig { max_steps: 2 },
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
                    config: TurnConfig { max_steps: 2 },
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
                        config: TurnConfig { max_steps: 2 },
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
                config: TurnConfig { max_steps: 2 },
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
                config: TurnConfig { max_steps: 2 },
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
            .resume_approval(approval, "turn-tampered-call-tampered")
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
                    config: TurnConfig { max_steps: 2 },
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
    }
}
