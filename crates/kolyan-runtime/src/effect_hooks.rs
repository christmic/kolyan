//! Host-owned effect gates. No script protocol, grants, retries or effect replay.

use std::future::Future;
use std::pin::Pin;

use kolyan_core::{IssuedToolAuthority, ToolError, ToolExecutionWindow, TurnControl};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ToolResult;
use thiserror::Error;

use crate::reconciliation::receipt::{PreparedEvidence, check_event};

pub type EffectHookFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, EffectHookError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectHookDecision {
    Continue,
    Denied { reason: String },
}

/// Host failures are not model argument errors or successful policy decisions.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EffectHookError {
    #[error("effect hook host failure: {message}")]
    Host { message: String },
    #[error("effect hook observation incomplete: {message}")]
    Incomplete { message: String },
    #[error("effect hook cancelled")]
    Cancelled,
    #[error("effect hook execution window exhausted")]
    TimedOut,
}

/// Exact Runtime-verified authority plus the original live execution window.
/// No deserialization, default authority or timeout renewal is supported.
#[derive(Clone)]
pub struct EffectHookContext {
    issued: IssuedToolAuthority,
    effect_id: String,
    input_digest: String,
    control: TurnControl,
    window: ToolExecutionWindow,
}

impl EffectHookContext {
    pub(crate) fn new(
        bound: &PreparedEvidence,
        control: TurnControl,
        window: ToolExecutionWindow,
    ) -> Self {
        Self {
            issued: bound.issued(),
            effect_id: bound.request.effect_id.clone(),
            input_digest: bound.request.input_digest.clone(),
            control,
            window,
        }
    }

    pub fn issued(&self) -> &IssuedToolAuthority {
        &self.issued
    }

    pub fn effect_id(&self) -> &str {
        &self.effect_id
    }

    pub fn input_digest(&self) -> &str {
        &self.input_digest
    }

    pub fn control(&self) -> &TurnControl {
        &self.control
    }

    pub fn window(&self) -> ToolExecutionWindow {
        self.window
    }

    pub(crate) fn check_live(&self) -> Result<(), EffectHookError> {
        if self.control.is_cancelled() {
            return Err(EffectHookError::Cancelled);
        }
        if self.window.remaining().is_zero() {
            return Err(EffectHookError::TimedOut);
        }
        Ok(())
    }

    pub(crate) async fn wait<T>(
        &self,
        future: EffectHookFuture<'_, T>,
    ) -> Result<T, EffectHookError> {
        self.check_live()?;
        tokio::select! {
            biased;
            () = self.control.cancelled() => Err(EffectHookError::Cancelled),
            result = tokio::time::timeout_at(self.window.deadline().into(), future) =>
                result.unwrap_or(Err(EffectHookError::TimedOut)),
        }
    }
}

/// Read-only receipt capability backed by an exact acknowledged Ledger event.
/// Hooks cannot construct or rewrite this value from an arbitrary ToolResult.
#[derive(Debug, Clone)]
pub struct CommittedEffectReceipt {
    event: LedgerEvent,
    result: Result<ToolResult, ToolError>,
}

impl CommittedEffectReceipt {
    pub(crate) fn from_ack<L: LedgerStore>(
        ledger: &L,
        bound: &PreparedEvidence,
        event: LedgerEvent,
    ) -> Result<Self, EffectHookError> {
        let validate = || -> Result<_, ToolError> {
            check_event(
                &event,
                &bound.scope.execution,
                &format!("{}/receipt", bound.prefix()),
                LedgerEventKind::EffectReceipt,
            )?;
            let actual =
                ledger
                    .event_by_id(&event.event_id)
                    .map_err(|error| ToolError::InvalidBatch {
                        message: error.to_string(),
                    })?;
            if event.cursor == 0 || actual.as_ref() != Some(&event) {
                return Err(ToolError::InvalidBatch {
                    message: "committed receipt acknowledgement differs from Ledger".into(),
                });
            }
            bound.validate_receipt(&event.payload)
        };
        let result = validate().map_err(|error| EffectHookError::Host {
            message: format!("committed receipt is not proven: {error}"),
        })?;
        Ok(Self { event, result })
    }

    pub fn event(&self) -> &LedgerEvent {
        &self.event
    }

    /// The unmodified result already persisted in the target effect receipt.
    pub fn result(&self) -> &Result<ToolResult, ToolError> {
        &self.result
    }
}

/// Optional trusted host consumer, separate from tool grants and model routing.
///
/// Before effects, implement current hook policy/revocation checks and persist
/// hook intent/results. After receipts, commit independent observation proof
/// before returning success. Verification only reads exact durable proof: it
/// must reject missing, foreign, failed or interrupted observations, never run
/// hooks, poll tools, repair facts or authorize replay. All methods must retain
/// the supplied control/window and move blocking I/O off the async worker.
pub trait EffectHookPort: Send + Sync {
    fn before_effect(&self, context: EffectHookContext)
    -> EffectHookFuture<'_, EffectHookDecision>;

    fn after_receipt(
        &self,
        context: EffectHookContext,
        receipt: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()>;

    fn verify_observation(
        &self,
        context: EffectHookContext,
        receipt: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()>;
}

pub(crate) fn entry_error(error: EffectHookError) -> ToolError {
    match error {
        EffectHookError::Cancelled => ToolError::Cancelled,
        EffectHookError::TimedOut => ToolError::TimedOut,
        error => ToolError::InvalidBatch {
            message: format!("before_effect host failure: {error}"),
        },
    }
}

pub(crate) fn observation_error(
    phase: &str,
    event: &LedgerEvent,
    error: EffectHookError,
) -> ToolError {
    ToolError::InvalidBatch {
        message: format!(
            "{phase} host failure after committed receipt {} cursor {}: {error}",
            event.event_id, event.cursor
        ),
    }
}
