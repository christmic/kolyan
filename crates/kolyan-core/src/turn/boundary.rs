use super::{TurnEndReason, TurnError};
use std::future::Future;
use std::pin::Pin;

/// A logical operation that must be admitted before Core starts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnBoundary {
    pub turn_id: String,
    pub kind: TurnBoundaryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnBoundaryKind {
    Step { step_id: String },
    Tool { step_id: String, call_id: String },
    AwaitingApproval { approval_id: String },
    ResumeApproval { approval_id: String },
    Terminal { reason: TurnEndReason },
}

pub type TurnBoundaryFuture<'a> = Pin<Box<dyn Future<Output = Result<(), TurnError>> + Send + 'a>>;

/// Runtime-owned admission port, independent of storage and process lifetime.
///
/// Implementations atomically order cancellation against admission. Cancellation
/// that precedes admission returns `TurnError::Cancelled`; an admitted operation
/// may finish, but later operations require new admission. Terminal admission
/// also orders completion against cancellation. Errors must fail closed.
///
/// This port does not provide exactly-once effects. A durable implementation
/// owns transaction identities, receipts, and execution fencing separately.
pub trait TurnBoundaryControl: Send + Sync {
    fn admit(&self, boundary: TurnBoundary) -> TurnBoundaryFuture<'_>;
}
