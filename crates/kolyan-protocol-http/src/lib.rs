//! Pure bounded HTTP-opening policy, not SSE parsing or request execution.

mod policy;
mod report;

pub use policy::{
    HttpRetryPolicy, PolicyError, RetryDecision, RetryHeaders, RetryInput, RetryProfile,
    StopReason, WaitBasis,
};
pub use report::{ReportError, ResponseWithRetryReport, RetryObservation, RetryReport};

#[cfg(test)]
mod tests;
