//! Host verification for durable external work. Runtime validates exact tool
//! authority independently; Server interprets topology and journal evidence.

use std::future::Future;
use std::pin::Pin;

use kolyan_core::{ExternalWait, IssuedToolAuthority, ToolError};
use kolyan_model::ToolResult;
use kolyan_policy::ToolExecutionScope;

pub type ExternalVerificationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ToolError>> + Send + 'a>>;

/// Historical authority and exact wait returned by an admitted tool. Possessing
/// these serialized values does not prove that work was durably admitted.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalWaitContext {
    pub issued: IssuedToolAuthority,
    pub wait: ExternalWait,
}

impl ExternalWaitContext {
    /// Validate framing and binding against independently loaded Runtime scope.
    /// The host verifier must additionally resolve the durable admission facts.
    pub fn validate(&self, expected_scope: &ToolExecutionScope) -> Result<(), ToolError> {
        self.issued
            .validate(expected_scope)
            .and_then(|()| self.wait.validate())
            .map_err(|error| ToolError::PolicyDenied {
                message: format!("invalid external wait binding: {error}"),
            })
    }

    /// Keep immediate and delayed results under the same original grant. This
    /// validates content framing, not external admission or terminal proof.
    pub fn validate_result(
        &self,
        expected_scope: &ToolExecutionScope,
        result: &ToolResult,
    ) -> Result<(), ToolError> {
        self.validate(expected_scope)?;
        self.issued
            .validate_result(result)
            .map_err(|error| ToolError::PolicyDenied {
                message: format!("invalid external result binding: {error}"),
            })
    }
}

/// Trusted host port, not a model-selected plugin or permission grant. Methods
/// verify supported kind/version, exact admission ownership and committed result
/// evidence. They must not execute children, poll models or issue fresh grants.
/// Async implementations must move blocking journal reads off the async worker.
pub trait ExternalWaitVerifier: Send + Sync {
    fn verify_wait(&self, context: ExternalWaitContext) -> ExternalVerificationFuture<'_>;

    /// Verify this exact result against durable terminal/consumption evidence.
    /// Runtime still enforces original identity, authority and output ceilings.
    fn verify_result(
        &self,
        context: ExternalWaitContext,
        result: ToolResult,
    ) -> ExternalVerificationFuture<'_>;
}

/// No host verifier means no external waiting or result acceptance. There is no
/// allow-all fallback for unknown kinds, versions or missing evidence.
#[derive(Debug, Clone, Copy, Default)]
pub struct RefuseExternalWaits;

impl ExternalWaitVerifier for RefuseExternalWaits {
    fn verify_wait(&self, _: ExternalWaitContext) -> ExternalVerificationFuture<'_> {
        Box::pin(async { Err(not_configured()) })
    }

    fn verify_result(
        &self,
        _: ExternalWaitContext,
        _: ToolResult,
    ) -> ExternalVerificationFuture<'_> {
        Box::pin(async { Err(not_configured()) })
    }
}

fn not_configured() -> ToolError {
    ToolError::PolicyDenied {
        message: "host external-wait verification is not configured".into(),
    }
}

#[cfg(test)]
mod tests;
