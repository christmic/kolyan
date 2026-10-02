//! Read-only attestation is backed by an enforced adapter, not schema filtering.

mod tests;

use kolyan_core::{ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::ToolCall;
use kolyan_policy::{Capability, Effect, PreparedCall};
use kolyan_tools::IsolatedToolSet;

pub struct AuthorityTools {
    inner: IsolatedToolSet,
    read_only: bool,
}

impl AuthorityTools {
    pub(super) fn new(inner: IsolatedToolSet, read_only: bool) -> Self {
        Self { inner, read_only }
    }

    fn validate(&self, prepared: &PreparedCall) -> Result<(), ToolError> {
        if self.read_only
            && (prepared.call().name != "file.read"
                || prepared.claim().capabilities != [Capability::FilesystemRead].into()
                || prepared.claim().effects != [Effect::Read].into()
                || !prepared.requirements().process_sandbox)
        {
            return Err(denied());
        }
        Ok(())
    }
}

impl ToolExecutor for AuthorityTools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            if self.read_only && call.name != "file.read" {
                return Err(denied());
            }
            let prepared = self.inner.prepare(call).await?;
            self.validate(&prepared)?;
            Ok(prepared)
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            self.validate(&invocation.prepared)?;
            // The independently assembled SnapshotTools checks execution ownership.
            // The exact adapter repeats preparation and validates this grant before
            // selecting its Read operation's no-write Seatbelt profile.
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &invocation.policy_revision,
                    &invocation.scope,
                )
                .map_err(|error| ToolError::PolicyDenied {
                    message: error.to_string(),
                })?;
            self.inner.execute_invocation(invocation).await
        })
    }
}

fn denied() -> ToolError {
    ToolError::PolicyDenied {
        message: "trusted read-only adapter refuses writable or non-file authority".into(),
    }
}
