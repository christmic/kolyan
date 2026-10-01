//! Turn port for exact file execution. No authority issuance or legacy executor.

use kolyan_core::{ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::{ToolCall, ToolResult};
use kolyan_sandbox::{SandboxCancellation, SandboxError};

use super::{IsolatedFileError, IsolatedFileTools};

impl ToolExecutor for IsolatedFileTools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        let adapter = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || IsolatedFileTools::prepare(&adapter, call))
                .await
                .map_err(|error| failed(error.to_string()))?
                .map_err(tool_error)
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            if invocation.control.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
            let control = invocation.control.clone();
            let cancellation = SandboxCancellation::default();
            let executing = execute(self, invocation, cancellation.clone());
            tokio::pin!(executing);
            tokio::select! {
                biased;
                () = control.cancelled() => {
                    cancellation.cancel();
                    // Await sandbox kill/reap; future drop still uses its reaper.
                    match executing.await {
                        Ok(_) | Err(ToolError::Cancelled) => Err(ToolError::Cancelled),
                        Err(error) => Err(error),
                    }
                }
                result = &mut executing => result,
            }
        })
    }
}

async fn execute(
    adapter: &IsolatedFileTools,
    invocation: ToolInvocation,
    cancellation: SandboxCancellation,
) -> Result<ToolResult, ToolError> {
    let limit = invocation
        .grant
        .constraints()
        .max_output_bytes
        .and_then(|limit| usize::try_from(limit).ok())
        .ok_or_else(|| failed("missing or overflowing complete result ceiling"))?;
    let call_id = &invocation.prepared.call().id;
    let raw_cap = raw_cap(call_id, limit)?;
    let output = adapter
        .execute_bounded(
            &invocation.prepared,
            &invocation.grant,
            &invocation.policy_revision,
            &invocation.scope,
            cancellation,
            Some(raw_cap),
        )
        .await
        .map_err(tool_error)?;
    let result = ToolResult {
        call_id: call_id.clone(),
        content: serde_json::to_string(&output).map_err(|error| failed(error.to_string()))?,
        is_error: false,
    };
    if serde_json::to_vec(&result)
        .map_err(|error| failed(error.to_string()))?
        .len()
        > limit
    {
        return Err(failed("file result exceeds complete envelope ceiling"));
    }
    Ok(result)
}

fn raw_cap(call_id: &str, limit: usize) -> Result<usize, ToolError> {
    let reserve = serde_json::to_vec(&ToolResult {
        call_id: call_id.into(),
        content: String::new(),
        is_error: false,
    })
    .map_err(|error| failed(error.to_string()))?
    .len();
    // Worker output is already JSON. Its quotes and backslashes at most double
    // when embedded as ToolResult content. Stdin uses a separate trusted ceiling.
    limit
        .checked_sub(reserve)
        .map(|remaining| remaining / 2)
        .filter(|cap| *cap > 0)
        .ok_or_else(|| failed("file result ceiling cannot fit complete framing"))
}

fn tool_error(error: IsolatedFileError) -> ToolError {
    match error {
        IsolatedFileError::Sandbox(SandboxError::Cancelled) => ToolError::Cancelled,
        IsolatedFileError::Sandbox(SandboxError::Timeout) => ToolError::TimedOut,
        IsolatedFileError::Prepared(error) => ToolError::PolicyDenied {
            message: error.to_string(),
        },
        other => failed(other.to_string()),
    }
}

fn failed(message: impl Into<String>) -> ToolError {
    ToolError::Failed {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests;
