//! Turn port for the trusted shell adapter. No authority is issued here.
//!
//! Result content is JSON: `{"exit_code":0,"stdout":{"encoding":"utf8",
//! "data":"readable text"},"stderr":{"encoding":"hex","data":"ff"}}`.
//! Each stream independently preserves valid UTF-8 text; invalid UTF-8 uses
//! lowercase hexadecimal, two characters per original byte. Empty streams are
//! UTF-8. No lossy decoding is used. Null exit means signal termination; any
//! exit other than zero sets `is_error`. The granted output limit bounds the whole
//! serialized `ToolResult`, including call ID and escaped content, not just raw
//! pipes. Reserve the longest tags and worst exit-code/error framing, then allow
//! eight envelope bytes per raw byte. A control byte can become six inner JSON
//! bytes (`\u0000`) and seven after outer escaping; quotes/backslashes and hex
//! also fit this conservative factor. Insufficient space or overflow is an
//! explicit error, never truncation.
//! Cancellation awaits the process owner after signalling; future drop delegates
//! to the sandbox's independent reaper. Effects are not rolled back or retried.

use kolyan_core::{ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::{ToolCall, ToolResult};
use kolyan_sandbox::{
    SandboxCancellation, SandboxCommand, SandboxError, SandboxOutput, SandboxRequest,
};
use serde::Serialize;

use super::{IsolatedShellError, IsolatedShellTool};

#[derive(Serialize)]
struct ShellContent {
    exit_code: Option<i32>,
    stdout: ShellStream,
    stderr: ShellStream,
}

#[derive(Serialize)]
struct ShellStream {
    encoding: &'static str,
    data: String,
}

impl ToolExecutor for IsolatedShellTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        let adapter = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || IsolatedShellTool::prepare(&adapter, call))
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
                    // Do not drop the execution future on cancellation: returning
                    // requires the sandbox owner to finish its kill/reap sequence.
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
    adapter: &IsolatedShellTool,
    invocation: ToolInvocation,
    cancellation: SandboxCancellation,
) -> Result<ToolResult, ToolError> {
    let worker = adapter.clone();
    let call = invocation.prepared.call().clone();
    let (current, arguments, cwd, sandbox) = tokio::task::spawn_blocking(move || {
        let (current, arguments, cwd) = worker.prepare_with_context(call)?;
        Ok::<_, IsolatedShellError>((current, arguments, cwd, worker.sandbox()?))
    })
    .await
    .map_err(|error| failed(error.to_string()))?
    .map_err(tool_error)?;
    if current.digest() != invocation.prepared.digest() {
        return Err(tool_error(IsolatedShellError::Prepared(
            kolyan_policy::PreparedError::BindingMismatch,
        )));
    }
    invocation
        .grant
        .validate(
            &invocation.prepared,
            &invocation.policy_revision,
            &invocation.scope,
        )
        .map_err(|error| tool_error(IsolatedShellError::Prepared(error)))?;
    let limit = invocation
        .grant
        .constraints()
        .max_output_bytes
        .and_then(|limit| usize::try_from(limit).ok())
        .ok_or_else(|| failed("missing or overflowing result ceiling"))?;
    let call_id = &invocation.prepared.call().id;
    let raw_cap = raw_cap(call_id, limit)?;
    let timeout_ms = invocation
        .grant
        .constraints()
        .timeout_ms
        .ok_or_else(|| failed("missing timeout ceiling"))?;
    let output = sandbox
        .execute(
            SandboxRequest {
                command: SandboxCommand::Shell(arguments.command),
                cwd,
                stdin: Vec::new(),
                max_input_bytes: 0,
                timeout: std::time::Duration::from_millis(timeout_ms),
                max_output_bytes: raw_cap,
            },
            cancellation,
        )
        .await
        .map_err(|error| tool_error(IsolatedShellError::Sandbox(error)))?;
    let result = result(call_id, output)?;
    if envelope_len(&result)? > limit {
        return Err(failed("shell result exceeds its complete envelope ceiling"));
    }
    Ok(result)
}

fn raw_cap(call_id: &str, limit: usize) -> Result<usize, ToolError> {
    // i32::MIN is the longest possible exit representation. false is longer than
    // true. Empty streams select utf8, the longer encoding tag. Account for
    // control-byte escaping through both content JSON and ToolResult JSON.
    let mut framing = result(
        call_id,
        SandboxOutput {
            exit_code: Some(i32::MIN),
            stdout: Vec::new(),
            stderr: Vec::new(),
        },
    )?;
    framing.is_error = false;
    let reserve = envelope_len(&framing)?;
    limit
        .checked_sub(reserve)
        .map(|remaining| remaining / 8)
        .filter(|cap| *cap > 0)
        .ok_or_else(|| failed("shell result ceiling cannot fit framing and a raw byte"))
}

fn result(call_id: &str, output: SandboxOutput) -> Result<ToolResult, ToolError> {
    Ok(ToolResult {
        call_id: call_id.to_owned(),
        is_error: output.exit_code != Some(0),
        content: serde_json::to_string(&ShellContent {
            exit_code: output.exit_code,
            stdout: stream(output.stdout),
            stderr: stream(output.stderr),
        })
        .map_err(|error| failed(error.to_string()))?,
    })
}

fn stream(bytes: Vec<u8>) -> ShellStream {
    match String::from_utf8(bytes) {
        Ok(data) => ShellStream {
            encoding: "utf8",
            data,
        },
        Err(error) => ShellStream {
            encoding: "hex",
            data: hex(&error.into_bytes()),
        },
    }
}

fn envelope_len(result: &ToolResult) -> Result<usize, ToolError> {
    serde_json::to_vec(result)
        .map(|bytes| bytes.len())
        .map_err(|error| failed(error.to_string()))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    encoded
}

fn tool_error(error: IsolatedShellError) -> ToolError {
    match error {
        IsolatedShellError::Sandbox(SandboxError::Cancelled) => ToolError::Cancelled,
        IsolatedShellError::Sandbox(SandboxError::Timeout) => ToolError::TimedOut,
        IsolatedShellError::Prepared(error) => ToolError::PolicyDenied {
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
