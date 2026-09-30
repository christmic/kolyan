//! Exact shell preparation and mandatory macOS sandbox execution.
//! Command text is never parsed to infer safety. Grants are supplied by the
//! trusted host; this adapter does not issue authority or record effect receipts.

use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use kolyan_model::{ToolCall, ToolDefinition};
use kolyan_policy::{
    Capability, Effect, Idempotency, InvocationClaim, PreparedCall, PreparedError, PreparedGrant,
    ResourceClaim, ToolExecutionScope, ToolRequirements,
};
use kolyan_sandbox::{
    MacOsSandbox, SandboxCancellation, SandboxCommand, SandboxConfig, SandboxError, SandboxOutput,
    SandboxRequest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_HOST_BYTES: usize = 64 * 1024 * 1024;
const MAX_COMMAND_BYTES: usize = 256 * 1024;
const SHELL: &str = "/bin/sh";
const ADAPTER_REVISION: &str = "isolated-shell-v1";

/// Trusted host roots and ceilings. All workspace writes are conservatively
/// declared; protected roots and network denial remain mandatory in the sandbox.
#[derive(Debug, Clone)]
pub struct IsolatedShellConfig {
    pub workspace: PathBuf,
    pub protected_roots: Vec<PathBuf>,
    pub max_command_bytes: usize,
    pub max_output_bytes: usize,
    pub timeout: Duration,
}

/// Model input. Cwd is relative to the admitted workspace; no environment or
/// executable overrides are accepted. Unknown fields fail deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShellArguments {
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug)]
pub enum IsolatedShellError {
    Invalid(String),
    Prepared(PreparedError),
    Sandbox(SandboxError),
    Io(std::io::Error),
}

impl std::fmt::Display for IsolatedShellError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "invalid isolated shell: {message}"),
            Self::Prepared(error) => error.fmt(formatter),
            Self::Sandbox(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for IsolatedShellError {}

/// One-shot shell adapter with no ambient executor or automatic retry path.
#[derive(Debug, Clone)]
pub struct IsolatedShellTool {
    config: IsolatedShellConfig,
}

impl IsolatedShellTool {
    /// Canonicalize host-selected roots and require a usable isolation backend.
    /// This constructor and `prepare` perform blocking filesystem reads.
    pub fn new(mut config: IsolatedShellConfig) -> Result<Self, IsolatedShellError> {
        config.workspace = config
            .workspace
            .canonicalize()
            .map_err(IsolatedShellError::Io)?;
        config.protected_roots = config
            .protected_roots
            .into_iter()
            .map(|path| path.canonicalize().map_err(IsolatedShellError::Io))
            .collect::<Result<_, _>>()?;
        config.protected_roots.sort();
        config.protected_roots.dedup();
        if !config.workspace.is_dir()
            || config.max_command_bytes == 0
            || config.max_command_bytes > MAX_COMMAND_BYTES
            || config.max_output_bytes == 0
            || config.max_output_bytes > MAX_HOST_BYTES
            || config.timeout.is_zero()
            || config.timeout.as_millis() == 0
            || config.timeout.as_millis() > u64::MAX as u128
        {
            return Err(invalid("invalid trusted workspace or execution limits"));
        }
        let adapter = Self { config };
        adapter.sandbox()?;
        Ok(adapter)
    }

    pub fn tool_definition() -> ToolDefinition {
        ToolDefinition {
            name: "shell".into(),
            description: Some("Execute a bounded nonlogin shell command in the sandboxed workspace. Network and protected control paths are denied.".into()),
            input_schema: serde_json::json!({
                "type":"object", "properties":{
                    "command":{"type":"string"}, "path":{"type":"string"}
                }, "required":["command"], "additionalProperties":false
            }),
        }
    }

    /// Derive conservative claims from validated input and trusted roots. This
    /// does not execute commands or authorize their effects; async callers must
    /// use a blocking worker for this filesystem-dependent preparation.
    pub fn prepare(&self, call: ToolCall) -> Result<PreparedCall, IsolatedShellError> {
        self.prepare_with_context(call)
            .map(|(prepared, _, _)| prepared)
    }

    /// Reprepare exact arguments, claims, roots and shell revision before effects.
    /// `expected_scope` must be supplied by the trusted host independently of
    /// the presented grant; model-generated call IDs are not execution scope.
    /// Raw nonzero process exits are returned as outcomes. Cancellation/drop is
    /// handled by the sandbox reaper; committed side effects are not rolled back.
    pub async fn execute(
        &self,
        prepared: &PreparedCall,
        grant: &PreparedGrant,
        current_policy_revision: &str,
        expected_scope: &ToolExecutionScope,
        cancellation: SandboxCancellation,
    ) -> Result<SandboxOutput, IsolatedShellError> {
        let adapter = self.clone();
        let call = prepared.call().clone();
        let (current, arguments, cwd, sandbox) = tokio::task::spawn_blocking(move || {
            let (current, arguments, cwd) = adapter.prepare_with_context(call)?;
            Ok::<_, IsolatedShellError>((current, arguments, cwd, adapter.sandbox()?))
        })
        .await
        .map_err(|error| invalid(&error.to_string()))??;
        if current.digest() != prepared.digest() {
            return Err(IsolatedShellError::Prepared(PreparedError::BindingMismatch));
        }
        grant
            .validate(prepared, current_policy_revision, expected_scope)
            .map_err(IsolatedShellError::Prepared)?;
        let max_output_bytes = grant
            .constraints()
            .max_output_bytes
            .and_then(|limit| usize::try_from(limit).ok())
            .ok_or_else(|| invalid("missing or overflowing output ceiling"))?;
        let timeout_ms = grant
            .constraints()
            .timeout_ms
            .ok_or_else(|| invalid("missing timeout ceiling"))?;
        sandbox
            .execute(
                SandboxRequest {
                    command: SandboxCommand::Shell(arguments.command),
                    cwd,
                    stdin: Vec::new(),
                    timeout: Duration::from_millis(timeout_ms),
                    max_output_bytes,
                },
                cancellation,
            )
            .await
            .map_err(IsolatedShellError::Sandbox)
    }

    fn prepare_with_context(
        &self,
        call: ToolCall,
    ) -> Result<(PreparedCall, ShellArguments, PathBuf), IsolatedShellError> {
        let (arguments, cwd) = self.arguments(&call)?;
        let prepared = PreparedCall::new(
            call,
            self.revision(&cwd)?,
            InvocationClaim {
                tool_name: "shell".into(),
                capabilities: [
                    Capability::FilesystemRead,
                    Capability::FilesystemWrite,
                    Capability::ProcessExecute,
                ]
                .into_iter()
                .collect(),
                effects: [
                    Effect::Read,
                    Effect::Create,
                    Effect::Update,
                    Effect::Delete,
                    Effect::Execute,
                ]
                .into_iter()
                .collect(),
                // A shell can access its whole admitted workspace, not just cwd.
                resource: ResourceClaim {
                    path: Some(
                        self.config
                            .workspace
                            .to_str()
                            .ok_or_else(|| invalid("workspace is not UTF-8"))?
                            .into(),
                    ),
                },
                idempotency: Idempotency::NonIdempotent,
            },
            ToolRequirements {
                process_sandbox: true,
                max_output_bytes: self.config.max_output_bytes as u64,
                timeout_ms: self.config.timeout.as_millis() as u64,
            },
        )
        .map_err(IsolatedShellError::Prepared)?;
        Ok((prepared, arguments, cwd))
    }

    fn arguments(&self, call: &ToolCall) -> Result<(ShellArguments, PathBuf), IsolatedShellError> {
        if call.name != "shell" {
            return Err(invalid("unsupported tool name"));
        }
        let arguments: ShellArguments = serde_json::from_value(call.arguments.clone())
            .map_err(|error| invalid(&error.to_string()))?;
        if arguments.command.trim().is_empty()
            || arguments.command.len() > self.config.max_command_bytes
            || arguments.command.as_bytes().contains(&0)
        {
            return Err(invalid("command must be nonempty, NUL-free and bounded"));
        }
        let relative = arguments.path.as_deref().unwrap_or(".");
        if relative.is_empty() || relative.len() > 4096 || relative.as_bytes().contains(&0) {
            return Err(invalid("invalid working directory"));
        }
        let path = Path::new(relative);
        if path.is_absolute()
            || path.components().any(|part| {
                matches!(
                    part,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(invalid("working directory must be workspace-relative"));
        }
        let cwd = self
            .config
            .workspace
            .join(path)
            .canonicalize()
            .map_err(IsolatedShellError::Io)?;
        if !cwd.starts_with(&self.config.workspace) || !cwd.is_dir() {
            return Err(invalid(
                "working directory escapes workspace or is not a directory",
            ));
        }
        if self.config.protected_roots.iter().any(|root| cwd.starts_with(root))
            || cwd.strip_prefix(&self.config.workspace).expect("checked workspace scope").components()
                .any(|part| matches!(part, Component::Normal(name) if name == ".git" || name == ".kolyan"))
        { return Err(invalid("working directory is protected")); }
        Ok((arguments, cwd))
    }

    fn sandbox(&self) -> Result<MacOsSandbox, IsolatedShellError> {
        MacOsSandbox::new(SandboxConfig {
            read_roots: vec![self.config.workspace.clone()],
            write_roots: vec![self.config.workspace.clone()],
            protected_roots: self.config.protected_roots.clone(),
        })
        .map_err(IsolatedShellError::Sandbox)
    }

    fn revision(&self, cwd: &Path) -> Result<String, IsolatedShellError> {
        let mut shell = std::fs::File::open(SHELL).map_err(IsolatedShellError::Io)?;
        let mut digest = Sha256::new();
        std::io::copy(&mut shell, &mut digest).map_err(IsolatedShellError::Io)?;
        let binding = serde_json::to_vec(&serde_json::json!({
            "adapter":ADAPTER_REVISION, "executable":SHELL, "cwd":cwd,
            "sandbox_policy":kolyan_sandbox::MACOS_SEATBELT_POLICY_REVISION,
            "workspace":self.config.workspace, "protected_roots":self.config.protected_roots,
            "command_limit":self.config.max_command_bytes,
            "output_limit":self.config.max_output_bytes,
            "timeout_ms":self.config.timeout.as_millis() as u64,
        }))
        .map_err(|error| invalid(&error.to_string()))?;
        digest.update(binding);
        Ok(format!("{ADAPTER_REVISION}/{:x}", digest.finalize()))
    }
}

fn invalid(message: &str) -> IsolatedShellError {
    IsolatedShellError::Invalid(message.into())
}

#[cfg(test)]
mod tests;
