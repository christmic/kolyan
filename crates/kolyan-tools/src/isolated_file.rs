//! Exact prepared file calls executed only by a trusted sandboxed worker.
//! The host supplies policy decisions; this adapter never manufactures authority.

use std::path::{Component, PathBuf};
use std::time::Duration;

use kolyan_model::ToolCall;
use kolyan_policy::{
    Capability, Effect, Idempotency, InvocationClaim, PreparedCall, PreparedError, PreparedGrant,
    ResourceClaim, ToolExecutionScope, ToolRequirements,
};
use kolyan_sandbox::{
    MacOsSandbox, SandboxCancellation, SandboxCommand, SandboxConfig, SandboxError, SandboxRequest,
};
use sha2::{Digest, Sha256};

use crate::file_operations::{
    FileOperation, FileOperationError, FileOperationLimits, FileOperationResult, FileOperations,
};

/// Trusted configuration. The worker binary must reside outside writable roots.
#[derive(Debug, Clone)]
pub struct IsolatedFileConfig {
    pub workspace: PathBuf,
    pub worker: PathBuf,
    pub protected_roots: Vec<PathBuf>,
    pub file_limits: FileOperationLimits,
    pub max_output_bytes: usize,
    pub timeout: Duration,
}

#[derive(Debug)]
pub enum IsolatedFileError {
    Invalid(String),
    Prepared(PreparedError),
    Operation(FileOperationError),
    Sandbox(SandboxError),
    Io(std::io::Error),
}

impl std::fmt::Display for IsolatedFileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => {
                write!(formatter, "invalid isolated file execution: {message}")
            }
            Self::Prepared(error) => error.fmt(formatter),
            Self::Operation(error) => error.fmt(formatter),
            Self::Sandbox(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for IsolatedFileError {}

/// File adapter with no unsandboxed execution path.
pub struct IsolatedFileTools {
    config: IsolatedFileConfig,
}

impl IsolatedFileTools {
    pub fn new(mut config: IsolatedFileConfig) -> Result<Self, IsolatedFileError> {
        config.workspace = config
            .workspace
            .canonicalize()
            .map_err(IsolatedFileError::Io)?;
        config.worker = config
            .worker
            .canonicalize()
            .map_err(IsolatedFileError::Io)?;
        if !config.workspace.is_dir()
            || !config.worker.is_file()
            || config.worker.starts_with(&config.workspace)
            || config.file_limits.max_read_bytes == 0
            || config.file_limits.max_write_bytes == 0
            || config.file_limits.max_read_bytes > 64 * 1024 * 1024
            || config.file_limits.max_write_bytes > 64 * 1024 * 1024
            || config.max_output_bytes == 0
            || config.max_output_bytes > 64 * 1024 * 1024
            || config.timeout.is_zero()
            || config.timeout.as_millis() > u64::MAX as u128
        {
            return Err(IsolatedFileError::Invalid(
                "invalid trusted workspace, worker or limits".into(),
            ));
        }
        // Probe mandatory backend admission now; do not delay missing isolation
        // until after approval or quietly select an ambient executor.
        MacOsSandbox::new(SandboxConfig {
            read_roots: vec![config.workspace.clone()],
            write_roots: vec![],
            protected_roots: config.protected_roots.clone(),
        })
        .map_err(IsolatedFileError::Sandbox)?;
        Ok(Self { config })
    }

    /// Validate model arguments and derive claims without performing file effects.
    pub fn prepare(&self, call: ToolCall) -> Result<PreparedCall, IsolatedFileError> {
        let operation = self.operation(&call)?;
        let path = PathBuf::from(operation.path());
        let relative = path
            .components()
            .filter_map(|component| match component {
                Component::Normal(value) => Some(value),
                _ => None,
            })
            .collect::<PathBuf>();
        let target = self.config.workspace.join(relative);
        // Policy sees the actual referent, not an allowed-looking symlink alias.
        // Repreparation checks the same binding again immediately before dispatch.
        let resource = if target.exists() {
            target.canonicalize().map_err(IsolatedFileError::Io)?
        } else {
            let parent = target
                .parent()
                .ok_or_else(|| IsolatedFileError::Invalid("target has no parent".into()))?
                .canonicalize()
                .map_err(IsolatedFileError::Io)?;
            parent.join(
                target
                    .file_name()
                    .ok_or_else(|| IsolatedFileError::Invalid("target has no file name".into()))?,
            )
        };
        if !resource.starts_with(&self.config.workspace) {
            return Err(IsolatedFileError::Invalid(
                "resolved resource escapes workspace".into(),
            ));
        }
        let resource = resource
            .into_os_string()
            .into_string()
            .map_err(|_| IsolatedFileError::Invalid("resource is not UTF-8".into()))?;
        let read = matches!(operation, FileOperation::Read(_));
        PreparedCall::new(
            call,
            self.revision()?,
            InvocationClaim {
                tool_name: operation.name().into(),
                capabilities: [if read {
                    Capability::FilesystemRead
                } else {
                    Capability::FilesystemWrite
                }]
                .into_iter()
                .collect(),
                effects: [if read { Effect::Read } else { Effect::Update }]
                    .into_iter()
                    .collect(),
                resource: ResourceClaim {
                    path: Some(resource),
                },
                idempotency: if read {
                    Idempotency::Idempotent
                } else {
                    Idempotency::NonIdempotent
                },
            },
            ToolRequirements {
                process_sandbox: true,
                max_output_bytes: self.config.max_output_bytes as u64,
                timeout_ms: self.config.timeout.as_millis() as u64,
            },
        )
        .map_err(IsolatedFileError::Prepared)
    }

    /// Reprepare and verify exact authority before spawning a sandboxed helper.
    /// `expected_scope` comes from independent trusted host context, never from
    /// the presented grant or reusable model-generated tool-call identity.
    /// Cancellation returns after cleanup; interruption does not promise rollback
    /// of an already committed write. Runtime must persist effect receipts.
    pub async fn execute(
        &self,
        prepared: &PreparedCall,
        grant: &PreparedGrant,
        policy_revision: &str,
        expected_scope: &ToolExecutionScope,
        cancellation: SandboxCancellation,
    ) -> Result<FileOperationResult, IsolatedFileError> {
        let adapter = Self {
            config: self.config.clone(),
        };
        let call = prepared.call().clone();
        let current = tokio::task::spawn_blocking(move || adapter.prepare(call))
            .await
            .map_err(|error| IsolatedFileError::Invalid(error.to_string()))??;
        if current.digest() != prepared.digest() {
            return Err(IsolatedFileError::Prepared(PreparedError::BindingMismatch));
        }
        grant
            .validate(prepared, policy_revision, expected_scope)
            .map_err(IsolatedFileError::Prepared)?;
        let operation = self.operation(prepared.call())?;
        let input = serde_json::to_vec(&operation)
            .map_err(|error| IsolatedFileError::Invalid(error.to_string()))?;
        let output_limit = usize::try_from(
            grant
                .constraints()
                .max_output_bytes
                .expect("validated output limit"),
        )
        .map_err(|_| IsolatedFileError::Invalid("output limit overflow".into()))?;
        let writes = !matches!(operation, FileOperation::Read(_));
        let sandbox = MacOsSandbox::new(SandboxConfig {
            read_roots: vec![self.config.workspace.clone()],
            protected_roots: self.config.protected_roots.clone(),
            write_roots: if writes {
                vec![self.config.workspace.clone()]
            } else {
                vec![]
            },
        })
        .map_err(IsolatedFileError::Sandbox)?;
        let output = sandbox
            .execute(
                SandboxRequest {
                    command: SandboxCommand::Executable {
                        path: self.config.worker.clone(),
                        arguments: vec![
                            self.config.workspace.to_string_lossy().into_owned(),
                            output_limit.to_string(),
                            self.config.file_limits.max_read_bytes.to_string(),
                            self.config.file_limits.max_write_bytes.to_string(),
                        ],
                    },
                    cwd: self.config.workspace.clone(),
                    stdin: input,
                    timeout: Duration::from_millis(
                        grant.constraints().timeout_ms.expect("validated timeout"),
                    ),
                    max_output_bytes: output_limit,
                },
                cancellation,
            )
            .await
            .map_err(IsolatedFileError::Sandbox)?;
        if output.exit_code != Some(0) {
            return Err(IsolatedFileError::Invalid(format!(
                "worker failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        let result: FileOperationResult = serde_json::from_slice(&output.stdout)
            .map_err(|error| IsolatedFileError::Invalid(error.to_string()))?;
        if result.path != operation.path() {
            return Err(IsolatedFileError::Invalid(
                "worker result path mismatch".into(),
            ));
        }
        Ok(result)
    }

    fn operation(&self, call: &ToolCall) -> Result<FileOperation, IsolatedFileError> {
        let operation = serde_json::from_value(serde_json::json!({
            "name":call.name, "arguments":call.arguments,
        }))
        .map_err(|error| IsolatedFileError::Invalid(error.to_string()))?;
        FileOperations::new(&self.config.workspace, self.config.file_limits)
            .validate(&operation)
            .map_err(IsolatedFileError::Operation)?;
        Ok(operation)
    }

    fn revision(&self) -> Result<String, IsolatedFileError> {
        let mut file = std::fs::File::open(&self.config.worker).map_err(IsolatedFileError::Io)?;
        let mut digest = Sha256::new();
        std::io::copy(&mut file, &mut digest).map_err(IsolatedFileError::Io)?;
        Ok(format!(
            "file-worker-v1/{}/{:x}",
            kolyan_sandbox::MACOS_SEATBELT_POLICY_REVISION,
            digest.finalize()
        ))
    }
}
