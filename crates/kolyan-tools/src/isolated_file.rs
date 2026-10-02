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
    FileSandboxConfig, MacOsSandbox, SandboxCancellation, SandboxCommand, SandboxError,
    SandboxRequest,
};
use sha2::{Digest, Sha256};

use crate::exact_file::{
    ExactDirectoryBinding, ExactFileBinding, ExactFileStaging, ExactFileWorkerRequest,
    OpenedBinding, validate_operation,
};
use crate::file_operations::{
    FileOperation, FileOperationError, FileOperationLimits, FileOperationResult,
};

mod receipt;
mod turn;

const MAX_WORKER_INPUT_BYTES: usize = 64 * 1024 * 1024;

/// Trusted configuration. The worker binary must reside outside writable roots.
#[derive(Debug, Clone)]
pub struct IsolatedFileConfig {
    pub workspace: PathBuf,
    /// Existing host-private root outside workspace, protected by shell assembly.
    pub staging_root: PathBuf,
    pub worker: PathBuf,
    pub protected_roots: Vec<PathBuf>,
    pub file_limits: FileOperationLimits,
    pub max_output_bytes: usize,
    pub timeout: Duration,
}

#[derive(Debug)]
pub enum IsolatedFileError {
    Invalid(String),
    Uncertain(String),
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
            Self::Uncertain(message) => {
                write!(formatter, "uncertain isolated file outcome: {message}")
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
#[derive(Clone)]
pub struct IsolatedFileTools {
    config: IsolatedFileConfig,
    staging_binding: ExactDirectoryBinding,
    observer: Option<kolyan_sandbox::SandboxProcessObservationSender>,
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
        config.staging_root = config
            .staging_root
            .canonicalize()
            .map_err(IsolatedFileError::Io)?;
        for protected in &mut config.protected_roots {
            *protected = if protected.exists() {
                protected.canonicalize().map_err(IsolatedFileError::Io)?
            } else {
                let parent = protected.parent().ok_or_else(|| {
                    IsolatedFileError::Invalid("protected resource has no parent".into())
                })?;
                parent.canonicalize().map_err(IsolatedFileError::Io)?.join(
                    protected.file_name().ok_or_else(|| {
                        IsolatedFileError::Invalid("protected resource has no leaf".into())
                    })?,
                )
            };
        }
        if !config.workspace.is_dir()
            || !config.staging_root.is_dir()
            || config.staging_root.starts_with(&config.workspace)
            || config.workspace.starts_with(&config.staging_root)
            || !config.worker.is_file()
            || config.worker.starts_with(&config.workspace)
            || config.worker.starts_with(&config.staging_root)
            || config.protected_roots.iter().any(|root| {
                root != &config.staging_root
                    && (config.staging_root.starts_with(root)
                        || root.starts_with(&config.staging_root))
            })
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
        // A nonexistent exact probe leaf admits no workspace content or effects.
        MacOsSandbox::new_files(FileSandboxConfig {
            workspace: config.workspace.clone(),
            read_files: vec![config.workspace.join(".kolyan-exact-backend-probe")],
            write_files: vec![],
            protected_roots: config.protected_roots.clone(),
        })
        .map_err(IsolatedFileError::Sandbox)?;
        if !config.protected_roots.contains(&config.staging_root) {
            config.protected_roots.push(config.staging_root.clone());
        }
        let (staging_binding, _) = ExactDirectoryBinding::capture(&config.staging_root)
            .map_err(IsolatedFileError::Operation)?;
        Ok(Self {
            config,
            staging_binding,
            observer: None,
        })
    }

    /// Attach bounded telemetry without altering preparation/grant semantics.
    pub fn with_process_observer(
        mut self,
        observer: kolyan_sandbox::SandboxProcessObservationSender,
    ) -> Self {
        self.observer = Some(observer);
        self
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
        if !matches!(operation, FileOperation::Read(_))
            && std::fs::symlink_metadata(&target)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Err(IsolatedFileError::Invalid(
                "replacement target must not be a symlink".into(),
            ));
        }
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
        let binding = ExactFileBinding::prepare(
            &self.config.workspace,
            &resource,
            &self.config.protected_roots,
        )
        .map_err(IsolatedFileError::Operation)?;
        let resource = resource
            .into_os_string()
            .into_string()
            .map_err(|_| IsolatedFileError::Invalid("resource is not UTF-8".into()))?;
        let read = matches!(operation, FileOperation::Read(_));
        // Existence can change after preparation; Write always needs both effects.
        // Edit necessarily reads content before its exact atomic replacement.
        let (capabilities, effects) = match &operation {
            FileOperation::Read(_) => (
                [Capability::FilesystemRead].into_iter().collect(),
                [Effect::Read].into_iter().collect(),
            ),
            FileOperation::Write(_) => (
                [Capability::FilesystemWrite].into_iter().collect(),
                [Effect::Create, Effect::Update].into_iter().collect(),
            ),
            FileOperation::Edit(_) => (
                [Capability::FilesystemRead, Capability::FilesystemWrite]
                    .into_iter()
                    .collect(),
                [Effect::Read, Effect::Update].into_iter().collect(),
            ),
        };
        PreparedCall::new(
            call,
            self.adapter_revision()?,
            InvocationClaim {
                tool_name: operation.name().into(),
                capabilities,
                effects,
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
        .and_then(|prepared| {
            prepared.with_execution_binding(
                serde_json::to_value(binding).expect("serializable binding"),
            )
        })
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
        self.execute_bounded(
            prepared,
            grant,
            policy_revision,
            expected_scope,
            cancellation,
            None,
        )
        .await
    }

    async fn execute_bounded(
        &self,
        prepared: &PreparedCall,
        grant: &PreparedGrant,
        policy_revision: &str,
        expected_scope: &ToolExecutionScope,
        cancellation: SandboxCancellation,
        result_output_limit: Option<usize>,
    ) -> Result<FileOperationResult, IsolatedFileError> {
        let adapter = self.clone();
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
        let output_limit = usize::try_from(
            grant
                .constraints()
                .max_output_bytes
                .expect("validated output limit"),
        )
        .map_err(|_| IsolatedFileError::Invalid("output limit overflow".into()))?;
        let output_limit = result_output_limit
            .unwrap_or(output_limit)
            .min(output_limit);
        // Refuse replacements before allocating a stage when their mandatory
        // receipt cannot fit. Edit reserves the configured maximum byte count.
        if !matches!(operation, FileOperation::Read(_)) {
            let framing = FileOperationResult {
                path: operation.path().into(),
                bytes: match &operation {
                    FileOperation::Write(arguments) => arguments.content.len(),
                    _ => self.config.file_limits.max_write_bytes,
                },
                sha256: "0".repeat(64),
                content: None,
            };
            if serde_json::to_vec(&framing)
                .map_err(|error| IsolatedFileError::Invalid(error.to_string()))?
                .len()
                + 1
                > output_limit
            {
                return Err(IsolatedFileError::Invalid(
                    "output ceiling cannot fit replacement receipt".into(),
                ));
            }
        }
        let binding: ExactFileBinding =
            serde_json::from_value(prepared.execution_binding().clone())
                .map_err(|error| IsolatedFileError::Invalid(error.to_string()))?;
        let adapter = self.clone();
        let (request, _pinned, _stage_directory) =
            tokio::task::spawn_blocking(move || adapter.worker_request(operation, binding))
                .await
                .map_err(|error| IsolatedFileError::Invalid(error.to_string()))??;
        let input = serde_json::to_vec(&request)
            .map_err(|error| IsolatedFileError::Invalid(error.to_string()))?;
        if input.len() > MAX_WORKER_INPUT_BYTES {
            return Err(IsolatedFileError::Invalid(
                "worker envelope exceeds trusted input ceiling".into(),
            ));
        }
        let operation = &request.operation;
        let writes = !matches!(operation, FileOperation::Read(_));
        let target = request
            .binding
            .parent
            .physical_path
            .join(&request.binding.leaf);
        let mut write_files = if writes { vec![target.clone()] } else { vec![] };
        if let Some(stage) = &request.staging {
            write_files.push(stage.parent.physical_path.join(&stage.leaf));
        }
        let sandbox_config = FileSandboxConfig {
            workspace: self.config.workspace.clone(),
            read_files: if matches!(operation, FileOperation::Write(_)) {
                vec![]
            } else {
                vec![target]
            },
            // Only the internal stage leaf is exempt from its host-private root's
            // deny. Exact mode still grants no directory writes or sibling files.
            protected_roots: self
                .config
                .protected_roots
                .iter()
                .filter(|root| *root != &self.config.staging_root)
                .cloned()
                .collect(),
            write_files,
        };
        let sandbox = tokio::task::spawn_blocking(move || MacOsSandbox::new_files(sandbox_config))
            .await
            .map_err(|error| IsolatedFileError::Invalid(error.to_string()))?
            .map_err(IsolatedFileError::Sandbox)?;
        let sandbox = if let Some(observer) = crate::process_observation::bind_observer(
            self.observer.as_ref(),
            prepared,
            expected_scope,
        ) {
            sandbox.with_process_observer(observer)
        } else {
            sandbox
        };
        let output = sandbox
            .execute(
                SandboxRequest {
                    command: SandboxCommand::Executable {
                        path: self.config.worker.clone(),
                        arguments: vec![
                            self.config.workspace.to_string_lossy().into_owned(),
                            MAX_WORKER_INPUT_BYTES.to_string(),
                            self.config.file_limits.max_read_bytes.to_string(),
                            self.config.file_limits.max_write_bytes.to_string(),
                        ],
                    },
                    cwd: self.config.workspace.clone(),
                    stdin: input,
                    max_input_bytes: MAX_WORKER_INPUT_BYTES,
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
        receipt::decode(operation, &output.stdout, self.config.file_limits)
    }

    fn worker_request(
        &self,
        operation: FileOperation,
        binding: ExactFileBinding,
    ) -> Result<
        (
            ExactFileWorkerRequest,
            OpenedBinding,
            Option<tempfile::TempDir>,
        ),
        IsolatedFileError,
    > {
        let mut pinned = binding.open().map_err(IsolatedFileError::Operation)?;
        let stage_root = self
            .staging_binding
            .open()
            .map_err(IsolatedFileError::Operation)?;
        let directory = if matches!(operation, FileOperation::Read(_)) {
            None
        } else {
            let mut builder = tempfile::Builder::new();
            builder.prefix("kolyan-exact-");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                // TempDir defaults to ordinary directory permissions. Request
                // private permissions at mkdir, not a racy later chmod.
                builder.permissions(std::fs::Permissions::from_mode(0o700));
            }
            Some(
                builder
                    .tempdir_in(&self.config.staging_root)
                    .map_err(IsolatedFileError::Io)?,
            )
        };
        let staging = directory
            .as_ref()
            .map(|directory| {
                ExactFileStaging::prepare(&binding, &directory.path().join("replacement"))
            })
            .transpose()
            .map_err(IsolatedFileError::Operation)?;
        self.staging_binding
            .open()
            .map_err(IsolatedFileError::Operation)?;
        pinned._staging.push(stage_root);
        if let Some(stage) = &staging {
            pinned
                ._staging
                .push(stage.parent.open().map_err(IsolatedFileError::Operation)?);
        }
        Ok((
            ExactFileWorkerRequest {
                operation,
                binding,
                staging,
            },
            pinned,
            directory,
        ))
    }

    fn operation(&self, call: &ToolCall) -> Result<FileOperation, IsolatedFileError> {
        let operation = serde_json::from_value(serde_json::json!({
            "name":call.name, "arguments":call.arguments,
        }))
        .map_err(|error| IsolatedFileError::Invalid(error.to_string()))?;
        validate_operation(&operation, self.config.file_limits)
            .map_err(IsolatedFileError::Operation)?;
        Ok(operation)
    }

    /// Read the worker bytes and normalized configuration's exact revision.
    /// This synchronous file I/O does not execute the worker, prepare a call,
    /// inspect a target or grant authority. Hosts call it during blocking assembly.
    /// Recalculation observes current worker bytes, not historical proof.
    pub fn adapter_revision(&self) -> Result<String, IsolatedFileError> {
        let mut file = std::fs::File::open(&self.config.worker).map_err(IsolatedFileError::Io)?;
        let mut digest = Sha256::new();
        std::io::copy(&mut file, &mut digest).map_err(IsolatedFileError::Io)?;
        Ok(format!(
            "file-worker-v2/{}/{:x}/{:x}",
            kolyan_sandbox::MACOS_SEATBELT_POLICY_REVISION,
            digest.finalize(),
            Sha256::digest(serde_json::to_vec(&serde_json::json!({
                "workspace":self.config.workspace,"staging_root":self.config.staging_root,
                "staging_binding":self.staging_binding,
                "protected_roots":self.config.protected_roots,"max_input_bytes":MAX_WORKER_INPUT_BYTES,
                "max_read_bytes":self.config.file_limits.max_read_bytes,"max_write_bytes":self.config.file_limits.max_write_bytes,
                "max_output_bytes":self.config.max_output_bytes,"timeout_ms":self.config.timeout.as_millis(),
            })).expect("serializable trusted limits"))
        ))
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod adapter_revision_tests;
