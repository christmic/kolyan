//! Exact-file Seatbelt execution. Installation and identity are host-owned,
//! not script claims. Filesystem revalidation does not eliminate host TOCTOU.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Serialize;

use kolyan_core::TurnControl;
use kolyan_sandbox::{
    FileSandboxConfig, MacOsSandbox, SandboxCancellation, SandboxCommand, SandboxError,
    SandboxProcessObservationSender, SandboxRequest,
};

use super::{HookError, RegisteredHook, bytes_hash, hash};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct FileIdentity {
    device: u64,
    inode: u64,
    digest: String,
}

/// Trusted private installation root and empty cwd outside the Agent workspace.
/// Construct once per host; rebuilding verifies the same interpreter/content.
#[derive(Clone)]
pub struct NativeHookHost {
    root: PathBuf,
    cwd: PathBuf,
    interpreter: PathBuf,
    identity: FileIdentity,
    digest: String,
    observer: Option<SandboxProcessObservationSender>,
}
pub(super) struct InstalledHook {
    sandbox: MacOsSandbox,
    request: SandboxRequest,
    script: PathBuf,
    identity: FileIdentity,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum NativeOutcome {
    Exited {
        exit_code: Option<i32>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    Failed {
        kind: &'static str,
        message: String,
    },
}
impl NativeHookHost {
    /// No implicit directory creation or unsandboxed fallback. The caller must
    /// supply private physical directories outside workspace/control secrets.
    pub fn new(root: PathBuf, cwd: PathBuf) -> Result<Self, HookError> {
        private_directory(&root)?;
        private_directory(&cwd)?;
        if root.starts_with(&cwd) || cwd.starts_with(&root) {
            return Err(HookError::Invalid(
                "installation/cwd must be disjoint".into(),
            ));
        }
        let interpreter = PathBuf::from("/bin/sh").canonicalize()?;
        let identity = file_identity(&interpreter, 64 * 1024 * 1024, false)?;
        let digest = hash(&(
            "kolyan.hooks.native.v1",
            &root,
            &cwd,
            &interpreter,
            &identity,
            kolyan_sandbox::MACOS_SEATBELT_POLICY_REVISION,
        ))?;
        Ok(Self {
            root,
            cwd,
            interpreter,
            identity,
            digest,
            observer: None,
        })
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }
    pub fn with_process_observer(mut self, observer: SandboxProcessObservationSender) -> Self {
        self.observer = Some(observer);
        self
    }

    pub(super) fn prepare(
        &self,
        hook: &RegisteredHook,
        body: &[u8],
        stdin: Vec<u8>,
        timeout: Duration,
        run_id: &str,
    ) -> Result<InstalledHook, HookError> {
        private_directory(&self.root)?;
        private_directory(&self.cwd)?;
        if file_identity(&self.interpreter, 64 * 1024 * 1024, false)? != self.identity {
            return Err(HookError::Integrity("interpreter changed".into()));
        }
        if bytes_hash(body) != hook.body.digest {
            return Err(HookError::Integrity("script artifact changed".into()));
        }
        let script = self.root.join(&hook.body.digest);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o400)
            .open(&script)
        {
            Ok(mut file) => {
                file.write_all(body)?;
                file.sync_all()?;
                File::open(&self.root)?.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let identity = file_identity(&script, 32 * 1024, true)?;
        if identity.digest != hook.body.digest {
            return Err(HookError::Integrity("installed script changed".into()));
        }
        let mut sandbox = MacOsSandbox::new_files(FileSandboxConfig {
            workspace: self.cwd.clone(),
            read_files: vec![script.clone()],
            write_files: vec![],
            protected_roots: vec![],
        })
        .map_err(|e| HookError::Native(e.to_string()))?;
        if let Some(observer) = &self.observer {
            let bound = observer
                .bind_context(run_id.into())
                .ok_or_else(|| HookError::Invalid("observer context exceeded".into()))?;
            sandbox = sandbox.with_process_observer(bound);
        }
        let request = SandboxRequest {
            command: SandboxCommand::Executable {
                path: self.interpreter.clone(),
                arguments: vec![
                    script
                        .to_str()
                        .ok_or_else(|| HookError::Invalid("non UTF-8 script path".into()))?
                        .into(),
                ],
            },
            cwd: self.cwd.clone(),
            stdin,
            max_input_bytes: hook.manifest.max_input_bytes,
            timeout,
            max_output_bytes: hook.manifest.max_output_bytes,
        };
        Ok(InstalledHook {
            sandbox,
            request,
            script,
            identity,
        })
    }
}

impl InstalledHook {
    pub(super) async fn execute(self, control: &TurnControl) -> NativeOutcome {
        let cancellation = SandboxCancellation::default();
        let execution = self.sandbox.execute(self.request, cancellation.clone());
        tokio::pin!(execution);
        let result = tokio::select! {
            biased;
            _ = control.cancelled() => { cancellation.cancel(); execution.await },
            result = &mut execution => result,
        };
        // Check after the script stops as well; this is detection, not atomic pinning.
        let script = self.script;
        let checked =
            tokio::task::spawn_blocking(move || file_identity(&script, 32 * 1024, true)).await;
        match checked {
            Ok(Ok(actual)) if actual == self.identity => {}
            _ => {
                return NativeOutcome::Failed {
                    kind: "integrity",
                    message: "installed script identity changed".into(),
                };
            }
        }
        match result {
            Ok(output) => NativeOutcome::Exited {
                exit_code: output.exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
            },
            Err(error) => NativeOutcome::Failed {
                kind: match error {
                    SandboxError::Timeout => "timeout",
                    SandboxError::Cancelled => "cancelled",
                    SandboxError::OutputLimit => "output_limit",
                    SandboxError::Io(_) => "io",
                    SandboxError::Invalid(_) => "admission",
                    SandboxError::Unsupported => "unsupported",
                    SandboxError::BackendMissing => "backend_missing",
                    SandboxError::Worker => "worker",
                },
                message: error.to_string(),
            },
        }
    }
}

fn private_directory(path: &Path) -> Result<(), HookError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir()
        || path.parent().is_none()
        || path.canonicalize()? != path
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(HookError::Invalid(
            "hook directory must be private and physical".into(),
        ));
    }
    Ok(())
}
fn file_identity(path: &Path, limit: u64, readonly: bool) -> Result<FileIdentity, HookError> {
    let before = fs::symlink_metadata(path)?;
    if !before.is_file()
        || before.len() > limit
        || before.nlink() != 1
        || (readonly && before.permissions().mode() & 0o222 != 0)
    {
        return Err(HookError::Integrity(
            "file must be regular, singly linked and bounded; script readonly".into(),
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    let after = fs::symlink_metadata(path)?;
    if bytes.len() as u64 > limit
        || before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
    {
        return Err(HookError::Integrity(
            "file changed during identity read".into(),
        ));
    }
    Ok(FileIdentity {
        device: before.dev(),
        inode: before.ino(),
        digest: bytes_hash(&bytes),
    })
}
