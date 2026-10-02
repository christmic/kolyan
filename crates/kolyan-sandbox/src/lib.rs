//! Host-admitted process execution under macOS Seatbelt, not an authorization engine.
//! Cancellation and dropped futures notify an independent process-owning reaper.

mod exact_file;
mod observation;
mod process;
mod profile;

pub use exact_file::FileSandboxConfig;
pub use observation::{
    SandboxCancellationCause, SandboxOutputStream, SandboxProcessEvent, SandboxProcessObservation,
    SandboxProcessObservationReceiver, SandboxProcessObservationSender,
    sandbox_process_observation_channel,
};

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use thiserror::Error;

/// Bind this trusted policy revision into prepared execution requirements/grants.
/// Policy semantics changes require a revision change, not a compatibility branch.
pub const MACOS_SEATBELT_POLICY_REVISION: &str = "kolyan-seatbelt-v5";

pub type SandboxFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SandboxOutput, SandboxError>> + Send + 'a>>;

/// Trusted host configuration. Existing canonical directory roots are required.
/// Write roots also permit reads. Never admit credential or control directories.
#[derive(Clone, Debug)]
pub struct SandboxConfig {
    pub read_roots: Vec<PathBuf>,
    pub write_roots: Vec<PathBuf>,
    /// Additional existing control directories denied even inside admitted roots.
    /// `.git` and `.kolyan` paths are automatically protected, including creation.
    pub protected_roots: Vec<PathBuf>,
}

/// No arbitrary environment injection or login shell is accepted.
#[derive(Clone, Debug)]
pub enum SandboxCommand {
    Shell(String),
    /// Trusted absolute executable, with literal argv (for a tool worker).
    Executable {
        path: PathBuf,
        arguments: Vec<String>,
    },
}

/// Per-invocation limits. Output cap is shared across both pipes. Stdin has an
/// independent host-selected ceiling; zero permits only empty input. Cwd must be
/// within an admitted directory. Input ceilings cannot exceed 64 MiB.
#[derive(Clone, Debug)]
pub struct SandboxRequest {
    pub command: SandboxCommand,
    pub cwd: PathBuf,
    pub stdin: Vec<u8>,
    pub max_input_bytes: usize,
    pub timeout: Duration,
    pub max_output_bytes: usize,
}

/// Raw bytes are preserved; a nonzero exit is a process outcome, not an adapter error.
#[derive(Debug)]
pub struct SandboxOutput {
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("macOS Seatbelt is unsupported on this platform")]
    Unsupported,
    #[error("fixed /usr/bin/sandbox-exec backend is missing")]
    BackendMissing,
    #[error("invalid sandbox admission: {0}")]
    Invalid(String),
    #[error("sandbox process I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("sandbox invocation timed out")]
    Timeout,
    #[error("sandbox invocation cancelled")]
    Cancelled,
    #[error("sandbox combined output exceeded its limit")]
    OutputLimit,
    #[error("sandbox worker failed")]
    Worker,
}

/// Cooperative cancellation survives independently of the execution future.
#[derive(Clone, Default)]
pub struct SandboxCancellation(Arc<AtomicBool>);

impl SandboxCancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Host-selected enforced execution port. Implementations must preserve limits,
/// clean environment, cancellation/drop cleanup and fail-closed isolation.
/// This port never creates grants or retries uncertain effects.
pub trait SandboxExecutor: Send + Sync {
    /// Trusted implementation policy identity for exact preparation binding.
    fn policy_revision(&self) -> &'static str;

    fn execute(
        &self,
        request: SandboxRequest,
        cancellation: SandboxCancellation,
    ) -> SandboxFuture<'_>;
}

/// Replaceable Seatbelt adapter. Construction fails closed off macOS.
#[derive(Clone, Debug)]
pub struct MacOsSandbox {
    access: profile::Access,
    observer: Option<SandboxProcessObservationSender>,
}

impl MacOsSandbox {
    pub fn new(config: SandboxConfig) -> Result<Self, SandboxError> {
        if !cfg!(target_os = "macos") {
            return Err(SandboxError::Unsupported);
        }
        require_backend(std::path::Path::new("/usr/bin/sandbox-exec"))?;
        Ok(Self {
            access: profile::Access::Workspace(profile::canonical_config(config)?),
            observer: None,
        })
    }

    /// Admit exact physical files, independently of workspace directory contents.
    /// Only trusted executable requests are accepted. This does not pin filesystem
    /// identity: the worker must also use nofollow opens and validate its parents.
    pub fn new_files(config: FileSandboxConfig) -> Result<Self, SandboxError> {
        if !cfg!(target_os = "macos") {
            return Err(SandboxError::Unsupported);
        }
        require_backend(std::path::Path::new("/usr/bin/sandbox-exec"))?;
        Ok(Self {
            access: profile::Access::Files(exact_file::canonical_config(config)?),
            observer: None,
        })
    }

    /// Attach optional diagnostics; queue loss never changes execution authority.
    pub fn with_process_observer(mut self, observer: SandboxProcessObservationSender) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Execute once; no retries. Drop requests cleanup without requiring a Tokio
    /// runtime to remain alive. Explicit cancellation/timeout returns after reaping.
    pub async fn execute(
        &self,
        request: SandboxRequest,
        cancellation: SandboxCancellation,
    ) -> Result<SandboxOutput, SandboxError> {
        let access = self.access.clone();
        let observer = self.observer.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropCancellation(dropped.clone());
        let (send, receive) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("kolyan-sandbox-reaper".into())
            .spawn(move || {
                let result = profile::admit(&access, request).and_then(|admitted| {
                    process::execute(admitted, cancellation, dropped, observer)
                });
                let _ = send.send(result);
            })?;
        let result = receive.await.map_err(|_| SandboxError::Worker)?;
        drop(guard);
        result
    }
}

impl SandboxExecutor for MacOsSandbox {
    fn policy_revision(&self) -> &'static str {
        MACOS_SEATBELT_POLICY_REVISION
    }

    fn execute(
        &self,
        request: SandboxRequest,
        cancellation: SandboxCancellation,
    ) -> SandboxFuture<'_> {
        Box::pin(MacOsSandbox::execute(self, request, cancellation))
    }
}

fn require_backend(path: &std::path::Path) -> Result<(), SandboxError> {
    if !path.is_file() {
        return Err(SandboxError::BackendMissing);
    }
    Ok(())
}

struct DropCancellation(Arc<AtomicBool>);
impl Drop for DropCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod input_limits_tests;
