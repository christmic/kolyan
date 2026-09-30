//! Independent blocking owner: async cancellation never abandons the child.

#[cfg(target_os = "macos")]
use std::io::{Read, Write};
#[cfg(target_os = "macos")]
use std::process::{Child, Command, Stdio};
#[cfg(target_os = "macos")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, atomic::AtomicBool};
#[cfg(target_os = "macos")]
use std::time::{Duration, Instant};

use crate::{SandboxCancellation, SandboxError, SandboxOutput, profile::Admitted};

#[cfg(target_os = "macos")]
struct ProcessOwner {
    child: Child,
    group: nix::unistd::Pid,
    armed: bool,
}

#[cfg(target_os = "macos")]
impl Drop for ProcessOwner {
    fn drop(&mut self) {
        if self.armed {
            // Best effort on unwinding; ordinary completion propagates cleanup errors.
            let _ = nix::sys::signal::killpg(self.group, nix::sys::signal::Signal::SIGKILL);
            let _ = self.child.wait();
        }
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn execute(
    admitted: Admitted,
    cancel: SandboxCancellation,
    dropped: Arc<AtomicBool>,
) -> Result<SandboxOutput, SandboxError> {
    use nix::{
        sys::signal::{Signal, killpg},
        unistd::Pid,
    };
    use rustix::process::{WaitId, WaitIdOptions, waitid};
    use std::os::unix::process::CommandExt;

    if cancel.is_cancelled() || dropped.load(Ordering::Acquire) {
        return Err(SandboxError::Cancelled);
    }
    let mut command = Command::new("/usr/bin/sandbox-exec");
    command.arg("-p").arg(admitted.profile);
    for parameter in admitted.parameters {
        command.arg("-D").arg(parameter);
    }
    command
        .arg(admitted.executable)
        .args(admitted.arguments)
        .current_dir(&admitted.request.cwd)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("LANG", "C")
        .env("LC_ALL", "C")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let child = command
        .spawn()
        .map_err(|error| std::io::Error::new(error.kind(), format!("spawn sandbox: {error}")))?;
    let group = Pid::from_raw(i32::try_from(child.id()).map_err(|_| SandboxError::Worker)?);
    let wait_pid = rustix::process::Pid::from_raw(group.as_raw()).ok_or(SandboxError::Worker)?;
    let mut owner = ProcessOwner {
        child,
        group,
        armed: true,
    };
    let total = Arc::new(AtomicUsize::new(0));
    let overflow = Arc::new(AtomicBool::new(false));
    let stdout = capture(
        owner.child.stdout.take().ok_or(SandboxError::Worker)?,
        total.clone(),
        overflow.clone(),
        admitted.request.max_output_bytes,
    );
    let stderr = capture(
        owner.child.stderr.take().ok_or(SandboxError::Worker)?,
        total,
        overflow.clone(),
        admitted.request.max_output_bytes,
    );
    let mut input = owner.child.stdin.take().ok_or(SandboxError::Worker)?;
    let writer = std::thread::spawn(move || input.write_all(&admitted.request.stdin));
    let started = Instant::now();
    let outcome = loop {
        if cancel.is_cancelled() || dropped.load(Ordering::Acquire) {
            break Err(SandboxError::Cancelled);
        }
        if overflow.load(Ordering::Acquire) {
            break Err(SandboxError::OutputLimit);
        }
        if started.elapsed() >= admitted.request.timeout {
            break Err(SandboxError::Timeout);
        }
        // Keep the zombie leader unreaped until group termination, preventing
        // its PID from being recycled between observing exit and killpg.
        match waitid(
            WaitId::Pid(wait_pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        ) {
            Ok(None) => std::thread::sleep(Duration::from_millis(5)),
            Ok(Some(_)) => break Ok(()),
            Err(rustix::io::Errno::INTR) => continue,
            Err(error) => {
                break Err(SandboxError::Io(std::io::Error::other(format!(
                    "waitid sandbox: {error}"
                ))));
            }
        }
    };
    // Also terminate background descendants after a successful leader exit.
    let cleanup = match killpg(group, Signal::SIGKILL) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
        // Darwin reports EPERM when only an unreaped zombie remains in the group.
        Err(nix::errno::Errno::EPERM) if outcome.is_ok() => Ok(()),
        Err(error) => Err(SandboxError::Io(std::io::Error::other(format!(
            "killpg sandbox: {error}"
        )))),
    };
    let reap = owner.child.wait();
    if reap.is_ok() {
        owner.armed = false;
    }
    let out = stdout.join().map_err(|_| SandboxError::Worker)?;
    let err = stderr.join().map_err(|_| SandboxError::Worker)?;
    let input_result = writer.join().map_err(|_| SandboxError::Worker)?;
    cleanup?;
    let status = reap?;
    outcome?;
    if overflow.load(Ordering::Acquire) {
        return Err(SandboxError::OutputLimit);
    }
    // An early exit may legitimately close stdin without consuming it.
    if let Err(error) = input_result
        && error.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(error.into());
    }
    Ok(SandboxOutput {
        exit_code: status.code(),
        stdout: out?,
        stderr: err?,
    })
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn execute(
    _: Admitted,
    _: SandboxCancellation,
    _: Arc<AtomicBool>,
) -> Result<SandboxOutput, SandboxError> {
    Err(SandboxError::Unsupported)
}

#[cfg(target_os = "macos")]
fn capture<R: Read + Send + 'static>(
    mut reader: R,
    total: Arc<AtomicUsize>,
    overflow: Arc<AtomicBool>,
    limit: usize,
) -> std::thread::JoinHandle<Result<Vec<u8>, std::io::Error>> {
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                return Ok(output);
            }
            let previous = total.fetch_add(count, Ordering::AcqRel);
            let retain = count.min(limit.saturating_sub(previous));
            output.extend_from_slice(&buffer[..retain]);
            if retain != count {
                overflow.store(true, Ordering::Release);
            }
        }
    })
}
