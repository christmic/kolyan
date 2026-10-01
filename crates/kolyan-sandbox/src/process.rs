//! Independent blocking owner: async cancellation never abandons the child.

#[cfg(target_os = "macos")]
use std::io::{Read, Write};
#[cfg(target_os = "macos")]
use std::os::unix::process::CommandExt;
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
    let cleanup = check_group_cleanup(killpg(group, Signal::SIGKILL), || {
        zombie_only_group(group, wait_pid)
    });
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

#[cfg(target_os = "macos")]
fn check_group_cleanup(
    signal: Result<(), nix::errno::Errno>,
    verify_zombies: impl FnOnce() -> Result<bool, SandboxError>,
) -> Result<(), SandboxError> {
    match signal {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
        Err(nix::errno::Errno::EPERM) if verify_zombies()? => Ok(()),
        Err(error) => Err(SandboxError::Io(std::io::Error::other(format!(
            "killpg sandbox: {error}"
        )))),
    }
}

#[cfg(target_os = "macos")]
fn zombie_only_group(
    group: nix::unistd::Pid,
    leader: rustix::process::Pid,
) -> Result<bool, SandboxError> {
    use rustix::process::{WaitId, WaitIdOptions, waitid};

    // EPERM also means a real permission denial. An exited leader alone does
    // not prove that no live descendant remains. Keep it unreaped throughout
    // this check so the group identity cannot be recycled.
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match waitid(
            WaitId::Pid(leader),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        ) {
            // killpg's exit filter can run before waitid observes the zombie.
            // Wait only within the inspection bound; a live denied leader is
            // still an error, never inferred to be dead from EPERM alone.
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(None) => return Ok(false),
            Ok(Some(_)) => break,
            Err(rustix::io::Errno::INTR) => continue,
            Err(error) => {
                return Err(SandboxError::Io(std::io::Error::other(format!(
                    "verify sandbox leader: {error}"
                ))));
            }
        }
    }
    // Darwin's killpg skips SZOMB members (XNU kern_sig.c, killpg1). The
    // fixed host inspector exposes only states for this pinned group, not
    // argv/environment. No model-controlled executable or option is used.
    let inspector = Command::new("/bin/ps")
        .args(["-g", &group.as_raw().to_string(), "-o", "stat="])
        .env_clear()
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()?;
    let mut inspector = ProcessOwner {
        group: nix::unistd::Pid::from_raw(
            i32::try_from(inspector.id()).map_err(|_| SandboxError::Worker)?,
        ),
        child: inspector,
        armed: true,
    };
    let overflow = Arc::new(AtomicBool::new(false));
    let states = capture(
        inspector.child.stdout.take().ok_or(SandboxError::Worker)?,
        Arc::new(AtomicUsize::new(0)),
        overflow.clone(),
        65536,
    );
    let status = loop {
        if let Some(status) = inspector.child.try_wait()? {
            inspector.armed = false;
            break status;
        }
        if overflow.load(Ordering::Acquire) || Instant::now() >= deadline {
            inspector.child.kill()?;
            inspector.child.wait()?;
            inspector.armed = false;
            states.join().map_err(|_| SandboxError::Worker)??;
            return Err(SandboxError::Io(std::io::Error::other(
                "sandbox group inspection exceeded its bound",
            )));
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let states = states.join().map_err(|_| SandboxError::Worker)??;
    if !status.success() || overflow.load(Ordering::Acquire) {
        return Err(SandboxError::Io(std::io::Error::other(
            "sandbox group inspection failed",
        )));
    }
    let states = std::str::from_utf8(&states).map_err(|error| {
        SandboxError::Io(std::io::Error::other(format!(
            "invalid sandbox group states: {error}"
        )))
    })?;
    let mut states = states.split_whitespace().peekable();
    Ok(states.peek().is_some() && states.all(|state| state.starts_with('Z')))
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

#[cfg(all(test, target_os = "macos"))]
mod tests;
