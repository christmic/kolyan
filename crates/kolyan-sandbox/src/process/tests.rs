use super::*;

use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use rustix::process::{WaitId, WaitIdOptions, waitid};
use std::os::unix::process::CommandExt;

use crate::{MacOsSandbox, SandboxCommand, SandboxConfig, SandboxRequest};

fn exited_leader(script: &str) -> (ProcessOwner, rustix::process::Pid) {
    let child = Command::new("/bin/sh")
        .args(["-c", script])
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .unwrap();
    let group = Pid::from_raw(i32::try_from(child.id()).unwrap());
    let pid = rustix::process::Pid::from_raw(group.as_raw()).unwrap();
    let owner = ProcessOwner {
        child,
        group,
        armed: true,
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if waitid(
            WaitId::Pid(pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        )
        .unwrap()
        .is_some()
        {
            return (owner, pid);
        }
        assert!(Instant::now() < deadline, "leader did not exit");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn actual_darwin_zombie_group_eperm_is_verified_before_reaping() {
    let (mut owner, pid) = exited_leader("exit 0");
    let signal = killpg(owner.group, Signal::SIGKILL);
    assert_eq!(
        signal,
        Err(Errno::EPERM),
        "Darwin zombie-only positive control"
    );
    assert!(zombie_only_group(owner.group, pid).unwrap());
    check_group_cleanup(signal, || zombie_only_group(owner.group, pid)).unwrap();
    assert!(owner.child.wait().unwrap().success());
    owner.armed = false;
}

#[test]
fn exited_leader_does_not_hide_live_descendant_permission_failure() {
    let (mut owner, pid) = exited_leader("/bin/sleep 30 & exit 0");
    assert!(!zombie_only_group(owner.group, pid).unwrap());
    // Inject denial without needing privilege changes; the member-state check
    // uses the real live group, and teardown sends the real permitted signal.
    let result = check_group_cleanup(Err(Errno::EPERM), || zombie_only_group(owner.group, pid));
    assert!(
        matches!(result, Err(SandboxError::Io(ref error)) if error.to_string().contains("killpg sandbox: EPERM"))
    );
    killpg(owner.group, Signal::SIGKILL).unwrap();
    owner.child.wait().unwrap();
    owner.armed = false;
}

#[test]
fn real_cleanup_and_inspection_errors_are_not_suppressed() {
    let denied = check_group_cleanup(Err(Errno::EACCES), || panic!("no verification for EACCES"));
    assert!(
        matches!(denied, Err(SandboxError::Io(ref error)) if error.to_string().contains("EACCES"))
    );
    let failed = check_group_cleanup(Err(Errno::EPERM), || {
        Err(SandboxError::Io(std::io::Error::other(
            "inspection unavailable",
        )))
    });
    assert!(
        matches!(failed, Err(SandboxError::Io(ref error)) if error.to_string() == "inspection unavailable")
    );
    check_group_cleanup(Err(Errno::ESRCH), || {
        panic!("absent group needs no inspection")
    })
    .unwrap();
}

#[tokio::test]
async fn short_lived_output_overflow_retains_exact_error() {
    let root = tempfile::tempdir().unwrap();
    let sandbox = MacOsSandbox::new(SandboxConfig {
        read_roots: vec![],
        write_roots: vec![root.path().into()],
        protected_roots: vec![],
    })
    .unwrap();
    let logs = tempfile::Builder::new()
        .prefix("kolyan-cleanup-overflow-")
        .tempdir()
        .unwrap()
        .keep();
    let mut observations = String::new();
    for iteration in 0..64 {
        let result = sandbox.execute(SandboxRequest {
            command: SandboxCommand::Shell("i=0; while [ $i -lt 40 ]; do printf '\\000'; i=$((i+1)); done; printf '\\377' >&2".into()),
            cwd: root.path().into(), stdin: vec![], max_input_bytes: 0,
            timeout: Duration::from_secs(5), max_output_bytes: 40,
        }, SandboxCancellation::default()).await;
        observations.push_str(&format!("iteration={iteration} result={result:?}\n"));
        std::fs::write(logs.join("actual.log"), &observations).unwrap();
        assert!(
            matches!(result, Err(SandboxError::OutputLimit)),
            "iteration={iteration}, result={result:?}, log={}",
            logs.display()
        );
    }
    eprintln!(
        "cleanup overflow observations: {}",
        logs.join("actual.log").display()
    );
}
