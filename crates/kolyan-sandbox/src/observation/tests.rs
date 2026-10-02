use super::*;

#[cfg(target_os = "macos")]
mod retained;

#[cfg(target_os = "macos")]
mod lifecycle_stress;

#[test]
fn full_and_closed_channels_report_loss_without_waiting() {
    let (sender, receiver) = sandbox_process_observation_channel(1).unwrap();
    let launch = sender.launch(123, 123);
    launch.emit(SandboxProcessEvent::Spawned);
    launch.emit(SandboxProcessEvent::Spawned);
    assert_eq!(receiver.dropped(), 1);
    drop(receiver);
    launch.emit(SandboxProcessEvent::Spawned);
    assert_eq!(sender.dropped(), 2);
}

#[test]
fn bounded_context_and_capacity_are_not_execution_authority() {
    assert!(sandbox_process_observation_channel(0).is_err());
    assert!(sandbox_process_observation_channel(1025).is_err());
    let (sender, receiver) = sandbox_process_observation_channel(2).unwrap();
    assert!(sender.bind_context("a".repeat(4097)).is_none());
    assert_eq!(receiver.dropped(), 1);
    assert!(sender.bind_context("a".repeat(4096)).is_some());
}

#[test]
fn launches_with_the_same_pid_have_distinct_identity() {
    let (sender, mut receiver) = sandbox_process_observation_channel(2).unwrap();
    sender.launch(123, 123).emit(SandboxProcessEvent::Spawned);
    sender.launch(123, 123).emit(SandboxProcessEvent::Spawned);
    let first = receiver.try_recv().unwrap();
    let second = receiver.try_recv().unwrap();
    assert_ne!(first.launch_id, second.launch_id);
    assert_eq!(first.pid, second.pid);
}

#[test]
fn errors_have_utf8_byte_bound_and_explicit_truncation_evidence() {
    let (sender, mut receiver) = sandbox_process_observation_channel(1).unwrap();
    sender
        .launch(123, 123)
        .emit(SandboxProcessEvent::CleanupFailed {
            error: "界".repeat(2000),
        });
    let row = receiver.try_recv().unwrap();
    assert!(row.errors_truncated);
    let SandboxProcessEvent::CleanupFailed { error } = row.event else {
        panic!("wrong observation kind")
    };
    assert!(error.len() <= 1024);
    assert!(error.ends_with(" [truncated]"));
}
