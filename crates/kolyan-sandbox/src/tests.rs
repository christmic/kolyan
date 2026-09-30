use super::*;

#[test]
fn rejects_broad_and_missing_roots() {
    assert!(
        profile::canonical_config(SandboxConfig {
            read_roots: vec!["/".into()],
            write_roots: vec![],
            protected_roots: vec![]
        })
        .is_err()
    );
    assert!(
        profile::canonical_config(SandboxConfig {
            read_roots: vec![],
            write_roots: vec![],
            protected_roots: vec![]
        })
        .is_err()
    );
}

#[test]
fn missing_backend_is_explicit() {
    let root = tempfile::tempdir().unwrap();
    assert!(matches!(
        require_backend(&root.path().join("missing")),
        Err(SandboxError::BackendMissing)
    ));
}

#[cfg(not(target_os = "macos"))]
#[test]
fn unsupported_backend_is_explicit() {
    assert!(matches!(
        MacOsSandbox::new(SandboxConfig {
            read_roots: vec![],
            write_roots: vec![],
            protected_roots: vec![]
        }),
        Err(SandboxError::Unsupported)
    ));
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use std::io::Read;
    use std::os::unix::fs::symlink;

    fn setup() -> (tempfile::TempDir, MacOsSandbox) {
        let root = tempfile::tempdir().unwrap();
        let sandbox = MacOsSandbox::new(SandboxConfig {
            read_roots: vec![],
            write_roots: vec![root.path().into()],
            protected_roots: vec![],
        })
        .unwrap();
        (root, sandbox)
    }

    fn request(root: &std::path::Path, script: impl Into<String>) -> SandboxRequest {
        SandboxRequest {
            command: SandboxCommand::Shell(script.into()),
            cwd: root.into(),
            stdin: vec![],
            timeout: Duration::from_secs(5),
            max_output_bytes: 65536,
        }
    }

    #[tokio::test]
    async fn admitted_io_and_environment() {
        let (root, sandbox) = setup();
        let output = sandbox
            .execute(
                request(
                    root.path(),
                    "printf ok > inside; /bin/cat inside; /usr/bin/env",
                ),
                SandboxCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(output.exit_code, Some(0));
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.starts_with("ok"));
        for line in text[2..].lines() {
            assert!(
                line.starts_with("PATH=")
                    || line.starts_with("LANG=")
                    || line.starts_with("LC_ALL=")
                    || line.starts_with("PWD=")
                    || line.starts_with("SHLVL=")
                    || line.starts_with("_="),
                "unexpected inherited environment: {line}"
            );
        }
    }

    #[tokio::test]
    async fn denies_outside_and_symlink_reads_and_writes() {
        let (root, sandbox) = setup();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), "not-visible").unwrap();
        symlink(outside.path().join("secret"), root.path().join("escape")).unwrap();
        symlink(outside.path(), root.path().join("outside")).unwrap();
        let outside_path = outside.path().canonicalize().unwrap();
        for command in [
            format!("/bin/cat '{}'", outside_path.join("secret").display()),
            "/bin/cat escape".into(),
            "printf leak > outside/new".into(),
            format!("printf leak > '{}/new'", outside_path.display()),
        ] {
            let output = sandbox
                .execute(
                    request(root.path(), command),
                    SandboxCancellation::default(),
                )
                .await
                .unwrap();
            assert_ne!(output.exit_code, Some(0));
            assert!(!String::from_utf8_lossy(&output.stdout).contains("not-visible"));
        }
        assert!(!outside.path().join("new").exists());
    }

    #[tokio::test]
    async fn denies_loopback_and_external_network() {
        let (root, sandbox) = setup();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let connected = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        // A completed client connect does not guarantee immediate accept readiness.
        // Bound the positive-control wait without changing negative-probe assertions.
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let accepted = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "host positive connection was not accepted within one second"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => panic!("host positive-control accept failed: {error}"),
            }
        };
        drop((connected, accepted));
        for address in [format!("127.0.0.1:{port}"), "1.1.1.1:80".into()] {
            std::fs::write(root.path().join("network-address"), address).unwrap();
            let mut input = request(root.path(), "unused");
            input.command = SandboxCommand::Executable {
                path: std::env::current_exe().unwrap(),
                arguments: vec![
                    "--exact".into(),
                    "tests::macos::network_child_probe".into(),
                    "--nocapture".into(),
                ],
            };
            let output = sandbox
                .execute(input, SandboxCancellation::default())
                .await
                .unwrap();
            assert_eq!(
                output.exit_code,
                Some(0),
                "network probe failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                std::fs::read_to_string(root.path().join("network-result")).unwrap(),
                "denied"
            );
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn network_child_probe() {
        let cwd = std::env::current_dir().unwrap();
        let Ok(address) = std::fs::read_to_string(cwd.join("network-address")) else {
            return;
        };
        let endpoint = address.parse().unwrap();
        let error =
            std::net::TcpStream::connect_timeout(&endpoint, Duration::from_secs(1)).unwrap_err();
        assert_eq!(
            error.raw_os_error(),
            Some(1),
            "must be OS EPERM, not timeout/refused service: {error}"
        );
        std::fs::write(cwd.join("network-result"), "denied").unwrap();
    }

    #[tokio::test]
    async fn bounded_output_and_timeout() {
        let (root, sandbox) = setup();
        let mut input = request(
            root.path(),
            "while :; do printf 1234567890; printf abcdefghij >&2; done",
        );
        input.max_output_bytes = 128;
        assert!(matches!(
            sandbox.execute(input, SandboxCancellation::default()).await,
            Err(SandboxError::OutputLimit)
        ));
        let mut input = request(root.path(), "/bin/sleep 10");
        input.timeout = Duration::from_millis(100);
        assert!(matches!(
            sandbox.execute(input, SandboxCancellation::default()).await,
            Err(SandboxError::Timeout)
        ));
    }

    async fn child_pid(root: &std::path::Path) -> i32 {
        for _ in 0..100 {
            if let Ok(mut file) = std::fs::File::open(root.join("pid")) {
                let mut text = String::new();
                file.read_to_string(&mut text).unwrap();
                if let Ok(pid) = text.trim().parse() {
                    return pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("child did not start");
    }

    async fn assert_dead(pid: i32) {
        for _ in 0..100 {
            if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
                == Err(nix::errno::Errno::ESRCH)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("descendant {pid} survived cleanup");
    }

    #[tokio::test]
    async fn cancel_and_drop_reap_descendants() {
        for explicit in [true, false] {
            let (root, sandbox) = setup();
            let cancel = SandboxCancellation::default();
            let input = request(root.path(), "/bin/sleep 30 & echo $! > pid; wait");
            let token = cancel.clone();
            let execution = tokio::spawn(async move { sandbox.execute(input, token).await });
            let pid = child_pid(root.path()).await;
            if explicit {
                cancel.cancel();
                assert!(matches!(
                    execution.await.unwrap(),
                    Err(SandboxError::Cancelled)
                ));
            } else {
                execution.abort();
                assert!(execution.await.unwrap_err().is_cancelled());
            }
            assert_dead(pid).await;
        }
    }

    #[tokio::test]
    async fn canonical_parameters_cannot_inject_policy() {
        let root = tempfile::tempdir().unwrap();
        let strange = root.path().join("quote\" (allow network*)");
        std::fs::create_dir(&strange).unwrap();
        let sandbox = MacOsSandbox::new(SandboxConfig {
            read_roots: vec![],
            write_roots: vec![strange.clone()],
            protected_roots: vec![],
        })
        .unwrap();
        let output = sandbox
            .execute(
                request(&strange, "printf exact"),
                SandboxCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(output.stdout, b"exact");
        let outside = tempfile::tempdir().unwrap();
        assert!(matches!(
            sandbox
                .execute(
                    request(outside.path(), "true"),
                    SandboxCancellation::default()
                )
                .await,
            Err(SandboxError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn protected_controls_and_read_only_root() {
        let root = tempfile::tempdir().unwrap();
        for name in [".git", ".kolyan", "private-control"] {
            std::fs::create_dir(root.path().join(name)).unwrap();
            std::fs::write(root.path().join(name).join("state"), "protected").unwrap();
        }
        let sandbox = MacOsSandbox::new(SandboxConfig {
            read_roots: vec![],
            write_roots: vec![root.path().into()],
            protected_roots: vec![root.path().join("private-control")],
        })
        .unwrap();
        for name in [".git", ".kolyan", "private-control"] {
            for script in [
                format!("/bin/cat {name}/state"),
                format!("printf bad > {name}/state"),
                format!("/bin/mv {name} renamed"),
            ] {
                let output = sandbox
                    .execute(request(root.path(), script), SandboxCancellation::default())
                    .await
                    .unwrap();
                assert_ne!(output.exit_code, Some(0));
            }
            assert_eq!(
                std::fs::read(root.path().join(name).join("state")).unwrap(),
                b"protected"
            );
        }
        let sandbox = MacOsSandbox::new(SandboxConfig {
            read_roots: vec![root.path().into()],
            write_roots: vec![],
            protected_roots: vec![],
        })
        .unwrap();
        assert_ne!(
            sandbox
                .execute(
                    request(root.path(), "printf bad > new"),
                    SandboxCancellation::default()
                )
                .await
                .unwrap()
                .exit_code,
            Some(0)
        );
    }

    // Invoked as a child of the sandboxed shell, so it is NOT a group leader.
    // This tests actual syscall enforcement rather than setsid's leader EPERM.
    #[test]
    fn detached_child_probe() {
        let cwd = std::env::current_dir().unwrap();
        if !cwd.join("detach-mode").exists() {
            return;
        }
        assert_ne!(
            nix::unistd::getpgrp(),
            nix::unistd::getpid(),
            "probe must not be a group leader"
        );
        let result = nix::unistd::setsid();
        let regroup =
            nix::unistd::setpgid(nix::unistd::Pid::from_raw(0), nix::unistd::Pid::from_raw(0));
        use std::os::unix::process::CommandExt;
        let spawned = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn();
        let spawn_escape = spawned.is_ok();
        if let Ok(mut child) = spawned {
            let _ = child.kill();
            let _ = child.wait();
        }
        std::fs::write(
            cwd.join("detach-result"),
            if result.is_ok() || regroup.is_ok() || spawn_escape {
                "escaped"
            } else {
                "denied"
            },
        )
        .unwrap();
        std::fs::write(cwd.join("detached-pid"), std::process::id().to_string()).unwrap();
        if result.is_ok() || regroup.is_ok() {
            std::thread::sleep(Duration::from_secs(30));
        }
    }

    #[tokio::test]
    async fn denies_detached_session_escape() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("detach-mode"), "probe").unwrap();
        let executable = std::env::current_exe().unwrap();
        let mut input = request(
            root.path(),
            format!(
                "'{}' --exact tests::macos::detached_child_probe --nocapture & wait",
                executable.display()
            ),
        );
        // The executable is outside the workspace and is admitted explicitly;
        // its parent directory is not granted to the subprocess.
        let sandbox = MacOsSandbox::new(SandboxConfig {
            read_roots: vec![executable.parent().unwrap().into()],
            write_roots: vec![root.path().into()],
            protected_roots: vec![],
        })
        .unwrap();
        input.timeout = Duration::from_millis(500);
        let result = sandbox.execute(input, SandboxCancellation::default()).await;
        let observed = std::fs::read_to_string(root.path().join("detach-result"))
            .unwrap_or_else(|error| panic!("probe did not run: {error}; process={result:?}"));
        if observed == "escaped" {
            let pid: i32 = std::fs::read_to_string(root.path().join("detached-pid"))
                .unwrap()
                .parse()
                .unwrap();
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            )
            .unwrap();
        }
        assert_eq!(observed, "denied", "setsid escaped: {result:?}");
        assert_eq!(result.unwrap().exit_code, Some(0));
    }

    #[tokio::test]
    async fn executable_argv_and_bounded_stdin() {
        let (root, sandbox) = setup();
        let mut input = request(root.path(), "unused");
        input.command = SandboxCommand::Executable {
            path: "/usr/bin/wc".into(),
            arguments: vec!["-c".into()],
        };
        input.stdin = b"worker-input".to_vec();
        let output = sandbox
            .execute(input, SandboxCancellation::default())
            .await
            .unwrap();
        assert_eq!(output.exit_code, Some(0));
        assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "12");
    }

    #[tokio::test]
    async fn host_selected_executor_port_enforces_real_policy() {
        let (root, sandbox) = setup();
        let port: Arc<dyn SandboxExecutor> = Arc::new(sandbox);
        assert_eq!(port.policy_revision(), MACOS_SEATBELT_POLICY_REVISION);
        let output = port
            .execute(
                request(root.path(), "printf port"),
                SandboxCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(output.exit_code, Some(0));
        assert_eq!(output.stdout, b"port");
        let denied = port
            .execute(
                request(root.path(), "printf bad > .kolyan"),
                SandboxCancellation::default(),
            )
            .await
            .unwrap();
        assert_ne!(denied.exit_code, Some(0));
    }

    #[tokio::test]
    async fn timeout_and_success_remove_background_children() {
        for timeout in [true, false] {
            let (root, sandbox) = setup();
            let mut input = request(
                root.path(),
                if timeout {
                    "/bin/sleep 30 & echo $! > pid; wait"
                } else {
                    "/bin/sleep 30 & echo $! > pid; /bin/sleep 0.05; exit 0"
                },
            );
            if timeout {
                input.timeout = Duration::from_millis(100);
            }
            let result = sandbox.execute(input, SandboxCancellation::default()).await;
            let pid = child_pid(root.path()).await;
            if timeout {
                assert!(matches!(result, Err(SandboxError::Timeout)));
            } else {
                assert_eq!(result.unwrap().exit_code, Some(0));
            }
            assert_dead(pid).await;
        }
    }

    #[test]
    fn runtime_shutdown_does_not_abandon_descendants() {
        let (root, sandbox) = setup();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let pid = runtime.block_on(async {
            let input = request(root.path(), "/bin/sleep 30 & echo $! > pid; wait");
            let _handle = tokio::spawn(async move {
                sandbox.execute(input, SandboxCancellation::default()).await
            });
            child_pid(root.path()).await
        });
        drop(runtime);
        for _ in 0..100 {
            if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None)
                == Err(nix::errno::Errno::ESRCH)
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("runtime shutdown left descendant {pid} alive");
    }

    #[tokio::test]
    async fn cancelled_and_invalid_requests_have_no_effects() {
        let (root, sandbox) = setup();
        let token = SandboxCancellation::default();
        token.cancel();
        assert!(matches!(
            sandbox
                .execute(request(root.path(), "printf bad > forbidden"), token)
                .await,
            Err(SandboxError::Cancelled)
        ));
        let mut input = request(root.path(), "printf bad > forbidden");
        input.timeout = Duration::ZERO;
        assert!(matches!(
            sandbox.execute(input, SandboxCancellation::default()).await,
            Err(SandboxError::Invalid(_))
        ));
        assert!(!root.path().join("forbidden").exists());
        assert_ne!(
            sandbox
                .execute(
                    request(root.path(), "mkdir .git"),
                    SandboxCancellation::default()
                )
                .await
                .unwrap()
                .exit_code,
            Some(0)
        );
    }
}
