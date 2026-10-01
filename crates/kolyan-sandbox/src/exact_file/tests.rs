use super::*;
use crate::MacOsSandbox;
use std::time::Duration;

fn config(root: &Path) -> FileSandboxConfig {
    FileSandboxConfig {
        workspace: root.to_path_buf(),
        read_files: vec![root.join("read")],
        write_files: vec![root.join("target")],
        protected_roots: vec![],
    }
}

fn request(root: &Path, command: SandboxCommand) -> SandboxRequest {
    SandboxRequest {
        command,
        cwd: root.to_path_buf(),
        stdin: vec![],
        max_input_bytes: 65536,
        timeout: Duration::from_secs(5),
        max_output_bytes: 65536,
    }
}

#[test]
fn physical_admission_and_literal_rendering() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let unusual = root.join("quote\"(deny default)=file");
    let mut input = config(&root);
    input.read_files = vec![unusual.clone()];
    let admitted = canonical_config(input).unwrap();
    let mut profile = String::new();
    let mut parameters = vec![];
    render(&admitted, &mut profile, &mut parameters).unwrap();
    assert!(!profile.contains("subpath"));
    assert!(!profile.contains("path-ancestors"));
    assert!(!profile.contains("quote"));
    assert!(parameters.contains(&format!("READ0={}", unusual.display())));
    assert!(
        profile.contains("(allow file-write* file-read-metadata (literal (param \"WRITE0\")))")
    );
    assert!(admitted.protected_roots.contains(&root.join(".git")));
}

#[test]
fn refuses_invalid_and_protected_paths() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    std::fs::create_dir(root.join("protected")).unwrap();
    std::fs::create_dir(root.join(".git")).unwrap();
    for path in [
        PathBuf::from("/"),
        PathBuf::from("relative"),
        root.clone(),
        root.join("bad\npath"),
        root.join("bad\rpath"),
        root.join("bad\0path"),
        root.join("protected/file"),
        root.join(".git/config"),
        root.join(".kolyan"),
        root.join("missing-parent/file"),
        root.join("../other"),
        PathBuf::from(format!("{}/read/", root.display())),
        PathBuf::from(format!("{}/./read", root.display())),
    ] {
        let mut input = config(&root);
        input.read_files = vec![path.clone()];
        input.protected_roots = vec![root.join("protected")];
        assert!(
            canonical_config(input).is_err(),
            "admitted {}",
            path.display()
        );
    }
    let mut input = config(&root);
    input.protected_roots = vec![PathBuf::from("/")];
    assert!(canonical_config(input).is_err());
    let mut input = config(&root);
    input.protected_roots = vec![root.clone()];
    assert!(canonical_config(input).is_err());
    let mut input = config(&root);
    input.read_files.clear();
    input.write_files.clear();
    assert!(canonical_config(input).is_err());
}

#[cfg(unix)]
#[test]
fn refuses_final_symlinks_and_redirected_parents_even_for_missing_leaves() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    std::fs::write(root.join("original"), "data").unwrap();
    std::fs::create_dir(root.join("directory")).unwrap();
    symlink(root.join("original"), root.join("redirect")).unwrap();
    symlink(root.join("absent"), root.join("dangling")).unwrap();
    symlink(root.join("directory"), root.join("alias")).unwrap();
    for path in [
        root.join("redirect"),
        root.join("dangling"),
        root.join("alias/new"),
    ] {
        let mut input = config(&root);
        input.write_files = vec![path];
        assert!(canonical_config(input).is_err());
    }
    let admitted = canonical_config(config(&root)).unwrap();
    symlink(root.join("original"), root.join("target")).unwrap();
    assert!(
        admit(
            &admitted,
            &request(
                &root,
                SandboxCommand::Executable {
                    path: "/bin/cat".into(),
                    arguments: vec![],
                }
            )
        )
        .is_err()
    );
}

#[test]
fn exact_requests_refuse_shell_and_foreign_cwd() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let admitted = canonical_config(config(&root)).unwrap();
    assert!(
        admit(
            &admitted,
            &request(&root, SandboxCommand::Shell("true".into()))
        )
        .is_err()
    );
    std::fs::create_dir(root.join("nested")).unwrap();
    assert!(
        admit(
            &admitted,
            &request(
                &root.join("nested"),
                SandboxCommand::Executable {
                    path: "/bin/cat".into(),
                    arguments: vec![],
                }
            )
        )
        .is_err()
    );
}

#[cfg(not(target_os = "macos"))]
#[test]
fn unsupported_exact_backend_fails_closed() {
    assert!(matches!(
        MacOsSandbox::new_files(config(Path::new("/"))),
        Err(SandboxError::Unsupported)
    ));
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use crate::SandboxCancellation;
    use std::io::Read;
    use std::os::unix::fs::symlink;

    #[tokio::test]
    async fn cat_reads_only_the_selected_literal_and_shell_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let input = config(&root);
        std::fs::write(&input.read_files[0], "allowed").unwrap();
        std::fs::write(root.join("sibling"), "sibling-content").unwrap();
        std::fs::write(root.join("target"), "write-only-content").unwrap();
        symlink(root.join("sibling"), root.join("escape")).unwrap();
        let sandbox = MacOsSandbox::new_files(input).unwrap();
        for (leaf, expected) in [
            ("read", Some("allowed")),
            ("sibling", None),
            ("target", None),
            ("escape", None),
        ] {
            let output = sandbox
                .execute(
                    request(
                        &root,
                        SandboxCommand::Executable {
                            path: "/bin/cat".into(),
                            arguments: vec![root.join(leaf).to_str().unwrap().into()],
                        },
                    ),
                    SandboxCancellation::default(),
                )
                .await
                .unwrap();
            if let Some(expected) = expected {
                assert_eq!(
                    output.exit_code,
                    Some(0),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(output.stdout, expected.as_bytes());
            } else {
                assert_ne!(output.exit_code, Some(0));
                assert!(output.stdout.is_empty());
            }
        }
        assert!(matches!(
            sandbox
                .execute(
                    request(&root, SandboxCommand::Shell("true".into())),
                    SandboxCancellation::default()
                )
                .await,
            Err(SandboxError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn real_file_worker_enforces_metadata_write_controls_and_network() {
        let temp = tempfile::tempdir().unwrap();
        let staging_temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let staging_root = staging_temp.path().canonicalize().unwrap();
        let target = root.join("target");
        let staging = staging_root.join("staging");
        std::fs::write(root.join("sibling"), "untouched").unwrap();
        std::fs::write(staging_root.join("sibling"), "untouched").unwrap();
        std::fs::create_dir(root.join("protected")).unwrap();
        std::fs::write(root.join("protected/data"), "hidden").unwrap();
        std::fs::create_dir(root.join(".git")).unwrap();
        std::fs::write(root.join(".git/config"), "hidden").unwrap();
        let sandbox = MacOsSandbox::new_files(FileSandboxConfig {
            workspace: root.clone(),
            read_files: vec![target.clone()],
            write_files: vec![target.clone(), staging.clone()],
            protected_roots: vec![root.join("protected")],
        })
        .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let positive = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let accepted = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => panic!("host network positive control failed: {error}"),
            }
        };
        drop((positive, accepted));
        let mut input = request(
            &root,
            SandboxCommand::Executable {
                path: std::env::current_exe().unwrap(),
                arguments: vec![
                    "--exact".into(),
                    "exact_file::tests::macos::file_child_probe".into(),
                    "--nocapture".into(),
                ],
            },
        );
        input.stdin = format!(
            "{}\n{}\n{}\n",
            staging.display(),
            target.display(),
            listener.local_addr().unwrap()
        )
        .into_bytes();
        let output = sandbox
            .execute(input, SandboxCancellation::default())
            .await
            .unwrap();
        assert_eq!(
            output.exit_code,
            Some(0),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"replacement");
        assert!(!staging.exists());
        for parent in [&root, &staging_root] {
            assert_eq!(std::fs::read(parent.join("sibling")).unwrap(), b"untouched");
            assert!(!parent.join("unselected").exists());
            assert!(!parent.join("new-directory").exists());
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn file_child_probe() {
        if !std::env::args().any(|arg| arg == "--exact") {
            return;
        }
        let mut data = String::new();
        std::io::stdin().read_to_string(&mut data).unwrap();
        let lines: Vec<_> = data.lines().collect();
        assert_eq!(lines.len(), 3);
        let staging = Path::new(lines[0]);
        let target = Path::new(lines[1]);
        let cwd = std::env::current_dir().unwrap();
        for parent in [cwd.as_path(), staging.parent().unwrap()] {
            // Opening a directory is a positive gate, not just stat permission.
            std::fs::File::open(parent).unwrap();
            std::fs::metadata(parent).unwrap();
            assert!(std::fs::read_dir(parent).unwrap().next().is_some());
            assert!(std::fs::read(parent.join("sibling")).is_err());
            assert!(std::fs::write(parent.join("sibling"), "bad").is_err());
            assert!(std::fs::write(parent.join("unselected"), "bad").is_err());
            assert!(std::fs::create_dir(parent.join("new-directory")).is_err());
            assert!(std::fs::write(parent.join(".kolyan"), "bad").is_err());
        }
        for protected in [cwd.join("protected"), cwd.join(".git")] {
            assert!(std::fs::metadata(&protected).is_err());
            assert!(std::fs::read_dir(&protected).is_err());
            assert!(std::fs::write(protected.join("new"), "bad").is_err());
        }
        assert!(std::fs::read(cwd.join("protected/data")).is_err());
        assert!(std::fs::read(cwd.join(".git/config")).is_err());
        std::fs::write(target, "initial").unwrap();
        std::fs::write(staging, "replacement").unwrap();
        std::fs::rename(staging, target).unwrap();
        assert_eq!(std::fs::read(target).unwrap(), b"replacement");
        for address in [lines[2], "1.1.1.1:80"] {
            let error = std::net::TcpStream::connect_timeout(
                &address.parse().unwrap(),
                Duration::from_secs(1),
            )
            .unwrap_err();
            assert_eq!(
                error.raw_os_error(),
                Some(1),
                "expected kernel EPERM: {error}"
            );
        }
        for (key, _) in std::env::vars() {
            assert!(
                ["PATH", "LANG", "LC_ALL"].contains(&key.as_str()),
                "inherited {key}"
            );
        }
    }
}
