#![cfg(target_os = "macos")]

use super::*;

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

fn request(root: &std::path::Path) -> SandboxRequest {
    SandboxRequest {
        command: SandboxCommand::Shell("/usr/bin/wc -c".into()),
        cwd: root.into(),
        stdin: vec![b'x'; 4096],
        max_input_bytes: 4096,
        timeout: Duration::from_secs(5),
        max_output_bytes: 32,
    }
}

#[tokio::test]
async fn input_above_output_ceiling_is_admitted_with_its_own_bound() {
    let (root, sandbox) = setup();
    let output = sandbox
        .execute(request(root.path()), SandboxCancellation::default())
        .await
        .unwrap();
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "4096");
    assert!(output.stderr.is_empty());
}

#[tokio::test]
async fn excessive_input_is_refused_before_process_effects() {
    let (root, sandbox) = setup();
    for limit in [0, 4095, 64 * 1024 * 1024 + 1] {
        let mut input = request(root.path());
        input.max_input_bytes = limit;
        input.command = SandboxCommand::Shell("printf changed > marker".into());
        assert!(matches!(
            sandbox.execute(input, SandboxCancellation::default()).await,
            Err(SandboxError::Invalid(_))
        ));
        assert!(!root.path().join("marker").exists());
    }
}

#[tokio::test]
async fn zero_input_ceiling_permits_only_empty_stdin() {
    let (root, sandbox) = setup();
    let mut input = request(root.path());
    input.stdin.clear();
    input.max_input_bytes = 0;
    input.command = SandboxCommand::Shell("printf x".into());
    input.max_output_bytes = 1;
    let output = sandbox
        .execute(input, SandboxCancellation::default())
        .await
        .unwrap();
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.stdout, b"x");
    assert!(output.stderr.is_empty());
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EofCase {
    id: String,
    bytes: usize,
}

#[tokio::test]
async fn stdin_eof_completes_empty_small_and_backpressured_inputs() {
    let cases: Vec<EofCase> =
        serde_json::from_str(include_str!("input_limits_tests/eof_cases.json")).unwrap();
    let (root, sandbox) = setup();
    for case in cases {
        let mut input = request(root.path());
        input.stdin = vec![b'x'; case.bytes];
        input.max_input_bytes = case.bytes;
        let output = sandbox
            .execute(input, SandboxCancellation::default())
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", case.id));
        assert_eq!(output.exit_code, Some(0), "{}", case.id);
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            case.bytes.to_string(),
            "{}: wc must receive EOF after the complete input",
            case.id
        );
        assert!(output.stderr.is_empty(), "{}", case.id);
    }
}
