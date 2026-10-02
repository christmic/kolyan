//! Fixed host commands only. Credentials and arbitrary model host commands are absent.

use std::{
    fs,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::Serialize;
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};

use super::super::evidence::Evidence;
use super::{Plan, baseline};

const ORACLE_SHA: &str = "9297c3a5630dc44de313d77526a10e0027e28850526d05a635c44a67e772c4df";

pub(super) struct Verification {
    pub passed: bool,
    pub review_input: String,
}
#[derive(Serialize)]
struct Observation {
    id: String,
    program: PathBuf,
    arguments: Vec<String>,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
    output_overflow: bool,
}
struct StopGroup(u32);
impl Drop for StopGroup {
    fn drop(&mut self) {
        let _ = std::process::Command::new("/bin/kill")
            .args(["-KILL", &format!("-{}", self.0)])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

pub(super) async fn run(
    plan: &Plan,
    control: &Path,
    evidence: &Evidence,
) -> Result<Verification, String> {
    let oracle = plan.run.host_private.join("invocation_state_oracle.rs");
    if baseline::digest(&fs::read(&oracle).map_err(|e| e.to_string())?) != ORACLE_SHA {
        return Err("independent host oracle SHA differs".into());
    }
    let cargo = PathBuf::from(env!("CARGO"));
    let rustc = cargo
        .parent()
        .ok_or("cargo toolchain parent missing")?
        .join("rustc");
    let target = plan.run.host_private.join("cargo-target");
    let executable = control.join("invocation-state-oracle");
    let commands = [
        (
            "server-full",
            cargo.clone(),
            vec![
                "test".into(),
                "--offline".into(),
                "--locked".into(),
                "-p".into(),
                "kolyan-server".into(),
                "--lib".into(),
            ],
        ),
        (
            "server-focused",
            cargo.clone(),
            vec![
                "test".into(),
                "--offline".into(),
                "--locked".into(),
                "-p".into(),
                "kolyan-server".into(),
                "--lib".into(),
                "tasks::tests::invocation_state".into(),
                "--".into(),
                "--nocapture".into(),
            ],
        ),
        (
            "format",
            cargo.clone(),
            vec!["fmt".into(), "--all".into(), "--".into(), "--check".into()],
        ),
        (
            "server-strict",
            cargo.clone(),
            vec![
                "clippy".into(),
                "--offline".into(),
                "--locked".into(),
                "-p".into(),
                "kolyan-server".into(),
                "--lib".into(),
                "--tests".into(),
                "--".into(),
                "-D".into(),
                "warnings".into(),
            ],
        ),
        (
            "server-build",
            cargo,
            vec![
                "build".into(),
                "--offline".into(),
                "--locked".into(),
                "-p".into(),
                "kolyan-server".into(),
                "--lib".into(),
            ],
        ),
        (
            "oracle-compile",
            rustc,
            vec![
                oracle.to_string_lossy().into(),
                "--edition=2024".into(),
                "--extern".into(),
                format!(
                    "kolyan_server={}",
                    target.join("debug/libkolyan_server.rlib").display()
                ),
                "-L".into(),
                format!("dependency={}", target.join("debug/deps").display()),
                "-o".into(),
                executable.to_string_lossy().into(),
            ],
        ),
        ("oracle-run", executable, vec![]),
    ];
    let mut observations = Vec::new();
    for (id, program, arguments) in commands {
        // Never execute a stale oracle binary after a failed build/link.
        if id == "oracle-run"
            && observations.iter().any(|o: &Observation| {
                matches!(o.id.as_str(), "server-build" | "oracle-compile") && o.exit_code != Some(0)
            })
        {
            evidence.append(json!({"event":"self_iteration_host_validation_blocked","id":id,"reason":"current server build/oracle compile failed"}))?;
            continue;
        }
        let observed = observe(plan, &target, id, program, arguments).await?;
        fs::write(control.join(format!("{id}.stdout.log")), &observed.stdout)
            .map_err(|e| e.to_string())?;
        fs::write(control.join(format!("{id}.stderr.log")), &observed.stderr)
            .map_err(|e| e.to_string())?;
        evidence.append(json!({"event":"self_iteration_host_validation","oracle_sha256":ORACLE_SHA,"value":observed}))?;
        observations.push(observed);
    }
    let candidate_data = fs::read_to_string(plan.run.worktree.join(&plan.allowlist[3]))
        .map_err(|e| e.to_string())?;
    let candidate: serde_json::Value =
        serde_json::from_str(&candidate_data).map_err(|e| e.to_string())?;
    let oracle_expected = json!({"schema_version":1,"cases":plan.expected_states});
    let data_matches = valid_state_data(&candidate_data, &plan.expected_states);
    let test_source = fs::read_to_string(plan.run.worktree.join(&plan.allowlist[2]))
        .map_err(|e| e.to_string())?;
    let focused = observations
        .iter()
        .find(|o| o.id == "server-focused")
        .is_some_and(|o| {
            o.stdout.contains("test tasks::tests::invocation_state::")
                && !o.stdout.contains("running 0 tests")
        });
    let full_count = observations
        .iter()
        .find(|o| o.id == "server-full")
        .and_then(|o| {
            o.stdout.lines().find_map(|line| {
                line.strip_prefix("running ")
                    .and_then(|line| line.strip_suffix(" tests"))
                    .and_then(|count| count.parse::<usize>().ok())
            })
        });
    let old_tests_retained = plan
        .run
        .baseline_server_tests
        .checked_add(1)
        .is_some_and(|minimum| full_count.is_some_and(|actual| actual >= minimum));
    let old_modules = fs::read_to_string(plan.run.host_private.join("tests.rs.baseline"))
        .map_err(|e| e.to_string())?;
    let modules = fs::read_to_string(plan.run.worktree.join(&plan.allowlist[1]))
        .map_err(|e| e.to_string())?;
    let declarations = modules
        .lines()
        .filter(|line| line.trim() == "mod invocation_state;")
        .count();
    let remaining = modules
        .lines()
        .filter(|line| line.trim() != "mod invocation_state;")
        .collect::<Vec<_>>()
        .join("\n");
    let module_change_exact = declarations == 1 && remaining.trim_end() == old_modules.trim_end();
    let oracle_rows = observations
        .iter()
        .find(|o| o.id == "oracle-run")
        .map(|o| {
            o.stdout
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .filter(|row| row["oracle"] == "host_invocation_state_v1")
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    evidence.append(json!({"event":"self_iteration_independent_checks","candidate_data":candidate,"expected":oracle_expected,"data_matches":data_matches,"focused_tests_executed":focused,"server_full_test_count":full_count,"baseline_server_tests":plan.run.baseline_server_tests,"old_tests_retained":old_tests_retained,"module_change_exact":module_change_exact,"test_source_contains_public_helper_call":test_source.contains(".is_terminal()"),"test_source_audit_limit":"text presence is not an AST/runtime proof; independent compiled public oracle is mandatory","oracle_rows":oracle_rows}))?;
    let passed = observations.len() == 7
        && observations
            .iter()
            .all(|o| o.exit_code == Some(0) && !o.timed_out && !o.output_overflow)
        && focused
        && old_tests_retained
        && module_change_exact
        && data_matches
        && test_source.contains(".is_terminal()")
        && oracle_rows.len() == 7;
    Ok(Verification{passed,review_input:serde_json::to_string(&json!({"commands":observations,"passed":passed,"independent_checks":{"data_matches":data_matches,"oracle_rows":oracle_rows,"focused_tests_executed":focused}})).map_err(|e|e.to_string())?})
}

pub(super) fn valid_state_data(input: &str, expected: &[super::StateCase]) -> bool {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Dataset {
        schema_version: u32,
        cases: Vec<super::StateCase>,
    }
    let Ok(data) = serde_json::from_str::<Dataset>(input) else {
        return false;
    };
    let actual: std::collections::BTreeMap<_, _> = data
        .cases
        .iter()
        .map(|case| (case.state.as_str(), case.terminal))
        .collect();
    let expected: std::collections::BTreeMap<_, _> = expected
        .iter()
        .map(|case| (case.state.as_str(), case.terminal))
        .collect();
    data.schema_version == 1 && data.cases.len() == 7 && actual.len() == 7 && actual == expected
}

async fn observe(
    plan: &Plan,
    target: &Path,
    id: &str,
    program: PathBuf,
    arguments: Vec<String>,
) -> Result<Observation, String> {
    let mut command = Command::new(&program);
    command
        .args(&arguments)
        .current_dir(&plan.run.worktree)
        .env_clear()
        .env("CARGO_TARGET_DIR", target)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Host-only toolchain access; no API key enters a validation child or log.
    for key in [
        "PATH",
        "HOME",
        "RUSTUP_HOME",
        "RUSTUP_TOOLCHAIN",
        "CARGO_HOME",
        "TMPDIR",
    ] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    command.as_std_mut().process_group(0);
    let mut child = command
        .spawn()
        .map_err(|e| format!("fixed validation {id} spawn failed: {e}"))?;
    let group = StopGroup(child.id().ok_or("host validation PID missing")?);
    let stdout = child.stdout.take().ok_or("validation stdout absent")?;
    let stderr = child.stderr.take().ok_or("validation stderr absent")?;
    let out_bytes = Arc::new(Mutex::new((Vec::<u8>::new(), false)));
    let err_bytes = Arc::new(Mutex::new((Vec::<u8>::new(), false)));
    let mut out = tokio::spawn(capture(
        stdout,
        plan.host_validation_max_bytes,
        out_bytes.clone(),
    ));
    let mut err = tokio::spawn(capture(
        stderr,
        plan.host_validation_max_bytes,
        err_bytes.clone(),
    ));
    let joined = tokio::time::timeout(
        Duration::from_millis(plan.host_validation_timeout_ms),
        async {
            let status = child.wait().await.map_err(|e| e.to_string())?;
            let stdout = (&mut out).await.map_err(|e| e.to_string())??;
            let stderr = (&mut err).await.map_err(|e| e.to_string())??;
            Ok::<_, String>((status, stdout, stderr))
        },
    )
    .await;
    drop(group);
    match joined {
        Ok(Ok((status, (stdout, out_over), (stderr, err_over)))) => Ok(Observation {
            id: id.into(),
            program,
            arguments,
            exit_code: status.code(),
            stdout,
            stderr,
            timed_out: false,
            output_overflow: out_over || err_over,
        }),
        result => {
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
            out.abort();
            err.abort();
            match result {
                Err(_) => Ok(Observation {
                    id: id.into(),
                    program,
                    arguments,
                    exit_code: None,
                    stdout: captured(&out_bytes).0,
                    stderr: captured(&err_bytes).0,
                    timed_out: true,
                    output_overflow: captured(&out_bytes).1 || captured(&err_bytes).1,
                }),
                Ok(Err(error)) => Err(error),
                Ok(Ok(_)) => unreachable!(),
            }
        }
    }
}
async fn capture(
    mut reader: impl AsyncRead + Unpin,
    limit: usize,
    observed: Arc<Mutex<(Vec<u8>, bool)>>,
) -> Result<(String, bool), String> {
    let mut block = [0; 8192];
    loop {
        let count = reader.read(&mut block).await.map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        let mut observed = observed.lock().map_err(|e| e.to_string())?;
        let retained = count.min(limit.saturating_sub(observed.0.len()));
        observed.0.extend_from_slice(&block[..retained]);
        observed.1 |= retained != count;
    }
    Ok(captured(&observed))
}
fn captured(observed: &Mutex<(Vec<u8>, bool)>) -> (String, bool) {
    let observed = observed.lock().unwrap();
    (String::from_utf8_lossy(&observed.0).into(), observed.1)
}
