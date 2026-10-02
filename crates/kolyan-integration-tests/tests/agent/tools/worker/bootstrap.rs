//! Native-only Ready issuance above a shared bounded process observer.

mod plan;
pub(super) use plan::LaunchPlan;

use std::{
    io::{self, Read},
    os::{fd::AsFd, unix::process::CommandExt},
    process::{Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    super::Evidence,
    installation::{self, InstalledWorker, ReadyEvidence},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub schema_version: u32,
    pub preparation_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
    pub max_stdout_bytes: usize,
    pub max_stderr_bytes: usize,
}

pub(super) struct Observation {
    pub pid: u32,
    pub elapsed_ms: u64,
    pub status: Option<ExitStatus>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_eof: bool,
    pub stderr_eof: bool,
    pub failure: Option<String>,
}

impl Observation {
    // Output validation is not authority: this method cannot create Ready.
    pub fn check_marker(&self) -> Result<Value, String> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if !self.status.is_some_and(|status| status.success())
            || !self.stderr.is_empty()
            || !self.stdout_eof
            || !self.stderr_eof
        {
            return Err("worker bootstrap did not exit cleanly within output bounds".into());
        }
        let marker: Value =
            serde_json::from_slice(&self.stdout).map_err(|error| error.to_string())?;
        let expected: Value = serde_json::from_slice(kolyan_tool_worker::BOOTSTRAP_RESPONSE)
            .map_err(|error| error.to_string())?;
        if marker != expected {
            return Err("invalid worker Ready evidence".into());
        }
        Ok(marker)
    }
}

struct Capture<R> {
    reader: R,
    bytes: Vec<u8>,
    limit: usize,
    eof: bool,
}

impl<R: Read + AsFd> Capture<R> {
    fn new(reader: R, limit: usize) -> Result<Self, String> {
        // The descriptor is owned here; no other reader observes flag changes.
        let flags = fcntl(&reader, FcntlArg::F_GETFL).map_err(|error| error.to_string())?;
        fcntl(
            &reader,
            FcntlArg::F_SETFL(OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK),
        )
        .map_err(|error| error.to_string())?;
        Ok(Self {
            reader,
            bytes: Vec::new(),
            limit,
            eof: false,
        })
    }

    fn drain(&mut self) -> Result<(), String> {
        // Bound work per poll even when a producer never stops writing.
        let mut buffer = [0; 4096];
        for _ in 0..2 {
            match self.reader.read(&mut buffer) {
                Ok(0) => {
                    self.eof = true;
                    return Ok(());
                }
                Ok(count) => {
                    let remaining = (self.limit + 1).saturating_sub(self.bytes.len());
                    self.bytes
                        .extend_from_slice(&buffer[..count.min(remaining)]);
                    if self.bytes.len() > self.limit {
                        return Err("worker bootstrap output exceeded bound".into());
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(()),
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(())
    }
}

pub(super) fn dataset() -> Result<Config, String> {
    serde_json::from_str(include_str!(
        "../../../fixtures/agent/worker_installation.json"
    ))
    .map_err(|error| error.to_string())
}

pub(super) fn launch(
    installed: &InstalledWorker,
    config: &Config,
    evidence: &Evidence,
) -> Result<ReadyEvidence, String> {
    let observed = observe(&LaunchPlan::native(installed), config, evidence)?;
    let marker = observed.check_marker()?;
    installation::verify(installed)?;
    let ready = ReadyEvidence {
        schema_version: 1,
        installation: installed.clone(),
        observation_id: format!("{}/bootstrap-once", installed.installation_id),
        pid: observed.pid,
        arguments: vec!["--bootstrap-check".into()],
        empty_environment: true,
        empty_stdin: true,
        timeout_ms: config.preparation_timeout_ms,
        elapsed_ms: observed.elapsed_ms,
        exit_code: observed
            .status
            .and_then(|status| status.code())
            .ok_or("bootstrap had no exit code")?,
        marker,
    };
    ready.validate(installed)?;
    Ok(ready)
}

pub(super) fn observe(
    plan: &LaunchPlan,
    config: &Config,
    evidence: &Evidence,
) -> Result<Observation, String> {
    if config.schema_version != 1
        || !(1..=600000).contains(&config.preparation_timeout_ms)
        || !(1..=1000).contains(&config.cleanup_timeout_ms)
        || !(1..=4096).contains(&config.max_stdout_bytes)
        || !(1..=4096).contains(&config.max_stderr_bytes)
    {
        return Err("invalid explicit worker preparation budget".into());
    }
    plan.validate()?;
    let observation_id = plan.observation_id();
    let program = plan.program();
    let arguments = plan.arguments();
    evidence.append(json!({"event":plan.event("intent"),"observation_id":observation_id,"launch_plan":plan,"program":program,"arguments":arguments,"environment":{},"stdin_bytes":0,"preparation_timeout_ms":config.preparation_timeout_ms,"cleanup_timeout_ms":config.cleanup_timeout_ms,"tool_timeout_ms":30000,"stdout_limit":config.max_stdout_bytes,"stderr_limit":config.max_stderr_bytes,"may_issue_ready":plan.is_native()}))?;
    let start = Instant::now();
    let deadline = start + Duration::from_millis(config.preparation_timeout_ms);
    let mut child = Command::new(&program)
        .args(&arguments)
        .env_clear()
        .current_dir(plan.directory())
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| error.to_string())?;
    let pid = child.id();
    let mut stdout = Capture::new(
        child.stdout.take().expect("piped stdout"),
        config.max_stdout_bytes,
    );
    let mut stderr = Capture::new(
        child.stderr.take().expect("piped stderr"),
        config.max_stderr_bytes,
    );
    let mut failure = evidence.append(json!({"event":plan.event("started"),"observation_id":observation_id,"pid":pid,"program":program,"arguments":arguments,"elapsed_ns":start.elapsed().as_nanos()})).err();
    if let Err(error) = &stdout {
        failure = Some(error.clone());
    }
    if let Err(error) = &stderr {
        failure = Some(error.clone());
    }
    let mut status = None;
    loop {
        if status.is_none() {
            match child.try_wait() {
                Ok(value) => status = value,
                Err(error) => failure = Some(error.to_string()),
            }
        }
        for capture in [&mut stdout as &mut dyn Drain, &mut stderr as &mut dyn Drain] {
            if let Err(error) = capture.drain_available() {
                failure = Some(error);
            }
        }
        let eof = stdout.as_ref().is_ok_and(|pipe| pipe.eof)
            && stderr.as_ref().is_ok_and(|pipe| pipe.eof);
        if failure.is_some() || (status.is_some() && eof) {
            break;
        }
        if Instant::now() >= deadline {
            failure = Some("worker preparation timed out".into());
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let mut kill_error = None;
    if failure.is_some() {
        // A dedicated group includes ordinary descendants holding inherited pipes.
        // Escaped descendants cannot block capture: pipes are nonblocking and owned here.
        if let Err(error) = killpg(Pid::from_raw(pid as i32), Signal::SIGKILL) {
            kill_error = Some(error.to_string());
        }
        let cleanup_deadline = Instant::now() + Duration::from_millis(config.cleanup_timeout_ms);
        while status.is_none() && Instant::now() < cleanup_deadline {
            match child.try_wait() {
                Ok(value) => status = value,
                Err(error) => {
                    kill_error = Some(error.to_string());
                    break;
                }
            }
            if status.is_none() {
                thread::sleep(Duration::from_millis(10));
            }
        }
        evidence.append(json!({"event":plan.event("reaped"),"observation_id":observation_id,"pid":pid,"kill_error":kill_error,"exit":status.as_ref().and_then(|s|s.code()),"reason":failure,"reaped":status.is_some(),"cleanup_exhausted":status.is_none()}))?;
    }
    // Final nonblocking drain never waits for descendant EOF and no reader thread exists.
    let _ = stdout.drain_available();
    let _ = stderr.drain_available();
    let stdout_eof = stdout.as_ref().is_ok_and(|pipe| pipe.eof);
    let stderr_eof = stderr.as_ref().is_ok_and(|pipe| pipe.eof);
    let stdout = stdout.map(|pipe| pipe.bytes).unwrap_or_default();
    let stderr = stderr.map(|pipe| pipe.bytes).unwrap_or_default();
    let elapsed_ms =
        u64::try_from(start.elapsed().as_millis()).map_err(|error| error.to_string())?;
    let marker: Result<Value, _> = serde_json::from_slice(&stdout);
    evidence.append(json!({"event":plan.event("finished"),"observation_id":observation_id,"pid":pid,"program":program,"arguments":arguments,"exit":status.as_ref().and_then(|s|s.code()),"elapsed_ms":elapsed_ms,"stdout":String::from_utf8_lossy(&stdout),"stderr":String::from_utf8_lossy(&stderr),"stdout_eof":stdout_eof,"stderr_eof":stderr_eof,"marker":marker.as_ref().ok(),"decode_error":marker.as_ref().err().map(ToString::to_string),"failure":failure,"reaped":status.is_some(),"may_issue_ready":false}))?;
    Ok(Observation {
        pid,
        elapsed_ms,
        status,
        stdout,
        stderr,
        stdout_eof,
        stderr_eof,
        failure,
    })
}

trait Drain {
    fn drain_available(&mut self) -> Result<(), String>;
}
impl<R: Read + AsFd> Drain for Result<Capture<R>, String> {
    fn drain_available(&mut self) -> Result<(), String> {
        self.as_mut().map_err(|error| error.clone())?.drain()
    }
}
