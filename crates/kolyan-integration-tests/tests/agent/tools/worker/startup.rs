//! Native startup experiment, not a sandbox, authority or Agent acceptance gate.
//! Existing failed pins are never changed; fresh controls use identical bytes.

use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use kolyan_tools::{ExactFileBinding, ExactFileWorkerRequest, FileOperation, ReadArguments};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{Evidence, installation::write_private};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    timeout_ms: u64,
    sample_after_ms: u64,
    repetitions: usize,
    proof: String,
    sources: Vec<Source>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    id: String,
    failed_case: String,
    fresh_copy: bool,
}

#[test]
#[ignore = "independent native startup diagnostic; never counts as Agent or sandbox acceptance"]
fn native_failed_pins_and_same_hash_new_inode_startup_diagnostic() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("../../../fixtures/agent/worker_startup.json")).unwrap();
    assert_eq!(dataset.schema_version, 1);
    assert_eq!(
        dataset.timeout_ms, 30000,
        "retain the original worker deadline"
    );
    let root = tempfile::Builder::new()
        .prefix("kolyan-worker-startup-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = Evidence::new(&root.join("actual.jsonl"));
    println!(
        "WORKER_STARTUP_DIAGNOSTIC={}",
        root.join("actual.jsonl").display()
    );
    let workspace = root.join("workspace");
    fs::create_dir(&workspace).unwrap();
    fs::write(workspace.join("proof.txt"), &dataset.proof).unwrap();
    let workspace = workspace.canonicalize().unwrap();
    let request = ExactFileWorkerRequest {
        operation: FileOperation::Read(ReadArguments {
            path: "proof.txt".into(),
        }),
        binding: ExactFileBinding::prepare(&workspace, &workspace.join("proof.txt"), &[]).unwrap(),
        staging: None,
    };
    let input = root.join("request.json");
    fs::write(&input, serde_json::to_vec(&request).unwrap()).unwrap();
    evidence.append(json!({"event":"startup_scope","dataset":include_str!("../../../fixtures/agent/worker_startup.json"),
        "meaning":"direct_native_read_only_launch_no_sandbox_or_grant_not_original_effect_replay",
        "stdin_transport":"regular_file_with_EOF_not_production_pipe","request":request})).unwrap();
    let mut observations = Vec::new();
    let mut identities = Vec::new();
    let expected_launches = dataset.sources.len() * dataset.repetitions;
    for source in &dataset.sources {
        let failed = PathBuf::from(&source.failed_case);
        let original_rows: Vec<Value> = fs::read_to_string(failed.join("actual.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let original_requests: Vec<_> = original_rows
            .iter()
            .filter(|row| {
                row["event"] == "request"
                    || (row["event"] == "tool_adapter"
                        && row["phase"] == "execute"
                        && row["stage"] == "created")
            })
            .collect();
        let source_path = failed.join("trusted-worker/worker").canonicalize().unwrap();
        let source_hash = hash(&source_path);
        let directory = root.join(&source.id);
        fs::create_dir(&directory).unwrap();
        let path = if source.fresh_copy {
            let target = directory.join("worker");
            write_private(&target, &fs::read(&source_path).unwrap(), 0o500).unwrap();
            target.canonicalize().unwrap()
        } else {
            source_path.clone()
        };
        let metadata = fs::metadata(&path).unwrap();
        let source_metadata = fs::metadata(&source_path).unwrap();
        let identity = (metadata.dev(), metadata.ino());
        evidence.append(json!({"event":"startup_identity","id":source.id,"failed_case":failed,"original_requests":original_requests,
            "source_path":source_path,"source_sha256":source_hash,"path":path,"sha256":hash(&path),
            "dev":metadata.dev(),"ino":metadata.ino(),"source_dev":source_metadata.dev(),"source_ino":source_metadata.ino(),
            "fresh_copy":source.fresh_copy,"cold_claim":"first_observed_launch_only_OS_cache_not_reset"})).unwrap();
        identities.push((
            source.fresh_copy,
            identity,
            (source_metadata.dev(), source_metadata.ino()),
            source_hash.clone(),
            hash(&path),
        ));
        for repetition in 0..dataset.repetitions {
            let prefix = directory.join(format!("launch-{repetition}"));
            observations.push(launch(
                &source.id, repetition, &path, &input, &prefix, &dataset, &evidence,
            ));
            // These are predeclared distinct measurements, not retry-until-success.
            assert_eq!(
                hash(&path),
                source_hash,
                "diagnosis cannot mutate a pinned executable"
            );
        }
    }
    evidence
        .append(
            json!({"event":"startup_summary","observations":observations,
        "meaning":"launch outcomes are observations, not a repair or original acceptance"}),
        )
        .unwrap();
    for (fresh, identity, original, source_hash, actual_hash) in identities {
        assert_eq!(source_hash, actual_hash);
        if fresh {
            assert_ne!(
                identity, original,
                "fresh controls require independent inode identities"
            );
        } else {
            assert_eq!(identity, original);
        }
    }
    assert_eq!(
        fs::read_to_string(workspace.join("proof.txt")).unwrap(),
        dataset.proof
    );
    assert_eq!(
        observations.len(),
        expected_launches,
        "all predeclared launches must be recorded, regardless of outcome"
    );
}

fn hash(path: &Path) -> String {
    format!("{:x}", Sha256::digest(fs::read(path).unwrap()))
}

fn launch(
    id: &str,
    repetition: usize,
    path: &Path,
    input: &Path,
    prefix: &Path,
    dataset: &Dataset,
    evidence: &Evidence,
) -> Value {
    let workspace = input
        .parent()
        .unwrap()
        .join("workspace")
        .canonicalize()
        .unwrap();
    let stdout = prefix.with_extension("stdout");
    let stderr = prefix.with_extension("stderr");
    let args = vec![
        workspace.to_string_lossy().into_owned(),
        (64 * 1024 * 1024usize).to_string(),
        "65536".into(),
        "65536".into(),
    ];
    let start = Instant::now();
    let spawned = Command::new(path)
        .args(&args)
        .env_clear()
        .current_dir(&workspace)
        .stdin(Stdio::from(fs::File::open(input).unwrap()))
        .stdout(Stdio::from(fs::File::create(&stdout).unwrap()))
        .stderr(Stdio::from(fs::File::create(&stderr).unwrap()))
        .spawn();
    let mut child = match spawned {
        Ok(child) => child,
        Err(error) => {
            let row = json!({"event":"native_launch_spawn_error","id":id,"repetition":repetition,"path":path,"arguments":args,"error":error.to_string(),"elapsed_ns":start.elapsed().as_nanos()});
            evidence.append(row.clone()).unwrap();
            return row;
        }
    };
    let pid = child.id();
    evidence.append(json!({"event":"native_launch_started","id":id,"repetition":repetition,"pid":pid,"path":path,"arguments":args,
        "request_file":input,"stdout_file":stdout,"stderr_file":stderr,"elapsed_ns":start.elapsed().as_nanos()})).unwrap();
    let mut sampler = None;
    let mut sampled = false;
    let sample_path = prefix.with_extension("sample.txt");
    let mut timed_out = false;
    let exit = loop {
        if let Some(exit) = child.try_wait().unwrap() {
            break exit;
        }
        if start.elapsed() >= Duration::from_millis(dataset.timeout_ms) {
            timed_out = true;
            evidence.append(json!({"event":"native_launch_deadline","id":id,"repetition":repetition,"pid":pid,"elapsed_ns":start.elapsed().as_nanos()})).unwrap();
            let kill = child.kill();
            evidence.append(json!({"event":"native_launch_cleanup","pid":pid,"kill_error":kill.err().map(|error|error.to_string())})).unwrap();
            break child.wait().unwrap();
        }
        if !sampled && start.elapsed() >= Duration::from_millis(dataset.sample_after_ms) {
            sampled = true;
            let ps = Command::new("/bin/ps")
                .args(["-p", &pid.to_string(), "-o", "pid,ppid,state,etime,command"])
                .output()
                .unwrap();
            evidence.append(json!({"event":"native_launch_process_snapshot","pid":pid,"stdout":String::from_utf8_lossy(&ps.stdout),"stderr":String::from_utf8_lossy(&ps.stderr),"exit":ps.status.code()})).unwrap();
            match Command::new("/usr/bin/sample")
                .args([&pid.to_string(), "1", "1", "-file"])
                .arg(&sample_path)
                .stdout(Stdio::from(
                    fs::File::create(prefix.with_extension("sample.stdout")).unwrap(),
                ))
                .stderr(Stdio::from(
                    fs::File::create(prefix.with_extension("sample.stderr")).unwrap(),
                ))
                .spawn()
            {
                Ok(child) => sampler = Some(child),
                Err(error) => {
                    evidence.append(json!({"event":"native_sample_spawn_error","pid":pid,"error":error.to_string()})).unwrap();
                }
            }
        }
        thread::sleep(Duration::from_millis(20));
    };
    let launch_elapsed = start.elapsed().as_nanos();
    let sample_exit = sampler.map(|mut child| finish_sampler(&mut child));
    let stdout_bytes = fs::read(&stdout).unwrap();
    let response: Result<Value, _> = serde_json::from_slice(&stdout_bytes);
    let row = json!({"event":"native_launch_finished","id":id,"repetition":repetition,"pid":pid,"path":path,"timed_out":timed_out,
        "exit":exit.code(),"exit_display":exit.to_string(),"elapsed_ns":launch_elapsed,"stdout":String::from_utf8_lossy(&stdout_bytes),
        "stderr":fs::read_to_string(stderr).unwrap(),"response":response.as_ref().ok(),"response_decode_error":response.as_ref().err().map(ToString::to_string),
        "sample_taken":sampled,"sample_path":sample_path,"sample_exit":sample_exit});
    evidence.append(row.clone()).unwrap();
    row
}

fn finish_sampler(child: &mut Child) -> Value {
    let start = Instant::now();
    loop {
        if let Some(exit) = child.try_wait().unwrap() {
            return json!({"exit":exit.code(),"timed_out":false});
        }
        if start.elapsed() >= Duration::from_secs(5) {
            let kill = child.kill();
            let exit = child.wait().unwrap();
            return json!({"exit":exit.code(),"timed_out":true,"kill_error":kill.err().map(|error|error.to_string())});
        }
        thread::sleep(Duration::from_millis(20));
    }
}
