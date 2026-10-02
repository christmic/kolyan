//! Data-owned refusal semantics and one run installation shared across cases.

use std::os::unix::fs::PermissionsExt;

use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: String,
    expected_ready: bool,
}

#[tokio::test]
async fn one_bootstrap_shared_reference_reconstruction_and_fail_closed_evidence() {
    let run = WorkerRun::prepare().await;
    let pin = run
        .ready
        .as_ref()
        .expect("actual effect-less bootstrap must become Ready")
        .clone();
    let original_ready = fs::read(&pin.ready_path).unwrap();
    let original_proof = fs::read(run.root.join("actual.jsonl")).unwrap();
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../../fixtures/agent/worker_readiness.json"
    ))
    .unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-worker-readiness-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = Evidence::new(&root.join("actual.jsonl"));
    println!(
        "WORKER_READINESS_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let mut model_admissions = 0;
    let mut observations = Vec::new();
    for case in cases {
        let directory = root.join(&case.id);
        fs::create_dir(&directory).unwrap();
        run.bind_case(&directory, &evidence).unwrap();
        let case_pin = directory.join("trusted-worker/pin.json");
        let mut value: Value = serde_json::from_slice(&fs::read(&case_pin).unwrap()).unwrap();
        match case.mutation.as_str() {
            "none" => {}
            "schema_version" => value["schema_version"] = json!(999),
            "missing_ready_sha256" => {
                value.as_object_mut().unwrap().remove("ready_sha256");
            }
            "ready_sha256" => value["ready_sha256"] = json!("0".repeat(64)),
            "ready_path" => value["ready_path"] = json!(run.root.join("foreign.json")),
            "installation_ino" => value["installation"]["ino"] = json!(pin.installation.ino + 1),
            "unknown_field" => value["ignored_authority"] = json!(true),
            "missing_ready_evidence" => {
                fs::rename(&pin.ready_path, run.root.join("held-ready.json")).unwrap()
            }
            "changed_ready_marker" => {
                let mut ready: Value = serde_json::from_slice(&original_ready).unwrap();
                ready["marker"]["ready"] = json!(false);
                let bytes = serde_json::to_vec(&ready).unwrap();
                fs::set_permissions(&pin.ready_path, fs::Permissions::from_mode(0o600)).unwrap();
                fs::write(&pin.ready_path, &bytes).unwrap();
                fs::set_permissions(&pin.ready_path, fs::Permissions::from_mode(0o400)).unwrap();
                value["ready_sha256"] = json!(digest(&bytes));
            }
            other => panic!("unknown readiness mutation {other}"),
        }
        fs::set_permissions(&case_pin, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&case_pin, serde_json::to_vec(&value).unwrap()).unwrap();
        fs::set_permissions(&case_pin, fs::Permissions::from_mode(0o400)).unwrap();
        let result = verified_worker(&directory, &evidence);
        // Admission port observation, not model invocation or a trusted token counter.
        if result.is_ok() {
            model_admissions += 1;
        }
        evidence.append(json!({"event":"readiness_case","case":case.id,"pin":value,"path":result.as_ref().ok(),"error":result.as_ref().err(),"model_admitted":result.is_ok()})).unwrap();
        if case.mutation == "missing_ready_evidence" {
            fs::rename(run.root.join("held-ready.json"), &pin.ready_path).unwrap();
        }
        if case.mutation == "changed_ready_marker" {
            fs::set_permissions(&pin.ready_path, fs::Permissions::from_mode(0o600)).unwrap();
            fs::write(&pin.ready_path, &original_ready).unwrap();
            fs::set_permissions(&pin.ready_path, fs::Permissions::from_mode(0o400)).unwrap();
        }
        observations.push((case, result));
    }
    for (case, result) in observations {
        assert_eq!(result.is_ok(), case.expected_ready, "case {}", case.id);
        if case.expected_ready {
            assert_eq!(result.unwrap(), pin.installation.path);
        }
    }
    assert_eq!(model_admissions, 1);
    assert_eq!(
        fs::read(run.root.join("actual.jsonl")).unwrap(),
        original_proof,
        "case binding/recovery cannot start another bootstrap"
    );
    let rows: Vec<Value> = String::from_utf8(original_proof)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        rows.iter()
            .filter(|row| row["event"] == "worker_bootstrap_started")
            .count(),
        1
    );
    assert!(
        !rows
            .iter()
            .any(|row| row["event"] == "request" || row["event"] == "tool_adapter")
    );
}

#[test]
fn failed_installation_records_every_case_as_blocked_without_reference() {
    let root = tempfile::tempdir().unwrap();
    let run = WorkerRun {
        root: root.path().into(),
        ready: Err("explicit preparation failure".into()),
    };
    let evidence = Evidence::new(&root.path().join("actual.jsonl"));
    let mut observations = Vec::new();
    for id in ["named", "inline"] {
        let directory = root.path().join(id);
        fs::create_dir(&directory).unwrap();
        observations.push((
            run.bind_case(&directory, &evidence),
            directory.join("trusted-worker").exists(),
        ));
    }
    for (result, reference_exists) in observations {
        assert!(result.is_err());
        assert!(!reference_exists);
    }
    let rows = evidence.rows();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|row| row["model_admission"] == "blocked" && row["reference"].is_null())
    );
}
