//! Failure-handshake controls are trusted host scripts, not actual model evidence.

use serde::Deserialize;
use serde_json::json;

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    script: String,
    expected_error: Option<String>,
    expected_direct_exit: Option<i32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    config: bootstrap::Config,
    total_bound_ms: u64,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefusalCase {
    id: String,
    mutation: String,
    expected_error: String,
}

#[test]
fn one_launch_handshake_failures_are_bounded_and_never_ready() {
    let dataset: Dataset = serde_json::from_str(include_str!(
        "../../../fixtures/agent/worker_bootstrap_failures.json"
    ))
    .unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-bootstrap-failures-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    let evidence = Evidence::new(&root.join("actual.jsonl"));
    println!(
        "BOOTSTRAP_FAILURE_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let config = &dataset.config;
    let mut observations = Vec::new();
    for case in dataset.cases {
        let directory = root.join(&case.id);
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
        let source = root.join(format!("{}.sh", case.id));
        write_private(&source, case.script.as_bytes(), 0o400).unwrap();
        let plan = bootstrap::LaunchPlan::fixture(&case.id, &source).unwrap();
        let start = evidence.rows().len();
        let launched = std::time::Instant::now();
        let observed = bootstrap::observe(&plan, config, &evidence);
        let result = observed.and_then(|observation| observation.check_marker());
        let total_elapsed = launched.elapsed();
        evidence.append(json!({"event":"bootstrap_failure_case","case":case.id,"expected_error":case.expected_error,"error":result.as_ref().err(),"total_elapsed_ms":total_elapsed.as_millis(),"total_bound_ms":dataset.total_bound_ms})).unwrap();
        observations.push((
            case,
            result,
            total_elapsed,
            evidence.rows()[start..].to_vec(),
            directory.join("ready.json").exists(),
        ));
    }
    for (case, result, total_elapsed, rows, ready_exists) in observations {
        assert!(total_elapsed < std::time::Duration::from_millis(dataset.total_bound_ms));
        if let Some(expected) = &case.expected_error {
            let error = result.expect_err("fault must fail output validation");
            assert!(error.contains(expected), "case {}: {error}", case.id);
        } else {
            assert!(result.is_ok(), "valid marker is only an observation");
        }
        if let Some(exit) = case.expected_direct_exit {
            let finished = rows
                .iter()
                .find(|row| row["event"] == "interpreter_fixture_finished")
                .unwrap();
            assert_eq!(finished["exit"], exit);
            assert!(finished["elapsed_ms"].as_u64().unwrap() >= config.preparation_timeout_ms);
            assert!(
                rows.iter().any(
                    |row| row["event"] == "interpreter_fixture_reaped" && row["reaped"] == true
                )
            );
        }
        assert_eq!(
            rows.iter()
                .filter(|row| row["event"] == "interpreter_fixture_started")
                .count(),
            1
        );
        assert!(
            rows.iter()
                .any(|row| row["event"] == "interpreter_fixture_finished" && row["reaped"] == true)
        );
        let intent = rows
            .iter()
            .find(|row| row["event"] == "interpreter_fixture_intent")
            .unwrap();
        assert_eq!(intent["program"], "/bin/sh");
        assert_eq!(intent["may_issue_ready"], false);
        assert_eq!(intent["arguments"].as_array().unwrap().len(), 1);
        assert!(
            intent["launch_plan"]["sha256"]
                .as_str()
                .is_some_and(|hash| hash.len() == 64)
        );
        assert!(!rows.iter().any(|row| row["event"] == "installation_ready"
            || row["event"] == "worker_bootstrap_started"));
        assert!(!ready_exists);
    }
}

#[test]
fn recorder_refusal_prevents_bootstrap_spawn_and_case_admission() {
    let root = tempfile::tempdir().unwrap();
    let physical = root.path().canonicalize().unwrap();
    let evidence = Evidence::new(&root.path().join("actual.jsonl"));
    let source = physical.join("not-entered.sh");
    write_private(&source, b"exit 0\n", 0o400).unwrap();
    let plan = bootstrap::LaunchPlan::fixture("not-entered", &source).unwrap();
    evidence.append_from_drop(serde_json::Value::Null);
    let result = bootstrap::observe(&plan, &bootstrap::dataset().unwrap(), &evidence);
    assert!(
        result
            .err()
            .unwrap()
            .contains("prior Drop observation failed")
    );
    assert!(!root.path().join("ready.json").exists());
}

#[test]
fn interpreter_data_mutations_refuse_before_any_process_launch() {
    let cases: Vec<RefusalCase> = serde_json::from_str(include_str!(
        "../../../fixtures/agent/worker_interpreter_refusals.json"
    ))
    .unwrap();
    // Retain controls and evidence for inspection after the gate.
    let retained = tempfile::Builder::new()
        .prefix("kolyan-interpreter-refusals-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    let evidence = Evidence::new(&retained.join("actual.jsonl"));
    println!(
        "INTERPRETER_REFUSAL_TRACE={}",
        retained.join("actual.jsonl").display()
    );
    let mut observations = Vec::new();
    for case in cases {
        let script = retained.join(format!("{}.sh", case.id));
        write_private(&script, b"exit 0\n", 0o400).unwrap();
        let plan = bootstrap::LaunchPlan::fixture(&case.id, &script).unwrap();
        match case.mutation.as_str() {
            "bytes" => {
                fs::set_permissions(&script, fs::Permissions::from_mode(0o600)).unwrap();
                fs::write(&script, b"exit 7\n").unwrap();
                fs::set_permissions(&script, fs::Permissions::from_mode(0o400)).unwrap();
            }
            "inode" => {
                fs::rename(&script, script.with_extension("held")).unwrap();
                write_private(&script, b"exit 0\n", 0o400).unwrap();
            }
            "writable" => fs::set_permissions(&script, fs::Permissions::from_mode(0o600)).unwrap(),
            "executable" => {
                fs::set_permissions(&script, fs::Permissions::from_mode(0o500)).unwrap()
            }
            "symlink" => {
                let held = script.with_extension("held");
                fs::rename(&script, &held).unwrap();
                std::os::unix::fs::symlink(&held, &script).unwrap();
            }
            other => panic!("unknown control mutation {other}"),
        }
        let before = evidence.rows().len();
        let result = bootstrap::observe(&plan, &bootstrap::dataset().unwrap(), &evidence);
        let launched = evidence.rows()[before..]
            .iter()
            .any(|row| row["event"] == "interpreter_fixture_started");
        evidence.append(json!({"event":"interpreter_refusal_case","case":case.id,"plan":plan,"error":result.as_ref().err(),"launched":launched,"model_admission":false})).unwrap();
        observations.push((case, result.err(), launched));
    }
    for (case, error, launched) in observations {
        assert!(
            error.unwrap().contains(&case.expected_error),
            "case {}",
            case.id
        );
        assert!(!launched);
    }
    assert!(!retained.join("ready.json").exists());
}
