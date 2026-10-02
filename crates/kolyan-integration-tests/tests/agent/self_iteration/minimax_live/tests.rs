//! Strict independent plan guards. These tests never write a candidate worktree.

use super::*;
use crate::evidence::Evidence;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

pub(super) fn read_rows(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
pub(super) fn unit_plan() -> Plan {
    with_run(RunConfig {
        worktree: "/offline/not-an-authorized-candidate".into(),
        host_private: "/offline/control".into(),
        branch: "offline".into(),
        head: "0".repeat(40),
        source_snapshot_sha256: "0".repeat(64),
        baseline_manifest: "baseline.sha256".into(),
        baseline_manifest_sha256: "0".repeat(64),
        baseline_files: 0,
        baseline_server_tests: 0,
    })
}

#[test]
fn independent_plan_preserves_historical_scope_and_model_requests() {
    let plan = unit_plan();
    let fixture = repair_fixture();
    let root = tempfile::tempdir().unwrap().keep();
    let path = root.join("actual.jsonl");
    let evidence = Evidence::new(&path);
    let baseline: Value =
        serde_json::from_str(include_str!("../../../fixtures/agent/self_iteration.json")).unwrap();
    evidence.append(json!({"event":"plan","baseline":baseline,"baseline_digest":fixture.baseline_data_digest,
        "new_task":serde_json::from_str::<Value>(include_str!("../../../fixtures/agent/self_iteration_minimax_v1.json")).unwrap(),
        "preflight":preflight(&plan),"network_calls":0})).unwrap();
    for stage in &plan.stages {
        let mut request = super::super::driver::request(&plan, &stage.id, stage.input.clone());
        let result = input::configure(&plan, stage, &mut request);
        evidence.append(json!({"event":"stage_request","kind":stage.kind,"request":request,"result":result})).unwrap();
    }
    drop(evidence);
    let rows = read_rows(&path);
    println!("MINIMAX_INDEPENDENT_PLAN_TRACE={}", path.display());
    assert!(rows[0]["preflight"]["Ok"].is_null());
    assert!(preflight(&plan).is_ok());
    assert_eq!(workflow::validate(&plan).unwrap(), 6);
    assert_eq!(plan.max_steps, 16);
    assert_eq!(plan.max_tool_calls, 40);
    for row in &rows[1..] {
        assert!(row["result"]["Ok"].is_null());
        assert!(row["request"]["output_format"].is_null());
        if row["kind"] == "initial_review" || row["kind"] == "final_review" {
            assert!(
                row["request"]["system"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|s| s["text"].as_str().is_some_and(
                        |t| t.contains("LOCAL review") && t.contains("candidate_digests")
                    ))
            );
        }
    }
}

#[test]
fn altered_identity_scope_budget_or_stage_permissions_reject_before_admission() {
    let root = tempfile::tempdir().unwrap().keep();
    let path = root.join("actual.jsonl");
    let evidence = Evidence::new(&path);
    let cases: Value = serde_json::from_str(include_str!("guards.json")).unwrap();
    for c in cases["cases"].as_array().unwrap() {
        let mut p = unit_plan();
        match c["mutation"].as_str().unwrap() {
            "family" => p.base.task.family = "qwen".into(),
            "path" => p.base.task.allowlist[0] = "outside.rs".into(),
            "budget" => p.base.task.max_steps += 1,
            "identity" => p.base.task.task_id = super::super::plan().task_id,
            "review_write" => p.stages[3].writable = true,
            "missing_stage" => {
                p.stages.pop();
            }
            "valid" => {}
            other => panic!("unknown guard {other}"),
        }
        let result = preflight(&p);
        evidence
            .append(json!({"case":c,"result":result,"valid":result.is_ok(),"network_calls":0}))
            .unwrap();
    }
    drop(evidence);
    for row in read_rows(&path) {
        assert_eq!(row["valid"], row["case"]["expected_valid"], "{row}");
    }
}

#[test]
fn original_fixture_and_entry_remain_four_stage() {
    let original = super::super::plan();
    assert_eq!(original.stages.len(), 4);
    assert_eq!(original.family, "qwen");
    let expected: BTreeMap<_, _> = original
        .expected_states
        .iter()
        .map(|s| (&s.state, s.terminal))
        .collect();
    let actual = unit_plan();
    assert_eq!(
        expected,
        actual
            .expected_states
            .iter()
            .map(|s| (&s.state, s.terminal))
            .collect()
    );
}
