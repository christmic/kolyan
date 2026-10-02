//! Dataset-owned scheduling oracle; no model calls or candidate writes.

use super::*;
use crate::evidence::Evidence;
use serde_json::json;

#[test]
fn bounded_repair_reserves_six_before_admission() {
    assert_eq!(validate(&super::super::tests::unit_plan()).unwrap(), 6);
}

#[test]
fn dataset_flows_export_every_result_before_comparison() {
    let fixture = super::super::repair_fixture();
    let root = tempfile::tempdir().unwrap().keep();
    let evidence = Evidence::new(&root.join("actual.jsonl"));
    let mut rows = Vec::new();
    for flow in fixture.flows {
        let mut task = super::super::tests::unit_plan();
        task.stages
            .iter_mut()
            .find(|s| s.kind == StageKind::Repair)
            .unwrap()
            .writable = flow.repair_writable;
        let observed = (|| -> Result<(Vec<StageKind>, bool), String> {
            validate(&task)?;
            let mut stages = vec![
                StageKind::Inspect,
                StageKind::Implement,
                StageKind::Tests,
                StageKind::InitialReview,
            ];
            let checks = task
                .allowlist
                .iter()
                .enumerate()
                .map(|(i, p)| json!({"path":p,"verified":i<flow.initial_reads}))
                .collect::<Vec<_>>();
            if super::super::review::require_receipts(&checks).is_err() {
                return Ok((stages, false));
            }
            let mut progress = Progress::default();
            progress.initial_review(flow.initial_checks, flow.initial_verdict)?;
            if progress.enter_repair()? {
                stages.push(StageKind::Repair);
            }
            stages.push(StageKind::FinalReview);
            let checks = task
                .allowlist
                .iter()
                .enumerate()
                .map(|(i, p)| json!({"path":p,"verified":i<flow.final_reads}))
                .collect::<Vec<_>>();
            let passed = super::super::review::require_receipts(&checks).is_ok()
                && progress
                    .final_review(flow.post_checks, flow.final_verdict)
                    .is_ok();
            Ok((stages, passed))
        })();
        let (stages, passed, error) = match observed {
            Ok((s, p)) => (s, p, None),
            Err(e) => (vec![], false, Some(e)),
        };
        let row = json!({"case":flow.id,"provenance":"offline scheduling oracle; no model or OS effect claims","stages":stages,"passed":passed,"error":error,"expected_stages":flow.expected_stages,"expected_success":flow.expected_success,"attempts_per_admitted_stage":1,"reserved_capacity":6});
        evidence.append(row.clone()).unwrap();
        rows.push(row);
    }
    println!(
        "SELF_ITERATION_FLOW_TRACE={}",
        root.join("actual.jsonl").display()
    );
    drop(evidence);
    let rows = super::super::tests::read_rows(&root.join("actual.jsonl"));
    for row in rows {
        assert_eq!(row["stages"], row["expected_stages"], "{row}");
        assert_eq!(row["passed"], row["expected_success"], "{row}");
        assert!(row["stages"].as_array().unwrap().len() <= 6);
        assert!(
            row["stages"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|s| **s == "repair")
                .count()
                <= 1
        );
    }
}

#[test]
fn repair_and_final_review_are_non_replayable_and_fail_closed_out_of_order() {
    let mut progress = Progress::default();
    assert!(progress.enter_repair().is_err());
    assert!(progress.final_review(true, Verdict::Accept).is_err());
    assert!(progress.initial_review(false, Verdict::Accept).unwrap());
    assert!(progress.initial_review(true, Verdict::Accept).is_err());
    assert!(progress.final_review(true, Verdict::Accept).is_err());
    assert!(progress.enter_repair().unwrap());
    assert!(progress.enter_repair().is_err());
    assert!(progress.final_review(false, Verdict::Accept).is_err());
    assert!(progress.enter_repair().is_err());
    assert!(progress.final_review(true, Verdict::Accept).is_err());
}

#[test]
fn explicit_contract_has_no_missing_fields_fallback_and_readonly_stages_cannot_write() {
    let task = super::super::tests::unit_plan();
    for kind in [
        StageKind::Inspect,
        StageKind::InitialReview,
        StageKind::FinalReview,
    ] {
        let mut changed = task.clone();
        changed
            .stages
            .iter_mut()
            .find(|s| s.kind == kind)
            .unwrap()
            .writable = true;
        assert!(validate(&changed).is_err());
    }
}
