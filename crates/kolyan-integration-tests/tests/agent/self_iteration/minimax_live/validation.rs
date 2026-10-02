//! Reuse the seven fixed commands and oracle; add strict runtime observations.

use super::Plan;
use crate::evidence::Evidence;
use serde_json::{Value, json};
use std::path::Path;

pub(super) struct Verification {
    pub passed: bool,
    pub review_input: String,
    pub record: Value,
}

pub(super) async fn run(
    plan: &Plan,
    control: &Path,
    evidence: &Evidence,
) -> Result<Verification, String> {
    let original = super::super::validation::run(plan, control, evidence).await?;
    let stdout = std::fs::read_to_string(control.join("server-focused.stdout.log"))
        .map_err(|e| e.to_string())?;
    let actual =
        super::observations::validate_candidate_observations(&stdout, &plan.expected_states);
    let record = json!({"fixed_checks":original.passed,"fixed_review_input":original.review_input,
        "candidate_observations":actual,"source_audit_mandatory":true,"host_never_formats_candidate":true});
    evidence.append(json!({"event":"self_iteration_independent_local_checks","record":record}))?;
    let passed = original.passed && actual.is_ok();
    Ok(Verification {
        passed,
        review_input: serde_json::to_string(&record).map_err(|e| e.to_string())?,
        record,
    })
}
