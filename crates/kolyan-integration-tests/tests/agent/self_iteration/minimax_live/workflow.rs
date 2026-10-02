//! Explicit scheduling authority. Review text never substitutes for fixed checks.

mod tests;

use serde::{Deserialize, Serialize};

use super::{Plan, Stage, TaskPlan, review::Verdict};

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub(super) enum Workflow {
    BoundedRepair,
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub(super) enum StageKind {
    Inspect,
    Implement,
    Tests,
    InitialReview,
    Repair,
    FinalReview,
}

impl StageKind {
    pub fn writable(self) -> bool {
        matches!(self, Self::Implement | Self::Tests | Self::Repair)
    }
    pub fn structured_review(self) -> bool {
        matches!(self, Self::InitialReview | Self::FinalReview)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepairFixture {
    pub task: TaskPlan,
    pub stages: Vec<Stage>,
    pub workflow: Workflow,
    pub baseline_data_digest: String,
    pub flows: Vec<Flow>,
    pub review_cases: Vec<ReviewCase>,
    pub receipt_cases: Vec<ReceiptCase>,
    pub read_fixture_content: String,
    pub read_script: Vec<kolyan_model::ToolCall>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReviewCase {
    pub id: String,
    pub mutation: String,
    pub expected_valid: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceiptCase {
    pub mutation: String,
    pub expected_valid: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Flow {
    pub id: String,
    pub initial_checks: bool,
    pub initial_verdict: Verdict,
    pub initial_reads: usize,
    pub post_checks: bool,
    pub final_verdict: Verdict,
    pub final_reads: usize,
    pub repair_writable: bool,
    pub expected_stages: Vec<StageKind>,
    pub expected_success: bool,
}

pub(super) fn validate(plan: &Plan) -> Result<u64, String> {
    use StageKind::*;
    let expected: &[StageKind] = match plan.workflow {
        Workflow::BoundedRepair => &[
            Inspect,
            Implement,
            Tests,
            InitialReview,
            Repair,
            FinalReview,
        ],
    };
    if plan.schema_version != 1
        || plan.allowlist.len() != 4
        || plan
            .allowlist
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != 4
        || plan.stages.len() != expected.len()
        || plan
            .stages
            .iter()
            .map(|s| s.id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != expected.len()
        || plan
            .stages
            .iter()
            .zip(expected)
            .any(|(s, kind)| s.kind != *kind || s.writable != kind.writable() || s.id.is_empty())
    {
        return Err("invalid explicit workflow/stage permission contract".into());
    }
    for path in &plan.allowlist {
        super::baseline::safe_relative(path)?;
    }
    Ok(match plan.workflow {
        Workflow::BoundedRepair => 6,
    })
}

/// Called only after a completed initial review has independent receipt proof.
pub(super) fn needs_repair(checks_passed: bool, verdict: Verdict) -> bool {
    !checks_passed || verdict == Verdict::Repair
}

/// Scheduling facts are supplied only after receipt/JSON verification in the host.
#[derive(Default)]
pub(super) struct Progress {
    initial_decision: Option<bool>,
    repair_entered: bool,
    final_reviewed: bool,
}

impl Progress {
    pub fn initial_review(
        &mut self,
        checks_passed: bool,
        verdict: Verdict,
    ) -> Result<bool, String> {
        if self.initial_decision.is_some() || self.final_reviewed {
            return Err("initial review cannot be repeated".into());
        }
        let required = needs_repair(checks_passed, verdict);
        self.initial_decision = Some(required);
        Ok(required)
    }
    pub fn enter_repair(&mut self) -> Result<bool, String> {
        let required = self
            .initial_decision
            .ok_or("repair lacks an evidenced initial decision")?;
        if self.repair_entered || self.final_reviewed {
            return Err("repair budget exhausted".into());
        }
        self.repair_entered = required;
        Ok(required)
    }
    pub fn final_review(&mut self, checks_passed: bool, verdict: Verdict) -> Result<(), String> {
        let required = self
            .initial_decision
            .ok_or("final review lacks an initial decision")?;
        if self.final_reviewed || (required && !self.repair_entered) {
            return Err("final review out of workflow order".into());
        }
        self.final_reviewed = true;
        final_acceptance(checks_passed, verdict)
    }
}

pub(super) fn final_acceptance(checks_passed: bool, verdict: Verdict) -> Result<(), String> {
    if checks_passed && verdict == Verdict::Accept {
        Ok(())
    } else {
        Err("final review or independent validation failed; repair budget exhausted".into())
    }
}
