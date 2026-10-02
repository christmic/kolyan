//! Independent MiniMax experiment; the historical four-stage fixture is immutable.

mod correction;
mod driver;
mod execution;
mod failure;
mod input;
mod observations;
mod review;
mod tests;
mod validation;
mod workflow;

use std::ops::Deref;

use serde::Deserialize;
use serde_json::Value;

use super::{RunConfig, StateCase, TaskPlan, baseline, host, tools};
use workflow::{RepairFixture, StageKind, Workflow};

#[derive(Clone)]
struct Plan {
    base: super::Plan,
    stages: Vec<Stage>,
    workflow: Workflow,
}
impl Deref for Plan {
    type Target = super::Plan;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stage {
    id: String,
    kind: StageKind,
    writable: bool,
    input: String,
}
impl Stage {
    fn legacy(&self) -> super::Stage {
        super::Stage {
            id: self.id.clone(),
            writable: self.writable,
            input: self.input.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    baseline_data_digest: String,
    task: Value,
    flows: Vec<workflow::Flow>,
    review_cases: Vec<workflow::ReviewCase>,
    receipt_cases: Vec<workflow::ReceiptCase>,
    read_fixture_content: String,
    read_script: Vec<kolyan_model::ToolCall>,
}

fn repair_fixture() -> RepairFixture {
    let raw: Dataset = serde_json::from_str(include_str!(
        "../../fixtures/agent/self_iteration_minimax_v1.json"
    ))
    .expect("strict independent dataset");
    let mut task = raw.task;
    let workflow = serde_json::from_value(
        task.as_object_mut()
            .expect("task object")
            .remove("workflow")
            .expect("explicit workflow"),
    )
    .expect("typed workflow");
    let stages: Vec<Stage> =
        serde_json::from_value(task["stages"].clone()).expect("typed independent stages");
    for stage in task["stages"].as_array_mut().expect("stages array") {
        stage.as_object_mut().expect("stage object").remove("kind");
    }
    RepairFixture {
        task: serde_json::from_value(task).expect("original task DTO constraints"),
        stages,
        workflow,
        baseline_data_digest: raw.baseline_data_digest,
        flows: raw.flows,
        review_cases: raw.review_cases,
        receipt_cases: raw.receipt_cases,
        read_fixture_content: raw.read_fixture_content,
        read_script: raw.read_script,
    }
}
fn with_run(run: RunConfig) -> Plan {
    let fixture = repair_fixture();
    Plan {
        base: super::Plan {
            task: fixture.task,
            run,
        },
        stages: fixture.stages,
        workflow: fixture.workflow,
    }
}
fn preflight(plan: &Plan) -> Result<(), String> {
    let fixture = repair_fixture();
    let old = include_bytes!("../../fixtures/agent/self_iteration.json");
    let baseline: TaskPlan = serde_json::from_slice(old).map_err(|e| e.to_string())?;
    if baseline::digest(old) != fixture.baseline_data_digest
        || plan.family != "minimax"
        || plan.surface != "anthropic_compat"
        || plan.model != "MiniMax-M3"
        || plan.allowlist != baseline.allowlist
        || serde_json::to_value(&plan.expected_states).map_err(|e| e.to_string())?
            != serde_json::to_value(&baseline.expected_states).map_err(|e| e.to_string())?
        || plan.max_steps != baseline.max_steps
        || plan.max_tool_calls != baseline.max_tool_calls
        || plan.task_id == baseline.task_id
        || plan.logical_session_id == baseline.logical_session_id
        || plan.case_revision == baseline.case_revision
    {
        return Err("independent MiniMax contract or historical fixture digest differs".into());
    }
    let deployment = crate::deployments()
        .into_iter()
        .find(|d| d.family == plan.family && d.surface == plan.surface && d.model == plan.model)
        .ok_or("exact configured MiniMax deployment missing")?;
    if !matches!(deployment.provider, crate::Provider::Anthropic(_)) {
        return Err("MiniMax entry requires the real Anthropic adapter".into());
    }
    workflow::validate(plan)?;
    Ok(())
}

#[tokio::test]
#[ignore = "Actual authorized MiniMax authors a separate candidate; explicit pinned project-local RunConfig and host release required"]
async fn actual_minimax_worktree_self_iteration() {
    let path = std::env::var_os("KOLYAN_SELF_ITERATION_RUN_CONFIG")
        .expect("explicit RunConfig; no fallback");
    let run = RunConfig::load(std::path::Path::new(&path)).expect("pinned project-local RunConfig");
    driver::run(with_run(run)).await;
}
