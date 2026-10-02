//! A new authorized Root Task repairs model-authored bytes, never a Failed resume.

mod pins;
mod run;
mod tests;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Plan, RunConfig, Stage, baseline, workflow::StageKind};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    original_run_config: PathBuf,
    expected_current_inventory: Value,
    candidate_sha256: BTreeMap<String, String>,
    prior_trace: PathBuf,
    prior_trace_sha256: String,
    prior_task_id: String,
    prior_failed_fact: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    case_revision: String,
    task_id: String,
    logical_session_id: String,
    stage_id: String,
    instructions: String,
    input: String,
    guard_cases: Vec<GuardCase>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GuardCase {
    mutation: String,
    expected_valid: bool,
}

fn dataset() -> Dataset {
    serde_json::from_str(include_str!(
        "../../../fixtures/agent/self_iteration_minimax_correction_v1.json"
    ))
    .expect("strict independent correction dataset")
}

fn plan(run: RunConfig) -> Result<Plan, String> {
    let data = dataset();
    if data.schema_version != 1 {
        return Err("unsupported correction version".into());
    }
    let mut plan = super::with_run(run);
    if plan.task_id == data.task_id || plan.logical_session_id == data.logical_session_id {
        return Err("correction must use a new Task and Session".into());
    }
    plan.base.task.task_id = data.task_id;
    plan.base.task.logical_session_id = data.logical_session_id;
    plan.base.task.case_revision = data.case_revision;
    plan.base.task.instructions = data.instructions;
    plan.base.task.task_facts.initial_candidate_file_presence = vec![true; 4];
    plan.stages = vec![Stage {
        id: data.stage_id,
        kind: StageKind::Repair,
        writable: true,
        input: data.input,
    }];
    plan.base.task.stages = plan.stages.iter().map(Stage::legacy).collect();
    if plan.max_steps != 16 || plan.max_tool_calls != 40 {
        return Err("correction budget differs".into());
    }
    Ok(plan)
}

fn load(path: &Path) -> Result<(Config, Plan), String> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/config")
        .canonicalize()
        .map_err(|e| e.to_string())?;
    let path = path.canonicalize().map_err(|e| e.to_string())?;
    if !path.starts_with(root)
        || !path
            .file_name()
            .is_some_and(|p| p.to_string_lossy().ends_with(".local.json"))
    {
        return Err("explicit project-local correction .local.json required".into());
    }
    let config: Config = serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let plan = plan(RunConfig::load(&config.original_run_config)?)?;
    Ok((config, plan))
}

#[tokio::test]
#[ignore = "New explicitly pinned correction Task; host release and actual MiniMax required"]
async fn actual_minimax_worktree_independent_correction() {
    let path = std::env::var_os("KOLYAN_SELF_ITERATION_CORRECTION_CONFIG")
        .expect("explicit correction config, no fallback");
    let (config, plan) = load(Path::new(&path)).expect("strict correction config and scope");
    run::run(config, plan).await;
}
