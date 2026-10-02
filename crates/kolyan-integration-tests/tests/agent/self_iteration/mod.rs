//! A test-owned host; only actual model tools author the separate candidate.

mod baseline;
mod driver;
mod host;
mod input;
mod tests;
mod tools;
mod validation;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct Plan {
    task: TaskPlan,
    run: RunConfig,
}
impl std::ops::Deref for Plan {
    type Target = TaskPlan;
    fn deref(&self) -> &Self::Target {
        &self.task
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskPlan {
    schema_version: u32,
    case_revision: String,
    task_facts: TaskFacts,
    family: String,
    surface: String,
    model: String,
    task_id: String,
    logical_session_id: String,
    instructions: String,
    allowlist: Vec<String>,
    expected_states: Vec<StateCase>,
    stages: Vec<Stage>,
    max_steps: usize,
    max_tool_calls: usize,
    output_reserve_tokens: u32,
    inspection_window_assumption_tokens: u32,
    host_validation_timeout_ms: u64,
    host_validation_max_bytes: usize,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TaskFacts {
    module_entry: String,
    public_export_entry: String,
    initial_candidate_file_presence: Vec<bool>,
    dataset_requirements: String,
}

#[derive(Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct StateCase {
    state: String,
    terminal: bool,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stage {
    id: String,
    writable: bool,
    input: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RunConfig {
    worktree: PathBuf,
    host_private: PathBuf,
    branch: String,
    head: String,
    source_snapshot_sha256: String,
    baseline_manifest: String,
    baseline_manifest_sha256: String,
    baseline_files: usize,
    baseline_server_tests: usize,
}
impl RunConfig {
    fn load(path: &std::path::Path) -> Result<Self, String> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/config")
            .canonicalize()
            .map_err(|e| e.to_string())?;
        let path = path.canonicalize().map_err(|e| e.to_string())?;
        if !path.starts_with(root)
            || !path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with(".local.json"))
        {
            return Err("run config must be an explicit project-local .local.json file".into());
        }
        let config: Self = serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        if !config.worktree.is_absolute() || !config.host_private.is_absolute() {
            return Err("run paths must be absolute".into());
        }
        baseline::safe_relative(&config.baseline_manifest)?;
        Ok(config)
    }
}
fn plan() -> TaskPlan {
    serde_json::from_str(include_str!("../../fixtures/agent/self_iteration.json")).unwrap()
}

#[tokio::test]
#[ignore = "Actual Kolyan model authors a candidate in the specifically authorized worktree; requires explicit host release"]
async fn actual_model_worktree_self_iteration() {
    let path = std::env::var_os("KOLYAN_SELF_ITERATION_RUN_CONFIG")
        .expect("explicit KOLYAN_SELF_ITERATION_RUN_CONFIG required; no fallback");
    let run = RunConfig::load(std::path::Path::new(&path)).expect("valid project-local run config");
    driver::run(Plan { task: plan(), run }).await;
}
