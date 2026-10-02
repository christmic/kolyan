//! Complete data-owned task specification accompanies every actual stage input.

use serde_json::{Value, json};

use super::{
    Plan, Stage,
    baseline::{self, Inventory},
};

pub(super) fn specification(
    plan: &Plan,
    stage: &Stage,
    initial: &Inventory,
    current: &Inventory,
) -> Result<Value, String> {
    if plan.task_facts.initial_candidate_file_presence.len() != plan.allowlist.len() {
        return Err("candidate presence declarations do not match admitted scope".into());
    }
    let mut scope = Vec::new();
    for (path, expected) in plan
        .allowlist
        .iter()
        .zip(&plan.task_facts.initial_candidate_file_presence)
    {
        if initial.contains_key(path) != *expected {
            return Err(format!(
                "initial candidate presence differs from declared task fact: {path}"
            ));
        }
        scope.push(json!({"path":path,"initially_present":expected,"initial_observation":initial.get(path),"currently_present":current.contains_key(path),"current_observation":current.get(path)}));
    }
    let mut modules = Vec::new();
    for path in [
        &plan.task_facts.module_entry,
        &plan.task_facts.public_export_entry,
    ] {
        baseline::safe_relative(path)?;
        let state = initial.get(path).ok_or_else(|| {
            format!("declared module entry absent in trusted initial inventory: {path}")
        })?;
        let physical = plan
            .run
            .worktree
            .join(path)
            .canonicalize()
            .map_err(|e| e.to_string())?;
        if !physical.starts_with(
            plan.run
                .worktree
                .canonicalize()
                .map_err(|e| e.to_string())?,
        ) {
            return Err("module fact resolves outside the worktree".into());
        }
        let bytes = std::fs::read(physical).map_err(|e| e.to_string())?;
        if bytes.len() > 262144 || baseline::digest(&bytes) != state.sha256 {
            return Err(format!(
                "module fact bytes differ from pinned inventory: {path}"
            ));
        }
        modules.push(json!({"path":path,"sha256":state.sha256,"bytes":bytes.len(),"content":std::str::from_utf8(&bytes).map_err(|e|e.to_string())?,"meaning":"host-read existing source; not a model tool result or candidate edit"}));
    }
    Ok(json!({
        "case_revision":plan.case_revision,
        "task_instructions":plan.instructions,
        "complete_candidate_write_allowlist":scope,
        "actual_module_entries":modules,
        "target_dataset_example":{"schema_version":1,"cases":plan.expected_states},
        "dataset_requirements":plan.task_facts.dataset_requirements,
        "all_stage_restrictions":plan.stages.iter().map(|s|json!({"id":s.id,"writable":s.writable,"requirements":s.input})).collect::<Vec<_>>(),
        "current_stage":{"id":stage.id,"writable":stage.writable,"requirements":stage.input}
    }))
}

pub(super) fn prompt(specification: &Value, stage: &Stage) -> Result<String, String> {
    Ok(format!(
        "COMPLETE HOST-BOUND TASK SPECIFICATION (applies in every stage):\n{}\n\nCURRENT STAGE:\n{}",
        serde_json::to_string_pretty(specification).map_err(|e| e.to_string())?,
        stage.input
    ))
}
