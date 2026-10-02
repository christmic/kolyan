//! Current candidate and historical rejection are independently host-pinned.

use std::{collections::BTreeSet, fs, process::Command};

use serde_json::{Value, json};

use super::{Config, Plan, baseline};

pub(super) fn verify(
    config: &Config,
    plan: &Plan,
    inventory: &Value,
    rows: &[Value],
) -> Result<Value, String> {
    if inventory != &config.expected_current_inventory || config.prior_task_id == plan.task_id {
        return Err("current inventory or independent Task identity differs".into());
    }
    let expected_paths = plan.allowlist.iter().cloned().collect::<BTreeSet<_>>();
    if config
        .candidate_sha256
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>()
        != expected_paths
    {
        return Err("exact four candidate SHA pins required".into());
    }
    for (path, digest) in &config.candidate_sha256 {
        if digest.len() != 64
            || !digest.bytes().all(|b| b.is_ascii_hexdigit())
            || inventory[path]["kind"] != "file"
            || inventory[path]["sha256"] != *digest
        {
            return Err(format!("current physical candidate pin differs: {path}"));
        }
    }
    let matching = rows
        .iter()
        .filter(|r| {
            r.get("event") == Some(&json!("self_iteration_rejection_closure"))
                && r.get("task_id") == Some(&json!(config.prior_task_id))
        })
        .collect::<Vec<_>>();
    if matching.len() != 1
        || matching[0] != &config.prior_failed_fact
        || config.prior_failed_fact["result"]["Ok"]["state"] != "Failed"
        || config.prior_failed_fact["result"]["Ok"]["outcome"] != "failed"
        || config.prior_failed_fact["result"]["Ok"]["fact_id"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return Err("exact prior durable TaskFailed closure fact required".into());
    }
    let checks = rows
        .iter()
        .filter(|r| r["event"] == "self_iteration_host_validation")
        .map(|r| r["value"].clone())
        .collect::<Vec<_>>();
    let ids = checks
        .iter()
        .filter_map(|c| c["id"].as_str())
        .collect::<BTreeSet<_>>();
    if checks.len() != 7
        || ids
            != BTreeSet::from([
                "server-full",
                "server-focused",
                "format",
                "server-strict",
                "server-build",
                "oracle-compile",
                "oracle-run",
            ])
        || checks.iter().any(|c| {
            c["exit_code"] != 0 || c["timed_out"] != false || c["output_overflow"] != false
        })
    {
        return Err("prior seven successful fixed checks with bounded logs required".into());
    }
    Ok(
        json!({"prior_failed_fact":config.prior_failed_fact,"prior_trace_sha256":config.prior_trace_sha256,"prior_fixed_checks":checks,"current_candidate_sha256":config.candidate_sha256,"current_inventory":inventory,"new_task_id":plan.task_id,"failed_task_not_resumed":true}),
    )
}

pub(super) fn read(config: &Config, plan: &Plan) -> Result<Vec<Value>, String> {
    let path = config
        .prior_trace
        .canonicalize()
        .map_err(|e| e.to_string())?;
    let root = plan
        .run
        .host_private
        .canonicalize()
        .map_err(|e| e.to_string())?;
    if !path.starts_with(root)
        || fs::metadata(&path).map_err(|e| e.to_string())?.len() > 64 * 1024 * 1024
    {
        return Err("prior trace must be bounded and host-private".into());
    }
    let bytes = fs::read(path).map_err(|e| e.to_string())?;
    if baseline::digest(&bytes) != config.prior_trace_sha256 {
        return Err("prior trace SHA differs".into());
    }
    std::str::from_utf8(&bytes)
        .map_err(|e| e.to_string())?
        .lines()
        .map(|l| serde_json::from_str(l).map_err(|e| e.to_string()))
        .collect()
}

pub(super) fn repository(plan: &Plan) -> Result<(), String> {
    for (args, expected) in [
        (vec!["rev-parse", "HEAD"], plan.run.head.as_str()),
        (
            vec!["symbolic-ref", "--short", "HEAD"],
            plan.run.branch.as_str(),
        ),
    ] {
        let out = Command::new("/usr/bin/git")
            .arg("-C")
            .arg(&plan.run.worktree)
            .args(args)
            .output()
            .map_err(|e| e.to_string())?;
        if !out.status.success() || String::from_utf8_lossy(&out.stdout).trim() != expected {
            return Err("original candidate branch/HEAD pin differs".into());
        }
    }
    let manifest = fs::read(plan.run.host_private.join(&plan.run.baseline_manifest))
        .map_err(|e| e.to_string())?;
    if baseline::digest(&manifest) != plan.run.baseline_manifest_sha256 {
        return Err("original baseline manifest SHA differs".into());
    }
    Ok(())
}
