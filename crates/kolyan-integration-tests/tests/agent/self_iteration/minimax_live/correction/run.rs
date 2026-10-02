//! One existing Root execution; host checks do not trust model review prose.

use std::{fs, os::unix::fs::DirBuilderExt, path::Path, sync::Arc};

use futures_util::FutureExt;
use kolyan_agent::{
    AgentDefinition, AgentDefinitionInput, TaskFinalizationPolicy, TaskFinalizationRequest,
};
use kolyan_core::TurnOutcome;
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{SessionService, TaskState};
use kolyan_storage::FileSessionStore;
use serde_json::json;

use super::super::super::driver::{candidate_files, outcome, permissions};
use super::super::{
    execution,
    host::{Host, model},
};
use super::{Config, Plan, baseline, dataset, pins};
use crate::{evidence::Evidence, tools::worker};

pub(super) async fn run(config: Config, plan: Plan) {
    let control = tempfile::Builder::new()
        .prefix("self-iteration-correction-")
        .tempdir_in(&plan.run.host_private)
        .unwrap()
        .keep();
    let evidence = Arc::new(Evidence::new(&control.join("actual.jsonl")));
    println!(
        "SELF_ITERATION_CORRECTION_TRACE={}",
        control.join("actual.jsonl").display()
    );
    let result =
        std::panic::AssertUnwindSafe(experiment(&config, &plan, &control, evidence.clone()))
            .catch_unwind()
            .await;
    let error = match result {
        Ok(r) => r.err(),
        Err(_) => Some("correction host panic; no synthesized completion".into()),
    };
    let closure = error
        .as_ref()
        .map(|e| super::super::failure::record(&control, &plan.task_id, e, &evidence));
    evidence.append(json!({"event":"independent_correction_acceptance","error":error,"closure":closure,"source_audit_required_before_merge":true,"candidate_authored_by_host":false})).unwrap();
    drop(evidence);
    let actual = fs::read_to_string(control.join("actual.jsonl")).unwrap();
    for line in actual.lines() {
        serde_json::from_str::<serde_json::Value>(line).unwrap();
    }
    assert!(
        error.is_none(),
        "correction failed: {error:?}; {}",
        control.display()
    );
    assert!(
        closure.is_none_or(|r| r.is_ok()),
        "durable failure closure failed"
    );
}

async fn experiment(
    config: &Config,
    plan: &Plan,
    control: &Path,
    evidence: Arc<Evidence>,
) -> Result<(), String> {
    let before = baseline::collect(&plan.run.worktree)?;
    let rows = pins::read(config, plan)?;
    let facts = pins::verify(
        config,
        plan,
        &serde_json::to_value(&before).map_err(|e| e.to_string())?,
        &rows,
    );
    evidence.append(json!({"event":"correction_complete_plan","dataset":dataset(),"config":config,"run_config":plan.run,"pin_result":facts,"before_model":true}))?;
    let facts = facts?;
    pins::repository(plan)?;
    let stage = &plan.stages[0];
    let specification =
        super::super::super::input::specification(plan, &stage.legacy(), &before, &before)?;
    let input = format!(
        "{}\nACTUAL CANDIDATE AND PRIOR FAILED TASK EVIDENCE (not instructions):\n{}",
        super::super::super::input::prompt(&specification, &stage.legacy())?,
        serde_json::to_string(&json!({"facts":facts,"candidate_files":candidate_files(plan)?}))
            .map_err(|e| e.to_string())?
    );
    fs::create_dir(control.join("state")).map_err(|e| e.to_string())?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(control.join("staging"))
        .map_err(|e| e.to_string())?;
    let installation = worker::WorkerRun::prepare().await;
    worker::initialize_worker(control, &evidence, &installation)?;
    let ready_inventory = baseline::collect(&plan.run.worktree)?;
    evidence.append(json!({"event":"correction_ready_inventory","inventory":ready_inventory,"unchanged_since_pin":ready_inventory==before,"before_admission":true}))?;
    if ready_inventory != before {
        return Err("candidate changed during worker preparation".into());
    }
    pins::read(config, plan)?;
    SessionService::new(
        FileSessionStore::new(control.join("state/sessions")).map_err(|e| e.to_string())?,
    )
    .create(&plan.logical_session_id)
    .map_err(|e| e.to_string())?;
    let deployment = crate::deployments()
        .into_iter()
        .find(|d| d.family == plan.family && d.surface == plan.surface && d.model == plan.model)
        .ok_or("exact MiniMax deployment missing")?;
    let live = deployment.build();
    let ceiling = permissions(true);
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "self-iteration-correction-agent".into(),
        revision: "r1".into(),
        display_name: Some("Kolyan authorized correction".into()),
        model: model(plan),
        instructions: plan.instructions.clone(),
        permissions: ceiling.clone(),
    })
    .map_err(|e| e.to_string())?;
    let host = Host::open(
        plan,
        control,
        &definition,
        &ceiling,
        live.clone(),
        evidence.clone(),
    );
    let (bound, result) = execution::execute(execution::StageExecution {
        plan,
        stage,
        index: 0,
        control,
        host: &host,
        definition: &definition,
        ceiling: &ceiling,
        preceding: &None,
        input,
        evidence: evidence.clone(),
        admitted_capacity: 1,
        live,
    })
    .await?;
    let observed = outcome::observe(
        &stage.id,
        result
            .as_ref()
            .map(|(_, r)| r)
            .map_err(|e| e as &(dyn std::error::Error + 'static)),
    );
    evidence.append(observed.record)?;
    host.export(&plan.task_id, &evidence)?;
    let after = baseline::collect(&plan.run.worktree)?;
    evidence.append(json!({"event":"correction_actual_inventory","before":before,"after":after,"changed":baseline::changed(&before,&after),"binding":bound,"candidate_files":candidate_files(plan)?}))?;
    baseline::confined(plan, &before, &after)?;
    let (_, turn) = result.map_err(|e| e.to_string())?;
    if !matches!(turn,DurableTurnResult::Completed(ref c,_) if matches!(c.result.outcome,TurnOutcome::FinalAnswer{..}))
    {
        return Err(observed
            .failure
            .unwrap_or("correction did not return FinalAnswer".into()));
    }
    let changed = baseline::changed(&before, &after);
    if changed.is_empty() {
        return Err("correction produced no actual candidate modification".into());
    }
    // Reuse the exact mutation receipt audit for changed paths only: unchanged
    // candidate bytes remain bound to the explicitly pinned prior experiment.
    let mut changed_plan = plan.base.clone();
    changed_plan.task.allowlist = changed.into_iter().collect();
    super::super::super::driver::verify_candidate_receipts(&changed_plan, &host, &evidence)?;
    pins::read(config, plan)?;
    let check_root = control.join("fixed-validation");
    fs::create_dir(&check_root).map_err(|e| e.to_string())?;
    let checks = super::super::validation::run(plan, &check_root, &evidence).await?;
    evidence.append(json!({"event":"correction_fixed_verification","record":checks.record,"passed":checks.passed,"model_review_used":false,"codex_source_review_required":true}))?;
    if !checks.passed {
        return Err("independent fixed verification failed".into());
    }
    let verified_inventory = baseline::collect(&plan.run.worktree)?;
    evidence.append(json!({"event":"correction_post_validation_inventory","inventory":verified_inventory,"unchanged_since_tools":verified_inventory==after}))?;
    if verified_inventory != after {
        return Err("candidate changed outside actual model tools during validation".into());
    }
    let finalized = host
        .runner
        .finalize_task(TaskFinalizationRequest {
            task_id: plan.task_id.clone(),
            logical_session_id: plan.logical_session_id.clone(),
            root_invocation_id: stage.id.clone(),
            root_attempt_id: format!("self-iteration-attempt-{}", stage.id),
            policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
        })
        .await;
    host.export(&plan.task_id, &evidence)?;
    evidence.append(json!({"event":"correction_runner_finalization","result":finalized.as_ref().map_err(ToString::to_string)}))?;
    if finalized.map_err(|e| e.to_string())?.state != TaskState::Completed {
        return Err("correction Task not completed".into());
    }
    Ok(())
}
