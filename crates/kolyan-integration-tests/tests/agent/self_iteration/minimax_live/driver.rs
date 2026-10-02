//! Independent bounded workflow; actual admissions are delegated to execution.

use super::super::driver::{candidate_files, outcome, permissions, verify_candidate_receipts};
use super::{
    Plan, baseline,
    host::{Host, model},
    validation,
    workflow::{self, StageKind},
};
use crate::{evidence::Evidence, tools::worker};
use futures_util::FutureExt;
use kolyan_agent::{
    AgentDefinition, AgentDefinitionInput, TaskFinalizationPolicy, TaskFinalizationRequest,
};
use kolyan_core::TurnOutcome;
use kolyan_ledger::{LedgerEventKind, LedgerStore};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{AttemptBinding, SessionService};
use kolyan_storage::FileSessionStore;
use serde_json::json;
use std::{fs, os::unix::fs::DirBuilderExt, path::Path, sync::Arc};

pub(super) async fn run(plan: Plan) {
    run_impl(plan).await;
}

async fn run_impl(plan: Plan) {
    let control = tempfile::Builder::new()
        .prefix("self-iteration-run-")
        .tempdir_in(&plan.run.host_private)
        .unwrap()
        .keep();
    let evidence = Arc::new(Evidence::new(&control.join("actual.jsonl")));
    println!(
        "SELF_ITERATION_TRACE={}",
        control.join("actual.jsonl").display()
    );
    let result = std::panic::AssertUnwindSafe(experiment(&plan, &control, evidence.clone()))
        .catch_unwind()
        .await;
    let mut error = match result {
        Ok(result) => result.err(),
        Err(payload) => Some(
            payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or("self-iteration host panic".into()),
        ),
    };
    let original_error = error.clone();
    if let Some(reason) = original_error.as_deref()
        && let Err(closure_error) =
            super::failure::record(&control, &plan.task_id, reason, &evidence)
    {
        error = Some(format!(
            "{reason}; durable rejection closure failed: {closure_error}"
        ));
    }
    evidence.append(json!({"event":"self_iteration_acceptance","error":error,"original_error":original_error,"passed":error.is_none(),"candidate_authored_by_host":false,"matrix_replacement":false})).unwrap();
    drop(evidence);
    let exported =
        std::fs::read_to_string(control.join("actual.jsonl")).expect("closed actual trace");
    for line in exported.lines() {
        serde_json::from_str::<serde_json::Value>(line).expect("persisted JSONL");
    }
    assert!(
        error.is_none(),
        "Self iteration failed: {error:?}; {}",
        control.display()
    );
}

async fn experiment(plan: &Plan, control: &Path, evidence: Arc<Evidence>) -> Result<(), String> {
    evidence.append(json!({"event":"self_iteration_complete_plan",
        "historical":serde_json::from_str::<serde_json::Value>(include_str!("../../../fixtures/agent/self_iteration.json")).map_err(|e|e.to_string())?,
        "independent":serde_json::from_str::<serde_json::Value>(include_str!("../../../fixtures/agent/self_iteration_minimax_v1.json")).map_err(|e|e.to_string())?,
        "before_worker_or_model":true}))?;
    let admitted_capacity = workflow::validate(plan)?;
    let preflight = super::preflight(plan);
    evidence.append(json!({"event":"self_iteration_local_review_preflight","result":preflight.as_ref().map_err(ToString::to_string),"native_enforcement":false,"before_task_admission":true}))?;
    preflight?;
    evidence.append(json!({"event":"self_iteration_run_config","config":plan.run,"api_path":"AgentRunner prepare_root_input/prepare_continuation_input; TaskExecutionService.run; AgentRunner.finalize_task"}))?;
    let before = baseline::collect(&plan.run.worktree)?;
    evidence.append(json!({"event":"self_iteration_initial_inventory","files":before,"allowlist":plan.allowlist,"initial_new_files":{"invocation_state.rs":"absent-required","invocation_state.json":"absent-required"},"branch":plan.run.branch,"path":plan.run.worktree}))?;
    baseline::verify_initial(plan, &before)?;
    let mut current_inventory = before.clone();
    let initial_specification =
        super::input::specification(plan, &plan.stages[0], &before, &current_inventory)?;
    evidence.append(json!({"event":"self_iteration_case_revision","revision":plan.case_revision,"specification":initial_specification,"new_case_not_retry":true,"earlier_failed_run_unchanged":true}))?;
    fs::create_dir(control.join("state")).map_err(|e| e.to_string())?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(control.join("staging"))
        .map_err(|e| e.to_string())?;
    let installation = worker::WorkerRun::prepare().await;
    worker::initialize_worker(control, &evidence, &installation)?;
    SessionService::new(
        FileSessionStore::new(control.join("state/sessions")).map_err(|e| e.to_string())?,
    )
    .create(&plan.logical_session_id)
    .map_err(|e| e.to_string())?;
    let deployment = crate::deployments()
        .into_iter()
        .find(|d| d.family == plan.family && d.surface == plan.surface && d.model == plan.model)
        .ok_or("configured experiment deployment missing")?;
    let live = deployment.build();
    let ceiling = permissions(true);
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "self-iteration-agent".into(),
        revision: "r1".into(),
        display_name: Some("Kolyan self iteration".into()),
        model: model(plan),
        instructions: plan.instructions.clone(),
        permissions: ceiling.clone(),
    })
    .map_err(|e| e.to_string())?;
    let mut preceding: Option<AttemptBinding> = None;
    let mut validation_passed = false;
    let mut validation_log = String::new();
    let mut validation_record = None;
    let mut progress = workflow::Progress::default();
    let mut repair_feedback = None;
    for (index, stage) in plan.stages.iter().enumerate() {
        if stage.kind == StageKind::Repair && !progress.enter_repair()? {
            evidence.append(json!({"event":"self_iteration_optional_repair_skipped","stage":stage.id,"reason":"evidenced initial review accepted and fixed checks passed","new_attempt":false}))?;
            continue;
        }
        let observed_inventory = baseline::collect(&plan.run.worktree)?;
        evidence.append(json!({"event":"self_iteration_stage_inventory_pin","stage":stage.id,"inventory":observed_inventory,"unchanged_since_predecessor":observed_inventory==current_inventory}))?;
        if observed_inventory != current_inventory {
            return Err("candidate inventory changed outside actual predecessor tools".into());
        }
        if stage.kind.structured_review() {
            let validation_root = control.join(format!("validation-{}", stage.id));
            fs::create_dir(&validation_root).map_err(|e| e.to_string())?;
            let verification = validation::run(plan, &validation_root, &evidence).await?;
            validation_passed = verification.passed;
            validation_log = verification.review_input;
            validation_record = Some(verification.record);
        }
        let host = Host::open(
            plan,
            control,
            &definition,
            &ceiling,
            live.clone(),
            evidence.clone(),
        );
        evidence.append(json!({"event":"self_iteration_host_reconstructed","stage":stage.id,"ordinal":index,"worker_rebootstrap":false,"api_path":"AgentRunner source preparation; bound factories; Server TaskExecutionService.run; no Runner continuation-start"}))?;
        let specification = super::input::specification(plan, stage, &before, &current_inventory)?;
        let stage_input = super::input::prompt(&specification, stage)?;
        evidence.append(json!({"event":"self_iteration_stage_specification","stage":stage.id,"revision":plan.case_revision,"specification":specification}))?;
        let input = if matches!(
            stage.kind,
            StageKind::InitialReview | StageKind::FinalReview
        ) {
            format!(
                "{}\n\nHOST-PRODUCED FIXED VALIDATION (not model output):\n{}",
                stage_input, validation_log
            )
        } else if stage.kind == StageKind::Repair {
            format!(
                "{stage_input}\n\nHOST-BOUND REPAIR FEEDBACK (actual evidence, not a claimed success):\n{}",
                serde_json::to_string(
                    repair_feedback
                        .as_ref()
                        .ok_or("repair has no evidenced initial decision")?
                )
                .map_err(|e| e.to_string())?
            )
        } else {
            stage_input
        };
        let (bound, result) = super::execution::execute(super::execution::StageExecution {
            plan,
            stage,
            index,
            control,
            host: &host,
            definition: &definition,
            ceiling: &ceiling,
            preceding: &preceding,
            input,
            evidence: evidence.clone(),
            admitted_capacity,
            live: live.clone(),
        })
        .await?;
        let observed = outcome::observe(
            &stage.id,
            result
                .as_ref()
                .map(|(_, turn)| turn)
                .map_err(|error| error as &(dyn std::error::Error + 'static)),
        );
        evidence.append(observed.record)?;
        host.export(&plan.task_id, &evidence)?;
        let after = baseline::collect(&plan.run.worktree)?;
        let stage_before = current_inventory.clone();
        current_inventory = after.clone();
        evidence.append(json!({"event":"self_iteration_stage_observed","stage":stage.id,"binding":bound,"error":result.as_ref().err().map(ToString::to_string),"inventory":after,"changed":baseline::changed(&before,&after),"candidate_files":candidate_files(plan)?}))?;
        baseline::confined(plan, &before, &after)?;
        if !stage.writable && stage_before != after {
            return Err("read-only stage changed candidate inventory".into());
        }
        let (_, result) =
            result.map_err(|e| observed.failure.clone().unwrap_or_else(|| e.to_string()))?;
        let DurableTurnResult::Completed(completed, _) = result else {
            return Err(observed
                .failure
                .ok_or("missing actual suspension failure detail")?);
        };
        let TurnOutcome::FinalAnswer { response } = &completed.result.outcome else {
            return Err(observed
                .failure
                .ok_or("missing actual non-final outcome detail")?);
        };
        let events = host
            .ledger
            .execution_events_after(&bound.execution.execution_id, 0)
            .map_err(|e| e.to_string())?;
        if !events
            .iter()
            .any(|event| event.kind == LedgerEventKind::EffectReceipt)
        {
            return Err("stage has no actual model-generated tool receipt".into());
        }
        if stage.kind.structured_review() {
            let digests = super::review::digests(&plan.task, &current_inventory)?;
            let checks = super::review::receipt_checks(&events, &bound, &digests);
            evidence.append(json!({"event":"self_iteration_review_receipt_checks","stage":stage.id,"binding":bound,"candidate_digests":digests,"checks":checks}))?;
            super::review::require_receipts(&checks)?;
            let review = super::review::parse(response, &digests);
            evidence.append(json!({"event":"self_iteration_review_verdict","stage":stage.id,"review":review.as_ref().map_err(ToString::to_string),"response":response,"candidate_digests":digests}))?;
            let review = review?;
            if stage.kind == StageKind::InitialReview {
                let repair_required = progress.initial_review(validation_passed, review.verdict)?;
                repair_feedback = Some(
                    json!({"schema_version":1,"candidate_digests":digests,"initial_review":review,"read_receipt_checks":checks,"fixed_validation":validation_record.as_ref().ok_or("initial fixed validation record missing")?,"predecessor_binding":bound,"completed_invocation":true,"repair_limit":1}),
                );
                evidence.append(json!({"event":"self_iteration_repair_decision","required":repair_required,"feedback":repair_feedback}))?;
            } else {
                progress.final_review(validation_passed, review.verdict)?;
            }
        }
        preceding = Some(bound);
    }
    let host = Host::open(plan, control, &definition, &ceiling, live, evidence.clone());
    verify_candidate_receipts(plan, &host, &evidence)?;
    if !validation_passed {
        return Err(
            "independent fixed host verification failed; model review cannot override it".into(),
        );
    }
    let finalized = host
        .runner
        .finalize_task(TaskFinalizationRequest {
            task_id: plan.task_id.clone(),
            logical_session_id: plan.logical_session_id.clone(),
            root_invocation_id: plan.stages[0].id.clone(),
            root_attempt_id: format!("self-iteration-attempt-{}", plan.stages[0].id),
            policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
        })
        .await;
    host.export(&plan.task_id, &evidence)?;
    evidence.append(json!({"event":"self_iteration_runner_finalization","result":finalized.as_ref().map_err(ToString::to_string)}))?;
    let task = finalized.map_err(|e| e.to_string())?;
    if task.state != kolyan_server::TaskState::Completed {
        return Err("Runner did not verify Task completion".into());
    }
    Ok(())
}
