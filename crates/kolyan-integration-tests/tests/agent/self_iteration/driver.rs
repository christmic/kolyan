//! Trusted explicit Root/Continuation graph. The model, not this host, edits code.

mod outcome;

use std::{fs, os::unix::fs::DirBuilderExt, path::Path, sync::Arc};

use futures_util::FutureExt;
use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentSelector,
    ContinuationProjectionRequest, EnvironmentTool, EnvironmentToolFactory, ProviderFactory,
    RootInputPreparationRequest, TaskFinalizationPolicy, TaskFinalizationRequest,
    binding::{
        AgentInvocationBinding, AgentInvocationBindingStore, BindingContextKind,
        child_private_session_id,
    },
    context::{
        BudgetMode, ContextPolicy, ContextProjectionPlan, RetainedMessageRange,
        context_source_digest,
    },
};
use kolyan_core::{TurnConfig, TurnExecutor, TurnOutcome, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelDescriptor, ModelRequest, SystemInstruction,
    ToolChoice,
};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{
    AttemptBinding, AttemptOutcome, CancellationPolicy, CompletionCriterion, ConsumedResult,
    ExecutionEvidence, ExecutionRef, HistoricalContextRequest, InstanceOwner, InstanceRegistry,
    InvocationDefinition, InvocationInputSource, InvocationRole, PrivateContextOwner,
    PrivateContextService, SessionService, TaskDefinition, TaskLimits,
};
use kolyan_storage::FileSessionStore;
use serde_json::{Value, json};

use super::super::{evidence::Evidence, providers::Providers, tools::worker};
use super::{
    Plan, baseline,
    host::{Host, model},
    tools::Factory,
    validation,
};

pub(super) async fn run(plan: Plan) {
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
    let error = match result {
        Ok(result) => result.err(),
        Err(payload) => Some(
            payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or("self-iteration host panic".into()),
        ),
    };
    evidence.append(json!({"event":"self_iteration_acceptance","error":error,"passed":error.is_none(),"candidate_authored_by_host":false,"matrix_replacement":false})).unwrap();
    assert!(
        error.is_none(),
        "Self iteration failed: {error:?}; {}",
        control.display()
    );
}

async fn experiment(plan: &Plan, control: &Path, evidence: Arc<Evidence>) -> Result<(), String> {
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
    let deployment = super::super::deployments()
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
    for (index, stage) in plan.stages.iter().enumerate() {
        if index == 3 {
            let verification = validation::run(plan, control, &evidence).await?;
            validation_passed = verification.passed;
            validation_log = verification.review_input;
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
        let input = if index == 3 {
            format!(
                "{}\n\nHOST-PRODUCED FIXED VALIDATION (not model output):\n{}",
                stage_input, validation_log
            )
        } else {
            stage_input
        };
        let requested = permissions(stage.writable);
        let private = if index == 0 {
            plan.logical_session_id.clone()
        } else {
            child_private_session_id(&plan.logical_session_id, &plan.task_id, &stage.id)
                .map_err(|e| e.to_string())?
        };
        let execution = ExecutionRef {
            session_id: private.clone(),
            turn_id: format!("self-iteration-{}", stage.id),
            execution_id: format!("self-iteration-execution-{}", stage.id),
        };
        let mut request = request(plan, &stage.id, input);
        let (snapshot, source) = if let Some(prior) = &preceding {
            let journal: Arc<dyn FactJournal> = Arc::new(host.journal.clone());
            let registry = InstanceRegistry::new(journal.clone(), "self-iteration-host", 128)
                .map_err(|e| e.to_string())?;
            let reserved = registry
                .reserve(InstanceOwner {
                    logical_session_id: plan.logical_session_id.clone(),
                    task_id: plan.task_id.clone(),
                    invocation_id: stage.id.clone(),
                })
                .map_err(|e| e.to_string())?;
            let mut catalog = AgentCatalog::new(8).map_err(|e| e.to_string())?;
            catalog
                .register(definition.clone())
                .map_err(|e| e.to_string())?;
            let snapshot = catalog
                .resolve(
                    &AgentSelector::Named(definition.key()),
                    reserved.instance_id,
                    &ceiling,
                    &requested,
                )
                .map_err(|e| e.to_string())?;
            let saved = AgentInvocationBinding {
                task_id: plan.task_id.clone(),
                invocation_id: stage.id.clone(),
                logical_session_id: plan.logical_session_id.clone(),
                private_session_id: private.clone(),
                context_kind: BindingContextKind::Child,
                snapshot: snapshot.clone(),
            };
            let bindings = Arc::new(AgentInvocationBindingStore::new(journal.clone()));
            let ownership = bindings.save(&saved).map_err(|e| e.to_string())?;
            let history = frozen(&host, plan, prior)?;
            let contexts = PrivateContextService::new(
                SessionService::new(
                    FileSessionStore::new(control.join("state/sessions"))
                        .map_err(|e| e.to_string())?,
                ),
                journal,
                bindings,
            );
            let owner = PrivateContextOwner {
                logical_session_id: plan.logical_session_id.clone(),
                task_id: plan.task_id.clone(),
                invocation_id: stage.id.clone(),
                private_session_id: private,
                snapshot_digest: snapshot.digest().into(),
            };
            let initialized = contexts
                .initialize(&owner, &ownership, history.clone())
                .map_err(|e| e.to_string())?;
            let set = Factory {
                plan: plan.clone(),
                control: control.into(),
                evidence: evidence.clone(),
            }
            .build(&snapshot, &execution)
            .map_err(|e| e.to_string())?;
            request.tools = set.definitions;
            let policy = context_policy(plan);
            let mut full = request.clone();
            full.messages = history;
            full.messages.extend(request.messages.clone());
            let source = host
                .runner
                .prepare_continuation_input(ContinuationProjectionRequest {
                    task_id: plan.task_id.clone(),
                    logical_session_id: plan.logical_session_id.clone(),
                    invocation_id: stage.id.clone(),
                    predecessor_invocation_id: prior.invocation_id.clone(),
                    current_input: request.clone(),
                    descriptor: ModelDescriptor {
                        reference: model(plan),
                        context_window: Some(plan.inspection_window_assumption_tokens),
                        max_output_tokens: None,
                        features: Default::default(),
                    },
                    source_bounds: policy.clone(),
                    plan: ContextProjectionPlan {
                        policy_id: policy.id.clone(),
                        policy_revision: policy.revision.clone(),
                        expected_source_digest: context_source_digest(&full, &policy)
                            .map_err(|e| e.to_string())?,
                        retained_messages: vec![RetainedMessageRange {
                            start: 0,
                            end: full.messages.len(),
                        }],
                    },
                    target_policy: policy,
                })
                .await
                .map_err(|e| e.to_string())?;
            evidence.append(json!({"event":"self_iteration_continuation_source","owner":owner,"ownership":ownership,"initialization":initialized,"snapshot":snapshot,"source":source,"predecessor":prior}))?;
            (
                snapshot,
                InvocationInputSource::Derived {
                    fact: source.reference,
                },
            )
        } else {
            let prepared = host
                .runner
                .prepare_root_input(RootInputPreparationRequest {
                    task_id: plan.task_id.clone(),
                    invocation_id: stage.id.clone(),
                    execution: execution.clone(),
                    selector: AgentSelector::Named(definition.key()),
                    requested_permissions: requested.clone(),
                    model_request: request,
                })
                .await
                .map_err(|e| e.to_string())?;
            request = prepared.selected_input;
            evidence.append(json!({"event":"self_iteration_root_source","ownership":prepared.ownership,"snapshot":prepared.snapshot,"source":prepared.input_source,"request":request}))?;
            host.service
                .coordinator()
                .register_task(
                    "self-iteration-registered",
                    TaskDefinition {
                        task_id: plan.task_id.clone(),
                        objective: plan.instructions.clone(),
                        criteria: vec![CompletionCriterion::ExecutionCompleted {
                            id: "review-finished".into(),
                            invocation_id: plan.stages.last().unwrap().id.clone(),
                        }],
                        agent: prepared.snapshot.identity().clone(),
                        constraints_digest: prepared.snapshot.digest().into(),
                        limits: TaskLimits {
                            max_depth: 0,
                            max_invocations: 4,
                            max_attempts: 4,
                            max_tokens: None,
                            max_steps_per_turn: u32::try_from(plan.max_steps)
                                .map_err(|e| e.to_string())?,
                        },
                        cancellation_policy: CancellationPolicy::AllInvocations,
                    },
                )
                .map_err(|e| e.to_string())?;
            (
                prepared.snapshot,
                InvocationInputSource::Standalone {
                    fact: prepared.input_source.reference,
                },
            )
        };
        host.service
            .coordinator()
            .admit_invocation(
                &plan.task_id,
                &format!("self-iteration-admit-{}", stage.id),
                InvocationDefinition {
                    invocation_id: stage.id.clone(),
                    agent: snapshot.identity().clone(),
                    constraints_digest: snapshot.digest().into(),
                    role: if index == 0 {
                        InvocationRole::Root
                    } else {
                        InvocationRole::Continuation
                    },
                    parent_invocation_id: preceding.as_ref().map(|p| p.invocation_id.clone()),
                    dependencies: preceding.iter().map(|p| p.invocation_id.clone()).collect(),
                    input_source: source.clone(),
                },
            )
            .map_err(|e| e.to_string())?;
        if let Some(prior) = &preceding {
            let task = host
                .service
                .coordinator()
                .snapshot(&plan.task_id)
                .map_err(|e| e.to_string())?;
            let invocation = &task.invocations[&prior.invocation_id];
            let observation = task.attempts[&prior.attempt_id]
                .observation
                .as_ref()
                .ok_or("predecessor observation missing")?;
            let AttemptOutcome::Completed { evidence: proofs } = &observation.outcome else {
                return Err("predecessor not actually completed".into());
            };
            host.service
                .coordinator()
                .consume_child_result(
                    &plan.task_id,
                    &format!("self-iteration-consume-{}", stage.id),
                    &stage.id,
                    ConsumedResult {
                        child_invocation_id: prior.invocation_id.clone(),
                        completion_fact: invocation
                            .completion_fact
                            .clone()
                            .ok_or("predecessor completion fact missing")?,
                        evidence: proofs.clone(),
                    },
                )
                .map_err(|e| e.to_string())?;
        }
        let bound = AttemptBinding {
            attempt_id: format!("self-iteration-attempt-{}", stage.id),
            invocation_id: stage.id.clone(),
            execution: execution.clone(),
            agent: snapshot.identity().clone(),
            constraints_digest: snapshot.digest().into(),
            input_source: source,
        };
        let mut dataset = super::super::data::dataset();
        dataset.inspection_window_assumption_tokens = plan.inspection_window_assumption_tokens;
        dataset.output_reserve_tokens = plan.output_reserve_tokens;
        let provider = Providers {
            live: Some(live.clone()),
            dataset,
            evidence: evidence.clone(),
        }
        .build(&snapshot, &execution)
        .map_err(|e| e.to_string())?;
        let set = Factory {
            plan: plan.clone(),
            control: control.into(),
            evidence: evidence.clone(),
        }
        .build(&snapshot, &execution)
        .map_err(|e| e.to_string())?;
        if request.tools != set.definitions {
            return Err("selected input inventory differs from bound actual factory".into());
        }
        let executor = TurnExecutor::with_tools(provider, set.executor)
            .with_policy_engine(set.policy)
            .with_agent_snapshot_digest(snapshot.digest().into());
        let result = host
            .service
            .run(
                &plan.task_id,
                bound.clone(),
                executor,
                TurnRequest {
                    turn_id: execution.turn_id.clone(),
                    config: TurnConfig {
                        max_steps: plan.max_steps,
                        max_tool_calls: Some(plan.max_tool_calls),
                        deadline: None,
                    },
                    model_request: request,
                },
            )
            .await;
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
        current_inventory = after.clone();
        evidence.append(json!({"event":"self_iteration_stage_observed","stage":stage.id,"binding":bound,"error":result.as_ref().err().map(ToString::to_string),"inventory":after,"changed":baseline::changed(&before,&after),"candidate_files":candidate_files(plan)?}))?;
        baseline::confined(plan, &before, &after)?;
        let (_, result) =
            result.map_err(|e| observed.failure.clone().unwrap_or_else(|| e.to_string()))?;
        let DurableTurnResult::Completed(completed, _) = result else {
            return Err(observed.failure.unwrap_or_else(|| {
                "unexpected self-iteration suspension; no automatic approval".into()
            }));
        };
        if !matches!(completed.result.outcome, TurnOutcome::FinalAnswer { .. }) {
            return Err(observed
                .failure
                .unwrap_or_else(|| "stage did not reach actual final answer".into()));
        }
        let events = host
            .ledger
            .execution_events_after(&execution.execution_id, 0)
            .map_err(|e| e.to_string())?;
        if !events
            .iter()
            .any(|event| event.kind == LedgerEventKind::EffectReceipt)
        {
            return Err("stage has no actual model-generated tool receipt".into());
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

fn permissions(writable: bool) -> AgentPermissions {
    AgentPermissions {
        tools: if writable {
            [
                EnvironmentTool::Read,
                EnvironmentTool::Write,
                EnvironmentTool::Edit,
            ]
            .into()
        } else {
            [EnvironmentTool::Read].into()
        },
        delegation: Default::default(),
    }
}
fn request(plan: &Plan, id: &str, input: String) -> ModelRequest {
    ModelRequest {
        request_id: format!("self-iteration-input-{id}"),
        model: model(plan),
        system: vec![SystemInstruction {
            text: plan.instructions.clone(),
            cache: false,
        }],
        messages: vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text { text: input }],
        }],
        tools: vec![],
        tool_choice: ToolChoice::Auto,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: Some(plan.output_reserve_tokens),
        extensions: Value::Null,
    }
}
fn context_policy(plan: &Plan) -> ContextPolicy {
    ContextPolicy {
        id: "self-iteration-full-history".into(),
        revision: "1".into(),
        mode: BudgetMode::Inspect,
        max_serialized_bytes: 4 * 1024 * 1024,
        max_messages: 2048,
        max_content_blocks: 8192,
        context_limit_tokens: None,
        output_reserve_tokens: plan.output_reserve_tokens,
    }
}
fn frozen(host: &Host, plan: &Plan, binding: &AttemptBinding) -> Result<Vec<Message>, String> {
    let terminal = host
        .service
        .load_verified_historical_result(&plan.task_id, binding, 1048576)
        .map_err(|e| e.to_string())?;
    let endpoint = |suffix| -> Result<ExecutionEvidence, String> {
        let event = host
            .ledger
            .event_by_id(&format!(
                "{}/session/Completed/{suffix}",
                binding.execution.execution_id
            ))
            .map_err(|e| e.to_string())?
            .ok_or("committed context endpoint missing")?;
        Ok(ExecutionEvidence {
            execution: binding.execution.clone(),
            event_id: event.event_id,
            cursor: event.cursor,
        })
    };
    host.service
        .load_verified_historical_context(
            &plan.task_id,
            &HistoricalContextRequest {
                binding: binding.clone(),
                terminal_fact: terminal.terminal_fact,
                prepared: endpoint("prepared")?,
                committed: endpoint("committed")?,
                max_bytes: 16 * 1024 * 1024,
            },
        )
        .map(|context| context.messages)
        .map_err(|e| e.to_string())
}
fn candidate_files(plan: &Plan) -> Result<Value, String> {
    let mut files = serde_json::Map::new();
    for path in &plan.allowlist {
        match fs::read(plan.run.worktree.join(path)) {
            Ok(bytes) => {
                files.insert(
                    path.clone(),
                    json!({"sha256":baseline::digest(&bytes),"bytes":bytes}),
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                files.insert(path.clone(), json!({"absent":true}));
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(Value::Object(files))
}
fn verify_candidate_receipts(plan: &Plan, host: &Host, evidence: &Evidence) -> Result<(), String> {
    let events = host.ledger.events_after(0).map_err(|e| e.to_string())?;
    let task = host
        .service
        .coordinator()
        .snapshot(&plan.task_id)
        .map_err(|e| e.to_string())?;
    let mut checks = Vec::new();
    for path in &plan.allowlist {
        let check = (|| -> Result<Value, String> {
            let bytes = fs::read(plan.run.worktree.join(path)).map_err(|e| e.to_string())?;
            let digest = baseline::digest(&bytes);
            let receipt = events.iter().rev().find(|event| {
                event.kind == LedgerEventKind::EffectReceipt
                    && matches!(
                        event.payload["input"]["prepared"]["call"]["name"].as_str(),
                        Some("file.write" | "file.edit")
                    )
                    && event.payload["input"]["prepared"]["call"]["arguments"]["path"] == *path
            });
            let receipt = receipt
                .ok_or_else(|| format!("candidate has no actual mutation receipt: {path}"))?;
            let prepared: kolyan_policy::PreparedCall =
                serde_json::from_value(receipt.payload["input"]["prepared"].clone())
                    .map_err(|e| e.to_string())?;
            let scope: kolyan_policy::ToolExecutionScope =
                serde_json::from_value(receipt.payload["input"]["scope"].clone())
                    .map_err(|e| e.to_string())?;
            let admitted = task
                .attempts
                .values()
                .find(|attempt| attempt.binding.execution.execution_id == receipt.execution_id)
                .ok_or("receipt execution has no independently admitted Task attempt")?;
            if scope.execution.session_id != admitted.binding.execution.session_id
                || scope.execution.turn_id != admitted.binding.execution.turn_id
                || scope.execution.execution_id != admitted.binding.execution.execution_id
                || receipt.turn_id != admitted.binding.execution.turn_id
                || scope.agent_snapshot_digest.as_deref()
                    != Some(admitted.binding.constraints_digest.as_str())
            {
                return Err("receipt scope differs from independently admitted binding".into());
            }
            let grant: kolyan_policy::PreparedGrant =
                serde_json::from_value(receipt.payload["prepared_grant"].clone())
                    .map_err(|e| e.to_string())?;
            let authorization = events
                .iter()
                .find(|event| {
                    event.execution_id == receipt.execution_id
                        && event.kind == LedgerEventKind::EffectAuthorized
                        && event.payload["prepared_grant"] == receipt.payload["prepared_grant"]
                        && event.payload["authorization_id"]
                            == receipt.payload["authorization"]["authorization_id"]
                })
                .ok_or("receipt has no exact durable authorization")?;
            let revision = authorization.payload["authority_revision"]
                .as_str()
                .ok_or("authorization revision missing")?;
            grant
                .validate(&prepared, revision, &scope)
                .map_err(|e| e.to_string())?;
            let scope_matches = scope.execution.execution_id == receipt.execution_id
                && events.iter().any(|event| {
                    event.execution_id == receipt.execution_id
                        && event.kind == LedgerEventKind::EffectStarted
                        && event.payload["effect_id"] == receipt.payload["effect_id"]
                        && event.payload["scope"] == serde_json::to_value(&scope).unwrap()
                });
            let actual_model_call = events
                .iter()
                .filter(|event| {
                    event.execution_id == receipt.execution_id
                        && event.kind == LedgerEventKind::StepCompleted
                })
                .any(|event| {
                    exact_step_call(&event.payload["step"], &scope.step_id, prepared.call())
                });
            let output = &receipt.payload["output"]["content"];
            let output = if let Some(text) = output.as_str() {
                serde_json::from_str::<Value>(text).map_err(|e| e.to_string())?
            } else {
                output.clone()
            };
            let matches = output["sha256"] == digest
                && output["bytes"] == bytes.len()
                && actual_model_call
                && scope_matches;
            Ok(
                json!({"path":path,"physical_sha256":digest,"receipt":receipt,"actual_model_call_found":actual_model_call,"physical_bytes_match_receipt":matches}),
            )
        })();
        checks.push(check.unwrap_or_else(
            |error| json!({"path":path,"error":error,"physical_bytes_match_receipt":false}),
        ));
    }
    evidence.append(json!({"event":"self_iteration_candidate_receipt_links","checks":checks}))?;
    if checks
        .iter()
        .any(|check| check["physical_bytes_match_receipt"] != true)
    {
        return Err("candidate bytes not bound to actual model call/native receipt".into());
    }
    Ok(())
}

/// Match structured model output only; prose mentioning a call ID is not evidence.
pub(super) fn exact_step_call(
    value: &Value,
    step_id: &str,
    expected: &kolyan_model::ToolCall,
) -> bool {
    serde_json::from_value::<kolyan_core::StepResult>(value.clone()).is_ok_and(|step| {
        step.step_id == step_id
            && step
                .response
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolCall { call } if call == expected))
    })
}
