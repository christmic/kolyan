//! Actual Root/Continuation admission and bound execution, reconstructed per stage.

use super::super::driver::{context_policy, frozen, permissions, request};
use super::{
    Plan, Stage,
    host::{Host, model},
    tools::Factory,
};
use crate::{evidence::Evidence, providers::Providers};
use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentPermissions, AgentSelector, ContinuationProjectionRequest,
    EnvironmentToolFactory, ProviderFactory, RootInputPreparationRequest,
    binding::{
        AgentInvocationBinding, AgentInvocationBindingStore, BindingContextKind,
        child_private_session_id,
    },
    context::{ContextProjectionPlan, RetainedMessageRange, context_source_digest},
};
use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::FactJournal;
use kolyan_model::{ModelDescriptor, ModelProvider};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::{
    AttemptBinding, AttemptOutcome, CancellationPolicy, CompletionCriterion, ConsumedResult,
    ExecutionRef, InstanceOwner, InstanceRegistry, InvocationDefinition, InvocationInputSource,
    InvocationRole, PrivateContextOwner, PrivateContextService, SessionService, TaskDefinition,
    TaskExecutionError, TaskLimits, TaskSnapshot,
};
use kolyan_storage::FileSessionStore;
use serde_json::json;
use std::{path::Path, sync::Arc};

pub(super) struct StageExecution<'a> {
    pub plan: &'a Plan,
    pub stage: &'a Stage,
    pub index: usize,
    pub control: &'a Path,
    pub host: &'a Host,
    pub definition: &'a AgentDefinition,
    pub ceiling: &'a AgentPermissions,
    pub preceding: &'a Option<AttemptBinding>,
    pub input: String,
    pub evidence: Arc<Evidence>,
    pub admitted_capacity: u64,
    pub live: Arc<dyn ModelProvider>,
}
pub(super) async fn execute(
    spec: StageExecution<'_>,
) -> Result<
    (
        AttemptBinding,
        Result<(TaskSnapshot, DurableTurnResult), TaskExecutionError>,
    ),
    String,
> {
    let StageExecution {
        plan,
        stage,
        index,
        control,
        host,
        definition,
        ceiling,
        preceding,
        input,
        evidence,
        admitted_capacity,
        live,
    } = spec;
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
    super::input::configure(plan, stage, &mut request)?;
    evidence.append(json!({"event":"self_iteration_stage_output_contract","mode":"local_strict","stage":stage.id,"kind":stage.kind,"request":request,"output_format":request.output_format}))?;
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
                ceiling,
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
        let history = frozen(host, plan, prior)?;
        let contexts = PrivateContextService::new(
            SessionService::new(
                FileSessionStore::new(control.join("state/sessions")).map_err(|e| e.to_string())?,
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
            plan: plan.base.clone(),
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
                        max_invocations: admitted_capacity,
                        max_attempts: admitted_capacity,
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
    let mut dataset = crate::data::dataset();
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
        plan: plan.base.clone(),
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

    Ok((bound, result))
}
