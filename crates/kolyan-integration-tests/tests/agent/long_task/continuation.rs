//! Single Task graph, externally orchestrated by the trusted dataset host.
//! Model frames exist only in offline mode; actual-model entry never injects them.
//! This exercises TaskExecutionService, not a new AgentRunner continuation API.
use super::*;
#[path = "continuation/context_boundary.rs"]
mod context_boundary;
#[path = "continuation/finalization.rs"]
mod finalization;
#[path = "continuation/minimax_live.rs"]
mod minimax_live;
#[path = "continuation/projection.rs"]
mod projection;
use kolyan_agent::{
    EnvironmentToolFactory, ProviderFactory,
    binding::{AgentInvocationBinding, BindingContextKind, child_private_session_id},
};
use kolyan_core::TurnExecutor;
use kolyan_server::{
    AttemptBinding, AttemptOutcome, CompletionCriterion, ConsumedResult, InstanceOwner,
    InvocationDefinition, InvocationInputSource, InvocationRole, PrivateContextOwner,
    PrivateContextService, TaskDefinition,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphCase {
    schema_version: u32,
    task_id: String,
    logical_session_id: String,
    host_namespace: String,
    input_case: String,
    orchestration: String,
    projection: String,
    roles: Vec<InvocationRole>,
}
fn graph_case() -> GraphCase {
    serde_json::from_str(include_str!(
        "../../fixtures/agent/long_task_continuation.json"
    ))
    .unwrap()
}
fn expected() -> Value {
    serde_json::from_str(include_str!("../../expected/agent/long_task_continuation.jsonl").trim())
        .unwrap()
}
type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;
fn open(root: &Path) -> Service {
    TaskExecutionService::new(
        TaskCoordinator::new(SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap()),
        SessionExecutionService::new(
            ExecutionService::new(
                SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap(),
                NoopTraceSink,
            ),
            SessionService::new(FileSessionStore::new(root.join("state/sessions")).unwrap()),
        )
        .with_context_policy(SessionContextPolicy::FullTrajectory),
    )
}

async fn graph_run(
    selector: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &tools::worker::WorkerRun,
) {
    graph_run_case(selector, model, live, false, installation).await;
}

async fn graph_run_case(
    selector: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    positive_projection: bool,
    installation: &tools::worker::WorkerRun,
) {
    graph_run_contract(
        selector,
        model,
        live,
        positive_projection,
        false,
        installation,
    )
    .await;
}

async fn graph_run_contract(
    selector: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    positive_projection: bool,
    runner_finalization: bool,
    installation: &tools::worker::WorkerRun,
) {
    let graph = graph_case();
    let case = case();
    let oracle = expected();
    assert_eq!(graph.schema_version, 1);
    assert_eq!(graph.input_case, "long_task.json");
    assert_eq!(graph.orchestration, "trusted_host_explicit_graph");
    assert_eq!(graph.projection, "full_completed_predecessor_context");
    assert_eq!(graph.roles.len(), case.turns.len());
    let root = tempfile::Builder::new()
        .prefix("kolyan-long-continuation-")
        .tempdir()
        .unwrap()
        .keep();
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join("staging"))
            .unwrap();
    }
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    tools::initialize_worker(&root, &evidence, installation).unwrap();
    println!("CONTINUATION_TRACE={}", root.join("actual.jsonl").display());
    let mut plan = json!({"event":"plan","graph":serde_json::from_str::<Value>(include_str!("../../fixtures/agent/long_task_continuation.json")).unwrap(),"selector":selector,"model":model,"mode":if live.is_some(){"actual_model"}else{"offline_scripted_real_os"},"budget_mode":"Inspect","counter":"Unsupported","context_reduction_acceptance":false});
    if positive_projection {
        plan.as_object_mut()
            .unwrap()
            .remove("context_reduction_acceptance");
        plan["projection_fixture"] = serde_json::from_str(include_str!(
            "../../fixtures/agent/long_task_projection.json"
        ))
        .unwrap();
        plan["graph"]["projection"] = plan["projection_fixture"]["selection"].clone();
        plan["positive_selection_validation"] = json!(true);
        plan["trusted_token_acceptance"] = json!(false);
    }
    if runner_finalization {
        plan["runner_finalization_fixture"] = json!(finalization::fixture());
        plan["source_scope"] =
            json!("exact_frozen_predecessor_with_all_prior_source_archives_retained");
    }
    evidence.append(plan).unwrap();
    let permissions = AgentPermissions {
        tools: [
            EnvironmentTool::Read,
            EnvironmentTool::Write,
            EnvironmentTool::Edit,
        ]
        .into_iter()
        .collect(),
        delegation: Default::default(),
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "continuation-agent".into(),
        revision: "r1".into(),
        display_name: (selector == "named").then(|| "Continuation agent".into()),
        model: model.clone(),
        instructions: case.instructions.clone(),
        permissions: permissions.clone(),
    })
    .unwrap();
    SessionService::new(FileSessionStore::new(root.join("state/sessions")).unwrap())
        .create(&graph.logical_session_id)
        .unwrap();
    let mut preceding_context = vec![];
    let mut events = vec![];
    let mut instances = BTreeSet::new();
    let mut waits = 0;
    let mut waits_with_receipt = 0;
    for (index, turn) in case.turns.iter().enumerate() {
        // Reopen every store; never allocate through volatile per-process counters.
        let mut service = open(&root);
        let journal: Arc<dyn FactJournal> =
            Arc::new(SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap());
        let private = if index == 0 {
            graph.logical_session_id.clone()
        } else {
            child_private_session_id(&graph.logical_session_id, &graph.task_id, &turn.id).unwrap()
        };
        let execution = ExecutionRef {
            session_id: private.clone(),
            turn_id: format!("turn-{}", turn.id),
            execution_id: format!("execution-{}", turn.id),
        };
        let mut source = dataset(&case);
        for manifest in &mut source.policy {
            if manifest.tool_name == "file.read" && case.approved_read_turns.contains(&turn.id) {
                manifest.approval = ApprovalMode::Always;
            }
        }
        let original_input = ModelRequest {
            request_id: format!("input-{}", turn.id),
            model: model.clone(),
            system: vec![SystemInstruction {
                text: case.instructions.clone(),
                cache: false,
            }],
            messages: vec![Message {
                role: MessageRole::User,
                content: vec![ContentBlock::Text {
                    text: turn.input.clone(),
                }],
            }],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(case.output_reserve_tokens),
            extensions: Value::Null,
        };
        let selection = match selector {
            "named" => AgentSelector::Named(definition.key()),
            "inline" => AgentSelector::Inline(definition.clone()),
            _ => panic!("unknown selector"),
        };
        let root_prepared = if index == 0 {
            let request = || kolyan_agent::RootInputPreparationRequest {
                task_id: graph.task_id.clone(),
                invocation_id: turn.id.clone(),
                execution: execution.clone(),
                selector: selection.clone(),
                requested_permissions: permissions.clone(),
                model_request: original_input.clone(),
            };
            let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
            let physical_before = ledger.events_after(0).unwrap();
            let task_before = journal.read(&graph.task_id, 0, 32).unwrap();
            let result =
                finalization::runner(&root, &graph, &definition, &permissions, &source, &evidence)
                    .prepare_root_input(request())
                    .await;
            evidence.append(json!({"event":"root_input_prepared_before_task","result":result.as_ref().map(|prepared|json!({"snapshot":prepared.snapshot,"ownership":prepared.ownership,"selected_input":prepared.selected_input,"source":prepared.input_source})).map_err(ToString::to_string)})).unwrap();
            let prepared = result.unwrap();
            let retry =
                finalization::runner(&root, &graph, &definition, &permissions, &source, &evidence)
                    .prepare_root_input(request())
                    .await;
            let physical_after = ledger.events_after(0).unwrap();
            let task_after = journal.read(&graph.task_id, 0, 32).unwrap();
            evidence.append(json!({"event":"root_input_fresh_runner_retry","result":retry.as_ref().map(|prepared|json!({"snapshot":prepared.snapshot,"ownership":prepared.ownership,"selected_input":prepared.selected_input,"source":prepared.input_source})).map_err(ToString::to_string),"physical_before":physical_before,"physical_after":physical_after,"task_before":task_before,"task_after":task_after})).unwrap();
            let retry = retry.unwrap();
            assert_eq!(prepared.snapshot, retry.snapshot);
            assert_eq!(prepared.ownership, retry.ownership);
            assert_eq!(prepared.selected_input, retry.selected_input);
            assert_eq!(prepared.input_source, retry.input_source);
            assert_eq!(physical_before, physical_after);
            assert_eq!(task_before, task_after);
            Some(prepared)
        } else {
            None
        };
        let snapshot = if let Some(prepared) = &root_prepared {
            prepared.snapshot.clone()
        } else {
            let registry =
                InstanceRegistry::new(journal.clone(), &graph.host_namespace, 128).unwrap();
            let reservation = registry
                .reserve(InstanceOwner {
                    logical_session_id: graph.logical_session_id.clone(),
                    task_id: graph.task_id.clone(),
                    invocation_id: turn.id.clone(),
                })
                .unwrap();
            let mut catalog = AgentCatalog::new(8).unwrap();
            catalog.register(definition.clone()).unwrap();
            catalog
                .resolve(
                    &selection,
                    reservation.instance_id,
                    &permissions,
                    &permissions,
                )
                .unwrap()
        };
        assert!(instances.insert(snapshot.identity().instance_id.clone()));
        let bindings = Arc::new(AgentInvocationBindingStore::new(journal.clone()));
        let saved = if root_prepared.is_some() {
            bindings
                .load(&graph.task_id, &turn.id, &graph.logical_session_id)
                .unwrap()
                .unwrap()
        } else {
            AgentInvocationBinding {
                task_id: graph.task_id.clone(),
                invocation_id: turn.id.clone(),
                logical_session_id: graph.logical_session_id.clone(),
                private_session_id: private.clone(),
                context_kind: BindingContextKind::Child,
                snapshot: snapshot.clone(),
            }
        };
        assert_eq!(saved.snapshot, snapshot);
        let ownership = if let Some(prepared) = &root_prepared {
            prepared.ownership.clone()
        } else {
            bindings.save(&saved).unwrap()
        };
        if index > 0 && !positive_projection {
            let owner = PrivateContextOwner {
                logical_session_id: graph.logical_session_id.clone(),
                task_id: graph.task_id.clone(),
                invocation_id: turn.id.clone(),
                private_session_id: private.clone(),
                snapshot_digest: snapshot.digest().into(),
            };
            let contexts = PrivateContextService::new(
                SessionService::new(FileSessionStore::new(root.join("state/sessions")).unwrap()),
                journal.clone(),
                bindings.clone(),
            );
            let initialized = contexts
                .initialize(&owner, &ownership, preceding_context.clone())
                .unwrap();
            evidence.append(json!({"event":"private_projection_initialized","owner":owner,"ownership":ownership,"initialization":initialized.initialization})).unwrap();
            assert_eq!(initialized.context_messages, preceding_context);
            assert_eq!(
                contexts
                    .initialize(&owner, &ownership, preceding_context.clone())
                    .unwrap(),
                initialized
            );
        } else if index == 0 {
            service
                .coordinator()
                .register_task(
                    "long/register",
                    TaskDefinition {
                        task_id: graph.task_id.clone(),
                        objective: case.instructions.clone(),
                        criteria: vec![CompletionCriterion::ExecutionCompleted {
                            id: "final-continuation".into(),
                            invocation_id: case.turns.last().unwrap().id.clone(),
                        }],
                        agent: snapshot.identity().clone(),
                        constraints_digest: snapshot.digest().into(),
                        limits: TaskLimits {
                            max_depth: 0,
                            max_invocations: 10,
                            max_attempts: 10,
                            max_tokens: None,
                            max_steps_per_turn: case.max_steps.try_into().unwrap(),
                        },
                        cancellation_policy: CancellationPolicy::AllInvocations,
                    },
                )
                .unwrap();
        }
        let predecessor = index.checked_sub(1).map(|i| case.turns[i].id.clone());
        let build = |offset: usize| {
            let restored = AgentInvocationBindingStore::new(Arc::new(
                SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap(),
            ))
            .load(&graph.task_id, &turn.id, &graph.logical_session_id)
            .unwrap()
            .unwrap();
            assert_eq!(restored, saved);
            let mut script = source.clone();
            if live.is_none() {
                script
                    .turns
                    .iter_mut()
                    .find(|t| t.id == turn.id)
                    .unwrap()
                    .script
                    .drain(..offset);
            }
            let provider = Providers {
                live: live.clone(),
                dataset: script,
                evidence: evidence.clone(),
            }
            .build(&restored.snapshot, &execution)
            .unwrap();
            let set = Tools {
                root: root.clone(),
                dataset: source.clone(),
                evidence: evidence.clone(),
            }
            .build(&restored.snapshot, &execution)
            .unwrap();
            let definitions = set
                .definitions
                .into_iter()
                .filter(|tool| {
                    permissions
                        .tools
                        .iter()
                        .any(|allowed| allowed.name() == tool.name)
                })
                .collect::<Vec<_>>();
            (
                TurnExecutor::with_tools(provider, set.executor)
                    .with_policy_engine(set.policy)
                    .with_agent_snapshot_digest(restored.snapshot.digest().into()),
                definitions,
            )
        };
        let (executor, definitions) = build(0);
        let selected_input = if let Some(prepared) = &root_prepared {
            assert_eq!(prepared.selected_input.tools, definitions);
            prepared.selected_input.clone()
        } else {
            let mut input = original_input;
            input.tools = definitions;
            input
        };
        let request = TurnRequest {
            turn_id: execution.turn_id.clone(),
            config: TurnConfig {
                max_steps: case.max_steps,
                max_tool_calls: Some(case.max_tool_calls),
                deadline: None,
            },
            model_request: selected_input,
        };
        let selected_history = if positive_projection && index > 0 {
            projection::initialize(
                &root,
                &graph,
                &saved,
                &ownership,
                &request.model_request,
                &preceding_context,
                &case,
                &evidence,
            )
        } else {
            preceding_context.clone()
        };
        let input_source = if let Some(prepared) = root_prepared {
            InvocationInputSource::Standalone {
                fact: prepared.input_source.reference,
            }
        } else {
            let prepared = finalization::prepare_continuation_source(
                &root,
                &graph,
                &definition,
                &permissions,
                &source,
                &saved,
                &case.turns[index - 1].id,
                &request.model_request,
                &preceding_context,
                positive_projection,
                &evidence,
            )
            .await;
            InvocationInputSource::Derived {
                fact: prepared.reference,
            }
        };
        let before_admission = service.coordinator().snapshot(&graph.task_id).unwrap();
        assert!(!before_admission.invocations.contains_key(&turn.id));
        evidence.append(json!({"event":"source_prepared_before_admission","invocation":turn.id,"source":input_source,"selected_input":request.model_request,"task_before":before_admission})).unwrap();
        service
            .coordinator()
            .admit_invocation(
                &graph.task_id,
                &format!("admit/{}", turn.id),
                InvocationDefinition {
                    invocation_id: turn.id.clone(),
                    agent: snapshot.identity().clone(),
                    constraints_digest: snapshot.digest().into(),
                    role: graph.roles[index],
                    parent_invocation_id: predecessor.clone(),
                    dependencies: predecessor.iter().cloned().collect(),
                    input_source: input_source.clone(),
                },
            )
            .unwrap();
        if let Some(predecessor) = predecessor {
            let state = service.coordinator().snapshot(&graph.task_id).unwrap();
            let prior = &state.invocations[&predecessor];
            let attempt = &state.attempts[prior.attempts.last().unwrap()];
            let AttemptOutcome::Completed { evidence: proofs } =
                &attempt.observation.as_ref().unwrap().outcome
            else {
                panic!("predecessor not completed")
            };
            service
                .coordinator()
                .consume_child_result(
                    &graph.task_id,
                    &format!("consume/{}", turn.id),
                    &turn.id,
                    ConsumedResult {
                        child_invocation_id: predecessor,
                        completion_fact: prior.completion_fact.clone().unwrap(),
                        evidence: proofs.clone(),
                    },
                )
                .unwrap();
        }
        let binding = AttemptBinding {
            attempt_id: format!("attempt-{}", turn.id),
            invocation_id: turn.id.clone(),
            execution: execution.clone(),
            agent: snapshot.identity().clone(),
            constraints_digest: snapshot.digest().into(),
            input_source,
        };
        let mut result = service
            .run(&graph.task_id, binding.clone(), executor, request)
            .await;
        let mut local_waits = 0;
        loop {
            let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
            let observed = ledger
                .execution_events_after(&execution.execution_id, 0)
                .unwrap();
            for event in &observed {
                evidence
                    .append(json!({"event":"ledger","value":event}))
                    .unwrap();
            }
            evidence.append(json!({"event":"execution_observed","invocation":turn.id,"task":result.as_ref().ok().map(|r|&r.0),"error":result.as_ref().err().map(ToString::to_string)})).unwrap();
            let (task, outcome) = result.unwrap();
            match outcome {
                DurableTurnResult::Suspended { suspension, .. } => {
                    assert!(local_waits < case.max_suspensions);
                    assert_eq!(suspension.waiting.approvals.len(), 1);
                    let approval = suspension.waiting.approvals[0].approval_id.clone();
                    let offset = observed
                        .iter()
                        .filter(|e| e.kind == LedgerEventKind::ModelRequested)
                        .count();
                    let receipts = observed
                        .iter()
                        .filter(|e| e.kind == LedgerEventKind::EffectReceipt)
                        .count();
                    evidence.append(json!({"event":"wait_host_rebuild","invocation":turn.id,"suspension":suspension,"prior_receipts":receipts})).unwrap();
                    drop(service);
                    service = open(&root);
                    let (executor, _) = build(offset);
                    result = service
                        .resume_approval(&graph.task_id, binding.clone(), &approval, executor)
                        .await;
                    local_waits += 1;
                    waits += 1;
                    waits_with_receipt += usize::from(receipts > 0);
                }
                DurableTurnResult::Completed(completed, _) => {
                    assert!(matches!(
                        completed.result.outcome,
                        TurnOutcome::FinalAnswer { .. }
                    ));
                    assert_ne!(
                        task.state,
                        TaskState::Completed,
                        "only host final completion may close the graph"
                    );
                    let session = FileSessionStore::new(root.join("state/sessions"))
                        .unwrap()
                        .load(&execution.session_id)
                        .unwrap();
                    assert!(session.context_messages.starts_with(&selected_history));
                    let initial: ModelRequest = serde_json::from_value(
                        observed
                            .iter()
                            .find(|e| e.kind == LedgerEventKind::ModelRequested)
                            .unwrap()
                            .payload["request"]
                            .clone(),
                    )
                    .unwrap();
                    assert!(initial.messages.starts_with(&selected_history));
                    if positive_projection && index > 0 {
                        projection::verify(&root, &execution, &initial, &evidence);
                    }
                    let bytes = fs::read(root.join("workspace").join(&case.artifact_path)).unwrap();
                    evidence.append(json!({"event":"continuation_completed","invocation":turn.id,"role":graph.roles[index],"binding":saved,"session":session,"physical_bytes":bytes})).unwrap();
                    if live.is_none() || positive_projection || runner_finalization {
                        assert_eq!(
                            String::from_utf8(bytes).unwrap(),
                            expectations()[index]["file"]
                        );
                    }
                    if index + 1 < case.turns.len() {
                        preceding_context = finalization::frozen_context(
                            &root,
                            &graph.task_id,
                            &binding,
                            &evidence,
                        );
                    }
                    events.extend(observed);
                    break;
                }
            }
        }
    }
    let service = open(&root);
    let completed = if runner_finalization {
        finalization::finish(
            &root,
            &graph,
            &definition,
            &permissions,
            &dataset(&case),
            &evidence,
        )
        .await
    } else {
        service
            .complete(&graph.task_id, "long/final-complete")
            .unwrap()
    };
    let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
    let mut after = 0;
    loop {
        let page = journal.read(&graph.task_id, after, 512).unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().unwrap().position;
        for fact in page {
            evidence
                .append(json!({"event":"journal","value":fact}))
                .unwrap();
        }
    }
    let bytes = fs::read(root.join("workspace").join(&case.artifact_path)).unwrap();
    let artifact = ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024)
        .unwrap()
        .put(&bytes, Retention::Required)
        .unwrap();
    let verified = ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024)
        .unwrap()
        .read(&artifact, 4 * 1024 * 1024)
        .unwrap();
    let steps = events
        .iter()
        .filter(|e| e.kind == LedgerEventKind::StepCompleted)
        .count();
    let receipts = events
        .iter()
        .filter(|e| e.kind == LedgerEventKind::EffectReceipt)
        .count();
    let admitted = events
        .iter()
        .filter(|e| e.kind == LedgerEventKind::ExecutionInputAdmitted)
        .count();
    let step_keys = events
        .iter()
        .filter(|e| e.kind == LedgerEventKind::StepCompleted)
        .map(|e| (&e.execution_id, &e.payload["step_id"]))
        .map(|(execution, step)| (execution.clone(), step.as_str().unwrap().to_owned()))
        .collect::<BTreeSet<_>>();
    let last_request: ModelRequest = serde_json::from_value(
        events
            .iter()
            .rev()
            .find(|e| e.kind == LedgerEventKind::ModelRequested)
            .unwrap()
            .payload["request"]
            .clone(),
    )
    .unwrap();
    context_boundary::verify(&last_request, &case, &evidence);
    if positive_projection {
        projection::finish(&evidence);
    }
    let mut verification = json!({"event":"single_task_artifact_backed_verification","task":completed,"artifact":artifact,"actual_bytes":verified,"steps":steps,"receipts":receipts,"waits":waits,"waits_with_prior_receipt":waits_with_receipt,"scope":"host_dataset_verification_not_workspace_artifact_criterion","context_reduction_acceptance":false});
    if positive_projection {
        verification
            .as_object_mut()
            .unwrap()
            .remove("context_reduction_acceptance");
        verification["positive_selection_verified"] = json!(true);
        verification["trusted_token_acceptance"] = json!(false);
    }
    evidence.append(verification).unwrap();
    assert_eq!(completed.state, TaskState::Completed);
    assert_eq!(admitted, oracle["invocations"].as_u64().unwrap() as usize);
    assert_eq!(
        step_keys.len(),
        steps,
        "no recorded Step replay after host reconstruction"
    );
    assert_eq!(case.artifact_path, oracle["artifact"]);
    assert_eq!(
        completed.invocations.len(),
        oracle["invocations"].as_u64().unwrap() as usize
    );
    assert_eq!(
        completed.attempts.len(),
        oracle["attempts"].as_u64().unwrap() as usize
    );
    assert_eq!(
        completed
            .invocations
            .values()
            .filter(|i| i.definition.role == InvocationRole::Continuation)
            .count(),
        oracle["continuations"].as_u64().unwrap() as usize
    );
    assert!(steps >= oracle["minimum_steps"].as_u64().unwrap() as usize);
    assert!(waits >= oracle["minimum_wait_restarts"].as_u64().unwrap() as usize);
    assert!(
        waits_with_receipt >= oracle["minimum_waits_with_prior_receipt"].as_u64().unwrap() as usize
    );
    assert_eq!(bytes, verified);
    if live.is_none() {
        assert_eq!(steps, oracle["offline_steps"].as_u64().unwrap() as usize);
        assert_eq!(
            receipts,
            oracle["offline_receipts"].as_u64().unwrap() as usize
        );
    }
}

#[tokio::test]
async fn long_task_continuation_offline_real_os_single_graph() {
    let installation = tools::worker::WorkerRun::prepare().await;
    graph_run(
        "named",
        ModelRef::new("fixture", "long-continuation"),
        None,
        &installation,
    )
    .await;
}

#[tokio::test]
#[ignore = "Actual configured model matrix; host must explicitly opt in"]
async fn actual_model_long_task_continuation_matrix() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let deployments = deployments();
    let mut report = matrix::Matrix::new(deployments.iter().flat_map(|d| {
        [
            format!("continuation/{}/named", d.label()),
            format!("continuation/{}/inline", d.label()),
        ]
    }));
    for (index, deployment) in deployments.iter().enumerate() {
        for (offset, selector) in ["named", "inline"].iter().enumerate() {
            report
                .run(index * 2 + offset, async {
                    let (model, provider) = deployment.build();
                    graph_run(selector, model, Some(provider), &installation).await;
                })
                .await;
        }
    }
    assert!(
        report.complete(),
        "Continuation matrix evidence: {}",
        report.directory.display()
    );
}
