//! Real source/initialization/Continuation topology and routed Skill execution.
//! Only the model responses and inspection byte estimator are synthetic fixtures.
use super::*;
use crate::context::{
    BudgetMode, ContextPolicy, ContextProjectionPlan, RetainedMessageRange, SerializedByteEstimator,
};
use crate::runner::{routing::RoutedTools, tools::SnapshotTools};
use kolyan_core::TurnExecutor;
use kolyan_server::{
    AttemptBinding, ExecutionEvidence, HistoricalContextRequest, InstanceOwner,
    InvocationDefinition, InvocationInputSource, InvocationRole, PrivateContextOwner,
    PrivateContextService,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    revoke: bool,
    requests: usize,
    receipts: usize,
}

async fn run(row: &Row, path: &std::path::Path) -> Value {
    fs::create_dir_all(path).unwrap();
    let harness = Harness::new();
    let journal = adapters::Journal {
        inner: Arc::new(SqliteFactJournal::open(path.join("facts.sqlite")).unwrap()),
        streams: Arc::new(Mutex::new(Default::default())),
    };
    let service = Arc::new(TaskExecutionService::new(
        TaskCoordinator::new(journal.clone()),
        SessionExecutionService::new(
            ExecutionService::new(kolyan_ledger::InMemoryLedger::default(), NoopTraceSink),
            harness.service.sessions().sessions().clone(),
        ),
    ));
    let catalog = SkillCatalog::new(
        Arc::new(journal.clone()),
        Arc::new(ArtifactStore::new(path.join("skills"), 65536).unwrap()),
        "continuation.skills".into(),
        SkillLimits::default(),
    )
    .unwrap();
    let skill = catalog
        .register(
            SkillDescriptorInput {
                key: SkillKey::new("guide".into(), "1".into()).unwrap(),
                title: "Guide".into(),
                description: "Untrusted knowledge".into(),
            },
            "Continuation knowledge 🦀\n\n",
        )
        .unwrap();
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "knowledge-agent".into(),
        revision: "1".into(),
        display_name: None,
        model: ModelRef::new("fixture", "scripted"),
        instructions: "Use selected knowledge.".into(),
        permissions: AgentPermissions::default(),
    })
    .unwrap();
    let runtime = Arc::new(SkillRuntime::new(
        catalog.clone(),
        SkillAccessPolicy::new(
            "acl".into(),
            "1".into(),
            vec![SkillAccessRuleInput {
                agent: definition.key(),
                logical_session_id: "session".into(),
                task_id: Some(row.id.clone()),
                invocation_id: None,
                skills: [skill.metadata().descriptor().key.clone()].into(),
            }],
        )
        .unwrap(),
    ));
    let observations = Arc::new(Observations::default());
    let factory_case = Case {
        id: row.id.clone(),
        named: false,
        sqlite: true,
        mode: Mode::Load,
        revision: "1".into(),
        allow: true,
        body: "Continuation knowledge 🦀\n\n".into(),
        success: true,
        requests: 4,
        loaded: 2,
    };
    let runner = Arc::new(
        AgentRunner::new(
            service.clone(),
            InstanceRegistry::new(Arc::new(journal.clone()), "continuation.host", 16).unwrap(),
            AgentInvocationBindingStore::new(Arc::new(journal.clone())),
            AgentCatalog::new(4).unwrap(),
            AgentPermissions::default(),
            (
                adapters::Factory {
                    observations: observations.clone(),
                    case: factory_case,
                },
                adapters::environment(observations.clone()),
            ),
            Arc::new(ArtifactStore::new(path.join("inputs"), 16 * 1024 * 1024).unwrap()),
        )
        .unwrap()
        .with_skills(runtime.clone())
        .with_continuation_projection(ContinuationProjectionConfig {
            counter: Arc::new(SerializedByteEstimator),
        }),
    );
    let mut request = harness.request(&row.id, false);
    request.selector = AgentSelector::Inline(definition);
    request.requested_permissions = AgentPermissions::default();
    request.limits.max_tokens = None;
    request.limits.max_invocations = 2;
    request.limits.max_attempts = 2;
    request.limits.max_steps_per_turn = 4;
    request.turn.config.max_steps = 4;
    request.turn.model_request.max_output_tokens = Some(50);
    let root = runner.clone();
    let (task_id, snapshot, predecessor, turn, executor) =
        tokio::task::spawn_blocking(move || root.admit(request))
            .await
            .unwrap()
            .unwrap();
    service
        .run(&task_id, predecessor.clone(), executor, turn)
        .await
        .unwrap();
    let terminal = service
        .load_verified_historical_result(&task_id, &predecessor, 1024 * 1024)
        .unwrap();
    let ledger = service
        .sessions()
        .execution()
        .server()
        .coordinator()
        .ledger();
    let endpoint = |suffix: &str| {
        let id = format!(
            "{}/session/Completed/{suffix}",
            predecessor.execution.execution_id
        );
        ExecutionEvidence {
            execution: predecessor.execution.clone(),
            cursor: ledger.event_by_id(&id).unwrap().unwrap().cursor,
            event_id: id,
        }
    };
    let history = service
        .load_verified_historical_context(
            &task_id,
            &HistoricalContextRequest {
                binding: predecessor.clone(),
                terminal_fact: terminal.terminal_fact.clone(),
                prepared: endpoint("prepared"),
                committed: endpoint("committed"),
                max_bytes: 16 * 1024 * 1024,
            },
        )
        .unwrap();
    let reservation = runner
        .instances
        .reserve(InstanceOwner {
            logical_session_id: "session".into(),
            task_id: task_id.clone(),
            invocation_id: "successor".into(),
        })
        .unwrap();
    let saved = AgentInvocationBinding {
        task_id: task_id.clone(),
        invocation_id: "successor".into(),
        logical_session_id: "session".into(),
        private_session_id: child_private_session_id("session", &task_id, "successor").unwrap(),
        context_kind: BindingContextKind::Child,
        snapshot: AgentSnapshot::new(
            snapshot.definition().clone(),
            reservation.instance_id,
            snapshot.permissions().clone(),
        )
        .unwrap(),
    };
    let ownership = runner.bindings.save(&saved).unwrap();
    let owner = PrivateContextOwner {
        logical_session_id: "session".into(),
        task_id: task_id.clone(),
        invocation_id: "successor".into(),
        private_session_id: saved.private_session_id.clone(),
        snapshot_digest: saved.snapshot.digest().into(),
    };
    PrivateContextService::new(
        service.sessions().sessions().clone(),
        Arc::new(journal.clone()),
        Arc::new(runner.bindings.clone()),
    )
    .initialize(&owner, &ownership, history.messages.clone())
    .unwrap();
    let binding = runtime
        .bind(
            &runtime
                .discover(&saved.snapshot, &SkillScope::from_binding(&saved).unwrap())
                .unwrap(),
            &ownership,
        )
        .unwrap();
    let mut successor = harness.request("successor", false).turn;
    successor.turn_id = "successor-turn".into();
    successor.config.max_steps = 4;
    successor.model_request.model = saved.snapshot.definition().model().clone();
    successor.model_request.max_output_tokens = Some(50);
    successor.model_request.tools = vec![crate::skills::skill_load_definition(&binding).unwrap()];
    let bounds = ContextPolicy {
        id: "inspect".into(),
        revision: "1".into(),
        mode: BudgetMode::Inspect,
        max_serialized_bytes: 65536,
        max_messages: 100,
        max_content_blocks: 200,
        context_limit_tokens: None,
        output_reserve_tokens: 50,
    };
    let mut source = successor.model_request.clone();
    source.messages = history.messages.clone();
    source
        .messages
        .extend(successor.model_request.messages.clone());
    let prepared = runner
        .prepare_continuation_input(ContinuationProjectionRequest {
            task_id: task_id.clone(),
            logical_session_id: "session".into(),
            invocation_id: "successor".into(),
            predecessor_invocation_id: "root".into(),
            current_input: successor.model_request.clone(),
            descriptor: kolyan_model::ModelDescriptor {
                reference: source.model.clone(),
                context_window: Some(10000),
                max_output_tokens: Some(50),
                features: Default::default(),
            },
            source_bounds: bounds.clone(),
            plan: ContextProjectionPlan {
                policy_id: "full".into(),
                policy_revision: "1".into(),
                expected_source_digest: crate::context::context_source_digest(&source, &bounds)
                    .unwrap(),
                retained_messages: vec![RetainedMessageRange {
                    start: 0,
                    end: source.messages.len(),
                }],
            },
            target_policy: bounds,
        })
        .await
        .unwrap();
    let input_source = InvocationInputSource::Derived {
        fact: prepared.reference.clone(),
    };
    let (_, document): (_, Value) = runner.load_input_document(&saved, &input_source).unwrap();
    let decoded = crate::runner::finalization::decode_continuation_test_body(document.clone());
    let mut missing = document.clone();
    missing.as_object_mut().unwrap().remove("skill_binding");
    let missing_error =
        crate::runner::finalization::decode_continuation_test_body(missing.clone()).err();
    let mut unknown_ref = document.clone();
    unknown_ref["skill_binding"]["unexpected"] = json!(true);
    let unknown_error =
        crate::runner::finalization::decode_continuation_test_body(unknown_ref.clone()).err();
    let definition = InvocationDefinition {
        invocation_id: "successor".into(),
        agent: saved.snapshot.identity().clone(),
        constraints_digest: saved.snapshot.digest().into(),
        role: InvocationRole::Continuation,
        parent_invocation_id: Some("root".into()),
        dependencies: vec!["root".into()],
        input_source: input_source.clone(),
    };
    service
        .coordinator()
        .admit_invocation(&task_id, "successor.admit", definition.clone())
        .unwrap();
    let prior = service.coordinator().snapshot(&task_id).unwrap();
    let kolyan_server::AttemptOutcome::Completed { evidence } = &prior.attempts
        [&predecessor.attempt_id]
        .observation
        .as_ref()
        .unwrap()
        .outcome
    else {
        panic!("actual predecessor must complete");
    };
    service
        .coordinator()
        .consume_child_result(
            &task_id,
            "successor.dependency",
            "successor",
            kolyan_server::ConsumedResult {
                child_invocation_id: "root".into(),
                completion_fact: terminal.terminal_fact.clone(),
                evidence: evidence.clone(),
            },
        )
        .unwrap();
    let restored = runner
        .restored_skills(&saved, &input_source)
        .unwrap()
        .unwrap();
    if row.revoke {
        catalog
            .revoke(
                &skill.metadata().descriptor().key,
                skill.reference(),
                "revoke.1",
                "host revoked",
            )
            .unwrap();
    }
    let historical = runner
        .verify_continuation_origin(&saved, &definition)
        .err()
        .map(|e| e.to_string());
    let attempt = AttemptBinding {
        attempt_id: "successor-attempt".into(),
        invocation_id: "successor".into(),
        agent: saved.snapshot.identity().clone(),
        constraints_digest: saved.snapshot.digest().into(),
        input_source,
        execution: kolyan_server::ExecutionRef {
            session_id: saved.private_session_id.clone(),
            turn_id: "successor-turn".into(),
            execution_id: format!("{task_id}-successor"),
        },
    };
    let provider = runner
        .routed_provider(&saved.snapshot, &attempt.execution, Some(&restored))
        .unwrap();
    let set = runner
        .routed_tool_set(&saved.snapshot, &attempt.execution, Some(&restored))
        .unwrap();
    successor.model_request =
        serde_json::from_value(document["projected"]["prepared"]["request"].clone()).unwrap();
    let tools = RoutedTools {
        runner: runner.clone(),
        saved: saved.clone(),
        parent: attempt.clone(),
        environment: SnapshotTools {
            inner: set.executor,
            snapshot: saved.snapshot.clone(),
            execution: attempt.execution.clone(),
            policy: set.policy.clone(),
        },
        skill: runner
            .skill_executor(Some(&restored), &attempt.execution, set.policy.clone())
            .unwrap(),
    };
    let executor = TurnExecutor::with_tools(provider, tools)
        .with_policy_engine(set.policy)
        .with_agent_snapshot_digest(saved.snapshot.digest().into());
    let execution = service
        .run(&task_id, attempt.clone(), executor, successor)
        .await;
    let mut events = ledger
        .execution_events_after(&predecessor.execution.execution_id, 0)
        .unwrap();
    events.extend(
        ledger
            .execution_events_after(&attempt.execution.execution_id, 0)
            .unwrap(),
    );
    json!({"id":row.id,"source":source,"document":document,"input_source":prepared,"saved":saved,"binding":binding,
        "historical_error":historical,"decoded":decoded,"missing_document":missing,"missing_error":missing_error,"unknown_document":unknown_ref,"unknown_error":unknown_error,
        "execution":format!("{execution:?}"),"requests":*observations.requests.lock().unwrap(),"ledger":events,
        "journal":journal.records(),"environment_effects":*observations.effects.lock().unwrap()})
}

#[tokio::test]
async fn continuation_skill_source_restore_and_actual_load_data_matrix() {
    let cases: Vec<Row> = serde_json::from_str(include_str!("continuation.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-skills-continuation-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    for row in &cases {
        writeln!(output, "{}", run(row, &root.join(&row.id)).await).unwrap();
    }
    output.flush().unwrap();
    output.sync_all().unwrap();
    drop(output);
    println!("SKILL_CONTINUATION_ACTUAL={}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (actual, case) in rows.iter().zip(&cases) {
        assert_eq!(actual["historical_error"], Value::Null);
        assert_eq!(actual["decoded"]["Ok"], actual["document"]);
        assert!(
            actual["missing_error"]
                .as_str()
                .unwrap()
                .contains("skill_binding")
        );
        assert!(actual["unknown_error"].is_string());
        assert_eq!(actual["document"]["source"], actual["source"]);
        assert_eq!(
            actual["document"]["skill_binding"],
            actual["binding"]["reference"]
        );
        assert_eq!(
            actual["requests"].as_array().unwrap().len(),
            case.requests,
            "{}: {actual}",
            case.id
        );
        assert_eq!(actual["environment_effects"], json!([]));
        let receipts = actual["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "effect_receipt")
            .count();
        assert_eq!(receipts, case.receipts, "{}: {actual}", case.id);
    }
}
