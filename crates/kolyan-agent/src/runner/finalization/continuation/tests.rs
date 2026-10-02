//! Actual host-managed Task successors; no Agent loop or fake terminal evidence.

use super::*;
use crate::runner::finalization::tests::{capture_evidence, outcome, provider, runner};
use crate::runner::tests::support::{Harness, Tools};
use kolyan_core::TurnExecutor;
use kolyan_server::{AttemptBinding, ConsumedResult, InstanceOwner, InvocationDefinition};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    successors: usize,
    mutation: Mutation,
    root_script: provider::Script,
    child_script: provider::Script,
    expected_error: bool,
    expected_state: String,
    expected_requests: usize,
    #[serde(default)]
    projection: Option<ProjectionCase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionCase {
    ranges: Vec<crate::context::RetainedMessageRange>,
    publish: bool,
    restore_config: bool,
    change_input: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    WrongInitialContext,
    MissingInitialization,
    HostRevoked,
    LaterSessionHistory,
}

async fn scenario(case: &Case, harness: &Harness, row: &mut Value) -> Result<(), String> {
    let config = ContinuationProjectionConfig {
        counter: Arc::new(provider::Counter),
    };
    let root = runner(harness, &case.root_script, false);
    let mut request = harness.request(&case.id, false);
    request.limits.max_invocations = case.successors as u64 + 1;
    request.limits.max_attempts = case.successors as u64 + 1;
    request.limits.max_tokens = None;
    let prepare = root.clone();
    let (task_id, snapshot, mut predecessor, turn, executor) =
        tokio::task::spawn_blocking(move || prepare.admit(request))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
    Box::pin(
        harness
            .service
            .run(&task_id, predecessor.clone(), executor, turn),
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut contexts = Vec::new();
    for index in 0..case.successors {
        let inv = format!("successor-{index}");
        let terminal = harness
            .service
            .load_verified_historical_result(&task_id, &predecessor, 1024 * 1024)
            .map_err(|error| error.to_string())?;
        let history = harness
            .service
            .load_verified_historical_context(
                &task_id,
                &HistoricalContextRequest {
                    binding: predecessor.clone(),
                    terminal_fact: terminal.terminal_fact.clone(),
                    prepared: root
                        .context_endpoint(&predecessor.execution, "prepared")
                        .map_err(|error| error.to_string())?,
                    committed: root
                        .context_endpoint(&predecessor.execution, "committed")
                        .map_err(|error| error.to_string())?,
                    max_bytes: MAX_CONTEXT_BYTES,
                },
            )
            .map_err(|error| error.to_string())?;
        let reservation = root
            .instances
            .reserve(InstanceOwner {
                logical_session_id: "session".into(),
                task_id: task_id.clone(),
                invocation_id: inv.clone(),
            })
            .map_err(|error| error.to_string())?;
        let child_snapshot = crate::AgentSnapshot::new(
            snapshot.definition().clone(),
            reservation.instance_id,
            snapshot.permissions().clone(),
        )
        .map_err(|error| error.to_string())?;
        let saved = crate::AgentInvocationBinding {
            task_id: task_id.clone(),
            invocation_id: inv.clone(),
            logical_session_id: "session".into(),
            private_session_id: crate::child_private_session_id("session", &task_id, &inv)
                .map_err(|error| error.to_string())?,
            context_kind: BindingContextKind::Child,
            snapshot: child_snapshot,
        };
        let child = configured_runner(harness, &case.child_script, false, Some(config.clone()));
        let mut child_request = harness.request("child-template", false).turn;
        child_request.turn_id = format!("{inv}-turn");
        child_request.model_request.request_id = format!("{inv}-input");
        // The host template is not an admitted Agent model selection. Use the
        // saved immutable definition, exactly as the production root driver does.
        child_request.model_request.model = saved.snapshot.definition().model().clone();
        child_request.model_request.tools = Tools(harness.observations.clone(), false)
            .build(&saved.snapshot, &predecessor.execution)
            .map_err(|error| error.to_string())?
            .definitions;
        let projection_request = {
            let policy = crate::context::ContextPolicy {
                id: "continuation-selection".into(),
                revision: "1".into(),
                mode: crate::context::BudgetMode::Strict,
                max_serialized_bytes: 65536,
                max_messages: 100,
                max_content_blocks: 200,
                context_limit_tokens: None,
                output_reserve_tokens: 100,
            };
            let mut source = child_request.model_request.clone();
            source.messages = history.messages.clone();
            source
                .messages
                .extend(child_request.model_request.messages.clone());
            ContinuationProjectionRequest {
                task_id: task_id.clone(),
                logical_session_id: "session".into(),
                invocation_id: inv.clone(),
                predecessor_invocation_id: predecessor.invocation_id.clone(),
                current_input: child_request.model_request.clone(),
                descriptor: kolyan_model::ModelDescriptor {
                    reference: source.model.clone(),
                    context_window: Some(10000),
                    max_output_tokens: Some(100),
                    features: Default::default(),
                },
                source_bounds: policy.clone(),
                plan: crate::context::ContextProjectionPlan {
                    policy_id: "selection".into(),
                    policy_revision: "1".into(),
                    expected_source_digest: crate::context::context_source_digest(&source, &policy)
                        .map_err(|error| error.to_string())?,
                    retained_messages: case
                        .projection
                        .as_ref()
                        .map(|projection| projection.ranges.clone())
                        .unwrap_or_else(|| {
                            vec![crate::context::RetainedMessageRange {
                                start: 0,
                                end: source.messages.len(),
                            }]
                        }),
                },
                target_policy: policy,
            }
        };
        let ownership = harness
            .bindings
            .save(&saved)
            .map_err(|error| error.to_string())?;
        let owner = PrivateContextOwner {
            logical_session_id: saved.logical_session_id.clone(),
            task_id: task_id.clone(),
            invocation_id: inv.clone(),
            private_session_id: saved.private_session_id.clone(),
            snapshot_digest: saved.snapshot.digest().into(),
        };
        let service = PrivateContextService::new(
            harness.service.sessions().sessions().clone(),
            Arc::new(ContextJournal(harness.service.clone())),
            Arc::new(harness.bindings.clone()),
        );
        if matches!(case.mutation, Mutation::MissingInitialization) {
            harness
                .service
                .sessions()
                .sessions()
                .create(&saved.private_session_id)
                .map_err(|error| error.to_string())?;
        } else {
            let mut selected = history.messages.clone();
            if let Some(projection) = &case.projection {
                selected.clear();
                for range in &projection.ranges {
                    selected.extend_from_slice(
                        &history.messages[range.start.min(history.messages.len())
                            ..range.end.min(history.messages.len())],
                    );
                }
            }
            if matches!(case.mutation, Mutation::WrongInitialContext) {
                selected.clear();
            }
            // Initial publication is a host action, not finalization/recovery.
            let publisher = PrivateContextService::new(
                harness.service.sessions().sessions().clone(),
                Arc::new(harness.service.coordinator().journal().clone()),
                Arc::new(harness.bindings.clone()),
            );
            publisher
                .initialize(&owner, &ownership, selected)
                .map_err(|error| error.to_string())?;
        }
        contexts.push(json!({"owner":owner,"ownership":ownership,"source":history,"initialization":service.load_verified_initialization(&owner,&ownership,MAX_CONTEXT_BYTES).map_err(|error| error.to_string())}));
        let fault_source = matches!(
            case.mutation,
            Mutation::WrongInitialContext | Mutation::MissingInitialization
        ) || case
            .projection
            .as_ref()
            .is_some_and(|projection| !projection.publish);
        let input = if fault_source {
            // An adversarial host candidate reaches generic admission, but has
            // no valid Agent proof. Preserve the old physical-execution refusal
            // scenarios instead of replacing their assertions with early exits.
            let mut actual_source = child_request.model_request.clone();
            actual_source.messages = history.messages.clone();
            actual_source
                .messages
                .extend(child_request.model_request.messages.clone());
            child.publish_input_document(
                &saved,
                kolyan_server::InvocationInputKind::Derived,
                &json!({"request":projection_request,"source":actual_source,"invalid_agent_proof":case.id}),
                vec![ownership.clone(), terminal.terminal_fact.clone()],
            ).map_err(|error| error.to_string())?
        } else {
            let first = child
                .prepare_continuation_input(projection_request.clone())
                .await
                .map_err(|error| error.to_string())?;
            let second = child
                .prepare_continuation_input(projection_request)
                .await
                .map_err(|error| error.to_string())?;
            row["projection"] = json!({"first":first,"second":second});
            if first != second {
                return Err("source preparation is not idempotent".into());
            }
            first
        };
        let input_source = kolyan_server::InvocationInputSource::Derived {
            fact: input.reference,
        };
        harness
            .service
            .coordinator()
            .admit_invocation(
                &task_id,
                &format!("{inv}/admit"),
                InvocationDefinition {
                    invocation_id: inv.clone(),
                    agent: saved.snapshot.identity().clone(),
                    constraints_digest: saved.snapshot.digest().into(),
                    role: InvocationRole::Continuation,
                    parent_invocation_id: Some(predecessor.invocation_id.clone()),
                    dependencies: vec![predecessor.invocation_id.clone()],
                    input_source: input_source.clone(),
                },
            )
            .map_err(|error| error.to_string())?;
        let prior = harness
            .service
            .coordinator()
            .snapshot(&task_id)
            .map_err(|error| error.to_string())?;
        let kolyan_server::AttemptOutcome::Completed { evidence } = &prior.attempts
            [&predecessor.attempt_id]
            .observation
            .as_ref()
            .ok_or("predecessor observation absent")?
            .outcome
        else {
            return Err("predecessor failed".into());
        };
        harness
            .service
            .coordinator()
            .consume_child_result(
                &task_id,
                &format!("{inv}/dependency"),
                &inv,
                ConsumedResult {
                    child_invocation_id: predecessor.invocation_id.clone(),
                    completion_fact: terminal.terminal_fact,
                    evidence: evidence.clone(),
                },
            )
            .map_err(|error| error.to_string())?;
        if case
            .projection
            .as_ref()
            .is_some_and(|projection| projection.change_input)
        {
            child_request.model_request.messages[0] = kolyan_model::Message {
                role: kolyan_model::MessageRole::User,
                content: vec![kolyan_model::ContentBlock::Text {
                    text: "Changed actual input after source publication".into(),
                }],
            };
        }
        let binding = AttemptBinding {
            attempt_id: format!("{inv}-attempt"),
            invocation_id: inv.clone(),
            agent: saved.snapshot.identity().clone(),
            constraints_digest: saved.snapshot.digest().into(),
            input_source,
            execution: kolyan_server::ExecutionRef {
                session_id: saved.private_session_id.clone(),
                turn_id: format!("{inv}-turn"),
                execution_id: format!("{}-{inv}-execution", case.id),
            },
        };
        let provider = child
            .providers
            .build(&saved.snapshot, &binding.execution)
            .map_err(|error| error.to_string())?;
        let set = Tools(harness.observations.clone(), false)
            .build(&saved.snapshot, &binding.execution)
            .map_err(|error| error.to_string())?;
        child_request.model_request.tools = set.definitions;
        let executor = TurnExecutor::with_tools(
            provider,
            crate::runner::tools::SnapshotTools {
                inner: set.executor,
                snapshot: saved.snapshot.clone(),
                execution: binding.execution.clone(),
                policy: set.policy.clone(),
            },
        )
        .with_policy_engine(set.policy)
        .with_agent_snapshot_digest(saved.snapshot.digest().into());
        row[format!("run_{index}")] = match Box::pin(harness.service.run(
            &task_id,
            binding.clone(),
            executor,
            child_request,
        ))
        .await
        {
            Ok((task, execution)) => json!({"task":task,"execution":format!("{execution:?}")}),
            Err(error) => json!({"error":error.to_string()}),
        };
        predecessor = binding;
    }
    row["contexts"] = json!(contexts);
    if matches!(case.mutation, Mutation::LaterSessionHistory) {
        let later = root
            .start(harness.request(&format!("{}-later", case.id), false))
            .await
            .map_err(|error| error.to_string())?;
        row["later_task"] = json!(later.task);
    }
    let request = TaskFinalizationRequest {
        task_id: task_id.clone(),
        logical_session_id: "session".into(),
        root_invocation_id: "root".into(),
        root_attempt_id: "attempt".into(),
        policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
    };
    row["before"] = json!(
        harness
            .service
            .coordinator()
            .journal()
            .read(&task_id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["requests_before"] = json!(harness.observations.requests.lock().unwrap().clone());
    row["first"] = outcome(
        configured_runner(
            harness,
            &case.child_script,
            matches!(case.mutation, Mutation::HostRevoked),
            (!case
                .projection
                .as_ref()
                .is_some_and(|projection| !projection.restore_config))
            .then(|| config.clone()),
        )
        .finalize_task(request.clone())
        .await,
    );
    row["after_first"] = json!(
        harness
            .service
            .coordinator()
            .journal()
            .read(&task_id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["second"] = outcome(
        configured_runner(
            harness,
            &case.child_script,
            matches!(case.mutation, Mutation::HostRevoked),
            (!case
                .projection
                .as_ref()
                .is_some_and(|projection| !projection.restore_config))
            .then(|| config.clone()),
        )
        .finalize_task(request)
        .await,
    );
    row["after_second"] = json!(
        harness
            .service
            .coordinator()
            .journal()
            .read(&task_id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["state"] = json!(
        harness
            .service
            .coordinator()
            .snapshot(&task_id)
            .map_err(|error| error.to_string())?
            .state
    );
    Ok(())
}

fn configured_runner(
    harness: &Harness,
    script: &provider::Script,
    revoked: bool,
    config: Option<ContinuationProjectionConfig>,
) -> Arc<super::super::tests::Runner> {
    let runner = runner(harness, script, revoked);
    match config {
        Some(config) => Arc::new(
            Arc::try_unwrap(runner)
                .unwrap_or_else(|_| panic!("fresh runner is shared"))
                .with_continuation_projection(config),
        ),
        None => runner,
    }
}

#[tokio::test]
async fn continuation_finalization_uses_frozen_context_and_exact_dependency_direction() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-continuation-finalization-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("AGENT_CONTINUATION_FINALIZATION_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let harness = Harness::new();
        let mut row = json!({"id":case.id});
        if let Err(error) = Box::pin(scenario(case, &harness, &mut row)).await {
            row["error"] = json!(error);
        }
        capture_evidence(&case.id, &harness, &harness.service, &mut row);
        writeln!(output, "{row}").unwrap();
        output.sync_all().unwrap();
    }
    let actual = std::fs::read_to_string(path).unwrap();
    assert_eq!(actual.lines().count(), cases.len());
    for (line, case) in actual.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert!(row["error"].is_null(), "{}: {row}", case.id);
        assert_eq!(
            row["first"]["error"].is_string(),
            case.expected_error,
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["second"]["error"].is_string(),
            case.expected_error,
            "{row}"
        );
        assert_eq!(row["state"], case.expected_state);
        assert_eq!(row["after_first"], row["after_second"]);
        if case.expected_error {
            assert_eq!(row["before"], row["after_first"]);
        }
        assert_eq!(row["requests_before"], row["requests_after"]);
        assert_eq!(
            row["requests_after"].as_array().unwrap().len(),
            case.expected_requests
        );
        assert!(row["effects_after"].as_array().unwrap().is_empty());
    }
}
