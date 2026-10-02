//! New Runner-finalized scenes; historical host-only scenes stay unchanged.
#[path = "finalization/minimax_live.rs"]
mod minimax_live;
use super::*;
use kolyan_agent::context::{
    BudgetMode, ContextPolicy, ContextProjectionPlan, RetainedMessageRange, context_source_digest,
};
use kolyan_agent::{
    ContinuationProjectionConfig, ContinuationProjectionRequest, TaskFinalizationPolicy,
    TaskFinalizationRequest,
};
use kolyan_model::ModelDescriptor;
use kolyan_server::{HistoricalContextRequest, TaskSnapshot};

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FinalizationCase {
    schema_version: u32,
    selectors: Vec<String>,
    selections: Vec<String>,
    configured_combinations: usize,
    finalizer: String,
    source: String,
    counter: String,
}
#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    invocations: usize,
    continuations: usize,
    duplicate_finalization_unchanged: bool,
    trusted_token_acceptance: bool,
}
pub(super) fn fixture() -> FinalizationCase {
    serde_json::from_str(include_str!(
        "../../../fixtures/agent/long_task_runner_finalization.json"
    ))
    .unwrap()
}
pub(super) fn runner(
    root: &Path,
    graph: &GraphCase,
    definition: &AgentDefinition,
    permissions: &AgentPermissions,
    source: &data::Dataset,
    evidence: &Arc<Evidence>,
) -> Arc<Runner> {
    let journal: Arc<dyn FactJournal> =
        Arc::new(SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap());
    let mut catalog = AgentCatalog::new(8).unwrap();
    catalog.register(definition.clone()).unwrap();
    let runner = AgentRunner::new(
        Arc::new(open(root)),
        InstanceRegistry::new(journal.clone(), &graph.host_namespace, 128).unwrap(),
        AgentInvocationBindingStore::new(journal),
        catalog,
        permissions.clone(),
        (
            Providers {
                live: None,
                dataset: source.clone(),
                evidence: evidence.clone(),
            },
            Tools {
                root: root.into(),
                dataset: source.clone(),
                evidence: evidence.clone(),
            },
        ),
        Arc::new(ArtifactStore::new(root.join("state/artifacts"), 16 * 1024 * 1024).unwrap()),
    )
    .unwrap();
    Arc::new(
        runner.with_continuation_projection(ContinuationProjectionConfig {
            counter: Arc::new(providers::UnsupportedCounter),
        }),
    )
}
pub(super) fn frozen_context(
    root: &Path,
    task: &str,
    binding: &AttemptBinding,
    evidence: &Evidence,
) -> Vec<Message> {
    let service = open(root);
    let terminal = service
        .load_verified_historical_result(task, binding, 1024 * 1024)
        .unwrap();
    let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
    let endpoint = |suffix| {
        let id = format!(
            "{}/session/Completed/{suffix}",
            binding.execution.execution_id
        );
        let event = ledger.event_by_id(&id).unwrap().unwrap();
        kolyan_server::ExecutionEvidence {
            execution: binding.execution.clone(),
            event_id: id,
            cursor: event.cursor,
        }
    };
    let result = service.load_verified_historical_context(
        task,
        &HistoricalContextRequest {
            binding: binding.clone(),
            terminal_fact: terminal.terminal_fact,
            prepared: endpoint("prepared"),
            committed: endpoint("committed"),
            max_bytes: 16 * 1024 * 1024,
        },
    );
    evidence.append(json!({"event":"runner_frozen_predecessor","result":result.as_ref().map_err(ToString::to_string)})).unwrap();
    result.unwrap().messages
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_continuation_source(
    root: &Path,
    graph: &GraphCase,
    definition: &AgentDefinition,
    permissions: &AgentPermissions,
    source: &data::Dataset,
    saved: &AgentInvocationBinding,
    predecessor_invocation_id: &str,
    input: &ModelRequest,
    frozen_history: &[Message],
    positive_projection: bool,
    evidence: &Arc<Evidence>,
) -> kolyan_server::VerifiedInvocationInputSource {
    let case = case();
    let mut policy = ContextPolicy {
        id: "long-full-predecessor".into(),
        revision: "1".into(),
        mode: BudgetMode::Inspect,
        max_serialized_bytes: 4 * 1024 * 1024,
        max_messages: 2048,
        max_content_blocks: 8192,
        context_limit_tokens: None,
        output_reserve_tokens: case.output_reserve_tokens,
    };
    let plan = if positive_projection {
        // Reuse this invocation's exported proposal, never a prior turn's plan.
        let proposal = evidence
            .rows()
            .into_iter()
            .rev()
            .find(|row| {
                row["event"] == "host_projection_proposal"
                    && row["binding"]["invocation_id"] == saved.invocation_id
            })
            .unwrap();
        let plan: ContextProjectionPlan = serde_json::from_value(proposal["plan"].clone()).unwrap();
        policy.id = plan.policy_id.clone();
        policy.revision = plan.policy_revision.clone();
        plan
    } else {
        let mut full = input.clone();
        full.messages = frozen_history.to_vec();
        full.messages.extend(input.messages.clone());
        ContextProjectionPlan {
            policy_id: policy.id.clone(),
            policy_revision: policy.revision.clone(),
            expected_source_digest: context_source_digest(&full, &policy).unwrap(),
            retained_messages: vec![RetainedMessageRange {
                start: 0,
                end: full.messages.len(),
            }],
        }
    };
    let request = ContinuationProjectionRequest {
        task_id: graph.task_id.clone(),
        logical_session_id: graph.logical_session_id.clone(),
        invocation_id: saved.invocation_id.clone(),
        predecessor_invocation_id: predecessor_invocation_id.into(),
        current_input: input.clone(),
        descriptor: ModelDescriptor {
            reference: input.model.clone(),
            context_window: Some(case.inspection_window_assumption_tokens),
            max_output_tokens: None,
            features: Default::default(),
        },
        source_bounds: policy.clone(),
        plan,
        target_policy: policy,
    };
    let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
    let physical_before = ledger.events_after(0).unwrap();
    let task_before = task_records(root, &graph.task_id);
    let result = runner(root, graph, definition, permissions, source, evidence)
        .prepare_continuation_input(request.clone())
        .await;
    evidence.append(json!({"event":"runner_source_prepared_before_invocation","invocation":saved.invocation_id,"predecessor":predecessor_invocation_id,"positive_projection":positive_projection,"result":result.as_ref().map_err(ToString::to_string)})).unwrap();
    let prepared = result.unwrap();
    let retry = runner(root, graph, definition, permissions, source, evidence)
        .prepare_continuation_input(request)
        .await;
    let physical_after = ledger.events_after(0).unwrap();
    let task_after = task_records(root, &graph.task_id);
    evidence.append(json!({"event":"continuation_source_fresh_runner_retry","invocation":saved.invocation_id,"result":retry.as_ref().map_err(ToString::to_string),"physical_before":physical_before,"physical_after":physical_after,"task_before":task_before,"task_after":task_after})).unwrap();
    assert_eq!(prepared, retry.unwrap());
    assert_eq!(physical_before, physical_after);
    assert_eq!(task_before, task_after);
    prepared
}

fn task_records(root: &Path, task: &str) -> Vec<kolyan_ledger::FactRecord> {
    let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
    let mut records = Vec::new();
    let mut after = 0;
    loop {
        let page = journal.read(task, after, 512).unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().unwrap().position;
        records.extend(page);
    }
    records
}

pub(super) async fn finish(
    root: &Path,
    graph: &GraphCase,
    definition: &AgentDefinition,
    permissions: &AgentPermissions,
    source: &data::Dataset,
    evidence: &Arc<Evidence>,
) -> TaskSnapshot {
    let expected: Expected = serde_json::from_str(
        include_str!("../../../expected/agent/long_task_runner_finalization.jsonl").trim(),
    )
    .unwrap();
    let first_turn = &source.turns[0].id;
    let request = TaskFinalizationRequest {
        task_id: graph.task_id.clone(),
        logical_session_id: graph.logical_session_id.clone(),
        root_invocation_id: first_turn.clone(),
        root_attempt_id: format!("attempt-{first_turn}"),
        policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
    };
    let before = task_records(root, &graph.task_id);
    let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
    let physical_before = ledger.events_after(0).unwrap();
    let first = runner(root, graph, definition, permissions, source, evidence)
        .finalize_task(request.clone())
        .await;
    let after_first = task_records(root, &graph.task_id);
    let physical_after_first = ledger.events_after(0).unwrap();
    let second = runner(root, graph, definition, permissions, source, evidence)
        .finalize_task(request)
        .await;
    let after_second = task_records(root, &graph.task_id);
    let physical_after = ledger.events_after(0).unwrap();
    evidence.append(json!({"event":"runner_role_aware_finalization","before":before,"first":first.as_ref().map_err(ToString::to_string),"second":second.as_ref().map_err(ToString::to_string),"after_first":after_first,"after_second":after_second,"physical_before":physical_before,"physical_after_first":physical_after_first,"physical_after":physical_after,"expected":expected,"trusted_token_acceptance":false})).unwrap();
    let first = first.unwrap();
    assert_eq!(first, second.unwrap());
    assert_eq!(physical_before, physical_after_first);
    assert_eq!(physical_after_first, physical_after);
    assert_eq!(first.invocations.len(), expected.invocations);
    assert_eq!(
        first
            .invocations
            .values()
            .filter(|i| i.definition.role == InvocationRole::Continuation)
            .count(),
        expected.continuations
    );
    assert_eq!(
        after_first == after_second && physical_before == physical_after,
        expected.duplicate_finalization_unchanged
    );
    assert!(!expected.trusted_token_acceptance);
    first
}

async fn run(
    selector: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    projection: bool,
    installation: &tools::worker::WorkerRun,
) {
    let data = fixture();
    assert_eq!(data.schema_version, 1);
    assert_eq!(data.finalizer, "AgentRunner.role_aware");
    assert_eq!(data.source, "exact_frozen_predecessor");
    assert_eq!(data.counter, "Unsupported");
    assert!(data.selectors.iter().any(|value| value == selector));
    assert_eq!(data.selections, ["full", "explicit_projection"]);
    graph_run_contract(selector, model, live, projection, true, installation).await;
}

#[tokio::test]
async fn runner_finalized_long_graph_complete_offline_real_os() {
    let installation = tools::worker::WorkerRun::prepare().await;
    for selector in &fixture().selectors {
        for projection in [false, true] {
            run(
                selector,
                ModelRef::new("fixture", "runner-finalized-long"),
                None,
                projection,
                &installation,
            )
            .await;
        }
    }
}

#[tokio::test]
#[ignore = "New Runner-finalized matrices; complete offline gate required first"]
async fn runner_finalized_long_graph_actual_model_matrices() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let deployments = deployments();
    assert_eq!(deployments.len(), fixture().configured_combinations);
    let mut reports = Vec::new();
    for projection in [false, true] {
        let mut report = matrix::Matrix::new(deployments.iter().flat_map(|d| {
            [
                format!("runner-finalized/{projection}/{}/named", d.label()),
                format!("runner-finalized/{projection}/{}/inline", d.label()),
            ]
        }));
        for (index, deployment) in deployments.iter().enumerate() {
            for (offset, selector) in fixture().selectors.iter().enumerate() {
                report
                    .run(index * 2 + offset, async {
                        let (model, provider) = deployment.build();
                        run(selector, model, Some(provider), projection, &installation).await;
                    })
                    .await;
            }
        }
        // Export every planned matrix before comparing either scope.
        println!(
            "RUNNER_FINALIZED_MATRIX projection={projection} report={}",
            report.directory.display()
        );
        reports.push(report);
    }
    for report in reports {
        assert!(report.complete(), "{}", report.directory.display());
    }
}
