//! Explicit host projection before Turn admission; never Provider-side reduction.
#[path = "projection/minimax_live.rs"]
mod minimax_live;
use super::*;
use kolyan_agent::context::{
    BudgetMode, BudgetStatus, ContextPolicy, ContextProjectionPlan, ProjectedContext,
    RetainedMessageRange, context_source_digest, project_context,
};
use kolyan_model::ModelDescriptor;

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ProjectionCase {
    schema_version: u32,
    policy_id: String,
    source: String,
    selection: String,
    mode: BudgetMode,
    counter: String,
    turns: usize,
    roles: String,
    live_per_turn_artifact_oracle: bool,
}
#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ProjectionExpected {
    minimum_reduced_turns: usize,
    full_source_retained: bool,
    core_provider_same_input: bool,
    trusted_token_acceptance: bool,
}
fn fixture() -> ProjectionCase {
    serde_json::from_str(include_str!(
        "../../../fixtures/agent/long_task_projection.json"
    ))
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn initialize(
    root: &Path,
    graph: &GraphCase,
    saved: &AgentInvocationBinding,
    ownership: &FactRef,
    input: &ModelRequest,
    history: &[Message],
    case: &Case,
    evidence: &Evidence,
) -> Vec<Message> {
    let fixture = fixture();
    evidence.append(json!({"event":"projection_contract","fixture":fixture,"actual_turns":case.turns.len(),"actual_roles":graph.roles})).unwrap();
    assert_eq!(fixture.schema_version, 1);
    assert_eq!(fixture.turns, case.turns.len());
    assert_eq!(
        fixture.source,
        "complete_predecessor_trajectory_plus_current_input"
    );
    assert_eq!(
        fixture.selection,
        "first_user_anchor_and_complete_last_turn_plus_current_tail"
    );
    assert_eq!(fixture.mode, BudgetMode::Inspect);
    assert_eq!(fixture.counter, "Unsupported");
    assert_eq!(fixture.roles, "Root_then_nine_Continuation");
    assert_eq!(graph.roles[0], InvocationRole::Root);
    assert!(
        graph.roles[1..]
            .iter()
            .all(|role| *role == InvocationRole::Continuation)
    );
    assert!(fixture.live_per_turn_artifact_oracle);
    let mut source = input.clone();
    source.messages = history.to_vec();
    source.messages.extend(input.messages.clone());
    let last_prior_input = history
        .iter()
        .rposition(|message| {
            message.role == MessageRole::User
                && message
                    .content
                    .iter()
                    .any(|block| !matches!(block, ContentBlock::ToolResult { .. }))
        })
        .unwrap();
    let policy = ContextPolicy {
        id: fixture.policy_id,
        revision: "1".into(),
        mode: BudgetMode::Inspect,
        max_serialized_bytes: 4 * 1024 * 1024,
        max_messages: 2048,
        max_content_blocks: 8192,
        context_limit_tokens: None,
        output_reserve_tokens: case.output_reserve_tokens,
    };
    let plan = ContextProjectionPlan {
        policy_id: policy.id.clone(),
        policy_revision: policy.revision.clone(),
        expected_source_digest: context_source_digest(&source, &policy).unwrap(),
        retained_messages: if last_prior_input <= 1 {
            vec![RetainedMessageRange {
                start: 0,
                end: source.messages.len(),
            }]
        } else {
            vec![
                RetainedMessageRange { start: 0, end: 1 },
                RetainedMessageRange {
                    start: last_prior_input,
                    end: source.messages.len(),
                },
            ]
        },
    };
    let projected = project_context(
        &source,
        &ModelDescriptor {
            reference: source.model.clone(),
            context_window: Some(case.inspection_window_assumption_tokens),
            max_output_tokens: None,
            features: Default::default(),
        },
        &policy,
        &plan,
        &policy,
        &providers::UnsupportedCounter,
    );
    evidence.append(json!({"event":"host_projection_proposal","binding":saved,"ownership":ownership,"source":source,"plan":plan,"result":projected.as_ref().ok(),"error":projected.as_ref().err().map(ToString::to_string)})).unwrap();
    let projected = projected.unwrap();
    let artifacts = ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap();
    let source_artifact = artifacts
        .put(&serde_json::to_vec(&source).unwrap(), Retention::Required)
        .unwrap();
    let proof_artifact = artifacts
        .put(
            &serde_json::to_vec(
                &json!({"plan":plan,"projected":projected,"source_artifact":source_artifact}),
            )
            .unwrap(),
            Retention::Required,
        )
        .unwrap();
    let stream = format!("projection/{}", saved.invocation_id);
    let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
    let facts = journal.append(&stream,0,vec![FactDraft{fact_id:format!("{stream}/prepared"),subject:FactSubject{kind:"integration.host-projection".into(),id:saved.invocation_id.clone()},kind:"integration.host-projection.prepared".into(),schema_version:1,critical:true,causes:vec![ownership.clone()],payload:json!({"binding":saved,"source_artifact":source_artifact,"proof_artifact":proof_artifact,"projected":projected})}]).unwrap();
    // Read back from new stores before creating context or admitting the Turn.
    let read = SqliteFactJournal::open(root.join("state/ledger.sqlite"))
        .unwrap()
        .read(&stream, 0, 2)
        .unwrap();
    let durable = ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap();
    let restored: ModelRequest =
        serde_json::from_slice(&durable.read(&source_artifact, 4 * 1024 * 1024).unwrap()).unwrap();
    let proof: Value =
        serde_json::from_slice(&durable.read(&proof_artifact, 4 * 1024 * 1024).unwrap()).unwrap();
    evidence.append(json!({"event":"host_projection_durable_before_turn","facts":read,"source":restored,"proof":proof})).unwrap();
    assert_eq!(read, facts);
    assert_eq!(restored, source);
    assert_eq!(proof["projected"], json!(projected));
    assert!(matches!(
        projected.prepared.budget,
        BudgetStatus::Unverified {
            estimated_input_tokens: None,
            ..
        }
    ));
    let selected = projected.prepared.request.messages
        [..projected.prepared.request.messages.len() - input.messages.len()]
        .to_vec();
    let contexts = PrivateContextService::new(
        SessionService::new(FileSessionStore::new(root.join("state/sessions")).unwrap()),
        Arc::new(journal),
        Arc::new(AgentInvocationBindingStore::new(Arc::new(
            SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap(),
        ))),
    );
    let owner = PrivateContextOwner {
        logical_session_id: graph.logical_session_id.clone(),
        task_id: graph.task_id.clone(),
        invocation_id: saved.invocation_id.clone(),
        private_session_id: saved.private_session_id.clone(),
        snapshot_digest: saved.snapshot.digest().into(),
    };
    let initialized = contexts
        .initialize(&owner, ownership, selected.clone())
        .unwrap();
    evidence.append(json!({"event":"selected_context_initialized","owner":owner,"initialization":initialized.initialization})).unwrap();
    assert_eq!(initialized.context_messages, selected);
    assert_eq!(
        contexts
            .initialize(&owner, ownership, selected.clone())
            .unwrap(),
        initialized
    );
    selected
}

pub(super) fn verify(
    root: &Path,
    execution: &ExecutionRef,
    initial: &ModelRequest,
    evidence: &Evidence,
) {
    let invocation = execution.turn_id.strip_prefix("turn-").unwrap();
    let records = SqliteFactJournal::open(root.join("state/ledger.sqlite"))
        .unwrap()
        .read(&format!("projection/{invocation}"), 0, 2)
        .unwrap();
    let projected: ProjectedContext =
        serde_json::from_value(records[0].draft.payload["projected"].clone()).unwrap();
    let provider = evidence
        .rows()
        .into_iter()
        .find(|row| row["event"] == "request" && row["request"]["request_id"] == initial.request_id)
        .unwrap();
    evidence.append(json!({"event":"projection_core_provider_roundtrip","execution":execution,"facts":records,"core_request":initial,"provider_request":provider["request"]})).unwrap();
    // Core assigns a step request identity; every other input field must match.
    let mut expected = projected.prepared.request;
    expected.request_id = initial.request_id.clone();
    assert_eq!(*initial, expected);
    assert_eq!(provider["request"], json!(initial));
}

pub(super) fn finish(evidence: &Evidence) {
    let expected: ProjectionExpected = serde_json::from_str(
        include_str!("../../../expected/agent/long_task_projection.jsonl").trim(),
    )
    .unwrap();
    let rows = evidence.rows();
    let proposals = rows
        .iter()
        .filter(|row| row["event"] == "host_projection_proposal")
        .collect::<Vec<_>>();
    let readbacks = rows
        .iter()
        .filter(|row| row["event"] == "host_projection_durable_before_turn")
        .collect::<Vec<_>>();
    let roundtrips = rows
        .iter()
        .filter(|row| row["event"] == "projection_core_provider_roundtrip")
        .collect::<Vec<_>>();
    let full_source_retained = proposals.len() == readbacks.len()
        && proposals.iter().all(|proposal| {
            readbacks.iter().any(|readback| {
                readback["source"] == proposal["source"]
                    && readback["proof"]["projected"] == proposal["result"]
                    && readback["proof"]["plan"] == proposal["plan"]
            })
        });
    let core_provider_same_input = roundtrips.len() == proposals.len()
        && roundtrips
            .iter()
            .all(|row| row["core_request"] == row["provider_request"]);
    let trusted_token_acceptance = proposals.iter().any(|row| {
        let projected: ProjectedContext = serde_json::from_value(row["result"].clone()).unwrap();
        matches!(projected.prepared.budget, BudgetStatus::Verified { .. })
    });
    let reduced = rows
        .iter()
        .filter(|row| {
            row["event"] == "host_projection_proposal"
                && row["result"]["provenance"]["omitted_messages"]
                    .as_array()
                    .is_some_and(|ranges| !ranges.is_empty())
        })
        .count();
    evidence.append(json!({"event":"positive_projection_summary","reduced_turns":reduced,"full_source_retained":full_source_retained,"core_provider_same_input":core_provider_same_input,"trusted_token_acceptance":trusted_token_acceptance,"proposals":proposals.len(),"readbacks":readbacks.len(),"roundtrips":roundtrips.len(),"counter":"Unsupported","budget_mode":"Inspect","expected":expected})).unwrap();
    assert_eq!(proposals.len(), fixture().turns - 1);
    assert!(reduced >= expected.minimum_reduced_turns);
    assert_eq!(full_source_retained, expected.full_source_retained);
    assert_eq!(core_provider_same_input, expected.core_provider_same_input);
    assert_eq!(trusted_token_acceptance, expected.trusted_token_acceptance);
}

#[path = "projection/tests.rs"]
mod tests;

#[tokio::test]
async fn positive_projection_offline_real_os_single_task() {
    let installation = tools::worker::WorkerRun::prepare().await;
    graph_run_case(
        "named",
        ModelRef::new("fixture", "long-projection"),
        None,
        true,
        &installation,
    )
    .await;
}

#[tokio::test]
#[ignore = "Separate actual-model projection matrix; explicit host opt-in"]
async fn positive_projection_actual_model_matrix() {
    let installation = tools::worker::WorkerRun::prepare().await;
    let deployments = deployments();
    let mut report = matrix::Matrix::new(deployments.iter().flat_map(|d| {
        [
            format!("projection/{}/named", d.label()),
            format!("projection/{}/inline", d.label()),
        ]
    }));
    for (index, deployment) in deployments.iter().enumerate() {
        for (offset, selector) in ["named", "inline"].iter().enumerate() {
            report
                .run(index * 2 + offset, async {
                    let (model, provider) = deployment.build();
                    graph_run_case(selector, model, Some(provider), true, &installation).await;
                })
                .await;
        }
    }
    assert!(
        report.complete(),
        "Projection evidence: {}",
        report.directory.display()
    );
}
