//! Explicit host feedback policy over unchanged cases, not model-call rewriting.

use super::*;
use kolyan_core::ToolErrorPolicy;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    groups: Vec<Group>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Group {
    scope: String,
    cases: Vec<String>,
}

fn groups() -> Vec<Group> {
    let data: Dataset = serde_json::from_str(include_str!(
        "../../fixtures/agent/error_feedback_matrix.json"
    ))
    .unwrap();
    let observations = data
        .groups
        .iter()
        .map(|group| {
            let registered = match group.scope.as_str() {
                "delegation" => dataset()
                    .cases
                    .into_iter()
                    .filter(|case| case.live)
                    .map(|case| case.id)
                    .collect::<Vec<_>>(),
                "parallel" => parallel::policy_inventory(),
                "topology" => topology::policy_inventory(),
                other => panic!("unknown feedback scope {other}"),
            };
            json!({"scope":group.scope,"cases":group.cases,"registered":registered,
            "policy":"ContinueBatch","execution_state":"not_run","actual_model_acceptance":false})
        })
        .collect::<Vec<_>>();
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-feedback-plan-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("plan.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&observations).unwrap()).unwrap();
    println!("AGENT_FEEDBACK_PLAN={}", path.display());
    assert_eq!(data.schema_version, 1);
    for row in observations {
        assert_eq!(row["cases"], row["registered"], "{row}");
    }
    data.groups
}

async fn run_case(
    group: &Group,
    id: &str,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &super::super::tools::worker::WorkerRun,
) {
    let policy = ToolErrorPolicy::ContinueBatch;
    match group.scope.as_str() {
        "delegation" => {
            let case = dataset()
                .cases
                .into_iter()
                .find(|case| case.id == id)
                .unwrap();
            run_with_policy(case, model, live, installation, policy).await;
        }
        "parallel" => parallel::run_policy_case(id, model, live, installation, policy).await,
        "topology" => topology::run_policy_case(id, model, live, installation, policy).await,
        other => panic!("unknown feedback scope {other}"),
    }
}

#[test]
fn host_feedback_matrix_preserves_every_original_live_case() {
    assert_eq!(
        groups()
            .iter()
            .map(|group| group.cases.len())
            .sum::<usize>(),
        8
    );
}

#[tokio::test]
async fn host_feedback_delegation_topology_and_scheduling_offline_real_os() {
    let installation = super::super::tools::worker::WorkerRun::prepare().await;
    for group in groups() {
        let mut report = matrix::Matrix::new(
            group
                .cases
                .iter()
                .map(|id| format!("feedback/{}/{id}", group.scope)),
        );
        for (index, id) in group.cases.iter().enumerate() {
            report
                .run(
                    index,
                    run_case(
                        &group,
                        id,
                        ModelRef::new("fixture", "feedback"),
                        None,
                        &installation,
                    ),
                )
                .await;
        }
        assert!(
            report.complete(),
            "Feedback evidence: {}",
            report.directory.display()
        );
    }
}

#[tokio::test]
#[ignore = "Actual MiniMax over both protocols with explicit host error feedback; retains original strict cases"]
async fn actual_minimax_host_feedback_matrix() {
    let installation = super::super::tools::worker::WorkerRun::prepare().await;
    let mut failed = Vec::new();
    for group in groups() {
        let selected = crate::minimax_live::selected(&group.scope, group.cases.clone());
        let scope = group.scope.as_str();
        let mut report = matrix::Matrix::new(selected.iter().flat_map(|deployment| {
            group.cases.iter().map(move |id| {
                format!(
                    "minimax/feedback/{}/{}/{}/{id}",
                    scope, deployment.surface, deployment.model
                )
            })
        }));
        let mut index = 0;
        for deployment in selected {
            for id in &group.cases {
                report
                    .run(
                        index,
                        run_case(
                            &group,
                            id,
                            ModelRef::new(deployment.family, &deployment.model),
                            Some(deployment.build()),
                            &installation,
                        ),
                    )
                    .await;
                index += 1;
            }
        }
        if !report.complete() {
            failed.push(report.directory.clone());
        }
    }
    assert!(failed.is_empty(), "Actual feedback failures: {failed:?}");
}
