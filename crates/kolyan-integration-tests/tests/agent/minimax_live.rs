//! Independent MiniMax roots; historical full-provider entries remain unchanged.
mod retry;
mod selection;
use super::*;

pub(super) fn selected(scope: &str, cases: Vec<String>) -> Vec<Deployment> {
    selection::select(
        scope,
        deployments(),
        |entry| selection::Identity {
            family: entry.family.into(),
            surface: entry.surface.into(),
            model: entry.model.clone(),
        },
        cases,
    )
}
pub(super) fn plan(scope: &str, selected: &[Deployment], cases: &[String]) -> matrix::Matrix {
    matrix::Matrix::new(selected.iter().flat_map(|entry| {
        cases
            .iter()
            .map(move |case| format!("minimax/{scope}/{}/{}/{case}", entry.surface, entry.model))
    }))
}
#[test]
fn minimax_selection_root_and_restart_inventory() {
    for scope in ["root", "approval_restart"] {
        let cases = data::dataset().selectors;
        let selected = selected(scope, cases.clone());
        assert_eq!(plan(scope, &selected, &cases).rows.len(), 4);
    }
}
#[tokio::test]
#[ignore = "Actual MiniMax roots only; requires explicit network authorization"]
async fn actual_minimax_root_matrix() {
    let dataset = data::dataset();
    let selected = selected("root", dataset.selectors.clone());
    let mut report = plan("root", &selected, &dataset.selectors);
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut index = 0;
    for deployment in selected {
        for selector in &dataset.selectors {
            report
                .run(index, async {
                    harness::run(
                        dataset.clone(),
                        selector,
                        ModelRef::new(deployment.family, &deployment.model),
                        Some(deployment.build()),
                        &installation,
                    )
                    .await;
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "MiniMax root evidence: {}",
        report.directory.display()
    );
}
#[tokio::test]
#[ignore = "Actual MiniMax approval reconstruction only; requires explicit network authorization"]
async fn actual_minimax_approval_restart_matrix() {
    let cases = data::dataset().selectors;
    let selected = selected("approval_restart", cases.clone());
    let mut report = plan("approval_restart", &selected, &cases);
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut index = 0;
    for deployment in selected {
        for selector in &cases {
            report
                .run(index, async {
                    restart::run(
                        selector,
                        ModelRef::new(deployment.family, &deployment.model),
                        Some(deployment.build()),
                        &installation,
                    )
                    .await;
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "MiniMax restart evidence: {}",
        report.directory.display()
    );
}
