//! MiniMax-only ten-Turn acceptance using the unchanged long-task runner.
#[path = "../minimax_live/selection.rs"]
mod selection;
use super::*;

pub(super) fn selected(scope: &str, cases: Vec<String>) -> Vec<Deployment> {
    selection::select(
        scope,
        deployments(),
        |entry| match entry {
            Deployment::OpenAi(family, _, model) => selection::Identity {
                family: (*family).into(),
                surface: "openai_compat".into(),
                model: model.clone(),
            },
            Deployment::Anthropic(family, _, model) => selection::Identity {
                family: (*family).into(),
                surface: "anthropic_compat".into(),
                model: model.clone(),
            },
        },
        cases,
    )
}
pub(super) fn plan(scope: &str, selected: &[Deployment], cases: &[String]) -> matrix::Matrix {
    matrix::Matrix::new(selected.iter().flat_map(|entry| {
        cases
            .iter()
            .map(move |case| format!("minimax/{scope}/{}/{case}", entry.label()))
    }))
}
#[test]
fn minimax_selection_long_task_inventory() {
    let cases = case().selectors;
    let selected = selected("long_task", cases.clone());
    assert_eq!(plan("long_task", &selected, &cases).rows.len(), 4);
}
#[tokio::test]
#[ignore = "Actual MiniMax ten-Turn tasks only; requires explicit network authorization"]
async fn actual_minimax_long_task_matrix() {
    let cases = case().selectors;
    let selected = selected("long_task", cases.clone());
    let mut report = plan("long_task", &selected, &cases);
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut index = 0;
    for deployment in selected {
        for selector in &cases {
            report
                .run(index, async {
                    let (model, provider) = deployment.build();
                    run(case(), selector, model, Some(provider), &installation).await;
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "MiniMax long-task evidence: {}",
        report.directory.display()
    );
}
