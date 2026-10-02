//! MiniMax-only role-aware finalization: full and explicit projection remain separate scenes.
use super::*;
use crate::minimax_live::{plan, selected};
fn inventory() -> Vec<String> {
    let data = fixture();
    data.selections
        .iter()
        .flat_map(|selection| {
            data.selectors
                .iter()
                .map(move |selector| format!("{selection}/{selector}"))
        })
        .collect()
}
#[test]
fn minimax_selection_finalization_inventory() {
    let cases = inventory();
    let selected = selected("finalization", cases.clone());
    assert_eq!(plan("finalization", &selected, &cases).rows.len(), 8);
}
#[tokio::test]
#[ignore = "Actual MiniMax Runner finalization only; requires explicit network authorization"]
async fn actual_minimax_finalization_matrices() {
    let cases = inventory();
    let selected = selected("finalization", cases.clone());
    let mut report = plan("finalization", &selected, &cases);
    let installation = tools::worker::WorkerRun::prepare().await;
    let data = fixture();
    let mut index = 0;
    for deployment in selected {
        for selection in &data.selections {
            let projection = match selection.as_str() {
                "full" => false,
                "explicit_projection" => true,
                other => panic!("unvalidated finalization selection: {other}"),
            };
            for selector in &data.selectors {
                report
                    .run(index, async {
                        let (model, provider) = deployment.build();
                        run(selector, model, Some(provider), projection, &installation).await;
                    })
                    .await;
                index += 1;
            }
        }
    }
    assert!(
        report.complete(),
        "MiniMax finalization evidence: {}",
        report.directory.display()
    );
}
