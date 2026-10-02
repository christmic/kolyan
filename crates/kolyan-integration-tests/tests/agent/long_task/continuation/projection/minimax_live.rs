//! MiniMax-only durable projection with unchanged physical and ledger assertions.
use super::*;
use crate::minimax_live::{plan, selected};
fn inventory() -> Vec<String> {
    crate::case().selectors
}
#[test]
fn minimax_selection_projection_inventory() {
    let cases = inventory();
    let selected = selected("projection", cases.clone());
    assert_eq!(plan("projection", &selected, &cases).rows.len(), 4);
}
#[tokio::test]
#[ignore = "Actual MiniMax projection only; requires explicit network authorization"]
async fn actual_minimax_projection_matrix() {
    let cases = inventory();
    let selected = selected("projection", cases.clone());
    let mut report = plan("projection", &selected, &cases);
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut index = 0;
    for deployment in selected {
        for selector in &cases {
            report
                .run(index, async {
                    let (model, provider) = deployment.build();
                    graph_run_case(selector, model, Some(provider), true, &installation).await;
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "MiniMax projection evidence: {}",
        report.directory.display()
    );
}
