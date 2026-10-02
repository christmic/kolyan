//! MiniMax-only durable Continuation with unchanged physical and ledger assertions.
use super::*;
use crate::minimax_live::{plan, selected};
fn inventory() -> Vec<String> {
    crate::case().selectors
}
#[test]
fn minimax_selection_continuation_inventory() {
    let cases = inventory();
    let selected = selected("continuation", cases.clone());
    assert_eq!(plan("continuation", &selected, &cases).rows.len(), 4);
}
#[tokio::test]
#[ignore = "Actual MiniMax Continuation only; requires explicit network authorization"]
async fn actual_minimax_continuation_matrix() {
    let cases = inventory();
    let selected = selected("continuation", cases.clone());
    let mut report = plan("continuation", &selected, &cases);
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut index = 0;
    for deployment in selected {
        for selector in &cases {
            report
                .run(index, async {
                    let (model, provider) = deployment.build();
                    graph_run(selector, model, Some(provider), &installation).await;
                })
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "MiniMax Continuation evidence: {}",
        report.directory.display()
    );
}
