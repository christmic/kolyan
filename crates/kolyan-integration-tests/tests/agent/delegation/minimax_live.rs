//! MiniMax-only delegation using the exact historical cases and effect assertions.
use super::*;
use crate::minimax_live::{plan, selected};
fn inventory() -> Vec<String> {
    dataset()
        .cases
        .into_iter()
        .filter(|case| case.live)
        .collect::<Vec<_>>()
        .iter()
        .map(|case| case.id.clone())
        .collect()
}
#[test]
fn minimax_selection_delegation_inventory() {
    let cases = inventory();
    let selected = selected("delegation", cases.clone());
    assert_eq!(plan("delegation", &selected, &cases).rows.len(), 8);
}
#[tokio::test]
#[ignore = "Actual MiniMax delegation only; requires explicit network authorization"]
async fn actual_minimax_delegation_matrix() {
    let cases = dataset()
        .cases
        .into_iter()
        .filter(|case| case.live)
        .collect::<Vec<_>>();
    let ids = inventory();
    let selected = selected("delegation", ids.clone());
    let mut report = plan("delegation", &selected, &ids);
    let installation = crate::tools::worker::WorkerRun::prepare().await;
    let mut index = 0;
    for deployment in selected {
        for case in &cases {
            report
                .run(index, async {
                    run(
                        case.clone(),
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
        "MiniMax delegation evidence: {}",
        report.directory.display()
    );
}
