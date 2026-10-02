//! MiniMax-only parallel using the exact historical cases and effect assertions.
use super::*;
use crate::minimax_live::{plan, selected};
fn inventory() -> Vec<String> {
    dataset().cases.iter().map(|case| case.id.clone()).collect()
}
#[test]
fn minimax_selection_parallel_inventory() {
    let cases = inventory();
    let selected = selected("parallel", cases.clone());
    assert_eq!(plan("parallel", &selected, &cases).rows.len(), 4);
}
#[tokio::test]
#[ignore = "Actual MiniMax parallel only; requires explicit network authorization"]
async fn actual_minimax_parallel_matrix() {
    let cases = dataset().cases;
    let ids = inventory();
    let selected = selected("parallel", ids.clone());
    let mut report = plan("parallel", &selected, &ids);
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
        "MiniMax parallel evidence: {}",
        report.directory.display()
    );
}
