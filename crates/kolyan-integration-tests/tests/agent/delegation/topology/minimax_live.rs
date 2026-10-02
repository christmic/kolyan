//! Preserve both recursive-self and multiple-child-approval scenarios.
use super::*;
use crate::minimax_live::selected;
fn inventory() -> Vec<String> {
    cases().iter().map(|case| case.id.clone()).collect()
}
#[test]
fn minimax_selection_topology_inventory() {
    let cases = inventory();
    let selected = selected("topology", cases.clone());
    assert_eq!(
        crate::minimax_live::plan("topology", &selected, &cases)
            .rows
            .len(),
        4
    );
}
#[tokio::test]
#[ignore = "Actual MiniMax recursive and child-approval tasks only; requires explicit network authorization"]
async fn actual_minimax_topology_matrix() {
    let selected = selected("topology", inventory());
    execute(Some(selected)).await;
}
