//! Input-only overlay; existing approval reconstruction and comparisons remain.

use super::*;

pub(in crate::delegation) async fn run(
    candidate: Value,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &tools::worker::WorkerRun,
) {
    let case: Case = serde_json::from_value(candidate).unwrap();
    run_with_policy(
        case,
        model,
        live,
        installation,
        kolyan_core::ToolErrorPolicy::ContinueBatch,
    )
    .await;
}
