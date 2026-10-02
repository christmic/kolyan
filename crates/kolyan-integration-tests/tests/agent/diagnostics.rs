//! A selected failed deployment uses the same requests, real OS tools and
//! assertions as full acceptance. Passing this diagnostic never proves a matrix.

use kolyan_model::ModelRef;
use serde::Deserialize;

use super::{data, deployments, harness, matrix, restart};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticCase {
    schema_version: u32,
    family: String,
    surface: String,
    model: String,
    selector: String,
}

#[tokio::test]
#[ignore = "actual model and real isolated tools; requires the selected deployment credential"]
async fn actual_model_tool_timeout_diagnostic() {
    let case: DiagnosticCase =
        serde_json::from_str(include_str!("../fixtures/agent/timeout_diagnostic.json")).unwrap();
    assert_eq!(case.schema_version, 1);
    let dataset = data::dataset();
    assert!(dataset.selectors.contains(&case.selector));
    let mut selected = deployments().into_iter().filter(|deployment| {
        deployment.family == case.family
            && deployment.surface == case.surface
            && deployment.model == case.model
    });
    let deployment = selected
        .next()
        .expect("diagnostic deployment must exist in the configured matrix");
    assert!(
        selected.next().is_none(),
        "diagnostic deployment must be unique"
    );
    let installation = super::tools::worker::WorkerRun::prepare().await;
    let provider = deployment.build();
    harness::run(
        dataset,
        &case.selector,
        ModelRef::new(deployment.family, &deployment.model),
        Some(provider),
        &installation,
    )
    .await;
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Flow {
    Root,
    ApprovalRestart,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservedFailureCase {
    id: String,
    flow: Flow,
    deployment: DiagnosticCase,
}

fn observed_failures() -> Vec<ObservedFailureCase> {
    serde_json::from_str(include_str!(
        "../fixtures/agent/observed_failures_diagnostic.json"
    ))
    .unwrap()
}

#[test]
fn observed_failure_cases_select_unique_configured_deployments_without_credentials() {
    let cases = observed_failures();
    assert!(!cases.is_empty(), "diagnostic plan must not be empty");
    let dataset = data::dataset();
    let configured = deployments();
    let mut ids = std::collections::BTreeSet::new();
    for case in cases {
        assert!(!case.id.is_empty());
        assert!(ids.insert(case.id), "duplicate diagnostic case ID");
        let selected = case.deployment;
        assert_eq!(selected.schema_version, 1);
        assert!(dataset.selectors.contains(&selected.selector));
        assert_eq!(
            configured
                .iter()
                .filter(|deployment| {
                    deployment.family == selected.family
                        && deployment.surface == selected.surface
                        && deployment.model == selected.model
                })
                .count(),
            1,
            "diagnostic selector must resolve to exactly one configured deployment"
        );
    }
}

#[tokio::test]
#[ignore = "fresh isolated reproductions of recorded actual-model failures; not retries or matrix acceptance"]
async fn actual_model_observed_failures_diagnostic() {
    let cases = observed_failures();
    let installation = super::tools::worker::WorkerRun::prepare().await;
    let mut report = matrix::Matrix::new(cases.iter().map(|case| case.id.clone()));
    for (index, case) in cases.into_iter().enumerate() {
        report
            .run(index, async {
                let selected = case.deployment;
                assert_eq!(selected.schema_version, 1);
                let dataset = data::dataset();
                assert!(dataset.selectors.contains(&selected.selector));
                let matches: Vec<_> = deployments()
                    .into_iter()
                    .filter(|deployment| {
                        deployment.family == selected.family
                            && deployment.surface == selected.surface
                            && deployment.model == selected.model
                    })
                    .collect();
                assert_eq!(matches.len(), 1, "diagnostic deployment must be unique");
                let deployment = matches.into_iter().next().unwrap();
                let model = ModelRef::new(deployment.family, &deployment.model);
                let provider = Some(deployment.build());
                // Each framework creates fresh stores and a new workspace. Never
                // recover or replay effects from the failed execution's evidence.
                match case.flow {
                    Flow::Root => {
                        harness::run(dataset, &selected.selector, model, provider, &installation)
                            .await
                    }
                    Flow::ApprovalRestart => {
                        restart::run(&selected.selector, model, provider, &installation).await
                    }
                }
            })
            .await;
    }
    assert!(
        report.complete(),
        "diagnostic evidence: {}",
        report.directory.display()
    );
}
