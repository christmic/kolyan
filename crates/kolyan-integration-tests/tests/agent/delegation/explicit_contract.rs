//! Separate task-contract revision, not a replacement for historical matrices.
//! Only identity and task text change. Live Providers never receive scripted calls.

mod tests;

use std::collections::BTreeSet;

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::*;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Contract {
    schema_version: u32,
    revision: String,
    cases: Vec<ContractCase>,
    guard_cases: Vec<GuardCase>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ContractCase {
    case_id: String,
    baseline_scope: String,
    baseline_case_id: String,
    allowed_fields: Vec<String>,
    baseline_data_digest: String,
    new_input_digest: String,
    input: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GuardCase {
    id: String,
    pointer: String,
    value: Value,
    accepted: bool,
}

fn contract() -> Contract {
    serde_json::from_str(include_str!("../../fixtures/agent/explicit_contract.json")).unwrap()
}

fn baseline(scope: &str) -> (&'static str, Value) {
    let text = match scope {
        "delegation" => include_str!("../../fixtures/agent/delegation.json"),
        "parallel" => include_str!("../../fixtures/agent/parallel_runtime.json"),
        "topology" => include_str!("../../fixtures/agent/topology.json"),
        other => panic!("unknown baseline scope {other}"),
    };
    (text, serde_json::from_str(text).unwrap())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn overlay(spec: &ContractCase, original: &Value) -> Value {
    let mut candidate = original.clone();
    candidate["id"] = json!(spec.case_id);
    candidate[input_field(&spec.baseline_scope)] = json!(spec.input);
    candidate
}

fn input_field(scope: &str) -> &'static str {
    if scope == "delegation" {
        "parent_input"
    } else {
        "input"
    }
}

fn validate_overlay(
    spec: &ContractCase,
    bytes: &str,
    original: &Value,
    candidate: &Value,
) -> Result<(), String> {
    let field = input_field(&spec.baseline_scope);
    if spec.allowed_fields != ["id", field] || spec.case_id == spec.baseline_case_id {
        return Err("overlay identity or allowed fields differ from contract".into());
    }
    if sha256(bytes.as_bytes()) != spec.baseline_data_digest
        || sha256(spec.input.as_bytes()) != spec.new_input_digest
    {
        return Err("baseline or task input digest mismatch".into());
    }
    if original["id"] != spec.baseline_case_id || candidate != &overlay(spec, original) {
        return Err("candidate changed a field outside the declared task overlay".into());
    }
    Ok(())
}

/// Persist the complete baseline and derived case before checking any row.
fn prepared_cases() -> Vec<(ContractCase, Value)> {
    let data = contract();
    let observations: Vec<Value> = data.cases.iter().map(|spec| {
        let (bytes, full) = baseline(&spec.baseline_scope);
        let cases = if spec.baseline_scope == "topology" { full.as_array().unwrap() } else { full["cases"].as_array().unwrap() };
        let original = cases.iter().find(|case| case["id"] == spec.baseline_case_id).unwrap();
        let candidate = overlay(spec, original);
        json!({"schema_version":data.schema_version,"revision":data.revision,
            "spec":spec,
            "baseline_full":full,"baseline_case":original,"candidate":candidate,
            "baseline_data_digest":sha256(bytes.as_bytes()),"new_input_digest":sha256(spec.input.as_bytes()),
            "validation":validate_overlay(spec,bytes,original,&candidate),"policy":"ContinueBatch",
            "global_instructions":"unchanged","live_calls":"model_generated_only"})
    }).collect();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-explicit-contract-plan-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("plan.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&observations).unwrap()).unwrap();
    println!("EXPLICIT_CONTRACT_PLAN={}", path.display());
    let reloaded: Vec<Value> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(data.schema_version, 1);
    assert_eq!(data.revision, "explicit-contract-v1");
    assert_eq!(reloaded.len(), 8);
    let mut identities = BTreeSet::new();
    for row in &reloaded {
        assert_eq!(row["validation"], json!({"Ok":null}), "{row}");
        assert!(identities.insert(row["spec"]["case_id"].as_str().unwrap()));
    }
    for scope in ["delegation", "parallel", "topology"] {
        let (_, full) = baseline(scope);
        let original = if scope == "topology" {
            full.as_array().unwrap()
        } else {
            full["cases"].as_array().unwrap()
        };
        let expected: Vec<&str> = original
            .iter()
            .filter(|row| scope != "delegation" || row["live"] == true)
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        let observed: Vec<&str> = reloaded
            .iter()
            .filter(|row| row["spec"]["baseline_scope"] == scope)
            .map(|row| row["spec"]["baseline_case_id"].as_str().unwrap())
            .collect();
        assert_eq!(observed, expected, "exported {scope} inventory");
    }
    data.cases
        .into_iter()
        .zip(reloaded)
        .map(|(spec, row)| (spec, row["candidate"].clone()))
        .collect()
}

fn live_labels(selected: &[crate::Deployment], cases: &[(ContractCase, Value)]) -> Vec<String> {
    selected
        .iter()
        .flat_map(|deployment| {
            cases.iter().map(move |(spec, _)| {
                format!(
                    "minimax/explicit-contract-v1/{}/{}/{}",
                    deployment.surface, deployment.model, spec.case_id
                )
            })
        })
        .collect()
}

async fn run_case(
    spec: &ContractCase,
    candidate: Value,
    model: ModelRef,
    live: Option<Arc<dyn ModelProvider>>,
    installation: &super::super::tools::worker::WorkerRun,
) {
    match spec.baseline_scope.as_str() {
        "delegation" => {
            run_with_policy(
                serde_json::from_value(candidate).unwrap(),
                model,
                live,
                installation,
                kolyan_core::ToolErrorPolicy::ContinueBatch,
            )
            .await
        }
        "parallel" => parallel::explicit_contract::run(candidate, model, live, installation).await,
        "topology" => topology::explicit_contract::run(candidate, model, live, installation).await,
        other => panic!("unknown scope {other}"),
    }
}

#[tokio::test]
#[ignore = "Actual MiniMax explicit-contract-v1 only; separate authorization required"]
async fn actual_minimax_explicit_contract_matrix() {
    let cases = prepared_cases();
    let selected = crate::minimax_live::selected(
        "delegation",
        dataset()
            .cases
            .into_iter()
            .filter(|case| case.live)
            .map(|case| case.id)
            .collect(),
    );
    let mut report = matrix::Matrix::new(live_labels(&selected, &cases));
    let installation = super::super::tools::worker::WorkerRun::prepare().await;
    let mut index = 0;
    for deployment in selected {
        for (spec, candidate) in &cases {
            report
                .run(
                    index,
                    run_case(
                        spec,
                        candidate.clone(),
                        ModelRef::new(deployment.family, &deployment.model),
                        Some(deployment.build()),
                        &installation,
                    ),
                )
                .await;
            index += 1;
        }
    }
    assert!(
        report.complete(),
        "Explicit contract evidence: {}",
        report.directory.display()
    );
}
