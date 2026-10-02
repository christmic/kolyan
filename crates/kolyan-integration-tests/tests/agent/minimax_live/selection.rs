//! Selection evidence only; it never builds Providers, changes cases or retries rows.
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Identity {
    pub family: String,
    pub surface: String,
    pub model: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Contract {
    schema_version: u32,
    family: String,
    surfaces: Vec<String>,
    scopes: BTreeMap<String, Vec<String>>,
    selection_cases: Vec<SelectionCase>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectionCase {
    id: String,
    entries: Vec<[String; 3]>,
    accepted: bool,
    scope: Option<String>,
    cases: Option<Vec<String>>,
}
fn contract() -> Contract {
    serde_json::from_str(include_str!("../../fixtures/agent/minimax_live.json")).unwrap()
}
fn validate(scope: &str, configured: &[Identity], cases: &[String]) -> Result<(), String> {
    let data = contract();
    if data.schema_version != 1 || data.family != "minimax" {
        return Err("invalid selection contract".into());
    }
    let expected = data.scopes.get(scope).ok_or("unknown capability scope")?;
    if cases != expected {
        return Err("existing case inventory differs from selection contract".into());
    }
    let selected: Vec<_> = configured
        .iter()
        .filter(|entry| entry.family == data.family)
        .collect();
    let surfaces: BTreeSet<_> = selected
        .iter()
        .map(|entry| entry.surface.as_str())
        .collect();
    let expected_surfaces: BTreeSet<_> = data.surfaces.iter().map(String::as_str).collect();
    if selected.len() != expected_surfaces.len() || surfaces != expected_surfaces {
        return Err(
            "selection requires exactly one configured MiniMax deployment per protocol".into(),
        );
    }
    if selected.iter().any(|entry| entry.model.is_empty()) {
        return Err("empty configured model".into());
    }
    Ok(())
}
pub(super) fn select<T>(
    scope: &str,
    configured: Vec<T>,
    describe: impl Fn(&T) -> Identity,
    cases: Vec<String>,
) -> Vec<T> {
    let inventory: Vec<_> = configured.iter().map(&describe).collect();
    let result = validate(scope, &inventory, &cases);
    let selected: Vec<_> = inventory
        .iter()
        .filter(|entry| entry.family == "minimax")
        .collect();
    let labels: Vec<_> = selected
        .iter()
        .flat_map(|entry| {
            cases.iter().map(move |case| {
                format!("minimax/{scope}/{}/{}/{case}", entry.surface, entry.model)
            })
        })
        .collect();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-minimax-selection-")
        .tempdir()
        .unwrap()
        .keep();
    std::fs::write(
        directory.join("selection.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version":1, "event":"minimax_selection", "scope":scope,
            "configured":inventory, "selected":selected, "cases":cases,
            "planned_labels":labels, "validation":result,
            "execution_state":"not_run", "attempts":0, "actual_model_acceptance":false
        }))
        .unwrap(),
    )
    .unwrap();
    println!(
        "MINIMAX_SELECTION={}",
        directory.join("selection.json").display()
    );
    result.expect("MiniMax selection must match the exported capability contract");
    configured
        .into_iter()
        .filter(|entry| describe(entry).family == "minimax")
        .collect()
}
#[test]
fn minimax_selection_data_rejects_missing_duplicate_and_foreign_protocols() {
    let data = contract();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-minimax-selection-tests-")
        .tempdir()
        .unwrap()
        .keep();
    let observations: Vec<_> = data.selection_cases.iter().map(|case| {
        let entries: Vec<_> = case.entries.iter().map(|[family,surface,model]| Identity {
            family:family.clone(), surface:surface.clone(), model:model.clone()
        }).collect();
            let scope = case.scope.as_deref().unwrap_or("root");
            let cases = case.cases.as_ref().unwrap_or(&data.scopes["root"]);
            let result = validate(scope, &entries, cases);
            json!({"id":case.id,"scope":scope,"cases":cases,"configured":entries,"result":result,"accepted":result.is_ok(),"expected":case.accepted})
    }).collect();
    std::fs::write(
        directory.join("actual.json"),
        serde_json::to_vec_pretty(&observations).unwrap(),
    )
    .unwrap();
    println!(
        "MINIMAX_SELECTION_TESTS={}",
        directory.join("actual.json").display()
    );
    for row in observations {
        assert_eq!(row["accepted"], row["expected"], "{row}");
    }
}
