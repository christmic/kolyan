//! Project-local deployment facts; no vendor/model branching in executable code.

use serde_json::{Value, json};

pub fn parameter_profile(provider: &str, protocol: &str, model: &str) -> String {
    let catalog: Value =
        serde_json::from_str(include_str!("../config/provider-parameters.json")).unwrap();
    catalog["bindings"][format!("{provider}/{protocol}/{model}")]
        .as_str()
        .unwrap_or("default")
        .into()
}

pub fn parameter_table(provider: &str, protocol: &str, model: &str) -> Value {
    let catalog: Value =
        serde_json::from_str(include_str!("../config/provider-parameters.json")).unwrap();
    let profile = parameter_profile(provider, protocol, model);
    let model_rules = catalog["profiles"][&profile].clone();
    assert!(
        model_rules.is_object(),
        "unregistered parameter profile: {profile}"
    );
    let mut table: Value =
        serde_json::from_str(include_str!("../config/request-parameters.json")).unwrap();
    table["provider"] = json!(provider);
    table["protocol"] = json!(protocol);
    table["models"] = json!({model: model_rules});
    table
}
