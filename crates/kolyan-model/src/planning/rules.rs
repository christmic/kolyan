//! Configuration validation stays separate from per-request planning.

use std::collections::BTreeSet;

use serde_json::Value;

use super::{PARAMETERS, ParameterSupport, ParameterTable, invalid};
use crate::{ModelFeature, ProviderError};

pub(super) fn validate(
    table: &ParameterTable,
    protocol: &str,
    reserved: &[&str],
) -> Result<(), ProviderError> {
    if table.provider.is_empty() || table.protocol != protocol || table.models.is_empty() {
        return Err(invalid(
            "invalid parameter table identity, protocol or model registry",
        ));
    }
    for (key, rule) in table
        .defaults
        .iter()
        .chain(table.models.values().flat_map(|m| &m.parameters))
    {
        if let Some(extension) = key.strip_prefix("extensions.") {
            let Some((namespace, name)) = extension.split_once('.') else {
                return Err(invalid("extension requires a namespace and name"));
            };
            let Some(wire) = &rule.wire_name else {
                return Err(invalid("extension requires wire_name"));
            };
            if namespace.is_empty()
                || name.is_empty()
                || wire.is_empty()
                || !wire.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                || reserved.contains(&wire.as_str())
            {
                return Err(invalid("invalid or reserved extension binding"));
            }
        } else if !PARAMETERS.contains(&key.as_str()) || rule.wire_name.is_some() {
            return Err(invalid("unrecognized neutral parameter or binding"));
        }
        jsonschema::validator_for(&rule.schema).map_err(|_| invalid("invalid parameter schema"))?;
        if let Some(default) = &rule.default {
            validate_value(&rule.schema, default, key)?;
        }
    }
    for (name, model) in &table.models {
        if name.is_empty() {
            return Err(invalid("model identity cannot be empty"));
        }
        let mut effective = table.defaults.clone();
        effective.extend(model.parameters.clone());
        let features = model.features.as_ref().unwrap_or(&table.features);
        for (key, rule) in &effective {
            let feature = if key.starts_with("reasoning.") {
                Some(ModelFeature::Reasoning)
            } else if key.starts_with("prompt_cache.") {
                Some(ModelFeature::PromptCaching)
            } else {
                None
            };
            if rule.support == ParameterSupport::Supported
                && feature.is_some_and(|f| !features.contains(&f))
            {
                return Err(invalid("supported parameter conflicts with model features"));
            }
        }
        let mut bindings = BTreeSet::new();
        for rule in effective.values() {
            if let Some(wire) = &rule.wire_name
                && rule.support == ParameterSupport::Supported
                && !bindings.insert(wire)
            {
                return Err(invalid("duplicate extension wire binding"));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_value(
    schema: &Value,
    value: &Value,
    key: &str,
) -> Result<(), ProviderError> {
    let validator =
        jsonschema::validator_for(schema).map_err(|_| invalid("invalid parameter schema"))?;
    if !validator.is_valid(value) {
        return Err(invalid(format!(
            "parameter violates configured schema: {key}"
        )));
    }
    Ok(())
}
