//! Pure, explicit request planning. No configuration loading, HTTP or model-name heuristics.

mod rules;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    ContentBlock, ModelFeature, ModelFeatures, ModelRequest, PromptCacheConfig, ProviderError,
    ProviderErrorKind, ProviderErrorPhase, ReasoningConfig, ToolChoice,
};

const PARAMETERS: &[&str] = &[
    "reasoning.effort",
    "reasoning.budget_tokens",
    "prompt_cache.key",
    "prompt_cache.retention",
    "prompt_cache.breakpoints",
    "max_output_tokens",
    "tool_choice",
];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterSupport {
    Supported,
    Unsupported,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterRule {
    pub support: ParameterSupport,
    #[serde(default = "empty_schema")]
    pub schema: Value,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(default = "yes")]
    pub omittable: bool,
    #[serde(default)]
    pub wire_name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelParameterOverrides {
    #[serde(default)]
    pub features: Option<ModelFeatures>,
    #[serde(default)]
    pub parameters: BTreeMap<String, ParameterRule>,
}

/// A table belongs to one configured endpoint instance; credentials never belong here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterTable {
    pub provider: String,
    pub protocol: String,
    #[serde(default)]
    pub features: ModelFeatures,
    #[serde(default)]
    pub defaults: BTreeMap<String, ParameterRule>,
    pub models: BTreeMap<String, ModelParameterOverrides>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterAction {
    Sent,
    Defaulted,
    OmittedUnsupported,
    OmittedUnknown,
    MappedToAuto,
}

/// Deliberately excludes values, schemas and defaults, which may contain sensitive data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParameterDecision {
    pub parameter: String,
    pub action: ParameterAction,
}

#[derive(Debug, Clone)]
pub struct PlannedRequest {
    pub request: ModelRequest,
    pub wire_extensions: Map<String, Value>,
    pub decisions: Vec<ParameterDecision>,
}

impl PlannedRequest {
    /// Raw adapters have no extension registry and must not silently discard extensions.
    pub fn unconfigured(request: ModelRequest) -> Result<Self, ProviderError> {
        if !request.extensions.is_null()
            && request.extensions.as_object().is_none_or(|v| !v.is_empty())
        {
            return Err(invalid("extensions require an explicit parameter table"));
        }
        Ok(Self {
            request,
            wire_extensions: Map::new(),
            decisions: vec![],
        })
    }

    pub fn omitted(&self, parameter: &str) -> bool {
        self.decisions.iter().any(|d| {
            d.parameter == parameter
                && matches!(
                    d.action,
                    ParameterAction::OmittedUnsupported | ParameterAction::OmittedUnknown
                )
        })
    }
}

#[derive(Debug, Clone)]
pub struct RequestPlanner {
    table: ParameterTable,
}

impl RequestPlanner {
    /// Validate all rules before any request. Reserved wire keys come from the adapter.
    pub fn new(
        table: ParameterTable,
        protocol: &str,
        reserved: &[&str],
    ) -> Result<Self, ProviderError> {
        rules::validate(&table, protocol, reserved)?;
        Ok(Self { table })
    }

    /// Borrow the original and return a separate effective request and value-free audit.
    pub fn plan(&self, original: &ModelRequest) -> Result<PlannedRequest, ProviderError> {
        if original.model.provider != self.table.provider {
            return Err(invalid("request provider does not match parameter table"));
        }
        let model = self
            .table
            .models
            .get(&original.model.model)
            .ok_or_else(|| unsupported("model is not registered in parameter table"))?;
        let features = model.features.as_ref().unwrap_or(&self.table.features);
        check_features(original, features)?;
        let mut rules = self.table.defaults.clone();
        rules.extend(model.parameters.clone());
        let mut inputs = parameters(original)?;
        for key in rules.keys() {
            inputs.entry(key.clone()).or_insert(Value::Null);
        }
        let mut values = BTreeMap::new();
        let mut wire_extensions = Map::new();
        let mut decisions = Vec::new();
        for (key, mut input) in inputs {
            let rule = rules.get(&key);
            let compatible_choice =
                key == "tool_choice" && original.tool_choice == ToolChoice::Required;
            let mut mapped_to_auto = false;
            if compatible_choice
                && let Some(rule) = rule
                && rule.support == ParameterSupport::Supported
                && rules::validate_value(&rule.schema, &input, &key).is_err()
            {
                let auto = serde_json::json!("auto");
                if rules::validate_value(&rule.schema, &auto, &key).is_ok() {
                    input = auto;
                    mapped_to_auto = true;
                } else if rule.omittable {
                    decisions.push(ParameterDecision {
                        parameter: key,
                        action: ParameterAction::OmittedUnsupported,
                    });
                    continue;
                } else {
                    return Err(unsupported(
                        "no supported tool_choice value; omission forbidden",
                    ));
                }
            }
            let provided = !input.is_null()
                || key
                    .strip_prefix("extensions.")
                    .is_some_and(|key| original.extensions.get(key).is_some());
            if key.starts_with("extensions.") && rule.is_none() {
                return Err(invalid("unregistered extension parameter"));
            }
            let support = rule.map(|r| r.support).unwrap_or_default();
            let default = rule.and_then(|r| r.default.as_ref());
            if !provided && (support != ParameterSupport::Supported || default.is_none()) {
                continue;
            }
            if support != ParameterSupport::Supported {
                if compatible_choice && support == ParameterSupport::Unknown {
                    return Err(unsupported(
                        "required tool_choice support is unknown; declare model capability",
                    ));
                }
                if rule.is_some_and(|r| !r.omittable)
                    || (key == "tool_choice"
                        && original.tool_choice != ToolChoice::Auto
                        && !compatible_choice)
                {
                    return Err(unsupported(format!(
                        "required parameter unavailable: {key}"
                    )));
                }
                decisions.push(ParameterDecision {
                    parameter: key,
                    action: if support == ParameterSupport::Unsupported {
                        ParameterAction::OmittedUnsupported
                    } else {
                        ParameterAction::OmittedUnknown
                    },
                });
                continue;
            }
            let rule = rule.expect("supported status requires a rule");
            let action = if mapped_to_auto {
                ParameterAction::MappedToAuto
            } else if !provided {
                ParameterAction::Defaulted
            } else {
                ParameterAction::Sent
            };
            let value = if !provided {
                default.cloned().expect("default checked above")
            } else {
                input
            };
            rules::validate_value(&rule.schema, &value, &key)?;
            if key.starts_with("extensions.") {
                wire_extensions.insert(
                    rule.wire_name
                        .clone()
                        .expect("binding validated at construction"),
                    value.clone(),
                );
            }
            decisions.push(ParameterDecision {
                parameter: key.clone(),
                action,
            });
            values.insert(key, value);
        }
        let mut request = original.clone();
        request.reasoning = {
            let effort = take(&mut values, "reasoning.effort")?;
            let budget_tokens = take(&mut values, "reasoning.budget_tokens")?;
            (effort.is_some() || budget_tokens.is_some()).then_some(ReasoningConfig {
                effort,
                budget_tokens,
            })
        };
        request.prompt_cache = {
            let key = take(&mut values, "prompt_cache.key")?;
            let retention = take(&mut values, "prompt_cache.retention")?;
            let breakpoints: Vec<_> =
                take(&mut values, "prompt_cache.breakpoints")?.unwrap_or_default();
            (key.is_some() || retention.is_some() || !breakpoints.is_empty()).then_some(
                PromptCacheConfig {
                    key,
                    retention,
                    breakpoints,
                },
            )
        };
        request.max_output_tokens = take(&mut values, "max_output_tokens")?;
        request.tool_choice = take(&mut values, "tool_choice")?.unwrap_or(ToolChoice::Auto);
        request.extensions = Value::Object(
            values
                .into_iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix("extensions.")
                        .map(|key| (key.to_owned(), value))
                })
                .collect(),
        );
        Ok(PlannedRequest {
            request,
            wire_extensions,
            decisions,
        })
    }
}

fn check_features(request: &ModelRequest, features: &ModelFeatures) -> Result<(), ProviderError> {
    let mut required = vec![
        ModelFeature::Streaming,
        ModelFeature::TextInput,
        ModelFeature::TextOutput,
    ];
    if request.output_format.is_some() {
        required.push(ModelFeature::StructuredOutput);
    }
    if !request.tools.is_empty()
        || matches!(
            request.tool_choice,
            ToolChoice::Required | ToolChoice::Tool(_)
        )
    {
        required.push(ModelFeature::ToolUse);
    }
    for block in request.messages.iter().flat_map(|message| &message.content) {
        match block {
            ContentBlock::Image { .. } => required.push(ModelFeature::ImageInput),
            ContentBlock::Document { .. } => required.push(ModelFeature::DocumentInput),
            ContentBlock::ToolCall { .. } | ContentBlock::ToolResult { .. } => {
                required.push(ModelFeature::ToolUse)
            }
            _ => {}
        }
    }
    if let Some(feature) = required
        .into_iter()
        .find(|feature| !features.contains(feature))
    {
        return Err(unsupported(format!(
            "required model feature unavailable: {feature:?}"
        )));
    }
    Ok(())
}

fn parameters(request: &ModelRequest) -> Result<BTreeMap<String, Value>, ProviderError> {
    let body =
        serde_json::to_value(request).map_err(|_| invalid("request cannot be serialized"))?;
    let mut inputs = BTreeMap::new();
    for key in PARAMETERS {
        let value = body
            .pointer(&format!("/{}", key.replace('.', "/")))
            .cloned()
            .unwrap_or_default();
        inputs.insert((*key).to_owned(), value);
    }
    if let Value::Object(extensions) = &request.extensions {
        for (key, value) in extensions {
            inputs.insert(format!("extensions.{key}"), value.clone());
        }
    } else if !request.extensions.is_null() {
        return Err(invalid("extensions must be a namespaced object"));
    }
    Ok(inputs)
}

fn take<T: serde::de::DeserializeOwned>(
    values: &mut BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<T>, ProviderError> {
    values
        .remove(key)
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|_| invalid(format!("invalid neutral parameter type: {key}")))
        })
        .transpose()
}
fn invalid(message: impl Into<String>) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        ProviderErrorPhase::Open,
        message,
    )
}
fn unsupported(message: impl Into<String>) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Unsupported,
        ProviderErrorPhase::Open,
        message,
    )
}
fn empty_schema() -> Value {
    serde_json::json!({})
}
fn yes() -> bool {
    true
}

#[cfg(test)]
mod tests;
