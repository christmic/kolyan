//! Strict persisted DTOs; Model's opaque mapping/accounting types stay opaque.

use kolyan_core::{StepResult, ToolDispatchPolicy};
use kolyan_ledger::FactRef;
use kolyan_model::{ModelRef, ModelRequest};
use serde::{Deserialize, Deserializer, Serialize};

use super::ModelOpeningEventRef;
use crate::ExecutionKey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelOpeningProtocol {
    OpenAiResponses,
    AnthropicMessages,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOpeningMappingIdentity {
    pub endpoint_id: String,
    pub protocol: ModelOpeningProtocol,
    #[serde(deserialize_with = "strict_value")]
    pub model: ModelRef,
    pub mapping_revision: String,
    pub coverage_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelOpeningAccounting {
    ProviderReported {
        counter_revision: String,
        input_tokens: u64,
        max_input_tokens: u64,
    },
    WireBytes {
        policy_revision: String,
        max_generation_wire_bytes: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelContextPrepared {
    pub opening_protocol: u32,
    pub execution: ExecutionKey,
    pub step_id: String,
    pub model_requested: ModelOpeningEventRef,
    pub neutral_digest: String,
    pub mapping_identity: ModelOpeningMappingIdentity,
    pub count_profile_digest: String,
    pub generation_wire_digest: String,
    pub generation_wire_bytes: u64,
    pub count_input_digest: String,
    pub unsupported_count_fields: Vec<String>,
    pub accounting: ModelOpeningAccounting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOpeningAdmitted {
    pub schema_version: u32,
    pub opening_protocol: u32,
    pub execution: ExecutionKey,
    pub step_id: String,
    pub model_requested: ModelOpeningEventRef,
    #[serde(deserialize_with = "strict_value")]
    pub preparation: FactRef,
    pub neutral_digest: String,
    pub generation_wire_digest: String,
    pub generation_wire_bytes: u64,
    pub count_profile_digest: String,
    #[serde(deserialize_with = "required_option")]
    pub deadline_at_ms: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Admission {
    pub schema_version: u32,
    pub opening_protocol: u32,
    pub key: ExecutionKey,
    pub model_request: ModelRequest,
    pub max_steps: usize,
    #[serde(deserialize_with = "required_option")]
    pub max_tool_calls: Option<usize>,
    #[serde(deserialize_with = "required_option")]
    pub deadline_at_ms: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub tool_timeout_ms: Option<u64>,
    pub dispatch: ToolDispatchPolicy,
    #[serde(deserialize_with = "required_option")]
    pub agent_snapshot_digest: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StepStarted {
    pub step_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Requested {
    pub request: ModelRequest,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Completed {
    pub step_id: String,
    pub outcome: String,
    pub step: StepResult,
    pub opening_protocol: u32,
    pub model_requested: ModelOpeningEventRef,
    pub model_opening: ModelOpeningEventRef,
}

fn required_option<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn strict_value<'de, D: Deserializer<'de>, T: serde::de::DeserializeOwned + Serialize>(
    deserializer: D,
) -> Result<T, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    let decoded: T = serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
    if serde_json::to_value(&decoded).map_err(serde::de::Error::custom)? != value {
        return Err(serde::de::Error::custom(
            "unknown or missing persisted fields",
        ));
    }
    Ok(decoded)
}
