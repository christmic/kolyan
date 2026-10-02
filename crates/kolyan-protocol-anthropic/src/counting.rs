//! Explicit count DTOs, separate from generation and SSE. Reports are not trusted bounds.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Count-operation failures are not stream or SSE API errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CountFailure {
    #[error("count total timeout elapsed")]
    Timeout,
    #[error("count response exceeds 64 KiB")]
    ResponseLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageCountTokensRequest {
    pub model: String,
    pub messages: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_config: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Value>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageTokensCount {
    pub input_tokens: u64,
}
