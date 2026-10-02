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
pub struct InputTokenCountRequest {
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<Value>,
    pub input: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub personality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputTokenCountResponse {
    pub input_tokens: u64,
    pub object: InputTokenCountObject,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputTokenCountObject {
    #[serde(rename = "response.input_tokens")]
    ResponseInputTokens,
}
