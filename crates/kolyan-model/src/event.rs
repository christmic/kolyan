use crate::{ModelResponse, TokenUsage, ToolCall};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderMetadata {
    pub provider: String,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ModelEvent {
    Started,
    TextDelta(String),
    ReasoningDelta(String),
    ToolCallStarted { id: String, name: String },
    ToolCallArgumentsDelta { id: String, delta: String },
    ToolCallCompleted(ToolCall),
    Usage(TokenUsage),
    Completed(ModelResponse),
    Provider(ProviderMetadata),
}
