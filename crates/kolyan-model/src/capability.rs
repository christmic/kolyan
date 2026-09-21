use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelRef {
    pub provider: String,
    pub model: String,
}

impl ModelRef {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFeature {
    TextInput,
    TextOutput,
    ImageInput,
    DocumentInput,
    ToolUse,
    ParallelToolUse,
    StructuredOutput,
    Reasoning,
    PromptCaching,
    Streaming,
}

pub type ModelFeatures = BTreeSet<ModelFeature>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub reference: ModelRef,
    pub context_window: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub features: ModelFeatures,
}
