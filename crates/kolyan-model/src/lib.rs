//! Provider-neutral model invocation types and interfaces.

mod cache;
mod capability;
mod error;
mod event;
mod message;
mod provider;
mod tool;
mod types;
mod usage;

pub use cache::{CacheBreakpoint, CacheRetention, PromptCacheConfig};
pub use capability::{ModelDescriptor, ModelFeature, ModelFeatures, ModelRef};
pub use error::{ProviderError, ProviderErrorKind, ProviderErrorPhase};
pub use event::{ModelEvent, ProviderMetadata};
pub use message::{ContentBlock, ImageSource, Message, MessageRole, SystemInstruction};
pub use provider::{ModelEventStream, ModelProvider, ProviderFuture, aggregate_stream};
pub use tool::{ToolCall, ToolChoice, ToolDefinition, ToolResult};
pub use types::{ModelRequest, ModelResponse, OutputFormat, ReasoningConfig, StopReason};
pub use usage::TokenUsage;
