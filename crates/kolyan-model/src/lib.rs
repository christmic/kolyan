//! Provider-neutral model invocation types and interfaces.

mod accounting;
mod cache;
mod capability;
mod error;
mod event;
mod generation;
mod message;
mod planning;
mod provider;
mod structured;
mod tool;
mod types;
mod usage;

pub use accounting::{
    ContextProtocol, CountCoverage, CountProfile, CountSource, MAX_CONTEXT_JSON_BYTES,
    MappingIdentity, PreparedContextWire, ProviderInputCount, context_json_bytes, digest_json,
    endpoint_identity,
};
pub use cache::{CacheBreakpoint, CacheRetention, PromptCacheConfig};
pub use capability::{ModelDescriptor, ModelFeature, ModelFeatures, ModelRef};
pub use error::{ProviderError, ProviderErrorKind, ProviderErrorPhase};
pub use event::{ModelEvent, ProviderMetadata};
pub use generation::{PreparedModelGeneration, PreparedModelProvider, ProviderCountFuture};
pub use message::{ContentBlock, ImageSource, Message, MessageRole, SystemInstruction};
pub use planning::{
    ModelParameterOverrides, ParameterAction, ParameterConstraints, ParameterDecision,
    ParameterRule, ParameterSupport, ParameterTable, PlannedRequest, RequestPlanner,
};
pub use provider::{ModelEventStream, ModelProvider, ProviderFuture, aggregate_stream};
pub use structured::OutputValidator;
pub use tool::{ToolCall, ToolChoice, ToolDefinition, ToolResult};
pub use types::{ModelRequest, ModelResponse, OutputFormat, ReasoningConfig, StopReason};
pub use usage::TokenUsage;
