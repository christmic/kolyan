use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    /// Input tokens. `None` means the provider did not report this field
    /// (distinct from `Some(0)`, which means "the model saw zero tokens").
    /// Do **not** `.unwrap_or(0)` blindly — that conflates the two and makes
    /// MiniMax-Anthropic-style "server omits the field" look like a free
    /// request.
    pub input_tokens: Option<u64>,
    /// Output tokens. Same `None`/zero semantics as [`Self::input_tokens`].
    pub output_tokens: Option<u64>,
    /// Anthropic-style `cache_read_input_tokens`. `None` means the provider
    /// either doesn't support prompt caching or didn't report hit tokens.
    pub cache_read_tokens: Option<u64>,
    /// Anthropic-style `cache_creation_input_tokens`. Same semantics.
    pub cache_write_tokens: Option<u64>,
    /// Anthropic / OpenAI reasoning tokens. `None` means not reported.
    pub reasoning_tokens: Option<u64>,
}
