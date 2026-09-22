//! Shared live-test plumbing: config loading, env-var injection, provider
//! construction, capability-driven assertion helpers, and fixture →
//! `ModelRequest` translation.
//!
//! Every per-protocol integration test file (`tests/openai_compat.rs`,
//! `tests/anthropic_compat.rs`) compiles as a separate binary and pulls
//! what it needs from this module.

// Each integration test file is compiled into a separate binary, so any
// helper used by only one of the protocols will appear "dead" from the
// other's perspective. That's expected for a shared helper module.
#![allow(dead_code)]

use std::time::Duration;

use kolyan_model::{
    CacheBreakpoint, CacheRetention, ContentBlock, Message, MessageRole, ModelEvent, ModelProvider,
    ModelRef, ModelRequest, ModelResponse, OutputFormat, PromptCacheConfig, StopReason,
    SystemInstruction, TokenUsage, ToolChoice, ToolDefinition, aggregate_stream,
};
use kolyan_protocol_anthropic::{AnthropicClient, AnthropicConfig};
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig};
use kolyan_provider_anthropic::AnthropicProvider;
use kolyan_provider_openai::OpenAiProvider;
use serde::Deserialize;
use serde_json::Value;

// --------------------------------------------------------------------------
// Configuration (parses tests/config/live-tests.toml at compile time).
// --------------------------------------------------------------------------

const LIVE_TESTS_TOML: &str = include_str!("../config/live-tests.toml");

/// Provider-family identifiers — stable string keys used both to address
/// entries in `live-tests.toml` and to label `ModelRef::provider`.
pub mod family {
    pub const MINIMAX: &str = "minimax";
    pub const QWEN: &str = "qwen";
}

/// Protocol-surface identifiers within a family.
pub mod surface {
    pub const OPENAI_COMPAT: &str = "openai_compat";
    pub const ANTHROPIC_COMPAT: &str = "anthropic_compat";
}

#[derive(Deserialize)]
struct RawConfig {
    provider: RawProviders,
}

#[derive(Deserialize)]
struct RawProviders {
    minimax: RawFamily,
    qwen: RawFamily,
}

#[derive(Deserialize)]
struct RawFamily {
    openai_compat: RawOpenAiProvider,
    anthropic_compat: RawAnthropicProvider,
}

#[derive(Deserialize)]
struct RawOpenAiProvider {
    base_url: String,
    model: String,
    timeout_secs: u64,
    api_key_env: String,
    capabilities: Capabilities,
    #[serde(default)]
    model_matrix: Vec<RawModelMatrixEntry>,
}

#[derive(Deserialize)]
struct RawAnthropicProvider {
    base_url: String,
    model: String,
    anthropic_version: String,
    timeout_secs: u64,
    api_key_env: String,
    capabilities: Capabilities,
    #[serde(default)]
    model_matrix: Vec<RawModelMatrixEntry>,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(from = "RawCapabilitiesInline")]
pub struct Capabilities {
    pub input_tokens_reporting: bool,
    pub output_tokens_reporting: bool,
    pub cache_control_ephemeral: bool,
    pub server_emits_completed: bool,
    pub structured_output_fenced: bool,
    pub supports_vision: bool,
}

/// Either `[provider.x.openai_compat.capabilities]` (nested form) or an
/// inline `{ ... }` (matrix form). `serde(from = ...)` means both shapes
/// deserialize to `Capabilities`.
#[derive(Deserialize)]
struct RawCapabilitiesInline {
    #[serde(default)]
    input_tokens_reporting: bool,
    #[serde(default)]
    output_tokens_reporting: bool,
    #[serde(default)]
    cache_control_ephemeral: bool,
    #[serde(default)]
    server_emits_completed: bool,
    #[serde(default)]
    structured_output_fenced: bool,
    #[serde(default)]
    supports_vision: bool,
}

impl From<RawCapabilitiesInline> for Capabilities {
    fn from(v: RawCapabilitiesInline) -> Self {
        Self {
            input_tokens_reporting: v.input_tokens_reporting,
            output_tokens_reporting: v.output_tokens_reporting,
            cache_control_ephemeral: v.cache_control_ephemeral,
            server_emits_completed: v.server_emits_completed,
            structured_output_fenced: v.structured_output_fenced,
            supports_vision: v.supports_vision,
        }
    }
}

/// Sub-section shape: `[[provider.qwen.openai_compat.model_matrix]]` with
/// `model` + inline `capabilities`. The nested `RawCapabilities` is read
/// via `serde(from = RawCapabilitiesInline)` so both table and inline forms
/// work.
#[derive(Deserialize)]
struct RawModelMatrixEntry {
    model: String,
    #[serde(default)]
    max_output_tokens: Option<u32>,
    capabilities: Capabilities,
}

#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub base_url: String,
    pub model: String,
    pub timeout_secs: u64,
    pub api_key_env: String,
    pub capabilities: Capabilities,
    pub model_matrix: Vec<ModelMatrixEntry>,
}

#[derive(Debug, Clone)]
pub struct AnthropicProviderConfig {
    pub base_url: String,
    pub model: String,
    pub anthropic_version: String,
    pub timeout_secs: u64,
    pub api_key_env: String,
    pub capabilities: Capabilities,
    pub model_matrix: Vec<ModelMatrixEntry>,
}

/// Per-model override on the live-test scaffolding. `max_output_tokens`
/// overrides the fixture's value when set — needed for reasoning models
/// (Qwen qwen3.x, DeepSeek-v4, GLM-5.x) where 256 tokens gets eaten by
/// `reasoning_tokens` before any message text can be emitted.
#[derive(Debug, Clone)]
pub struct ModelMatrixEntry {
    pub model: String,
    pub max_output_tokens: Option<u32>,
    pub capabilities: Capabilities,
}

#[derive(Debug, Clone)]
pub struct LiveTestConfig {
    pub minimax_openai: ProviderConfig,
    pub minimax_anthropic: AnthropicProviderConfig,
    pub qwen_openai: ProviderConfig,
    pub qwen_anthropic: AnthropicProviderConfig,
}

/// Read `live-tests.toml` (embedded via `include_str!`) into a typed config.
pub fn load_config() -> LiveTestConfig {
    let raw: RawConfig = toml::from_str(LIVE_TESTS_TOML)
        .expect("tests/config/live-tests.toml must parse into RawConfig");
    LiveTestConfig {
        minimax_openai: from_openai(raw.provider.minimax.openai_compat),
        minimax_anthropic: from_anthropic(raw.provider.minimax.anthropic_compat),
        qwen_openai: from_openai(raw.provider.qwen.openai_compat),
        qwen_anthropic: from_anthropic(raw.provider.qwen.anthropic_compat),
    }
}

fn from_openai(raw: RawOpenAiProvider) -> ProviderConfig {
    ProviderConfig {
        base_url: raw.base_url,
        model: raw.model,
        timeout_secs: raw.timeout_secs,
        api_key_env: raw.api_key_env,
        capabilities: raw.capabilities,
        model_matrix: raw
            .model_matrix
            .into_iter()
            .map(|m| ModelMatrixEntry {
                model: m.model,
                max_output_tokens: m.max_output_tokens,
                capabilities: m.capabilities,
            })
            .collect(),
    }
}

fn from_anthropic(raw: RawAnthropicProvider) -> AnthropicProviderConfig {
    AnthropicProviderConfig {
        base_url: raw.base_url,
        model: raw.model,
        anthropic_version: raw.anthropic_version,
        timeout_secs: raw.timeout_secs,
        api_key_env: raw.api_key_env,
        capabilities: raw.capabilities,
        model_matrix: raw
            .model_matrix
            .into_iter()
            .map(|m| ModelMatrixEntry {
                model: m.model,
                max_output_tokens: m.max_output_tokens,
                capabilities: m.capabilities,
            })
            .collect(),
    }
}

// --------------------------------------------------------------------------
// API key + per-provider config accessors.
// --------------------------------------------------------------------------

/// Returns `true` if the API key for this provider is present and non-empty.
/// Used to skip matrix entries whose env var isn't exported in the current
/// shell — panicking would abort the whole test (and block tests for other
/// providers sharing the same matrix runner).
pub fn has_api_key(cfg: &ProviderConfig) -> bool {
    std::env::var(&cfg.api_key_env)
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

/// Anthropic variant.
pub fn has_api_key_anthropic(cfg: &AnthropicProviderConfig) -> bool {
    std::env::var(&cfg.api_key_env)
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

/// Returns the API key from the env var named in `cfg.api_key_env`, or
/// panics with a precise message so the missing-variable condition is
/// immediately visible.
pub fn require_api_key(cfg: &ProviderConfig) -> String {
    require_api_key_env(&cfg.api_key_env)
}

/// Anthropic variant — same logic but typed against `AnthropicProviderConfig`.
pub fn require_api_key_anthropic(cfg: &AnthropicProviderConfig) -> String {
    require_api_key_env(&cfg.api_key_env)
}

fn require_api_key_env(env_name: &str) -> String {
    match std::env::var(env_name) {
        Ok(value) if !value.trim().is_empty() => value,
        Ok(_) => panic!("{env_name} is set but empty; live tests need a non-empty API key"),
        Err(_) => panic!(
            "{env_name} is not set; live tests need a real API key for the selected provider. \
             Export the variable and re-run with `cargo test -- --ignored`."
        ),
    }
}

// --------------------------------------------------------------------------
// Provider construction.
// --------------------------------------------------------------------------

pub fn build_openai_provider(cfg: &ProviderConfig, api_key: &str) -> OpenAiProvider {
    let protocol_cfg = OpenAiConfig {
        base_url: cfg.base_url.clone(),
        api_key: api_key.to_string(),
        timeout: Duration::from_secs(cfg.timeout_secs),
    };
    let client = OpenAiClient::new(protocol_cfg)
        .expect("OpenAiClient::new should succeed with a valid base_url and timeout");
    OpenAiProvider::new(client)
}

pub fn build_anthropic_provider(cfg: &AnthropicProviderConfig, api_key: &str) -> AnthropicProvider {
    let protocol_cfg = AnthropicConfig {
        base_url: cfg.base_url.clone(),
        api_key: api_key.to_string(),
        version: cfg.anthropic_version.clone(),
        timeout: Duration::from_secs(cfg.timeout_secs),
    };
    let client = AnthropicClient::new(protocol_cfg)
        .expect("AnthropicClient::new should succeed with a valid base_url and timeout");
    AnthropicProvider::new(client)
}

// --------------------------------------------------------------------------
// Fixture → ModelRequest translation.
// --------------------------------------------------------------------------

/// A test case lives as a single JSON file under `tests/fixtures/`. The
/// top-level shape is `{ "request": {...}, "expectations": {...} }` —
/// input on one side, expected output / behaviour on the other. To add a
/// new case, drop a new file and register its name in `load_fixture()`.
#[derive(Debug, Deserialize)]
pub struct Fixture {
    pub request: FixtureRequest,
    pub expectations: Expectations,
    #[serde(default)]
    pub turn: Option<TurnExpectations>,
}

#[derive(Debug, Deserialize, Default)]
pub struct TurnExpectations {
    pub min_steps: Option<usize>,
    pub min_tool_calls: Option<usize>,
    pub max_steps: Option<usize>,
}

/// Input half of a fixture — what to send to the provider.
#[derive(Debug, Deserialize)]
pub struct FixtureRequest {
    pub request_id: Option<String>,
    #[serde(default)]
    pub request_id_prefix: Option<String>,
    pub system: Option<String>,
    pub messages: Vec<FixtureMessage>,
    #[serde(default)]
    pub tools: Vec<FixtureTool>,
    pub tool_choice: Option<FixtureToolChoice>,
    pub output_format: Option<FixtureOutputFormat>,
    pub prompt_cache: Option<FixturePromptCache>,
    pub max_output_tokens: Option<u32>,
}

/// Output half of a fixture — what the response must satisfy. All fields
/// are optional; an absent field means "don't assert on this dimension."
#[derive(Debug, Deserialize, Default)]
pub struct Expectations {
    /// Expected `StopReason` — `"end_turn"`, `"tool_use"`,
    /// `"max_output_tokens"`, `"refusal"`, or absent.
    pub stop_reason: Option<String>,
    /// Constraints on the accumulated text content (see [`TextExpectations`]).
    pub text: Option<TextExpectations>,
    /// Constraints on the first tool call emitted (see [`ToolCallExpectations`]).
    pub tool_call: Option<ToolCallExpectations>,
    /// JSON-schema-shaped constraints on `response.structured_output`
    /// (see [`StructuredOutputExpectations`]).
    pub structured_output: Option<StructuredOutputExpectations>,
    /// Constraints comparing two responses (prompt-cache test runs the
    /// same fixture twice and asserts on the diff).
    pub prompt_cache: Option<PromptCacheExpectations>,
    /// Capability names that must be `true` for this fixture to run.
    /// If a capability is listed here but the model's capability flag is
    /// `false`, the test skips that model with a `[SKIP ...]` line.
    /// Names match the fields of [`Capabilities`].
    #[serde(default)]
    pub requires: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct TextExpectations {
    pub min_chars: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ToolCallExpectations {
    pub name: Option<String>,
    pub arguments: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
pub struct StructuredOutputExpectations {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub required: Option<Vec<String>>,
    pub properties: Option<Value>,
    /// Optional list of `(key, accepted_aliases)` pairs. When set, at
    /// least one alias from each tuple must be present on the response
    /// object, and the value at the chosen key is then asserted against
    /// the matching `properties` entry.
    #[serde(default)]
    pub key_aliases: Vec<KeyAlias>,
    /// Minimum number of keys in the top-level structured_output object.
    /// Different providers pick wildly different schemas for "city info"
    /// (some put `name` at the root, some nest it under `city`, some use
    /// Chinese keys, some use English). Enforcing `min_keys` keeps the
    /// assertion useful without forcing a single canonical schema.
    #[serde(default)]
    pub min_keys: Option<usize>,
    /// Walk every string value in the structured_output (recursive) and
    /// assert that at least one of them contains at least one of these
    /// substrings (case-insensitive). Catches the "model gave a JSON
    /// object back but it's not about the city we asked about" case
    /// without locking the schema.
    #[serde(default)]
    pub any_string_contains: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct KeyAlias {
    pub alias_of: String,
    pub names: Vec<String>,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct PromptCacheExpectations {
    pub second_cache_read_tokens_gt_first: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct FixtureMessage {
    pub role: String,
    pub content: Vec<FixtureContentBlock>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FixtureContentBlock {
    Text { text: String },
}

#[derive(Debug, Deserialize)]
pub struct FixtureTool {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureToolChoice {
    Auto,
    None,
    Required,
    Tool(String),
}

#[derive(Debug, Deserialize)]
pub struct FixtureOutputFormat {
    pub name: String,
    pub schema: Value,
    pub strict: bool,
}

#[derive(Debug, Deserialize)]
pub struct FixturePromptCache {
    pub key: Option<String>,
    pub retention: Option<String>,
    pub breakpoints: Vec<String>,
}

/// Parse a fixture JSON file (embedded at compile time).
pub fn load_fixture(name: &str) -> Fixture {
    let raw = match name {
        "text" => include_str!("../fixtures/text.json"),
        "tool_call" => include_str!("../fixtures/tool_call.json"),
        "structured_output" => include_str!("../fixtures/structured_output.json"),
        "prompt_cache" => include_str!("../fixtures/prompt_cache.json"),
        "turn_ten_step" => include_str!("../fixtures/turn_ten_step.json"),
        "turn_file_read_write" => include_str!("../fixtures/turn_file_read_write.json"),
        other => panic!("unknown fixture name: {other}"),
    };
    serde_json::from_str(raw).expect("fixture JSON must deserialize into Fixture")
}

/// Build a `ModelRequest` from a parsed fixture, injecting the provider's
/// model identifier and labeling the request with a concrete id. The
/// per-model `max_output_tokens` override from `live-tests.toml` (when
/// supplied) wins over the fixture's value — this lets reasoning models
/// declare their own output window without inflating the fixture.
pub fn build_request(
    family_label: &str,
    model: &str,
    fixture: &Fixture,
    request_id: String,
    max_output_tokens_override: Option<u32>,
) -> ModelRequest {
    let req = &fixture.request;
    let role_of = |raw: &str| match raw {
        "user" => MessageRole::User,
        "assistant" => MessageRole::Assistant,
        other => panic!("unsupported message role in fixture: {other}"),
    };
    let system = req
        .system
        .as_ref()
        .map(|text| {
            vec![SystemInstruction {
                text: text.clone(),
                cache: false,
            }]
        })
        .unwrap_or_default();
    let messages = req
        .messages
        .iter()
        .map(|m| Message {
            role: role_of(&m.role),
            content: m
                .content
                .iter()
                .map(|block| match block {
                    FixtureContentBlock::Text { text } => ContentBlock::Text { text: text.clone() },
                })
                .collect(),
        })
        .collect();
    let tools = req
        .tools
        .iter()
        .map(|t| ToolDefinition {
            name: t.name.clone(),
            description: t.description.clone(),
            input_schema: t.input_schema.clone(),
        })
        .collect();
    let tool_choice = match &req.tool_choice {
        None => ToolChoice::Auto,
        Some(FixtureToolChoice::Auto) => ToolChoice::Auto,
        Some(FixtureToolChoice::None) => ToolChoice::None,
        Some(FixtureToolChoice::Required) => ToolChoice::Required,
        Some(FixtureToolChoice::Tool(name)) => ToolChoice::Tool(name.clone()),
    };
    let output_format = req.output_format.as_ref().map(|o| OutputFormat {
        name: o.name.clone(),
        schema: o.schema.clone(),
        strict: o.strict,
    });
    let prompt_cache = req.prompt_cache.as_ref().map(|c| PromptCacheConfig {
        key: c.key.clone(),
        retention: c.retention.as_deref().map(parse_retention),
        breakpoints: c.breakpoints.iter().map(|b| parse_breakpoint(b)).collect(),
    });

    ModelRequest {
        request_id,
        model: ModelRef::new(family_label, model),
        system,
        messages,
        tools,
        tool_choice,
        output_format,
        prompt_cache,
        reasoning: None,
        max_output_tokens: max_output_tokens_override.or(req.max_output_tokens),
        extensions: Value::Null,
    }
}

fn parse_retention(raw: &str) -> CacheRetention {
    match raw {
        "in_memory" => CacheRetention::InMemory,
        "24h" => CacheRetention::TwentyFourHours,
        other => panic!("unsupported cache retention: {other}"),
    }
}

fn parse_breakpoint(raw: &str) -> CacheBreakpoint {
    match raw {
        "system" => CacheBreakpoint::System,
        "tools" => CacheBreakpoint::Tools,
        "messages" => CacheBreakpoint::Messages,
        other => panic!("unsupported cache breakpoint: {other}"),
    }
}

// --------------------------------------------------------------------------
// Scenario execution.
// --------------------------------------------------------------------------

/// Drive a provider with the given request and aggregate the event stream
/// into a terminal `ModelResponse`. The provider's own `ProviderError`s are
/// surfaced verbatim — the live tests want to see real failure messages,
/// not wrapped ones.
pub async fn run_scenario<P>(provider: &P, request: ModelRequest) -> ModelResponse
where
    P: ModelProvider + ?Sized,
{
    let stream = provider.stream(request).await.unwrap_or_else(|e| {
        panic!("provider.stream should produce a ModelEventStream; got error: {e:?}")
    });
    aggregate_stream(stream).await.unwrap_or_else(|e| {
        panic!("aggregate_stream should yield a terminal ModelResponse; got error: {e:?}")
    })
}

/// Like [`run_scenario`] but takes a label so provider errors can be tied
/// back to the matrix entry that produced them. Use this when you want
/// the failing (family, surface, model, fixture) on the panic message.
///
/// Provider and aggregation errors fail the matrix row. A live regression
/// test must not report success when the adapter returned an error; transient
/// provider failures should be retried or explicitly quarantined instead of
/// silently skipped.
pub async fn run_scenario_labeled<P>(
    provider: &P,
    request: ModelRequest,
    label: &str,
) -> ModelResponse
where
    P: ModelProvider + ?Sized,
{
    let stream = provider
        .stream(request)
        .await
        .unwrap_or_else(|e| panic!("[{label}] provider.stream returned error: {e:?}"));
    aggregate_stream(stream)
        .await
        .unwrap_or_else(|e| panic!("[{label}] aggregate_stream returned error: {e:?}"))
}

// --------------------------------------------------------------------------
// Assertion helpers — capability-driven so tests don't hardcode provider
// names or hardcoded "must be > 0" expectations on fields the server may
// omit.
// --------------------------------------------------------------------------

/// Returns `true` if the fixture's `requires:` list names a capability
/// the model lacks. Caller should emit a `[SKIP ...]` line and `continue`
/// the matrix loop.
pub fn should_skip(fixture: &Fixture, capabilities: &Capabilities, label: &str) -> bool {
    for cap in &fixture.expectations.requires {
        let have = match cap.as_str() {
            "input_tokens_reporting" => capabilities.input_tokens_reporting,
            "output_tokens_reporting" => capabilities.output_tokens_reporting,
            "cache_control_ephemeral" => capabilities.cache_control_ephemeral,
            "server_emits_completed" => capabilities.server_emits_completed,
            "structured_output_fenced" => capabilities.structured_output_fenced,
            "supports_vision" => capabilities.supports_vision,
            other => {
                eprintln!(
                    "[WARN {label}] fixture requires unknown capability {other:?}; treating as missing"
                );
                false
            }
        };
        if !have {
            eprintln!("[SKIP {label}] fixture requires capability {cap:?} but model lacks it");
            return true;
        }
    }
    false
}

/// Compare a single `ModelResponse` against the fixture's
/// `expectations`. Panics on the first violation. Token usage is checked
/// separately via [`assert_usage`] because it depends on capabilities,
/// not on the fixture.
pub fn assert_expectations(response: &ModelResponse, expectations: &Expectations, label: &str) {
    // stop_reason
    if let Some(expected) = &expectations.stop_reason {
        let actual = stop_reason_str(&response.stop_reason);
        assert_eq!(
            actual, expected,
            "[{label}] expected stop_reason={expected}; got {actual} ({:?})",
            response.stop_reason
        );
    }

    // text
    if let Some(text_exp) = &expectations.text {
        let text = response
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("");
        if let Some(min) = text_exp.min_chars {
            assert!(
                text.chars().count() >= min,
                "[{label}] expected text >= {min} chars; got {} (text={text:?})",
                text.chars().count()
            );
        }
    }

    // tool_call
    if let Some(tc_exp) = &expectations.tool_call {
        let calls: Vec<_> = response
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolCall { call } => Some(call),
                _ => None,
            })
            .collect();
        assert!(
            !calls.is_empty(),
            "[{label}] expected at least one ToolCall; got StopReason={:?}",
            response.stop_reason
        );
        let call = &calls[0];
        if let Some(name) = &tc_exp.name {
            assert_eq!(call.name, *name, "[{label}] expected tool name={name}");
        }
        if let Some(args_schema) = &tc_exp.arguments {
            assert_json_shape(&call.arguments, args_schema, &format!("{label}/tool_args"));
        }
    }

    // structured_output
    if let Some(so_exp) = &expectations.structured_output {
        let structured = response.structured_output.as_ref().unwrap_or_else(|| {
            panic!(
                "[{label}] expected Some(structured_output); got None. StopReason={:?}",
                response.stop_reason
            )
        });
        let structured = unwrap_single_key_object(structured);

        // Schema-driven assertions (skip if no usable schema — empty
        // `expectations.structured_output` blocks are valid for
        // fixtures that just want to confirm `Some(_)`).
        let schema = if !so_exp.key_aliases.is_empty() || so_exp.properties.is_some() {
            let mut schema = serde_json::Map::new();
            if let Some(t) = &so_exp.type_ {
                schema.insert("type".into(), Value::String(t.clone()));
            }
            if so_exp.key_aliases.is_empty()
                && let Some(req) = &so_exp.required
            {
                schema.insert(
                    "required".into(),
                    Value::Array(req.iter().map(|s| Value::String(s.clone())).collect()),
                );
            }
            if let Some(props) = &so_exp.properties {
                schema.insert("properties".into(), props.clone());
            }
            Some(Value::Object(schema))
        } else {
            None
        };
        if let Some(schema) = schema {
            assert_json_shape(structured, &schema, &format!("{label}/structured_output"));
        }
        // Key aliases: for each `(alias_of, [names])` tuple, assert that
        // the response carries at least one of the accepted key names,
        // and validate the value at that key against `properties[alias_of]`.
        if !so_exp.key_aliases.is_empty() {
            let obj = structured.as_object().unwrap_or_else(|| {
                panic!("[{label}] key_aliases requires structured_output to be an object; got {structured:?}")
            });
            let props = so_exp.properties.as_ref().and_then(Value::as_object);
            for alias in &so_exp.key_aliases {
                // Type-aware alias selection: if the schema for
                // `alias_of` declares a `type`, only accept an alias whose
                // value matches that type. Otherwise (no type in schema),
                // fall back to the first matching key.
                let prop_schema_for_alias = props
                    .and_then(|p| p.get(&alias.alias_of))
                    .and_then(Value::as_object);
                let expected_type = prop_schema_for_alias
                    .and_then(|s| s.get("type"))
                    .and_then(Value::as_str);
                let mut chosen: Option<&String> = None;
                for candidate in &alias.names {
                    if !obj.contains_key(candidate) {
                        continue;
                    }
                    if let Some(want) = expected_type {
                        let v = &obj[candidate];
                        let matches = match want {
                            "string" => v.is_string(),
                            "number" => v.is_number(),
                            "boolean" => v.is_boolean(),
                            "array" => v.is_array(),
                            "object" => v.is_object(),
                            _ => true,
                        };
                        if matches {
                            chosen = Some(candidate);
                            break;
                        }
                    } else {
                        chosen = Some(candidate);
                        break;
                    }
                }
                let chosen = match chosen {
                    Some(c) => c,
                    None if alias.optional => continue,
                    None => panic!(
                        "[{label}/structured_output] expected one of {:?} (alias_of={:?}) to be present; got keys {:?}",
                        alias.names,
                        alias.alias_of,
                        obj.keys().collect::<Vec<_>>()
                    ),
                };
                if let Some(props_map) = props
                    && let Some(field_schema) = props_map.get(&alias.alias_of)
                {
                    let value = &obj[chosen];
                    assert_json_shape(
                        value,
                        field_schema,
                        &format!("{label}/structured_output/{}", alias.alias_of),
                    );
                }
            }
        }
    }

    // prompt_cache — single-response side; the second-cache-greater
    // assertion is checked separately by [`assert_prompt_cache_diff`]
    // because it needs both responses.
    let _ = expectations.prompt_cache.as_ref();
}

/// Compare two responses from the same fixture (prompt-cache test runs
/// the same request twice) and check the diff expectations.
pub fn assert_prompt_cache_diff(
    first: &ModelResponse,
    second: &ModelResponse,
    expectations: &PromptCacheExpectations,
    label: &str,
) {
    if expectations
        .second_cache_read_tokens_gt_first
        .unwrap_or(false)
    {
        let first_cached = first.usage.cache_read_tokens.unwrap_or(0);
        let second_cached = second.usage.cache_read_tokens.unwrap_or(0);
        assert!(
            second_cached > first_cached,
            "[{label}] expected cache_read_tokens on second request to exceed first \
             (cache should warm between calls); first={first_cached}, second={second_cached}, \
             first.usage={:?}, second.usage={:?}",
            first.usage,
            second.usage
        );
    }
}

fn stop_reason_str(reason: &StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn => "end_turn",
        StopReason::ToolUse => "tool_use",
        StopReason::MaxOutputTokens => "max_output_tokens",
        StopReason::Refusal => "refusal",
        StopReason::Other(_) => "other",
    }
}

/// If `value` is an object with exactly one key whose value is also an
/// object, return that inner value. Models sometimes wrap their JSON
/// inside an extra named key (`{"city": {...}}`) and other times emit it
/// flat (`{...}`); this helper normalizes the wrapped case so fixtures
/// don't need to enumerate every possible wrapper key.
fn unwrap_single_key_object(value: &Value) -> &Value {
    if let Some(obj) = value.as_object()
        && obj.len() == 1
        && let Some((_, inner)) = obj.iter().next()
        && inner.is_object()
    {
        return inner;
    }
    value
}

/// Walk every string in `value` (recursively) and call `visit` on each.
/// Used by `assert_expectations` to implement `any_string_contains`
/// without writing a recursive descent inline.
fn scan_strings<F: FnMut(&str)>(value: &Value, visit: &mut F) {
    match value {
        Value::String(s) => visit(s),
        Value::Array(arr) => arr.iter().for_each(|v| scan_strings(v, visit)),
        Value::Object(obj) => obj.values().for_each(|v| scan_strings(v, visit)),
        _ => {}
    }
}

/// Minimal JSON-shape assertion. Supports the subset of expectations the
/// fixtures actually need:
///
/// * `"type": "object" | "string" | "boolean" | "number" | "array"`
/// * `"required": ["field1", ...]` — each name must be present
/// * `"properties": { name: schema, ... }` — recurse into each
/// * `"min_len": N` (string only) — string length must be ≥ N
/// * `"contains_any": ["foo", ...]` (string only) — string must contain
///   at least one of the listed substrings
///
/// Anything else is treated as a no-op (logged via `eprintln!`) so adding
/// new fixture fields doesn't break old test binaries.
fn assert_json_shape(value: &Value, schema: &Value, path: &str) {
    let obj = match schema.as_object() {
        Some(o) => o,
        None => return,
    };
    if let Some(type_) = obj.get("type").and_then(Value::as_str) {
        let ok = match type_ {
            "object" => value.is_object(),
            "string" => value.is_string(),
            "boolean" => value.is_boolean(),
            "number" => value.is_number(),
            "array" => value.is_array(),
            other => {
                eprintln!("[WARN {path}] unknown schema type {other:?}; skipping check");
                true
            }
        };
        assert!(ok, "[{path}] expected type={type_}; got {value:?}");
    }
    if let Some(required) = obj.get("required").and_then(Value::as_array) {
        let value_obj = value.as_object().unwrap_or_else(|| {
            panic!("[{path}] required list specified but value is not an object: {value:?}")
        });
        for name in required {
            let name = name
                .as_str()
                .unwrap_or_else(|| panic!("[{path}] required entry must be a string"));
            assert!(
                value_obj.contains_key(name),
                "[{path}] missing required field {name:?}; got {value:?}"
            );
        }
    }
    if let (Some(properties), Some(value_obj)) = (
        obj.get("properties").and_then(Value::as_object),
        value.as_object(),
    ) {
        for (prop_name, prop_schema) in properties {
            if let Some(child) = value_obj.get(prop_name) {
                assert_json_shape(child, prop_schema, &format!("{path}.{prop_name}"));
            }
        }
    }
    if let Some(min_len) = obj.get("min_len").and_then(Value::as_u64) {
        let s = value.as_str().unwrap_or_else(|| {
            panic!("[{path}] min_len specified but value is not a string: {value:?}")
        });
        assert!(
            s.chars().count() as u64 >= min_len,
            "[{path}] expected string length ≥ {min_len}; got {} (string={s:?})",
            s.chars().count()
        );
    }
    if let Some(contains_any) = obj.get("contains_any").and_then(Value::as_array) {
        let s = value.as_str().unwrap_or_else(|| {
            panic!("[{path}] contains_any specified but value is not a string: {value:?}")
        });
        let s_lower = s.to_lowercase();
        let mut matched = false;
        for needle in contains_any {
            let needle = needle
                .as_str()
                .unwrap_or_else(|| panic!("[{path}] contains_any entry must be a string"));
            if s_lower.contains(&needle.to_lowercase()) {
                matched = true;
                break;
            }
        }
        assert!(
            matched,
            "[{path}] expected string to contain at least one of {contains_any:?} (case-insensitive); got {s:?}"
        );
    }
}

/// Assert `usage` according to what `capabilities` says the server reports.
///
/// * If `capabilities.input_tokens_reporting` is true, `usage.input_tokens`
///   must be `Some(n>0)`. If false, `Some(0)` or `None` is fine and we just
///   emit a `WARN` so a server upgrade that starts reporting it is visible.
/// * Same logic for `output_tokens_reporting`.
pub fn assert_usage(usage: &TokenUsage, capabilities: &Capabilities, label: &str) {
    match (capabilities.input_tokens_reporting, usage.input_tokens) {
        (true, Some(n)) => assert!(
            n > 0,
            "[{label}] expected input_tokens > 0 (server claims to report it); got {n}, usage={usage:?}",
        ),
        (true, None) => panic!(
            "[{label}] expected input_tokens = Some(n) but server reported None (capabilities say it should report); usage={usage:?}",
        ),
        (false, Some(n)) if n > 0 => {
            eprintln!(
                "[WARN {label}] server unexpectedly reported input_tokens = Some({n}) despite capability flag; usage={usage:?}"
            );
        }
        (false, _) => {
            eprintln!(
                "[{label}] server does not report input_tokens (capability flag false); usage={usage:?}",
            );
        }
    }
    match (capabilities.output_tokens_reporting, usage.output_tokens) {
        (true, Some(n)) => assert!(
            n > 0,
            "[{label}] expected output_tokens > 0 (server claims to report it); got {n}, usage={usage:?}",
        ),
        (true, None) => panic!(
            "[{label}] expected output_tokens = Some(n) but server reported None (capabilities say it should report); usage={usage:?}",
        ),
        (false, Some(n)) if n > 0 => {
            eprintln!(
                "[WARN {label}] server unexpectedly reported output_tokens = Some({n}) despite capability flag"
            );
        }
        (false, _) => {
            eprintln!("[{label}] server does not report output_tokens (capability flag false)");
        }
    }
}

/// Walk the aggregated event stream once (before aggregation) so tests can
/// assert on intermediate events like `TextDelta`, `ToolCallStarted`, or
/// `Usage`. Returns `true` if at least one event of the requested kind was
/// seen.
pub async fn observe_has_event<F>(
    provider: &dyn ModelProvider,
    request: ModelRequest,
    mut pred: F,
) -> bool
where
    F: FnMut(&ModelEvent) -> bool,
{
    use futures_util::StreamExt;
    let mut stream = provider
        .stream(request)
        .await
        .expect("provider.stream should produce a ModelEventStream");
    let mut saw = false;
    while let Some(event) = stream.next().await {
        let event = event.expect("event stream should not yield an error before terminal");
        if pred(&event) {
            saw = true;
        }
    }
    saw
}
