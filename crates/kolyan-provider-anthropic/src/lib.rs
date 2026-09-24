use futures_util::StreamExt;
use kolyan_model::{
    ContentBlock, ImageSource, MessageRole, ModelEvent, ModelProvider, ModelRequest, ModelResponse,
    ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderFuture, StopReason, TokenUsage,
    ToolCall,
};
use kolyan_protocol_anthropic::{AnthropicClient, MessageCreateRequest, MessageStreamEvent, Tool};
use serde_json::{Value, json};

mod blocks;

#[derive(Clone)]
pub struct AnthropicProvider {
    client: AnthropicClient,
    planner: Option<kolyan_model::RequestPlanner>,
}

impl AnthropicProvider {
    pub fn new(client: AnthropicClient) -> Self {
        Self {
            client,
            planner: None,
        }
    }
    /// Bind endpoint/model policy once; every invocation is planned before HTTP I/O.
    pub fn with_parameter_table(
        mut self,
        table: kolyan_model::ParameterTable,
    ) -> Result<Self, ProviderError> {
        self.planner = Some(kolyan_model::RequestPlanner::new(
            table,
            "anthropic_messages",
            MessageCreateRequest::RESERVED_FIELDS,
        )?);
        Ok(self)
    }
    fn request(request: &ModelRequest) -> MessageCreateRequest {
        MessageCreateRequest {
            model: request.model.model.clone(),
            max_tokens: request.max_output_tokens.unwrap_or(4096),
            messages: request_messages(request),
            system: system(request),
            tools: request
                .tools
                .iter()
                .map(|tool| Tool {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    input_schema: tool.input_schema.clone(),
                    cache_control: request.prompt_cache.as_ref().and_then(|cache| {
                        cache
                            .breakpoints
                            .contains(&kolyan_model::CacheBreakpoint::Tools)
                            .then(|| json!({"type":"ephemeral"}))
                    }),
                })
                .collect(),
            tool_choice: tool_choice(&request.tool_choice),
            thinking: request.reasoning.as_ref().map(|r| match r.budget_tokens {
                Some(budget) => json!({"type":"enabled","budget_tokens":budget}),
                None => json!({"type":"adaptive"}),
            }),
            output_config: output_config(request),
            stream: true,
        }
    }
}

impl ModelProvider for AnthropicProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let client = self.client.clone();
        let planner = self.planner.clone();
        Box::pin(async move {
            let configured = planner.is_some();
            let plan = match planner {
                Some(planner) => planner.plan(&request)?,
                None => kolyan_model::PlannedRequest::unconfigured(request)?,
            };
            let omit_tool_choice = plan.omitted("tool_choice");
            let request = plan.request;
            if request
                .prompt_cache
                .as_ref()
                .is_some_and(|cache| cache.key.is_some() || cache.retention.is_some())
            {
                return Err(ProviderError::new(
                    ProviderErrorKind::Unsupported,
                    ProviderErrorPhase::Open,
                    "Anthropic cache mapping supports breakpoints, not key or retention",
                ));
            }
            if configured && request.max_output_tokens.is_none() {
                return Err(ProviderError::new(
                    ProviderErrorKind::InvalidRequest,
                    ProviderErrorPhase::Open,
                    "Anthropic requires supported max_output_tokens from request or parameter table default",
                ));
            }
            let validator = kolyan_model::OutputValidator::new(request.output_format.as_ref())?;
            let mut wire = Self::request(&request);
            if omit_tool_choice {
                wire.tool_choice = None;
            }
            let response = client
                .stream_message_with_extensions(&wire, &plan.wire_extensions)
                .await
                .map_err(anthropic_error)?;
            let model = request.model.clone();
            let structured = request.output_format.is_some();
            let stream = map_stream(
                response.map(|event| event.map_err(anthropic_error)),
                model,
                structured,
                validator,
            );
            let audit = plan.decisions;
            let prefix = futures_util::stream::iter((!audit.is_empty()).then(|| {
                Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                    provider: request.model.provider,
                    raw: Some(json!({"kind":"request_planning","decisions":audit})),
                }))
            }));
            Ok(Box::pin(prefix.chain(stream)) as _)
        })
    }
}

fn map_stream<S>(
    response: S,
    model: kolyan_model::ModelRef,
    structured: bool,
    validator: kolyan_model::OutputValidator,
) -> impl futures_core::Stream<Item = Result<ModelEvent, ProviderError>>
where
    S: futures_core::Stream<Item = Result<MessageStreamEvent, ProviderError>>,
{
    futures_util::stream::unfold(
        (
            Box::pin(response),
            AnthropicState {
                structured,
                ..Default::default()
            },
            model,
            validator,
        ),
        |(mut response, mut state, model, validator)| async move {
            if state.failed {
                return None;
            }
            let event = response
                .next()
                .await
                .unwrap_or_else(|| Err(protocol_error("stream ended before message_stop")));
            let result = event
                .and_then(|event| state.event(event, &model))
                .and_then(|event| {
                    if let ModelEvent::Completed(response) = &event {
                        validator.validate(response)?;
                    }
                    Ok(event)
                });
            state.failed = result.is_err() || matches!(result, Ok(ModelEvent::Completed(_)));
            Some((result, (response, state, model, validator)))
        },
    )
}

#[derive(Default)]
struct AnthropicState {
    failed: bool,
    id: String,
    blocks: std::collections::BTreeMap<u64, ContentBlock>,
    usage: TokenUsage,
    active: blocks::Blocks,
    stop_reason: Option<String>,
    structured: bool,
}
impl AnthropicState {
    fn event(
        &mut self,
        event: MessageStreamEvent,
        model: &kolyan_model::ModelRef,
    ) -> Result<ModelEvent, ProviderError> {
        match event.kind.as_str() {
            "message_start" => {
                if let Some(message) = event.fields.get("message") {
                    self.id = string_value(message, "id");
                    self.usage = usage(message.get("usage"));
                }
                Ok(ModelEvent::Started)
            }
            "content_block_start" | "content_block_delta" | "content_block_stop" => {
                let (mapped, block) = self.active.event(&event)?;
                if let Some(block) = block {
                    let index = event
                        .fields
                        .get("index")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    if self.blocks.insert(index, block).is_some() {
                        return Err(protocol_error("duplicate completed block index"));
                    }
                }
                Ok(mapped.unwrap_or_else(|| ModelEvent::Provider(metadata(event.fields))))
            }
            "message_delta" => {
                self.stop_reason = event
                    .fields
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                    .map(String::from);
                if let Some(usage_value) = event.fields.get("usage") {
                    // SDK totals are cumulative, not increments; absent fields preserve start usage.
                    let update = usage(Some(usage_value));
                    self.usage.input_tokens = update.input_tokens.or(self.usage.input_tokens);
                    self.usage.output_tokens = update.output_tokens.or(self.usage.output_tokens);
                    self.usage.cache_read_tokens =
                        update.cache_read_tokens.or(self.usage.cache_read_tokens);
                    self.usage.cache_write_tokens =
                        update.cache_write_tokens.or(self.usage.cache_write_tokens);
                }
                Ok(ModelEvent::Usage(self.usage.clone()))
            }
            "message_stop" => {
                if !self.active.is_empty() || self.stop_reason.is_none() {
                    return Err(protocol_error(
                        "message stopped without a reason or with an unfinished tool",
                    ));
                }
                let stop_reason = match self.stop_reason.as_deref() {
                    Some("tool_use") => StopReason::ToolUse,
                    Some("end_turn") => StopReason::EndTurn,
                    Some("max_tokens") => StopReason::MaxOutputTokens,
                    Some("refusal") => StopReason::Refusal,
                    Some(other) => StopReason::Other(other.into()),
                    None => StopReason::EndTurn,
                };
                let text = self
                    .blocks
                    .values()
                    .filter_map(|block| match block {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<String>();
                let structured_output = self.structured.then(|| parse_json(&text)).flatten();
                Ok(ModelEvent::Completed(ModelResponse {
                    id: self.id.clone(),
                    model: model.clone(),
                    content: self.blocks.values().cloned().collect(),
                    structured_output,
                    stop_reason,
                    usage: self.usage.clone(),
                    metadata: json!({"provider":"anthropic"}),
                }))
            }
            "error" => Err(protocol_error(format!(
                "Anthropic error event: {:?}",
                event.fields.get("error")
            ))),
            _ => Ok(ModelEvent::Provider(metadata(event.fields))),
        }
    }
}

fn anthropic_message(message: &kolyan_model::Message) -> Value {
    json!({"role": if message.role == MessageRole::User {"user"} else {"assistant"}, "content": message.content.iter().map(anthropic_block).collect::<Vec<_>>()})
}

fn request_messages(request: &ModelRequest) -> Vec<Value> {
    let mut messages = request
        .messages
        .iter()
        .map(anthropic_message)
        .collect::<Vec<_>>();
    if request.prompt_cache.as_ref().is_some_and(|cache| {
        cache
            .breakpoints
            .contains(&kolyan_model::CacheBreakpoint::Messages)
    }) && let Some(block) = messages
        .last_mut()
        .and_then(|message| message.get_mut("content"))
        .and_then(Value::as_array_mut)
        .and_then(|content| content.last_mut())
    {
        block["cache_control"] = json!({"type":"ephemeral"});
    }
    messages
}

fn output_config(request: &ModelRequest) -> Option<Value> {
    let mut config = serde_json::Map::new();
    if let Some(format) = &request.output_format {
        config.insert(
            "format".into(),
            json!({"type":"json_schema","schema":format.schema}),
        );
    }
    if let Some(effort) = request.reasoning.as_ref().and_then(|r| r.effort.as_ref()) {
        config.insert("effort".into(), json!(effort));
    }
    (!config.is_empty()).then_some(Value::Object(config))
}
fn anthropic_block(block: &ContentBlock) -> Value {
    match block {
        ContentBlock::Text { text } => json!({"type":"text","text":text}),
        ContentBlock::Image { source } => json!({"type":"image","source":image_source(source)}),
        ContentBlock::Document { source, .. } => {
            json!({"type":"document","source":image_source(source)})
        }
        ContentBlock::ToolCall { call } => {
            json!({"type":"tool_use","id":call.id,"name":call.name,"input":call.arguments})
        }
        ContentBlock::ToolResult { result } => {
            json!({"type":"tool_result","tool_use_id":result.call_id,"content":result.content,"is_error":result.is_error})
        }
        ContentBlock::Reasoning {
            opaque: Some(raw), ..
        } => raw.clone(),
        ContentBlock::Reasoning { text, .. } => json!({"type":"thinking","thinking":text}),
    }
}
fn image_source(source: &ImageSource) -> Value {
    match source {
        ImageSource::Url { url } => json!({"type":"url","url":url}),
        ImageSource::Base64 { media_type, data } => {
            json!({"type":"base64","media_type":media_type,"data":data})
        }
    }
}
fn system(request: &ModelRequest) -> Option<Value> {
    let cache_system = request.prompt_cache.as_ref().is_some_and(|cache| {
        cache
            .breakpoints
            .contains(&kolyan_model::CacheBreakpoint::System)
    });
    let blocks = request
        .system
        .iter()
        .map(|s| {
            let cached = s.cache || cache_system;
            if cached {
                json!({"type":"text","text":s.text,"cache_control":{"type":"ephemeral"}})
            } else {
                json!({"type":"text","text":s.text})
            }
        })
        .collect::<Vec<_>>();
    (!blocks.is_empty()).then_some(Value::Array(blocks))
}

fn tool_choice(choice: &kolyan_model::ToolChoice) -> Option<Value> {
    match choice {
        kolyan_model::ToolChoice::Auto => Some(json!({"type":"auto"})),
        kolyan_model::ToolChoice::None => Some(json!({"type":"none"})),
        kolyan_model::ToolChoice::Required => Some(json!({"type":"any"})),
        kolyan_model::ToolChoice::Tool(name) => Some(json!({"type":"tool","name":name})),
    }
}
fn usage(value: Option<&Value>) -> TokenUsage {
    TokenUsage {
        input_tokens: value
            .and_then(|v| v.get("input_tokens"))
            .and_then(Value::as_u64),
        output_tokens: value
            .and_then(|v| v.get("output_tokens"))
            .and_then(Value::as_u64),
        cache_read_tokens: value
            .and_then(|v| v.get("cache_read_input_tokens"))
            .and_then(Value::as_u64),
        cache_write_tokens: value
            .and_then(|v| v.get("cache_creation_input_tokens"))
            .and_then(Value::as_u64),
        reasoning_tokens: None,
    }
}
fn string_value(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .into()
}
fn metadata(fields: std::collections::BTreeMap<String, Value>) -> kolyan_model::ProviderMetadata {
    kolyan_model::ProviderMetadata {
        provider: "anthropic".into(),
        raw: Some(Value::Object(fields.into_iter().collect())),
    }
}
fn protocol_error(message: impl Into<String>) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Protocol,
        ProviderErrorPhase::Decode,
        message,
    )
}

fn anthropic_error(error: kolyan_protocol_anthropic::AnthropicError) -> ProviderError {
    let message = ProviderError::describe(&error);
    let status = match &error {
        kolyan_protocol_anthropic::AnthropicError::Http { status, .. } => Some(*status),
        _ => None,
    };
    let (kind, phase) = match error {
        kolyan_protocol_anthropic::AnthropicError::Api(_) => {
            (ProviderErrorKind::Other, ProviderErrorPhase::Stream)
        }
        kolyan_protocol_anthropic::AnthropicError::Transport {
            diagnostics: None, ..
        } => (ProviderErrorKind::Transport, ProviderErrorPhase::Open),
        kolyan_protocol_anthropic::AnthropicError::Transport { .. } => {
            (ProviderErrorKind::Transport, ProviderErrorPhase::Stream)
        }
        kolyan_protocol_anthropic::AnthropicError::Http { status, .. } => (
            match status {
                401 | 403 => ProviderErrorKind::Authentication,
                429 => ProviderErrorKind::RateLimited,
                500..=599 => ProviderErrorKind::Unavailable,
                _ => ProviderErrorKind::InvalidRequest,
            },
            ProviderErrorPhase::Open,
        ),
        kolyan_protocol_anthropic::AnthropicError::Decode(_)
        | kolyan_protocol_anthropic::AnthropicError::Framing(_) => {
            (ProviderErrorKind::Protocol, ProviderErrorPhase::Decode)
        }
    };
    let mut error = ProviderError::new(kind, phase, message);
    error.status = status;
    error.provider = Some("anthropic".into());
    error
}

fn parse_json(text: &str) -> Option<Value> {
    serde_json::from_str(text).ok()
}

#[cfg(test)]
mod tests;
