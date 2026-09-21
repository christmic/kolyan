use futures_util::StreamExt;
use kolyan_model::{
    ContentBlock, ImageSource, MessageRole, ModelEvent, ModelProvider, ModelRequest, ModelResponse,
    ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderFuture, StopReason, TokenUsage,
    ToolCall,
};
use kolyan_protocol_anthropic::{AnthropicClient, MessageCreateRequest, MessageStreamEvent, Tool};
use serde_json::{Value, json};

#[derive(Clone)]
pub struct AnthropicProvider {
    client: AnthropicClient,
}

impl AnthropicProvider {
    pub fn new(client: AnthropicClient) -> Self {
        Self { client }
    }
    fn request(request: &ModelRequest) -> MessageCreateRequest {
        MessageCreateRequest {
            model: request.model.model.clone(),
            max_tokens: request.max_output_tokens.unwrap_or(4096),
            messages: request.messages.iter().map(anthropic_message).collect(),
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
            thinking: request
                .reasoning
                .as_ref()
                .map(|r| json!({"type":"enabled","budget_tokens":r.budget_tokens.unwrap_or(1024)})),
            output_config: request
                .output_format
                .as_ref()
                .map(|format| json!({"format":{"type":"json_schema","schema":format.schema}})),
            stream: true,
        }
    }
}

impl ModelProvider for AnthropicProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let client = self.client.clone();
        Box::pin(async move {
            let response = client
                .stream_message(&Self::request(&request))
                .await
                .map_err(anthropic_error)?;
            let model = request.model.clone();
            let structured = request.output_format.is_some();
            let stream = response.scan(
                AnthropicState {
                    structured,
                    ..AnthropicState::default()
                },
                move |state, event| {
                    let result = event
                        .map_err(anthropic_error)
                        .and_then(|event| state.event(event, &model));
                    futures_util::future::ready(Some(result))
                },
            );
            Ok(Box::pin(stream) as _)
        })
    }
}

#[derive(Default)]
struct AnthropicState {
    id: String,
    text: String,
    blocks: Vec<ContentBlock>,
    usage: TokenUsage,
    active_tool: Option<(String, String, String)>,
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
            "content_block_start" => event
                .fields
                .get("content_block")
                .filter(|b| b.get("type").and_then(Value::as_str) == Some("tool_use"))
                .map(|b| {
                    self.active_tool = Some((
                        string_value(b, "id"),
                        string_value(b, "name"),
                        String::new(),
                    ));
                    Ok(ModelEvent::ToolCallStarted {
                        id: string_value(b, "id"),
                        name: string_value(b, "name"),
                    })
                })
                .unwrap_or_else(|| Ok(ModelEvent::Provider(metadata(event.fields)))),
            "content_block_delta" => {
                let delta = event.fields.get("delta").cloned().unwrap_or_default();
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => {
                        let text = string_value(&delta, "text");
                        self.text.push_str(&text);
                        Ok(ModelEvent::TextDelta(text))
                    }
                    Some("thinking_delta") => {
                        Ok(ModelEvent::ReasoningDelta(string_value(&delta, "thinking")))
                    }
                    Some("input_json_delta") => {
                        let partial = string_value(&delta, "partial_json");
                        if let Some((_, _, arguments)) = self.active_tool.as_mut() {
                            arguments.push_str(&partial);
                        }
                        Ok(ModelEvent::ToolCallArgumentsDelta {
                            id: self
                                .active_tool
                                .as_ref()
                                .map(|(id, _, _)| id.clone())
                                .unwrap_or_default(),
                            delta: partial,
                        })
                    }
                    _ => Ok(ModelEvent::Provider(metadata(event.fields))),
                }
            }
            "content_block_stop" => self
                .active_tool
                .take()
                .map(|(id, name, arguments)| {
                    let call = ToolCall {
                        id,
                        name,
                        arguments: serde_json::from_str(&arguments).unwrap_or_else(|_| json!({})),
                    };
                    self.blocks
                        .push(ContentBlock::ToolCall { call: call.clone() });
                    Ok(ModelEvent::ToolCallCompleted(call))
                })
                .unwrap_or_else(|| Ok(ModelEvent::Provider(metadata(event.fields)))),
            "message_delta" => {
                self.stop_reason = event
                    .fields
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                    .map(String::from);
                if let Some(usage_value) = event.fields.get("usage")
                    && let Some(n) = usage_value.get("output_tokens").and_then(Value::as_u64)
                {
                    self.usage.output_tokens = Some(n);
                }
                Ok(ModelEvent::Usage(self.usage.clone()))
            }
            "message_stop" => {
                if !self.text.is_empty() {
                    self.blocks.insert(
                        0,
                        ContentBlock::Text {
                            text: self.text.clone(),
                        },
                    );
                }
                let stop_reason = match self.stop_reason.as_deref() {
                    Some("tool_use") => StopReason::ToolUse,
                    Some("end_turn") => StopReason::EndTurn,
                    Some("max_tokens") => StopReason::MaxOutputTokens,
                    Some("refusal") => StopReason::Refusal,
                    Some(other) => StopReason::Other(other.into()),
                    None => StopReason::EndTurn,
                };
                let structured_output = self
                    .structured
                    .then(|| parse_json_relaxed(&self.text))
                    .flatten();
                Ok(ModelEvent::Completed(ModelResponse {
                    id: self.id.clone(),
                    model: model.clone(),
                    content: self.blocks.clone(),
                    structured_output,
                    stop_reason,
                    usage: self.usage.clone(),
                    metadata: json!({"provider":"anthropic"}),
                }))
            }
            _ => Ok(ModelEvent::Provider(metadata(event.fields))),
        }
    }
}

fn anthropic_message(message: &kolyan_model::Message) -> Value {
    json!({"role": if message.role == MessageRole::User {"user"} else {"assistant"}, "content": message.content.iter().map(anthropic_block).collect::<Vec<_>>()})
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
fn anthropic_error(error: kolyan_protocol_anthropic::AnthropicError) -> ProviderError {
    let message = error.to_string();
    let (kind, phase) = match error {
        kolyan_protocol_anthropic::AnthropicError::Transport(_) => {
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
        kolyan_protocol_anthropic::AnthropicError::Decode(_) => {
            (ProviderErrorKind::Protocol, ProviderErrorPhase::Decode)
        }
    };
    ProviderError::new(kind, phase, message)
}

/// Parse JSON from a string that may or may not be wrapped in a markdown
/// ```json ... ``` fence. Mirrors the openai provider's relaxed parser
/// because some Anthropic-compatible servers (notably MiniMax's
/// Anthropic-compat surface) emit their JSON inside a fenced block. Returns
/// `None` on parse failure (not an error — the caller decides what to do
/// with a non-JSON response).
#[allow(clippy::collapsible_if)]
fn parse_json_relaxed(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = serde_json::from_str::<Value>(trimmed)
        && (v.is_object() || v.is_array())
    {
        return Some(v);
    }
    let stripped = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```JSON"))
        .or_else(|| trimmed.strip_prefix("```"));
    let stripped = stripped.unwrap_or(trimmed);
    let stripped = stripped.strip_suffix("```").unwrap_or(stripped);
    if stripped != trimmed
        && let Ok(v) = serde_json::from_str::<Value>(stripped.trim())
        && (v.is_object() || v.is_array())
    {
        return Some(v);
    }
    if let Some(start) = trimmed.find('{').or_else(|| trimmed.find('['))
        && let Some(end_rel) = trimmed.rfind(['}', ']'])
    {
        let candidate = &trimmed[start..=end_rel];
        if let Ok(v) = serde_json::from_str::<Value>(candidate)
            && (v.is_object() || v.is_array())
        {
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relaxed_json_parser_accepts_fenced_object() {
        assert_eq!(
            parse_json_relaxed("```json\n{\"city\":\"Shanghai\"}\n```"),
            Some(json!({"city": "Shanghai"}))
        );
    }

    #[test]
    fn relaxed_json_parser_rejects_plain_text() {
        assert_eq!(parse_json_relaxed("not json"), None);
    }
}
