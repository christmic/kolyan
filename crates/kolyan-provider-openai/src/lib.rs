use futures_util::StreamExt;
use kolyan_model::{
    ContentBlock, ImageSource, ModelEvent, ModelProvider, ModelRequest, ModelResponse,
    ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderFuture, StopReason, TokenUsage,
    ToolCall,
};
use kolyan_protocol_openai::{
    FunctionTool, OpenAiClient, ResponseCreateRequest, ResponseStreamEvent, ResponseTextConfig,
};
use serde_json::{Value, json};

#[derive(Clone)]
pub struct OpenAiProvider {
    client: OpenAiClient,
}

impl OpenAiProvider {
    pub fn new(client: OpenAiClient) -> Self {
        Self { client }
    }
    fn request(request: &ModelRequest) -> ResponseCreateRequest {
        ResponseCreateRequest {
            model: request.model.model.clone(), input: openai_input(request),
            instructions: join_system(request), max_output_tokens: request.max_output_tokens,
            tools: request.tools.iter().map(|tool| FunctionTool { kind: "function".into(), name: tool.name.clone(), description: tool.description.clone(), parameters: tool.input_schema.clone(), strict: false }).collect(),
            tool_choice: tool_choice(&request.tool_choice),
            text: request.output_format.as_ref().map(|format| ResponseTextConfig { format: json!({"type":"json_schema","name":format.name,"schema":format.schema,"strict":format.strict}) }),
            reasoning: request.reasoning.as_ref().map(|r| json!({"effort":r.effort,"budget_tokens":r.budget_tokens})),
            prompt_cache_key: request.prompt_cache.as_ref().and_then(|cache| cache.key.clone()),
            prompt_cache_options: request.prompt_cache.as_ref().and_then(|cache| cache.retention.map(|retention| json!({"ttl": match retention { kolyan_model::CacheRetention::InMemory => "in_memory", kolyan_model::CacheRetention::TwentyFourHours => "24h" }}))),
            stream: true,
        }
    }
}

impl ModelProvider for OpenAiProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let client = self.client.clone();
        Box::pin(async move {
            let response = client
                .stream_response(&Self::request(&request))
                .await
                .map_err(openai_error)?;
            let model = request.model.clone();
            let stream = response.map(move |event| {
                event
                    .map_err(openai_error)
                    .and_then(|event| map_event(event, &model))
            });
            Ok(Box::pin(stream) as _)
        })
    }
}

fn openai_message(message: &kolyan_model::Message) -> Vec<Value> {
    message.content.iter().map(|block| match block {
        ContentBlock::Text { text } => json!({"role": role(message), "content":[{"type": if message.role == kolyan_model::MessageRole::User {"input_text"} else {"output_text"},"text":text}]}),
        ContentBlock::Image { source } => json!({"role":"user","content":[{"type":"input_image","image_url":image_url(source)}]}),
        ContentBlock::Document { source, title } => json!({"role":"user","content":[{"type":"input_file","file_data":image_data(source),"filename":title}]}),
        ContentBlock::ToolCall { call } => json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call.arguments.to_string()}),
        ContentBlock::ToolResult { result } => json!({"type":"function_call_output","call_id":result.call_id,"output":result.content}),
        ContentBlock::Reasoning { text, .. } => json!({"type":"reasoning","summary":[{"type":"summary_text","text":text}]}),
    }).collect()
}

fn openai_input(request: &ModelRequest) -> Value {
    let mut input = request
        .messages
        .iter()
        .flat_map(openai_message)
        .collect::<Vec<_>>();
    if request.prompt_cache.as_ref().is_some_and(|cache| {
        cache
            .breakpoints
            .contains(&kolyan_model::CacheBreakpoint::Messages)
    }) && let Some(Value::Object(item)) = input.last_mut()
    {
        item.insert("prompt_cache_breakpoint".into(), json!({"enabled": true}));
    }
    json!(input)
}

fn role(message: &kolyan_model::Message) -> &'static str {
    if message.role == kolyan_model::MessageRole::User {
        "user"
    } else {
        "assistant"
    }
}
fn image_url(source: &ImageSource) -> String {
    match source {
        ImageSource::Url { url } => url.clone(),
        ImageSource::Base64 { media_type, data } => format!("data:{media_type};base64,{data}"),
    }
}
fn image_data(source: &ImageSource) -> String {
    image_url(source)
}
fn join_system(request: &ModelRequest) -> Option<String> {
    let text = request
        .system
        .iter()
        .map(|s| s.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    (!text.is_empty()).then_some(text)
}

fn tool_choice(choice: &kolyan_model::ToolChoice) -> Option<Value> {
    match choice {
        kolyan_model::ToolChoice::Auto => Some(json!("auto")),
        kolyan_model::ToolChoice::None => Some(json!("none")),
        kolyan_model::ToolChoice::Required => Some(json!("required")),
        kolyan_model::ToolChoice::Tool(name) => Some(json!({"type":"function","name":name})),
    }
}

fn map_event(
    event: ResponseStreamEvent,
    model: &kolyan_model::ModelRef,
) -> Result<ModelEvent, ProviderError> {
    let kind = event.kind.as_str();
    match kind {
        "response.output_text.delta" => Ok(ModelEvent::TextDelta(string(&event.fields, "delta"))),
        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
            Ok(ModelEvent::ReasoningDelta(string(&event.fields, "delta")))
        }
        "response.function_call_arguments.delta" => Ok(ModelEvent::ToolCallArgumentsDelta {
            id: string_any(&event.fields, &["call_id", "item_id"]),
            delta: string(&event.fields, "delta"),
        }),
        "response.output_item.added" => event
            .fields
            .get("item")
            .and_then(Value::as_object)
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("function_call"))
            .map(|item| {
                Ok(ModelEvent::ToolCallStarted {
                    id: item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .into(),
                    name: item
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .into(),
                })
            })
            .unwrap_or_else(|| {
                Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                    provider: "openai".into(),
                    raw: Some(Value::Object(event.fields.into_iter().collect())),
                }))
            }),
        "response.output_item.done" => event
            .fields
            .get("item")
            .map(map_tool_call)
            .transpose()?
            .map(ModelEvent::ToolCallCompleted)
            .ok_or_else(|| provider_error("missing function call item")),
        "response.completed" => event
            .fields
            .get("response")
            .map(|response| map_response(response, model))
            .transpose()?
            .map(ModelEvent::Completed)
            .ok_or_else(|| provider_error("missing completed response")),
        "response.failed" => Err(provider_error("OpenAI response failed")),
        _ => Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
            provider: "openai".into(),
            raw: Some(Value::Object(event.fields.into_iter().collect())),
        })),
    }
}

fn map_response(
    value: &Value,
    model: &kolyan_model::ModelRef,
) -> Result<ModelResponse, ProviderError> {
    let output = value
        .get("output")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let content = output.iter().flat_map(map_output).collect::<Vec<_>>();
    let stop_reason = if content
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolCall { .. }))
    {
        StopReason::ToolUse
    } else {
        StopReason::EndTurn
    };
    Ok(ModelResponse {
        id: string_value(value, "id"),
        model: model.clone(),
        content,
        structured_output: value
            .get("output_text")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str(text).ok()),
        stop_reason,
        usage: usage(value.get("usage")),
        metadata: value.clone(),
    })
}
fn map_output(item: &Value) -> Vec<ContentBlock> {
    match item.get("type").and_then(Value::as_str) {
        Some("function_call") => map_tool_call(item)
            .map(|call| vec![ContentBlock::ToolCall { call }])
            .unwrap_or_default(),
        Some("reasoning") => vec![ContentBlock::Reasoning {
            text: item
                .get("summary")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(|v| v.get("text"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .into(),
            opaque: Some(item.clone()),
        }],
        _ => item
            .get("content")
            .and_then(Value::as_array)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|b| {
                        b.get("text")
                            .and_then(Value::as_str)
                            .map(|text| ContentBlock::Text { text: text.into() })
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}
fn map_tool_call(value: &Value) -> Result<ToolCall, ProviderError> {
    Ok(ToolCall {
        id: string_value(value, "call_id"),
        name: string_value(value, "name"),
        arguments: serde_json::from_str(
            value
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or("{}"),
        )
        .map_err(|_| provider_error("invalid function arguments"))?,
    })
}
fn usage(value: Option<&Value>) -> TokenUsage {
    TokenUsage {
        input_tokens: value
            .and_then(|v| v.get("input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output_tokens: value
            .and_then(|v| v.get("output_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        reasoning_tokens: value
            .and_then(|v| v.get("output_tokens_details"))
            .and_then(|v| v.get("reasoning_tokens"))
            .and_then(Value::as_u64),
        ..TokenUsage::default()
    }
}
fn string(fields: &std::collections::BTreeMap<String, Value>, key: &str) -> String {
    fields
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .into()
}
fn string_any(fields: &std::collections::BTreeMap<String, Value>, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| fields.get(*key).and_then(Value::as_str))
        .unwrap_or_default()
        .into()
}
fn string_value(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .into()
}
fn provider_error(message: impl Into<String>) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Protocol,
        ProviderErrorPhase::Decode,
        message,
    )
}
fn openai_error(error: kolyan_protocol_openai::OpenAiError) -> ProviderError {
    let message = error.to_string();
    let (kind, phase) = match error {
        kolyan_protocol_openai::OpenAiError::Transport(_) => {
            (ProviderErrorKind::Transport, ProviderErrorPhase::Stream)
        }
        kolyan_protocol_openai::OpenAiError::Http { status, .. } => (
            match status {
                401 | 403 => ProviderErrorKind::Authentication,
                429 => ProviderErrorKind::RateLimited,
                500..=599 => ProviderErrorKind::Unavailable,
                _ => ProviderErrorKind::InvalidRequest,
            },
            ProviderErrorPhase::Open,
        ),
        kolyan_protocol_openai::OpenAiError::Decode(_) => {
            (ProviderErrorKind::Protocol, ProviderErrorPhase::Decode)
        }
    };
    ProviderError::new(kind, phase, message)
}
