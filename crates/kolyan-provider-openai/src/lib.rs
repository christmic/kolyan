use std::collections::BTreeMap;

use futures_util::{Stream, StreamExt};
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
    planner: Option<kolyan_model::RequestPlanner>,
}

impl OpenAiProvider {
    pub fn new(client: OpenAiClient) -> Self {
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
            "openai_responses",
            ResponseCreateRequest::RESERVED_FIELDS,
        )?);
        Ok(self)
    }

    fn request(request: &ModelRequest) -> ResponseCreateRequest {
        ResponseCreateRequest {
            model: request.model.model.clone(), input: openai_input(request),
            instructions: join_system(request), max_output_tokens: request.max_output_tokens,
            tools: request.tools.iter().map(|tool| FunctionTool { kind: "function".into(), name: tool.name.clone(), description: tool.description.clone(), parameters: tool.input_schema.clone(), strict: false }).collect(),
            tool_choice: tool_choice(&request.tool_choice),
            text: request.output_format.as_ref().map(|format| ResponseTextConfig { format: json!({"type":"json_schema","name":format.name,"schema":format.schema,"strict":format.strict}) }),
            reasoning: request.reasoning.as_ref().and_then(|r| r.effort.as_ref().map(|effort| json!({"effort":effort}))),
            prompt_cache_key: request.prompt_cache.as_ref().and_then(|cache| cache.key.clone()),
            prompt_cache_retention: request.prompt_cache.as_ref().and_then(|cache| cache.retention.map(|retention| match retention { kolyan_model::CacheRetention::InMemory => "in_memory".into(), kolyan_model::CacheRetention::TwentyFourHours => "24h".into() })),
            stream: true,
        }
    }
}

impl ModelProvider for OpenAiProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let client = self.client.clone();
        let planner = self.planner.clone();
        Box::pin(async move {
            let plan = match planner {
                Some(planner) => planner.plan(&request)?,
                None => kolyan_model::PlannedRequest::unconfigured(request)?,
            };
            let omit_tool_choice = plan.omitted("tool_choice");
            let request = plan.request;
            validate_request(&request)?;
            let validator = kolyan_model::OutputValidator::new(request.output_format.as_ref())?;
            let mut wire = Self::request(&request);
            if omit_tool_choice {
                wire.tool_choice = None;
            }
            let response = client
                .stream_response_with_extensions(&wire, &plan.wire_extensions)
                .await
                .map_err(openai_error)?;
            let model = request.model.clone();
            let model_for_map = model.clone();
            let mut call_ids = BTreeMap::new();
            let mut finalized = BTreeMap::new();
            let mapped = response.map(move |event| {
                event.map_err(openai_error).and_then(|mut event| {
                    recover_finalized_output(&mut event, &mut finalized)?;
                    map_stream_event(event, &model_for_map, &mut call_ids)
                })
            });
            let mapped =
                mapped.flat_map(|event| futures_util::stream::iter(finalized_call_events(event)));
            let stream = require_terminal(mapped);
            let stream = stream.map(move |event| {
                event.and_then(|event| {
                    if let ModelEvent::Completed(response) = &event {
                        validator.validate(response)?;
                    }
                    Ok(event)
                })
            });
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

/// EOF is not completion. Only a protocol terminal can finish a model invocation.
fn require_terminal<S>(upstream: S) -> impl Stream<Item = Result<ModelEvent, ProviderError>> + Send
where
    S: Stream<Item = Result<ModelEvent, ProviderError>> + Send,
{
    futures_util::stream::unfold(
        (Box::pin(upstream), false),
        |(mut upstream, finished)| async move {
            if finished {
                return None;
            }
            let event = upstream.next().await.unwrap_or_else(|| {
                Err(provider_error(
                    "stream ended without a response terminal event",
                ))
            });
            let finished = event.is_err() || matches!(event, Ok(ModelEvent::Completed(_)));
            Some((event, (upstream, finished)))
        },
    )
}

/// Match official ResponseStreamState: recover finalized items only when output is absent/null.
fn recover_finalized_output(
    event: &mut ResponseStreamEvent,
    finalized: &mut BTreeMap<u64, Value>,
) -> Result<(), ProviderError> {
    if event.kind == "response.output_item.done" {
        let index = event
            .fields
            .get("output_index")
            .and_then(Value::as_u64)
            .ok_or_else(|| provider_error("output_item.done is missing output_index"))?;
        let item = event
            .fields
            .get("item")
            .ok_or_else(|| provider_error("output_item.done is missing item"))?;
        finalized.insert(index, item.clone());
    }
    if event.kind == "response.completed"
        && let Some(response) = event.fields.get_mut("response")
    {
        let response = response
            .as_object_mut()
            .ok_or_else(|| provider_error("completed response must be an object"))?;
        if response.get("output").is_none_or(Value::is_null) {
            response.insert(
                "output".into(),
                Value::Array(finalized.values().cloned().collect()),
            );
        }
    }
    Ok(())
}

fn openai_message(message: &kolyan_model::Message) -> Vec<Value> {
    message.content.iter().map(|block| match block {
        ContentBlock::Text { text } => json!({"role": role(message), "content":[{"type": if message.role == kolyan_model::MessageRole::User {"input_text"} else {"output_text"},"text":text}]}),
        ContentBlock::Image { source } => json!({"role":"user","content":[{"type":"input_image","image_url":image_url(source)}]}),
        ContentBlock::Document { source, title } => json!({"role":"user","content":[file_input(source, title.as_deref())]}),
        ContentBlock::ToolCall { call } => json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call.arguments.to_string()}),
        ContentBlock::ToolResult { result } => json!({"type":"function_call_output","call_id":result.call_id,"output":result.content}),
        ContentBlock::Reasoning {
            opaque: Some(raw), ..
        } => raw.clone(),
        ContentBlock::Reasoning { text, .. } => {
            json!({"type":"reasoning","summary":[{"type":"summary_text","text":text}]})
        }
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
    }) && let Some(Value::Object(block)) = input
        .last_mut()
        .and_then(|item| item.get_mut("content"))
        .and_then(Value::as_array_mut)
        .and_then(|blocks| blocks.last_mut())
    {
        block.insert(
            "prompt_cache_breakpoint".into(),
            json!({"mode": "explicit"}),
        );
    }
    json!(input)
}

fn validate_request(request: &ModelRequest) -> Result<(), ProviderError> {
    if request
        .reasoning
        .as_ref()
        .is_some_and(|r| r.budget_tokens.is_some())
    {
        return Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            ProviderErrorPhase::Open,
            "OpenAI Responses reasoning supports effort, not budget_tokens",
        ));
    }
    if request.prompt_cache.as_ref().is_some_and(|cache| {
        cache
            .breakpoints
            .iter()
            .any(|point| *point != kolyan_model::CacheBreakpoint::Messages)
    }) {
        return Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            ProviderErrorPhase::Open,
            "OpenAI adapter supports explicit message-content cache breakpoints only",
        ));
    }
    if request.prompt_cache.as_ref().is_some_and(|cache| {
        cache
            .breakpoints
            .contains(&kolyan_model::CacheBreakpoint::Messages)
    }) && request
        .messages
        .last()
        .and_then(|message| message.content.last())
        .is_none_or(|block| {
            !matches!(
                block,
                ContentBlock::Text { .. }
                    | ContentBlock::Image { .. }
                    | ContentBlock::Document { .. }
            )
        })
    {
        return Err(ProviderError::new(
            ProviderErrorKind::InvalidRequest,
            ProviderErrorPhase::Open,
            "message cache breakpoint requires a final text, image or document block",
        ));
    }
    Ok(())
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
fn file_input(source: &ImageSource, title: Option<&str>) -> Value {
    let mut block = json!({"type":"input_file"});
    match source {
        ImageSource::Url { url } => block["file_url"] = json!(url),
        ImageSource::Base64 { .. } => block["file_data"] = json!(image_url(source)),
    }
    if let Some(title) = title {
        block["filename"] = json!(title);
    }
    block
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

/// Responses deltas carry item_id, whereas neutral tool events use call_id.
fn map_stream_event(
    mut event: ResponseStreamEvent,
    model: &kolyan_model::ModelRef,
    call_ids: &mut BTreeMap<String, String>,
) -> Result<ModelEvent, ProviderError> {
    if event.kind == "response.output_item.added"
        && let Some(item) = event.fields.get("item")
        && item["type"] == "function_call"
        && let (Some(item_id), Some(call_id)) = (item["id"].as_str(), item["call_id"].as_str())
    {
        call_ids.insert(item_id.into(), call_id.into());
    }
    if event.kind == "response.function_call_arguments.delta"
        && !event.fields.contains_key("call_id")
    {
        let item_id = event
            .fields
            .get("item_id")
            .and_then(Value::as_str)
            .ok_or_else(|| provider_error("tool delta has no item_id or call_id"))?;
        let call_id = call_ids
            .get(item_id)
            .ok_or_else(|| provider_error("tool delta refers to an unknown item"))?;
        event.fields.insert("call_id".into(), json!(call_id));
    }
    map_event(event, model)
}

fn map_event(
    event: ResponseStreamEvent,
    model: &kolyan_model::ModelRef,
) -> Result<ModelEvent, ProviderError> {
    let kind = event.kind.as_str();
    match kind {
        "response.output_text.delta" => Ok(ModelEvent::TextDelta(string(&event.fields, "delta"))),
        "response.refusal.delta" => Ok(ModelEvent::TextDelta(string(&event.fields, "delta"))),
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
        "response.output_item.done" => {
            // Official ResponseStreamState retains this raw item and parses the
            // response.completed output, which can supersede an earlier item.
            // Do not expose executable calls or parse partial arguments here.
            let completed_text_item = event
                .fields
                .get("item")
                .is_some_and(|item| item["type"] == "message" && item["status"] == "completed");
            Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                provider: "openai".into(),
                raw: Some(json!({"completed_text_item":completed_text_item,"fields":event.fields})),
            }))
        }
        "response.completed" => event
            .fields
            .get("response")
            .map(|response| map_response(response, model))
            .transpose()?
            .map(ModelEvent::Completed)
            .ok_or_else(|| provider_error("missing completed response")),
        // `response.incomplete` is sent when the server stops mid-stream
        // (most commonly because `max_output_tokens` was hit on a reasoning
        // model that spent the entire budget on `reasoning_tokens`).
        // Surface it as a terminal `Completed` with `MaxOutputTokens` so
        // `aggregate_stream` doesn't error out waiting for `response.completed`.
        "response.incomplete" => event
            .fields
            .get("response")
            .map(|response| map_incomplete_response(response, model))
            .transpose()?
            .map(ModelEvent::Completed)
            .ok_or_else(|| provider_error("missing incomplete response body")),
        // `response.created` / `response.in_progress` carry the response
        // shell with status=queued/in_progress. They're informational —
        // surface as Provider metadata so tests can see them, but never
        // terminate the stream.
        "response.created" | "response.in_progress" => {
            Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                provider: "openai".into(),
                raw: Some(Value::Object(event.fields.into_iter().collect())),
            }))
        }
        "error" => Err(provider_error(format!(
            "OpenAI error event: {}",
            Value::Object(event.fields.into_iter().collect())
        ))),
        "response.failed" => Err(provider_error(format!(
            "OpenAI response failed: {}",
            event
                .fields
                .get("response")
                .and_then(|response| response.get("error"))
                .or_else(|| event.fields.get("error"))
                .map(Value::to_string)
                .unwrap_or_else(|| "unknown server error".into())
        ))),
        _ => Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
            provider: "openai".into(),
            raw: Some(Value::Object(event.fields.into_iter().collect())),
        })),
    }
}

/// Neutral completed calls are derived from the authoritative, validated final
/// response, not from provider-specific intermediate item snapshots.
fn finalized_call_events(
    event: Result<ModelEvent, ProviderError>,
) -> Vec<Result<ModelEvent, ProviderError>> {
    let mut events = Vec::new();
    if let Ok(ModelEvent::Completed(response)) = &event {
        for block in &response.content {
            if let ContentBlock::ToolCall { call } = block {
                events.push(Ok(ModelEvent::ToolCallCompleted(call.clone())));
            }
        }
    }
    events.push(event);
    events
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
    let content = output
        .iter()
        .map(map_output)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let refused = output.iter().any(|item| {
        item.get("content")
            .and_then(Value::as_array)
            .is_some_and(|blocks| blocks.iter().any(|block| block["type"] == "refusal"))
    });
    let stop_reason = if refused {
        StopReason::Refusal
    } else if content
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
        structured_output: extract_structured_output(value),
        stop_reason,
        usage: usage(value.get("usage")),
        metadata: value.clone(),
    })
}

/// The official Response.incomplete_details and its reason are optional.
/// Preserve unknown reasons as Incomplete, never turn them into a final answer.
fn map_incomplete_response(
    value: &Value,
    model: &kolyan_model::ModelRef,
) -> Result<ModelResponse, ProviderError> {
    if value
        .get("id")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
        || !value.get("output").is_some_and(Value::is_array)
    {
        return Err(provider_error(
            "incomplete response requires an id and output array",
        ));
    }
    let mut response = map_response(value, model)?;
    let stop = value
        .get("incomplete_details")
        .and_then(|d| d.get("reason"))
        .and_then(Value::as_str);
    response.stop_reason = match stop {
        Some("max_output_tokens") => StopReason::MaxOutputTokens,
        Some("content_filter") => StopReason::Refusal,
        Some(reason) => StopReason::Other(format!("incomplete:{reason}")),
        None => StopReason::Other("incomplete".into()),
    };
    Ok(response)
}

/// Parse the actual output_text content, never SDK convenience fields or surrounding prose.
fn extract_structured_output(value: &Value) -> Option<Value> {
    let texts = value
        .get("output")?
        .as_array()?
        .iter()
        .filter(|item| item["type"] == "message")
        .filter_map(|item| item.get("content").and_then(Value::as_array))
        .flatten()
        .filter(|block| block["type"] == "output_text")
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect::<String>();
    parse_json(&texts)
}

fn parse_json(text: &str) -> Option<Value> {
    serde_json::from_str(text).ok()
}

fn map_output(item: &Value) -> Result<Vec<ContentBlock>, ProviderError> {
    Ok(match item.get("type").and_then(Value::as_str) {
        Some("function_call") => vec![ContentBlock::ToolCall {
            call: map_tool_call(item)?,
        }],
        Some("reasoning") => vec![ContentBlock::Reasoning {
            text: item
                .get("summary")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.get("text").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default(),
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
                            .or_else(|| b.get("refusal"))
                            .and_then(Value::as_str)
                            .map(|text| ContentBlock::Text { text: text.into() })
                    })
                    .collect()
            })
            .unwrap_or_default(),
    })
}
fn map_tool_call(value: &Value) -> Result<ToolCall, ProviderError> {
    let id = string_value(value, "call_id");
    let name = string_value(value, "name");
    if id.is_empty() || name.is_empty() {
        return Err(provider_error(
            "function call requires nonempty call_id and name",
        ));
    }
    let raw_arguments = value
        .get("arguments")
        .and_then(Value::as_str)
        .ok_or_else(|| provider_error("missing function arguments"))?;
    let arguments: Value = serde_json::from_str(raw_arguments)
        .map_err(|error| provider_error(format!("invalid function arguments: {error}")))?;
    if !arguments.is_object() {
        return Err(provider_error("function arguments must be an object"));
    }
    Ok(ToolCall {
        id,
        name,
        arguments,
    })
}
fn usage(value: Option<&Value>) -> TokenUsage {
    TokenUsage {
        input_tokens: value
            .and_then(|v| v.get("input_tokens"))
            .and_then(Value::as_u64),
        output_tokens: value
            .and_then(|v| v.get("output_tokens"))
            .and_then(Value::as_u64),
        reasoning_tokens: value
            .and_then(|v| v.get("output_tokens_details"))
            .and_then(|v| v.get("reasoning_tokens"))
            .and_then(Value::as_u64),
        cache_read_tokens: None,
        cache_write_tokens: None,
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
    let message = ProviderError::describe(&error);
    let status = match &error {
        kolyan_protocol_openai::OpenAiError::Http { status, .. } => Some(*status),
        _ => None,
    };
    let (kind, phase) = match error {
        kolyan_protocol_openai::OpenAiError::Api(_) => {
            (ProviderErrorKind::Other, ProviderErrorPhase::Stream)
        }
        kolyan_protocol_openai::OpenAiError::Transport {
            diagnostics: None, ..
        } => (ProviderErrorKind::Transport, ProviderErrorPhase::Open),
        kolyan_protocol_openai::OpenAiError::Transport { .. } => {
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
        kolyan_protocol_openai::OpenAiError::Decode(_)
        | kolyan_protocol_openai::OpenAiError::Framing(_) => {
            (ProviderErrorKind::Protocol, ProviderErrorPhase::Decode)
        }
    };
    let mut error = ProviderError::new(kind, phase, message);
    error.status = status;
    error.provider = Some("openai".into());
    error
}

#[cfg(test)]
mod tests;
