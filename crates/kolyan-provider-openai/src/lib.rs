use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

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
            let model_for_map = model.clone();
            let mapped = response.map(move |event| {
                event
                    .map_err(openai_error)
                    .and_then(|event| map_event(event, &model_for_map))
            });
            // Some compatible servers (notably MiniMax on the structured
            // output path) end the stream without ever emitting
            // `response.completed`. `aggregate_stream` then errors out
            // waiting for a terminal event. We compensate by tracking
            // whether Completed has been emitted and synthesizing one at
            // stream end from whatever we accumulated.
            let state = Arc::new(Mutex::new(CompletionState::default()));
            let tracked = tracked_completion(mapped, Arc::clone(&state));
            let stream = synthesize_completion_if_missing(
                tracked,
                Arc::clone(&state),
                model,
                request.output_format.is_some(),
            );
            Ok(Box::pin(stream) as _)
        })
    }
}

#[derive(Default)]
struct CompletionState {
    completed_emitted: bool,
    text: String,
    reasoning: String,
    tool_builders: BTreeMap<String, (String, String)>,
    tool_calls: Vec<ToolCall>,
    usage: TokenUsage,
}

/// Wrap a mapped event stream so we know whether a terminal
/// `ModelEvent::Completed` was ever emitted, regardless of what came
/// before it.
fn tracked_completion<S>(
    upstream: S,
    state: Arc<Mutex<CompletionState>>,
) -> impl Stream<Item = Result<ModelEvent, ProviderError>> + Send
where
    S: Stream<Item = Result<ModelEvent, ProviderError>> + Send + 'static,
{
    upstream.map(move |event_result| {
        if let Ok(event) = &event_result
            && let Ok(mut s) = state.lock()
        {
            remember_event(&mut s, event);
        }
        event_result
    })
}

fn remember_event(state: &mut CompletionState, event: &ModelEvent) {
    match event {
        ModelEvent::TextDelta(text) => state.text.push_str(text),
        ModelEvent::ReasoningDelta(text) => state.reasoning.push_str(text),
        ModelEvent::ToolCallStarted { id, name } => {
            state
                .tool_builders
                .entry(id.clone())
                .or_insert((name.clone(), String::new()));
        }
        ModelEvent::ToolCallArgumentsDelta { id, delta } => {
            state
                .tool_builders
                .entry(id.clone())
                .or_insert((String::new(), String::new()))
                .1
                .push_str(delta);
        }
        ModelEvent::ToolCallCompleted(call) => {
            let Some((name, arguments)) = state.tool_builders.remove(&call.id) else {
                state.tool_calls.push(call.clone());
                return;
            };
            let mut call = call.clone();
            if call.name.is_empty() {
                call.name = name;
            }
            if call.arguments == json!({})
                && let Ok(arguments) = serde_json::from_str(&arguments)
            {
                call.arguments = arguments;
            }
            state.tool_calls.push(call);
        }
        ModelEvent::Usage(usage) => state.usage = usage.clone(),
        ModelEvent::Completed(_) => state.completed_emitted = true,
        ModelEvent::Started | ModelEvent::Provider(_) => {}
    }
}

/// After the upstream stream finishes, append a synthetic `Completed`
/// event if the server never sent one. This keeps `aggregate_stream` from
/// erroring on MiniMax-style providers that close the connection
/// without emitting `response.completed`.
fn synthesize_completion_if_missing<S>(
    upstream: S,
    state: Arc<Mutex<CompletionState>>,
    model: kolyan_model::ModelRef,
    structured: bool,
) -> std::pin::Pin<Box<dyn Stream<Item = Result<ModelEvent, ProviderError>> + Send>>
where
    S: Stream<Item = Result<ModelEvent, ProviderError>> + Send + 'static,
{
    use futures_util::stream::{self, StreamExt};
    let model_for_synth = model.clone();
    let tail = stream::once(async move {
        let snapshot = state.lock().map(|s| CompletionSnapshot::from(&*s)).ok();
        let snapshot = snapshot?;
        if snapshot.completed_emitted {
            // Either Completed was emitted upstream, or the lock is
            // poisoned (treat as already-emitted to avoid double-firing).
            return None;
        }
        let mut content = Vec::new();
        if !snapshot.text.is_empty() {
            content.push(ContentBlock::Text {
                text: snapshot.text.clone(),
            });
        }
        if !snapshot.reasoning.is_empty() {
            content.push(ContentBlock::Reasoning {
                text: snapshot.reasoning.clone(),
                opaque: None,
            });
        }
        let mut tool_calls = snapshot.tool_calls;
        for (id, (name, arguments)) in snapshot.tool_builders {
            let arguments = serde_json::from_str(&arguments).unwrap_or_else(|_| json!({}));
            tool_calls.push(ToolCall {
                id,
                name,
                arguments,
            });
        }
        content.extend(
            tool_calls
                .iter()
                .cloned()
                .map(|call| ContentBlock::ToolCall { call }),
        );
        let structured_output = structured
            .then(|| parse_json_relaxed(&snapshot.text))
            .flatten();
        Some(Ok(ModelEvent::Completed(ModelResponse {
            id: String::new(),
            model: model_for_synth,
            content,
            structured_output,
            stop_reason: if tool_calls.is_empty() {
                StopReason::EndTurn
            } else {
                StopReason::ToolUse
            },
            usage: snapshot.usage,
            metadata: json!({
                "provider": "openai",
                "synthetic_completed": true,
                "reason": "server stream ended without response.completed",
            }),
        })))
    })
    .filter_map(|x| async move { x });
    upstream.chain(tail).boxed()
}

#[derive(Clone)]
struct CompletionSnapshot {
    completed_emitted: bool,
    text: String,
    reasoning: String,
    tool_builders: BTreeMap<String, (String, String)>,
    tool_calls: Vec<ToolCall>,
    usage: TokenUsage,
}

impl From<&CompletionState> for CompletionSnapshot {
    fn from(state: &CompletionState) -> Self {
        Self {
            completed_emitted: state.completed_emitted,
            text: state.text.clone(),
            reasoning: state.reasoning.clone(),
            tool_builders: state.tool_builders.clone(),
            tool_calls: state.tool_calls.clone(),
            usage: state.usage.clone(),
        }
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
        "response.output_item.done" => {
            // Only function_call items produce a ToolCallCompleted. Other
            // item types (message, reasoning, ...) silently close as
            // Provider metadata — without this guard, message items were
            // being mapped to ToolCallCompleted with empty name/arguments,
            // which broke `aggregate_stream`'s first-call-wins assumption.
            let item = event.fields.get("item");
            let is_function_call = item
                .and_then(Value::as_object)
                .and_then(|i| i.get("type"))
                .and_then(Value::as_str)
                == Some("function_call");
            if is_function_call {
                item.map(map_tool_call)
                    .transpose()?
                    .map(ModelEvent::ToolCallCompleted)
                    .ok_or_else(|| provider_error("missing function call item"))
            } else {
                Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                    provider: "openai".into(),
                    raw: Some(Value::Object(event.fields.into_iter().collect())),
                }))
            }
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
        structured_output: extract_structured_output(value),
        stop_reason,
        usage: usage(value.get("usage")),
        metadata: value.clone(),
    })
}

/// Like [`map_response`] but for `response.incomplete` frames. Forces
/// `stop_reason = MaxOutputTokens` when the body carries
/// `incomplete_details.reason = "max_output_tokens"`; otherwise falls back
/// to `EndTurn`. Output content (text / reasoning / tool calls) is
/// preserved from the partial response.
fn map_incomplete_response(
    value: &Value,
    model: &kolyan_model::ModelRef,
) -> Result<ModelResponse, ProviderError> {
    let mut response = map_response(value, model)?;
    let stop = value
        .get("incomplete_details")
        .and_then(|d| d.get("reason"))
        .and_then(Value::as_str);
    response.stop_reason = match stop {
        Some("max_output_tokens") => StopReason::MaxOutputTokens,
        Some("content_filter") => StopReason::Refusal,
        _ => response.stop_reason,
    };
    Ok(response)
}

/// Try, in order, to obtain a parsed JSON object from a Responses API
/// terminal frame:
///
/// 1. `output[].parsed` (official path when `text.format=json_schema`).
/// 2. `parsed` at top level.
/// 3. `output_text` at top level — strip a leading/trailing markdown
///    ```json fence (some compatible servers wrap their JSON that way)
///    before parsing.
/// 4. Concatenate `output[].content[].text` and parse — last-resort for
///    servers that only stream text deltas and never populate
///    `output_text`/`parsed`.
#[allow(clippy::collapsible_if)]
fn extract_structured_output(value: &Value) -> Option<Value> {
    if let Some(parsed) = value.get("parsed") {
        if parsed.is_object() || parsed.is_array() {
            return Some(parsed.clone());
        }
    }
    if let Some(Value::Array(items)) = value.get("output") {
        for item in items {
            if let Some(parsed) = item.get("parsed") {
                if parsed.is_object() || parsed.is_array() {
                    return Some(parsed.clone());
                }
            }
        }
    }
    if let Some(text) = value.get("output_text").and_then(Value::as_str) {
        if let Some(parsed) = parse_json_relaxed(text) {
            return Some(parsed);
        }
    }
    if let Some(Value::Array(items)) = value.get("output") {
        let joined: String = items
            .iter()
            .filter(|item| item.get("type").and_then(Value::as_str) != Some("reasoning"))
            .flat_map(|item| item.get("content").and_then(Value::as_array).cloned())
            .flatten()
            .filter(|block| block.get("type").and_then(Value::as_str) != Some("reasoning_text"))
            .filter_map(|block| block.get("text").and_then(Value::as_str).map(String::from))
            .collect::<Vec<_>>()
            .join("");
        if let Some(parsed) = parse_json_relaxed(&joined) {
            return Some(parsed);
        }
    }
    None
}

/// Parse JSON from a string that may or may not be wrapped in a markdown
/// ```json ... ``` fence. Returns `None` on parse failure (not an error —
/// the caller decides what to do with a non-JSON response).
#[allow(clippy::collapsible_if)]
fn parse_json_relaxed(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        if v.is_object() || v.is_array() {
            return Some(v);
        }
    }
    // Strip ```json ... ``` (with or without language tag) and retry.
    let stripped = strip_markdown_fence(trimmed);
    if stripped != trimmed {
        if let Ok(v) = serde_json::from_str::<Value>(stripped.trim()) {
            if v.is_object() || v.is_array() {
                return Some(v);
            }
        }
    }
    // Last-ditch: find the first {...} or [...] block and parse it.
    if let Some(start) = trimmed.find('{').or_else(|| trimmed.find('[')) {
        if let Some(end_rel) = trimmed.rfind(['}', ']']) {
            let candidate = &trimmed[start..=end_rel];
            if let Ok(v) = serde_json::from_str::<Value>(candidate)
                && (v.is_object() || v.is_array())
            {
                return Some(v);
            }
        }
    }
    None
}

fn strip_markdown_fence(text: &str) -> &str {
    let trimmed = text.trim();
    let stripped = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```JSON"))
        .or_else(|| trimmed.strip_prefix("```"));
    let stripped = stripped.unwrap_or(trimmed);
    stripped.strip_suffix("```").unwrap_or(stripped)
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
    let raw_arguments = value
        .get("arguments")
        .and_then(Value::as_str)
        .unwrap_or("{}");
    let arguments = if raw_arguments.trim().is_empty() {
        json!({})
    } else {
        serde_json::from_str(raw_arguments).map_err(|error| {
            provider_error(format!(
                "invalid function arguments ({error}); raw={raw_arguments:?}"
            ))
        })?
    };
    Ok(ToolCall {
        id: string_value(value, "call_id"),
        name: string_value(value, "name"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{StreamExt, stream};

    #[tokio::test]
    async fn synthetic_completion_preserves_text_and_structured_output() {
        let state = Arc::new(Mutex::new(CompletionState::default()));
        let events = stream::iter(vec![Ok(ModelEvent::TextDelta(
            "```json\n{\"ok\":true}\n```".into(),
        ))]);
        let tracked = tracked_completion(events, Arc::clone(&state));
        let mut stream = synthesize_completion_if_missing(
            tracked,
            state,
            kolyan_model::ModelRef::new("test", "model"),
            true,
        );
        assert!(matches!(
            stream.next().await,
            Some(Ok(ModelEvent::TextDelta(_)))
        ));
        let Some(Ok(ModelEvent::Completed(response))) = stream.next().await else {
            panic!("expected synthetic completion");
        };
        assert_eq!(response.content.len(), 1);
        assert_eq!(response.structured_output, Some(json!({"ok": true})));
    }

    #[test]
    fn structured_output_ignores_reasoning_content() {
        let response = json!({
            "output": [
                {
                    "type": "reasoning",
                    "content": [{"type": "reasoning_text", "text": "example {not json}"}]
                },
                {
                    "type": "message",
                    "content": [{"type": "output_text", "text": "{\"ok\":true}"}]
                }
            ]
        });

        assert_eq!(
            extract_structured_output(&response),
            Some(json!({"ok": true}))
        );
    }
}
