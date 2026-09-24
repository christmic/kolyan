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
    completion_policy: CompletionPolicy,
}

/// Compatibility is opt-in and still requires a completed text item.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum CompletionPolicy {
    #[default]
    RequireResponseCompleted,
    AllowCompletedTextItemAtEof,
}

impl OpenAiProvider {
    pub fn new(client: OpenAiClient) -> Self {
        Self {
            client,
            completion_policy: CompletionPolicy::default(),
        }
    }

    /// Enable only for servers known to omit the response terminal event.
    /// Bare EOF, unfinished tools and upstream errors never count as success.
    pub fn with_completion_policy(mut self, policy: CompletionPolicy) -> Self {
        self.completion_policy = policy;
        self
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
        let completion_policy = self.completion_policy;
        Box::pin(async move {
            validate_request(&request)?;
            let validator = kolyan_model::OutputValidator::new(request.output_format.as_ref())?;
            let response = client
                .stream_response(&Self::request(&request))
                .await
                .map_err(openai_error)?;
            let model = request.model.clone();
            let model_for_map = model.clone();
            let mut call_ids = BTreeMap::new();
            let mapped = response.map(move |event| {
                event
                    .map_err(openai_error)
                    .and_then(|event| map_stream_event(event, &model_for_map, &mut call_ids))
            });
            let state = Arc::new(Mutex::new(CompletionState {
                completion_policy,
                ..CompletionState::default()
            }));
            let tracked =
                tracked_completion(mapped, Arc::clone(&state), request.output_format.is_some());
            let stream = synthesize_completion_if_missing(
                tracked,
                Arc::clone(&state),
                model,
                request.output_format.is_some(),
            );
            let stream = stream.map(move |event| {
                event.and_then(|event| {
                    if let ModelEvent::Completed(response) = &event {
                        validator.validate(response)?;
                    }
                    Ok(event)
                })
            });
            Ok(Box::pin(stream) as _)
        })
    }
}

#[derive(Default)]
struct CompletionState {
    completion_policy: CompletionPolicy,
    completed_text_item: bool,
    failed: bool,
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
    structured: bool,
) -> impl Stream<Item = Result<ModelEvent, ProviderError>> + Send
where
    S: Stream<Item = Result<ModelEvent, ProviderError>> + Send + 'static,
{
    upstream.scan(false, move |failed, event_result| {
        if *failed {
            return futures_util::future::ready(None);
        }
        let event_result = if structured {
            match event_result {
                Ok(ModelEvent::Completed(mut response)) if response.structured_output.is_none() => {
                    if let Ok(snapshot) = state.lock()
                        && let Some(parsed) = parse_json_relaxed(&snapshot.text)
                    {
                        response.structured_output = Some(parsed);
                    }
                    Ok(ModelEvent::Completed(response))
                }
                other => other,
            }
        } else {
            event_result
        };
        if let Ok(mut s) = state.lock() {
            match &event_result {
                Ok(event) => remember_event(&mut s, event),
                Err(_) => s.failed = true,
            }
        }
        *failed = event_result.is_err() || matches!(event_result, Ok(ModelEvent::Completed(_)));
        futures_util::future::ready(Some(event_result))
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
        ModelEvent::Provider(metadata) => {
            if metadata
                .raw
                .as_ref()
                .is_some_and(|raw| raw.get("completed_text_item") == Some(&Value::Bool(true)))
            {
                state.completed_text_item = true;
            }
        }
        ModelEvent::Started => {}
    }
}

/// Strict EOF validation; opt-in compatibility requires explicit item evidence.
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
        if snapshot.completed_emitted || snapshot.failed {
            return None;
        }
        if snapshot.completion_policy != CompletionPolicy::AllowCompletedTextItemAtEof
            || !snapshot.completed_text_item
            || !snapshot.tool_builders.is_empty()
            || !snapshot.tool_calls.is_empty()
        {
            return Some(Err(provider_error(
                "stream ended without a response terminal event",
            )));
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
        let tool_calls = snapshot.tool_calls;
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
    completion_policy: CompletionPolicy,
    completed_text_item: bool,
    failed: bool,
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
            completion_policy: state.completion_policy,
            completed_text_item: state.completed_text_item,
            failed: state.failed,
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
                let completed_text_item = item.is_some_and(|item| {
                    item.get("type").and_then(Value::as_str) == Some("message")
                        && item.get("status").and_then(Value::as_str) == Some("completed")
                });
                Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                    provider: "openai".into(),
                    raw: Some(
                        json!({"completed_text_item": completed_text_item, "fields": event.fields}),
                    ),
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
/// `incomplete_details.reason = "max_output_tokens"`; unknown or missing
/// reasons fail closed. Output content (text / reasoning / tool calls) is
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
        _ => {
            return Err(provider_error(
                "incomplete response has no supported stop reason",
            ));
        }
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
        if let Some(end_rel) = trimmed.rfind(['}', ']'])
            && start <= end_rel
        {
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
                            .and_then(Value::as_str)
                            .map(|text| ContentBlock::Text { text: text.into() })
                    })
                    .collect()
            })
            .unwrap_or_default(),
    })
}
fn map_tool_call(value: &Value) -> Result<ToolCall, ProviderError> {
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
    let message = ProviderError::describe(&error);
    let status = match &error {
        kolyan_protocol_openai::OpenAiError::Http { status, .. } => Some(*status),
        _ => None,
    };
    let (kind, phase) = match error {
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
