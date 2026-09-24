use super::*;

#[test]
fn cumulative_usage_updates_preserve_omitted_start_fields() {
    let model = kolyan_model::ModelRef::new("test", "model");
    let mut state = AnthropicState::default();
    state.event(wire_event("message_start", json!({"message":{"id":"msg","usage":{"input_tokens":10,"output_tokens":1,"cache_read_input_tokens":5}}})), &model).unwrap();
    state.event(wire_event("message_delta", json!({"delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":12,"output_tokens":20,"cache_creation_input_tokens":3}})), &model).unwrap();
    assert_eq!(state.usage.input_tokens, Some(12));
    assert_eq!(state.usage.output_tokens, Some(20));
    assert_eq!(state.usage.cache_read_tokens, Some(5));
    assert_eq!(state.usage.cache_write_tokens, Some(3));
}

#[test]
fn stream_requires_terminal_and_never_emits_after_terminal_or_error() {
    use futures_util::{FutureExt, stream};
    for finish in [false, true] {
        let mut events = vec![Ok(wire_event(
            "message_delta",
            json!({"delta":{"stop_reason":"end_turn"}}),
        ))];
        if finish {
            events.push(Ok(wire_event("message_stop", json!({}))));
        }
        events.push(Err(protocol_error("sentinel")));
        events.push(Ok(wire_event("message_stop", json!({}))));
        let result = map_stream(
            stream::iter(events),
            kolyan_model::ModelRef::new("test", "model"),
            false,
            kolyan_model::OutputValidator::new(None).unwrap(),
        )
        .collect::<Vec<_>>()
        .now_or_never()
        .unwrap();
        assert_eq!(result.len(), 2);
        if finish {
            assert!(matches!(result[1], Ok(ModelEvent::Completed(_))));
        } else {
            assert!(result[1].is_err());
        }
    }
    let result = map_stream(
        stream::empty(),
        kolyan_model::ModelRef::new("test", "model"),
        false,
        kolyan_model::OutputValidator::new(None).unwrap(),
    )
    .collect::<Vec<_>>()
    .now_or_never()
    .unwrap();
    assert_eq!(result.len(), 1);
    assert!(
        result[0]
            .as_ref()
            .unwrap_err()
            .message
            .contains("before message_stop")
    );
}

#[test]
fn effort_schema_and_message_cache_have_independent_wire_fields() {
    let request: ModelRequest = serde_json::from_value(json!({
        "request_id":"config", "model":{"provider":"anthropic","model":"test"},
        "system":[], "messages":[{"role":"user","content":[{"type":"text","text":"prefix"}]}],
        "tools":[], "tool_choice":"auto", "max_output_tokens":8192, "extensions":null,
        "reasoning":{"effort":"high","budget_tokens":null},
        "output_format":{"name":"answer","strict":true,"schema":{"type":"object"}},
        "prompt_cache":{"key":null,"retention":null,"breakpoints":["messages"]}
    }))
    .unwrap();
    let wire = serde_json::to_value(AnthropicProvider::request(&request)).unwrap();
    assert_eq!(wire["thinking"], json!({"type":"adaptive"}));
    assert_eq!(wire["output_config"]["effort"], "high");
    assert_eq!(
        wire["output_config"]["format"]["schema"],
        json!({"type":"object"})
    );
    assert_eq!(
        wire["messages"][0]["content"][0]["cache_control"],
        json!({"type":"ephemeral"})
    );
}

#[test]
fn signed_and_redacted_thinking_survive_completion_and_request_replay() {
    let model = kolyan_model::ModelRef::new("test", "model");
    let mut state = AnthropicState::default();
    let events = [
        (
            "content_block_start",
            json!({"index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}),
        ),
        (
            "content_block_delta",
            json!({"index":0,"delta":{"type":"thinking_delta","thinking":"reason"}}),
        ),
        (
            "content_block_delta",
            json!({"index":0,"delta":{"type":"signature_delta","signature":"signed-value"}}),
        ),
        ("content_block_stop", json!({"index":0})),
        (
            "content_block_start",
            json!({"index":1,"content_block":{"type":"redacted_thinking","data":"opaque"}}),
        ),
        ("content_block_stop", json!({"index":1})),
        (
            "content_block_start",
            json!({"index":2,"content_block":{"type":"text","text":""}}),
        ),
        (
            "content_block_delta",
            json!({"index":2,"delta":{"type":"text_delta","text":"answer"}}),
        ),
        ("content_block_stop", json!({"index":2})),
        ("message_delta", json!({"delta":{"stop_reason":"end_turn"}})),
    ];
    for (kind, fields) in events {
        state.event(wire_event(kind, fields), &model).unwrap();
    }
    let ModelEvent::Completed(response) = state
        .event(wire_event("message_stop", json!({})), &model)
        .unwrap()
    else {
        panic!("missing completion")
    };
    assert_eq!(response.content.len(), 3);
    assert_eq!(
        anthropic_block(&response.content[0]),
        json!({"type":"thinking","thinking":"reason","signature":"signed-value"})
    );
    assert_eq!(
        anthropic_block(&response.content[1]),
        json!({"type":"redacted_thinking","data":"opaque"})
    );
    assert_eq!(
        anthropic_block(&response.content[2]),
        json!({"type":"text","text":"answer"})
    );
}

#[test]
fn indexed_tool_deltas_cannot_overwrite_another_call() {
    let model = kolyan_model::ModelRef::new("test", "model");
    let mut state = AnthropicState::default();
    for index in 0..2 {
        state.event(wire_event("content_block_start", json!({"index":index,"content_block":{"type":"tool_use","id":format!("call-{index}"),"name":"read","input":{}}})), &model).unwrap();
    }
    for index in 0..2 {
        state.event(wire_event("content_block_delta", json!({"index":index,"delta":{"type":"input_json_delta","partial_json":format!("{{\"value\":{index}}}")}})), &model).unwrap();
        let ModelEvent::ToolCallCompleted(call) = state
            .event(
                wire_event("content_block_stop", json!({"index":index})),
                &model,
            )
            .unwrap()
        else {
            panic!("missing call")
        };
        assert_eq!(call.id, format!("call-{index}"));
        assert_eq!(call.arguments, json!({"value":index}));
    }
    assert!(
        state
            .event(
                wire_event(
                    "content_block_delta",
                    json!({"index":7,"delta":{"type":"input_json_delta","partial_json":"{}"}})
                ),
                &model
            )
            .is_err()
    );
}

#[test]
fn malformed_bracket_order_never_panics() {
    for text in ["] {", "} [", "中文] 🦀{"] {
        assert_eq!(parse_json_relaxed(text), None);
    }
}

fn wire_event(kind: &str, fields: Value) -> MessageStreamEvent {
    MessageStreamEvent {
        kind: kind.into(),
        fields: fields
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

#[test]
fn truncated_tool_json_is_an_error() {
    let model = kolyan_model::ModelRef::new("test", "model");
    let mut state = AnthropicState::default();
    state
        .event(
            wire_event(
                "content_block_start",
                json!({"content_block":{"type":"tool_use","id":"call","name":"write","input":{}}}),
            ),
            &model,
        )
        .unwrap();
    state
        .event(
            wire_event(
                "content_block_delta",
                json!({"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}),
            ),
            &model,
        )
        .unwrap();
    assert!(
        state
            .event(wire_event("content_block_stop", json!({})), &model)
            .is_err()
    );
    assert!(state.blocks.is_empty());
}

#[test]
fn stop_requires_a_reason_and_no_unfinished_tool() {
    let model = kolyan_model::ModelRef::new("test", "model");
    assert!(
        AnthropicState::default()
            .event(wire_event("message_stop", json!({})), &model)
            .is_err()
    );
    let mut state = AnthropicState::default();
    state
        .event(
            wire_event(
                "content_block_start",
                json!({"content_block":{"type":"tool_use","id":"call","name":"write","input":{}}}),
            ),
            &model,
        )
        .unwrap();
    state
        .event(
            wire_event("message_delta", json!({"delta":{"stop_reason":"tool_use"}})),
            &model,
        )
        .unwrap();
    assert!(
        state
            .event(wire_event("message_stop", json!({})), &model)
            .is_err()
    );
}

#[test]
fn error_event_is_not_informational_metadata() {
    assert!(
        AnthropicState::default()
            .event(
                wire_event("error", json!({"error":{"type":"overloaded_error"}})),
                &kolyan_model::ModelRef::new("test", "model")
            )
            .is_err()
    );
}

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
