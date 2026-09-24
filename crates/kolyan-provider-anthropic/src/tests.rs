use super::*;

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
