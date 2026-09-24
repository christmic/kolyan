use super::*;

#[test]
fn malformed_bracket_order_never_panics() {
    for text in ["] {", "} [", "中文] 🦀{"] {
        assert_eq!(parse_json_relaxed(text), None);
    }
}
use futures_util::{StreamExt, stream};

#[test]
fn malformed_terminal_tool_arguments_are_not_silently_dropped() {
    let response = json!({"output": [{"type":"function_call", "call_id":"call", "name":"write", "arguments":"{\"path\":"}]});
    assert!(map_response(&response, &kolyan_model::ModelRef::new("test", "model")).is_err());
}

#[test]
fn incomplete_response_without_known_reason_cannot_succeed() {
    assert!(
        map_incomplete_response(
            &json!({"output":[]}),
            &kolyan_model::ModelRef::new("test", "model")
        )
        .is_err()
    );
}

#[test]
fn missing_or_empty_tool_arguments_cannot_become_empty_object() {
    for arguments in [Value::Null, json!(""), json!(" "), json!("[]")] {
        assert!(
            map_tool_call(&json!({"call_id":"call", "name":"write", "arguments":arguments}))
                .is_err()
        );
    }
    assert!(map_tool_call(&json!({"call_id":"call", "name":"write", "arguments":"{}"})).is_ok());
}

#[tokio::test]
async fn synthetic_completion_preserves_text_and_structured_output() {
    // Compatibility now requires explicit opt-in and a completed item witness.
    let state = Arc::new(Mutex::new(CompletionState {
        completion_policy: CompletionPolicy::AllowCompletedTextItemAtEof,
        completed_text_item: true,
        ..CompletionState::default()
    }));
    let events = stream::iter(vec![Ok(ModelEvent::TextDelta(
        "```json\n{\"ok\":true}\n```".into(),
    ))]);
    let tracked = tracked_completion(events, Arc::clone(&state), true);
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

#[tokio::test]
async fn eof_never_synthesizes_success_after_error_or_without_evidence() {
    for policy in [
        CompletionPolicy::RequireResponseCompleted,
        CompletionPolicy::AllowCompletedTextItemAtEof,
    ] {
        for fail in [false, true] {
            let state = Arc::new(Mutex::new(CompletionState {
                completion_policy: policy,
                ..CompletionState::default()
            }));
            let mut events = vec![Ok(ModelEvent::TextDelta("partial".into()))];
            if fail {
                events.push(Err(provider_error("transport failed")));
            }
            let tracked = tracked_completion(stream::iter(events), state.clone(), false);
            let events = synthesize_completion_if_missing(
                tracked,
                state,
                kolyan_model::ModelRef::new("test", "model"),
                false,
            )
            .collect::<Vec<_>>()
            .await;
            assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, Ok(ModelEvent::Completed(_))))
            );
        }
    }
}

#[tokio::test]
async fn completed_frame_recovers_structured_output_from_prior_text_deltas() {
    let state = Arc::new(Mutex::new(CompletionState::default()));
    let events = stream::iter(vec![
        Ok(ModelEvent::TextDelta("{\"ok\":true}".into())),
        Ok(ModelEvent::Completed(ModelResponse {
            id: "response".into(),
            model: kolyan_model::ModelRef::new("provider", "model"),
            content: Vec::new(),
            structured_output: None,
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
            metadata: Value::Null,
        })),
    ]);
    let mut stream = tracked_completion(events, Arc::clone(&state), true);
    let _ = stream.next().await;
    let Some(Ok(ModelEvent::Completed(response))) = stream.next().await else {
        panic!("expected completed response");
    };
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
