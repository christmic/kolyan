use super::*;

#[test]
fn documents_use_distinct_url_and_inline_fields() {
    let url = file_input(
        &ImageSource::Url {
            url: "https://example.test/doc.pdf".into(),
        },
        None,
    );
    assert_eq!(
        url,
        json!({"type":"input_file","file_url":"https://example.test/doc.pdf"})
    );
    let inline = file_input(
        &ImageSource::Base64 {
            media_type: "application/pdf".into(),
            data: "AAAA".into(),
        },
        Some("doc.pdf"),
    );
    assert_eq!(
        inline,
        json!({"type":"input_file","file_data":"data:application/pdf;base64,AAAA","filename":"doc.pdf"})
    );
}

#[test]
fn reasoning_preserves_all_summary_parts_and_opaque_state() {
    let raw = json!({"type":"reasoning","id":"reason","summary":[{"type":"summary_text","text":"first"},{"type":"summary_text","text":"second"}],"encrypted_content":"opaque"});
    let blocks = map_output(&raw).unwrap();
    assert_eq!(
        blocks,
        vec![ContentBlock::Reasoning {
            text: "first\nsecond".into(),
            opaque: Some(raw)
        }]
    );
}

#[test]
fn tool_delta_item_identity_maps_to_the_started_call() {
    let model = kolyan_model::ModelRef::new("openai", "test");
    let mut ids = BTreeMap::new();
    for index in 0..2 {
        let event = serde_json::from_value(json!({"type":"response.output_item.added","item":{"type":"function_call","id":format!("item-{index}"),"call_id":format!("call-{index}"),"name":"read"}})).unwrap();
        map_stream_event(event, &model, &mut ids).unwrap();
    }
    for index in (0..2).rev() {
        let event = serde_json::from_value(json!({"type":"response.function_call_arguments.delta","item_id":format!("item-{index}"),"delta":"{}"})).unwrap();
        let ModelEvent::ToolCallArgumentsDelta { id, .. } =
            map_stream_event(event, &model, &mut ids).unwrap()
        else {
            panic!("expected tool delta")
        };
        assert_eq!(id, format!("call-{index}"));
    }
}

#[test]
fn message_cache_breakpoint_is_inside_the_content_block() {
    let mut request: kolyan_model::ModelRequest = serde_json::from_value(json!({
        "request_id":"cache", "model":{"provider":"openai","model":"test"},
        "system":[], "messages":[{"role":"user","content":[{"type":"text","text":"prefix"}]}],
        "tools":[], "tool_choice":"auto", "output_format":null, "reasoning":null,
        "max_output_tokens":null, "extensions":null,
        "prompt_cache":{"key":null,"retention":null,"breakpoints":["messages"]}
    }))
    .unwrap();
    assert!(validate_request(&request).is_ok());
    let input = openai_input(&request);
    assert!(input[0].get("prompt_cache_breakpoint").is_none());
    assert_eq!(
        input[0]["content"][0]["prompt_cache_breakpoint"],
        json!({"mode":"explicit"})
    );
    request.reasoning = Some(kolyan_model::ReasoningConfig {
        effort: None,
        budget_tokens: Some(1024),
    });
    assert!(matches!(
        validate_request(&request).unwrap_err().kind,
        ProviderErrorKind::Unsupported
    ));
}

#[test]
fn cache_retention_matches_official_responses_wire_format() {
    let mut request: kolyan_model::ModelRequest = serde_json::from_value(json!({
        "request_id":"cache", "model":{"provider":"openai","model":"test"},
        "system":[], "messages":[], "tools":[], "tool_choice":"auto",
        "output_format":null, "reasoning":null, "max_output_tokens":null,
        "extensions":null,
        "prompt_cache":{"key":"stable", "retention":"in_memory", "breakpoints":[]}
    }))
    .unwrap();
    for (retention, expected) in [
        (kolyan_model::CacheRetention::InMemory, "in_memory"),
        (kolyan_model::CacheRetention::TwentyFourHours, "24h"),
    ] {
        request.prompt_cache.as_mut().unwrap().retention = Some(retention);
        let wire = serde_json::to_value(OpenAiProvider::request(&request)).unwrap();
        assert_eq!(wire["prompt_cache_retention"], expected);
        assert_eq!(wire["prompt_cache_key"], "stable");
        assert!(wire.get("prompt_cache_options").is_none());
    }
}

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
