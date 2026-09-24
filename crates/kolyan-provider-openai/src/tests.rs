use super::*;

#[test]
fn terminal_does_not_poll_or_wait_for_the_next_network_event() {
    use futures_util::FutureExt;
    let response = map_response(
        &json!({"id":"done","output":[]}),
        &kolyan_model::ModelRef::new("test", "model"),
    )
    .unwrap();
    for event in [
        Ok(ModelEvent::Completed(response)),
        Err(provider_error("fatal")),
    ] {
        let input = stream::iter(vec![event]).chain(stream::pending());
        let results = require_terminal(input)
            .collect::<Vec<_>>()
            .now_or_never()
            .expect("terminal must not wait for the server to close the connection");
        assert_eq!(results.len(), 1);
    }
}

#[test]
fn tool_calls_require_identity_and_name() {
    for value in [
        json!({"arguments":"{}"}),
        json!({"call_id":"call","arguments":"{}"}),
        json!({"name":"read","arguments":"{}"}),
    ] {
        assert!(map_tool_call(&value).is_err());
    }
}

#[test]
fn official_error_event_is_fatal_and_refusal_is_not_an_empty_success() {
    let model = kolyan_model::ModelRef::new("openai", "test");
    let error =
        serde_json::from_value(json!({"type":"error","code":"server_error","message":"failed"}))
            .unwrap();
    assert!(map_event(error, &model).is_err());
    let delta =
        serde_json::from_value(json!({"type":"response.refusal.delta","delta":"Cannot help"}))
            .unwrap();
    assert!(
        matches!(map_event(delta, &model).unwrap(), ModelEvent::TextDelta(text) if text == "Cannot help")
    );
    let response = map_response(&json!({"id":"refused","output":[{"type":"message","content":[{"type":"refusal","refusal":"Cannot help"}]}]}), &model).unwrap();
    assert_eq!(response.stop_reason, StopReason::Refusal);
    assert_eq!(
        response.content,
        vec![ContentBlock::Text {
            text: "Cannot help".into()
        }]
    );
}

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
        assert_eq!(parse_json(text), None);
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
async fn eof_never_synthesizes_success_even_after_text() {
    for upstream_error in [false, true] {
        let mut events = vec![Ok(ModelEvent::TextDelta("{\"ok\":true}".into()))];
        if upstream_error {
            events.push(Err(provider_error("upstream")));
        }
        let results = require_terminal(stream::iter(events))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(results.iter().filter(|event| event.is_err()).count(), 1);
        assert!(
            !results
                .iter()
                .any(|event| matches!(event, Ok(ModelEvent::Completed(_))))
        );
    }
}

#[test]
fn completion_recovers_only_finalized_items_and_preserves_explicit_empty_output() {
    let mut items = BTreeMap::new();
    let item = json!({"type":"message","status":"completed","content":[{"type":"output_text","text":"{}"}]});
    let mut done = serde_json::from_value(
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
    )
    .unwrap();
    recover_finalized_output(&mut done, &mut items).unwrap();
    for output in [None, Some(Value::Null), Some(json!([]))] {
        let mut response = json!({"id":"response"});
        if let Some(output) = output.clone() {
            response["output"] = output;
        }
        let mut completed =
            serde_json::from_value(json!({"type":"response.completed","response":response}))
                .unwrap();
        recover_finalized_output(&mut completed, &mut items).unwrap();
        assert_eq!(
            completed.fields["response"]["output"],
            if output == Some(json!([])) {
                json!([])
            } else {
                json!([item])
            }
        );
    }
}

#[test]
fn malformed_completed_response_is_rejected_without_fabrication_or_panic() {
    for response in [Value::Null, json!("invalid"), json!([]), json!(1)] {
        let mut event =
            serde_json::from_value(json!({"type":"response.completed","response":response}))
                .unwrap();
        assert!(recover_finalized_output(&mut event, &mut BTreeMap::new()).is_err());
    }
}

#[test]
fn no_fences_prose_or_fabricated_parsed_fields_are_accepted() {
    for text in ["```json\n{}\n```", "answer: {}", "{} trailing"] {
        assert_eq!(parse_json(text), None);
    }
    assert_eq!(
        extract_structured_output(&json!({"parsed":{},"output_text":"{}","output":[]})),
        None
    );
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
