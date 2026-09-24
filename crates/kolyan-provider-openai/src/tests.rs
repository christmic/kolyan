use super::*;
use futures_util::{StreamExt, stream};

#[tokio::test]
async fn synthetic_completion_preserves_text_and_structured_output() {
    let state = Arc::new(Mutex::new(CompletionState::default()));
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
