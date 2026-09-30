use super::*;

#[test]
fn underlying_open_failure_keeps_original_fields_and_is_not_retried() {
    let observations = Arc::new(Observations::default());
    let provider = wrapper(
        observations.clone(),
        descriptor(),
        policy(),
        Arc::new(Counter {
            tokens: 10,
            fail: false,
        }),
        false,
        true,
    );
    let error = match ready(provider.stream(source())) {
        Err(error) => error,
        Ok(_) => panic!("expected inner open error"),
    };
    assert_eq!(error.kind, ProviderErrorKind::RateLimited);
    assert_eq!(error.phase, ProviderErrorPhase::Open);
    assert_eq!(error.provider.as_deref(), Some("test-provider"));
    assert_eq!(error.status, Some(429));
    assert_eq!(error.message, "original provider error");
    assert_eq!(observations.requests.lock().unwrap().len(), 1);
}

#[test]
fn events_errors_and_stream_drop_pass_through_without_consumption() {
    let observations = Arc::new(Observations::default());
    let provider = wrapper(
        observations.clone(),
        descriptor(),
        policy(),
        Arc::new(Counter {
            tokens: 10,
            fail: false,
        }),
        false,
        false,
    );
    let mut stream = ready(provider.stream(source())).unwrap();
    assert_eq!(next(&mut stream).unwrap().unwrap(), ModelEvent::Started);
    assert_eq!(
        next(&mut stream).unwrap().unwrap(),
        ModelEvent::ReasoningDelta("reasoning chunk".into())
    );
    assert_eq!(
        next(&mut stream).unwrap().unwrap(),
        ModelEvent::TextDelta("text chunk".into())
    );
    assert!(matches!(
        next(&mut stream).unwrap().unwrap(),
        ModelEvent::ToolCallStarted { .. }
    ));
    assert!(matches!(
        next(&mut stream).unwrap().unwrap(),
        ModelEvent::ToolCallArgumentsDelta { .. }
    ));
    assert!(matches!(
        next(&mut stream).unwrap().unwrap(),
        ModelEvent::ToolCallCompleted(_)
    ));
    let error = next(&mut stream).unwrap().unwrap_err();
    assert_eq!(error.phase, ProviderErrorPhase::Stream);
    assert_eq!(error.status, Some(429));
    assert_eq!(error.provider.as_deref(), Some("test-provider"));
    assert_eq!(error.message, "original provider error");
    assert!(next(&mut stream).is_none());
    drop(stream);
    assert_eq!(observations.stream_drops.load(Ordering::SeqCst), 1);
    assert_eq!(observations.requests.lock().unwrap().len(), 1);
}

#[test]
fn dropped_unpolled_open_future_has_no_recording_or_provider_effects() {
    let observations = Arc::new(Observations::default());
    let provider = wrapper(
        observations.clone(),
        descriptor(),
        policy(),
        Arc::new(Counter {
            tokens: 10,
            fail: false,
        }),
        false,
        false,
    );
    drop(provider.stream(source()));
    assert!(observations.records.lock().unwrap().is_empty());
    assert!(observations.requests.lock().unwrap().is_empty());
}

struct SuppliedStreamProvider {
    stream: Mutex<Option<ModelEventStream>>,
    observations: Arc<Observations>,
}

impl ModelProvider for SuppliedStreamProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.observations.order.lock().unwrap().push("provider");
        self.observations.requests.lock().unwrap().push(request);
        let stream = self.stream.lock().unwrap().take().unwrap();
        Box::pin(async move { Ok(stream) })
    }
}

#[test]
fn usage_metadata_and_completed_response_are_returned_verbatim() {
    use kolyan_model::{ModelResponse, ProviderMetadata, StopReason, TokenUsage};

    let observations = Arc::new(Observations::default());
    let usage = TokenUsage {
        input_tokens: Some(11),
        output_tokens: Some(7),
        reasoning_tokens: Some(3),
        ..Default::default()
    };
    let expected = vec![
        ModelEvent::Usage(usage.clone()),
        ModelEvent::Provider(ProviderMetadata {
            provider: "test-provider".into(),
            raw: Some(json!({"opaque":"keep-exact"})),
        }),
        ModelEvent::Completed(ModelResponse {
            id: "final-response".into(),
            model: source().model,
            content: vec![ContentBlock::Text {
                text: "Completed content".into(),
            }],
            structured_output: Some(json!({"done":true})),
            stop_reason: StopReason::EndTurn,
            usage,
            metadata: json!({"finish_reason":"stop"}),
        }),
    ];
    let stream = Box::pin(EventStream {
        events: expected.iter().cloned().map(Ok).collect(),
        drops: observations.stream_drops.clone(),
    }) as ModelEventStream;
    let inner = SuppliedStreamProvider {
        stream: Mutex::new(Some(stream)),
        observations: observations.clone(),
    };
    let provider = ContextPreparingProvider::new(
        inner,
        descriptor(),
        policy(),
        Arc::new(Counter {
            tokens: 10,
            fail: false,
        }),
        Arc::new(Recorder {
            observations: observations.clone(),
            fail: false,
        }),
    );
    let mut actual = ready(provider.stream(source())).unwrap();
    for event in expected {
        assert_eq!(next(&mut actual).unwrap().unwrap(), event);
    }
    assert!(next(&mut actual).is_none());
    drop(actual);
    assert_eq!(observations.stream_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        *observations.order.lock().unwrap(),
        ["prepared", "provider"]
    );
}
