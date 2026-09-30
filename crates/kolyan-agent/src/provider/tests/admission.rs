use super::*;

#[test]
fn every_initial_and_subsequent_request_is_recorded_before_exact_forwarding() {
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
    let initial = source();
    let mut subsequent = initial.clone();
    subsequent.request_id = "step-1".into();
    subsequent.messages.extend([
        Message {
            role: MessageRole::Assistant,
            content: vec![ContentBlock::ToolCall {
                call: ToolCall {
                    id: "call".into(),
                    name: "file.read".into(),
                    arguments: json!({"path":"input"}),
                },
            }],
        },
        Message {
            role: MessageRole::User,
            content: vec![ContentBlock::ToolResult {
                result: ToolResult {
                    call_id: "call".into(),
                    content: "actual file result".into(),
                    is_error: false,
                },
            }],
        },
    ]);
    for request in [initial, subsequent] {
        drop(ready(provider.stream(request)).unwrap());
    }
    assert_eq!(
        *observations.order.lock().unwrap(),
        ["prepared", "provider", "prepared", "provider"]
    );
    let records = observations.records.lock().unwrap();
    let requests = observations.requests.lock().unwrap();
    assert_eq!(records.len(), 2);
    for (index, record) in records.iter().enumerate() {
        let ContextRecord::Prepared { source, prepared } = record else {
            panic!("expected prepared record")
        };
        assert_eq!(prepared.request, requests[index]);
        assert_eq!(source.max_output_tokens, Some(100));
        assert_eq!(prepared.request.max_output_tokens, Some(100));
        assert_eq!(source.messages, prepared.request.messages);
        assert_eq!(source.extensions, prepared.request.extensions);
        assert_eq!(source.tool_choice, prepared.request.tool_choice);
        assert_eq!(prepared.provenance.source_digest.len(), 64);
        assert_eq!(
            prepared.provenance.source_digest,
            prepared.provenance.prepared_digest
        );
    }
}

#[test]
fn unknown_counts_require_explicit_inspection_and_strict_never_downgrades() {
    for mode in [BudgetMode::Strict, BudgetMode::Inspect] {
        let observations = Arc::new(Observations::default());
        let mut configured = policy();
        configured.mode = mode;
        let provider = wrapper(
            observations.clone(),
            descriptor(),
            configured,
            Arc::new(SerializedByteEstimator),
            false,
            false,
        );
        let result = ready(provider.stream(source()));
        let records = observations.records.lock().unwrap();
        if mode == BudgetMode::Strict {
            assert!(result.is_err());
            assert!(observations.requests.lock().unwrap().is_empty());
            assert!(
                matches!(&records[0], ContextRecord::Rejected { failure, .. } if matches!(failure.as_ref(), ContextError::UnknownBudget { .. }))
            );
        } else {
            drop(result.unwrap());
            assert!(
                matches!(&records[0], ContextRecord::Prepared { prepared, .. } if matches!(prepared.budget, BudgetStatus::Unverified { .. }))
            );
            assert_eq!(observations.requests.lock().unwrap().len(), 1);
        }
    }
}

#[test]
fn preparation_failures_record_actual_source_and_block_provider() {
    for case in 0..8 {
        let observations = Arc::new(Observations::default());
        let mut request = source();
        let mut model = descriptor();
        let mut configured = policy();
        let mut counter = Counter {
            tokens: 10,
            fail: false,
        };
        match case {
            0 => request.model.model = "wrong-model".into(),
            1 => model.context_window = None,
            2 => configured.max_serialized_bytes = 1,
            3 => request.max_output_tokens = Some(101),
            4 => counter.tokens = 901,
            5 => counter.fail = true,
            6 => configured.revision.clear(),
            7 => {
                request.messages[0].content = vec![ContentBlock::ToolResult {
                    result: ToolResult {
                        call_id: "orphan".into(),
                        content: "unpaired".into(),
                        is_error: false,
                    },
                }]
            }
            _ => unreachable!(),
        }
        let provider = wrapper(
            observations.clone(),
            model,
            configured,
            Arc::new(counter),
            false,
            false,
        );
        let error = match ready(provider.stream(request.clone())) {
            Err(error) => error,
            Ok(_) => panic!("expected blocked call"),
        };
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(error.phase, ProviderErrorPhase::Validate);
        assert!(observations.requests.lock().unwrap().is_empty());
        assert_eq!(*observations.order.lock().unwrap(), ["rejected"]);
        let records = observations.records.lock().unwrap();
        let ContextRecord::Rejected {
            source, failure, ..
        } = &records[0]
        else {
            panic!("expected rejection")
        };
        assert_eq!(source, &request);
        if case == 4 {
            assert!(
                matches!(failure.as_ref(), ContextError::Overflow { provenance, .. } if provenance.source_digest.len() == 64)
            );
        }
        if case == 5 {
            assert!(matches!(
                failure.as_ref(),
                ContextError::CounterFailed { .. }
            ));
        }
    }
}

#[test]
fn missing_output_limit_records_rejected_projection_and_never_calls_provider() {
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
    let mut request = source();
    request.max_output_tokens = None;
    let error = match ready(provider.stream(request.clone())) {
        Err(error) => error,
        Ok(_) => panic!("projection must not reach provider"),
    };
    assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
    assert_eq!(error.phase, ProviderErrorPhase::Validate);
    assert!(error.message.contains("projection is forbidden"));
    assert!(observations.requests.lock().unwrap().is_empty());
    assert_eq!(*observations.order.lock().unwrap(), ["rejected"]);
    let records = observations.records.lock().unwrap();
    let ContextRecord::Rejected {
        source,
        failure,
        preparation: Some(prepared),
    } = &records[0]
    else {
        panic!("expected failed projection evidence")
    };
    assert_eq!(source, &request);
    assert!(matches!(failure.as_ref(), ContextError::Invalid(_)));
    assert_eq!(prepared.request.max_output_tokens, Some(100));
    assert_ne!(
        prepared.provenance.source_digest,
        prepared.provenance.prepared_digest
    );
}

#[test]
fn recorder_failure_also_blocks_rejected_projection() {
    let observations = Arc::new(Observations::default());
    let provider = wrapper(
        observations.clone(),
        descriptor(),
        policy(),
        Arc::new(Counter {
            tokens: 10,
            fail: false,
        }),
        true,
        false,
    );
    let mut request = source();
    request.max_output_tokens = None;
    let error = match ready(provider.stream(request)) {
        Err(error) => error,
        Ok(_) => panic!("recorder failure must block projection"),
    };
    assert!(error.message.contains("evidence store refused"));
    assert_eq!(error.phase, ProviderErrorPhase::Validate);
    assert!(observations.requests.lock().unwrap().is_empty());
}

#[test]
fn recording_failures_block_both_prepared_and_rejected_paths() {
    for invalid in [false, true] {
        let observations = Arc::new(Observations::default());
        let mut request = source();
        if invalid {
            request.model.model = "mismatch".into();
        }
        let provider = wrapper(
            observations.clone(),
            descriptor(),
            policy(),
            Arc::new(Counter {
                tokens: 10,
                fail: false,
            }),
            true,
            false,
        );
        let error = match ready(provider.stream(request)) {
            Err(error) => error,
            Ok(_) => panic!("recorder failure must block"),
        };
        assert!(error.message.contains("evidence store refused"));
        assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
        assert!(observations.requests.lock().unwrap().is_empty());
        assert_eq!(observations.records.lock().unwrap().len(), 1);
    }
}
