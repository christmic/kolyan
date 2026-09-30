use kolyan_model::{
    CacheBreakpoint, ContentBlock, Message, MessageRole, ModelRef, PromptCacheConfig,
    SystemInstruction, ToolCall, ToolChoice, ToolDefinition, ToolResult,
};
use serde_json::json;

use super::*;

fn request() -> ModelRequest {
    ModelRequest {
        request_id: "context-request".into(),
        model: ModelRef::new("test-provider", "test-model"),
        system: vec![SystemInstruction {
            text: "Keep governing instructions.".into(),
            cache: true,
        }],
        messages: vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: "你好 🦀".into(),
            }],
        }],
        tools: vec![ToolDefinition {
            name: "file.read".into(),
            description: None,
            input_schema: json!({"type":"object"}),
        }],
        tool_choice: ToolChoice::Auto,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: Some(100),
        extensions: json!({}),
    }
}

fn descriptor() -> ModelDescriptor {
    ModelDescriptor {
        reference: request().model,
        context_window: Some(1000),
        max_output_tokens: Some(200),
        features: Default::default(),
    }
}

fn policy() -> ContextPolicy {
    ContextPolicy {
        id: "lossless-context".into(),
        revision: "1".into(),
        mode: BudgetMode::Strict,
        max_serialized_bytes: 65536,
        max_messages: 100,
        max_content_blocks: 200,
        context_limit_tokens: None,
        output_reserve_tokens: 100,
    }
}

struct TrustedCounter(u64);
impl ContextTokenCounter for TrustedCounter {
    fn id(&self) -> &str {
        "test-trusted-model-counter"
    }
    fn revision(&self) -> &str {
        "1"
    }
    fn count(&self, request: &ModelRequest, bytes: &[u8]) -> Result<TokenMeasurement, String> {
        assert_eq!(
            serde_json::from_slice::<ModelRequest>(bytes).unwrap(),
            *request
        );
        Ok(TokenMeasurement::Trusted {
            input_tokens: self.0,
        })
    }
}

struct FailingCounter;
impl ContextTokenCounter for FailingCounter {
    fn id(&self) -> &str {
        "test-failing-counter"
    }
    fn revision(&self) -> &str {
        "1"
    }
    fn count(&self, _: &ModelRequest, _: &[u8]) -> Result<TokenMeasurement, String> {
        Err("counter unavailable".into())
    }
}

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall {
        call: ToolCall {
            id: id.into(),
            name: "file.read".into(),
            arguments: json!({"path":"test"}),
        },
    }
}
fn result(id: &str) -> ContentBlock {
    ContentBlock::ToolResult {
        result: ToolResult {
            call_id: id.into(),
            content: "file contents".into(),
            is_error: false,
        },
    }
}
fn paired_request() -> ModelRequest {
    let mut request = request();
    request.messages.push(Message {
        role: MessageRole::Assistant,
        content: vec![
            ContentBlock::Reasoning {
                text: "Opaque reasoning must remain exact.".into(),
                opaque: Some(json!({"signature":"test-opaque-value"})),
            },
            call("call-1"),
            call("call-2"),
        ],
    });
    request.messages.push(Message {
        role: MessageRole::User,
        content: vec![result("call-2"), result("call-1")],
    });
    request
}

#[test]
fn trusted_budget_retains_every_section_and_deterministic_provenance() {
    let mut request = paired_request();
    request.prompt_cache = Some(PromptCacheConfig {
        key: Some("cache".into()),
        retention: None,
        breakpoints: vec![
            CacheBreakpoint::System,
            CacheBreakpoint::Tools,
            CacheBreakpoint::Messages,
        ],
    });
    let prepared =
        prepare_context(&request, &descriptor(), &policy(), &TrustedCounter(900)).unwrap();
    assert_eq!(prepared.request, request);
    assert_eq!(prepared.change, ContextChange::Unchanged);
    assert_eq!(
        prepared.budget,
        BudgetStatus::Verified { input_tokens: 900 }
    );
    assert_eq!(
        prepared.provenance.retained_messages,
        vec![RetainedMessageRange { start: 0, end: 3 }]
    );
    assert_eq!(
        prepared.provenance.source_digest,
        prepared.provenance.prepared_digest
    );
    assert_eq!(
        prepare_context(&request, &descriptor(), &policy(), &TrustedCounter(900)).unwrap(),
        prepared
    );
    let mut revised = policy();
    revised.revision = "2".into();
    assert_ne!(
        prepare_context(&request, &descriptor(), &revised, &TrustedCounter(900))
            .unwrap()
            .provenance
            .policy_digest,
        prepared.provenance.policy_digest
    );
}

#[test]
fn missing_output_limit_is_added_without_mutating_source() {
    let mut request = paired_request();
    request.max_output_tokens = None;
    let prepared =
        prepare_context(&request, &descriptor(), &policy(), &TrustedCounter(100)).unwrap();
    assert_eq!(request.max_output_tokens, None);
    assert_eq!(prepared.change, ContextChange::OutputLimitAdded);
    assert_eq!(prepared.request.max_output_tokens, Some(100));
    assert_eq!(request.messages, prepared.request.messages);
    assert_ne!(
        prepared.provenance.source_digest,
        prepared.provenance.prepared_digest
    );
}

#[test]
fn byte_estimate_never_passes_strict_token_admission() {
    assert!(matches!(
        prepare_context(
            &request(),
            &descriptor(),
            &policy(),
            &SerializedByteEstimator
        ),
        Err(ContextError::UnknownBudget {
            estimated_input_tokens: Some(_),
            ..
        })
    ));
    let mut inspection = policy();
    inspection.mode = BudgetMode::Inspect;
    let result = prepare_context(
        &request(),
        &descriptor(),
        &inspection,
        &SerializedByteEstimator,
    )
    .unwrap();
    assert!(matches!(
        result.budget,
        BudgetStatus::Unverified {
            estimated_input_tokens: Some(_),
            ..
        }
    ));
    assert_eq!(result.request, request());
}

#[test]
fn trusted_overflow_and_integer_overflow_fail_with_source_provenance() {
    for count in [901, u64::MAX] {
        match prepare_context(&request(), &descriptor(), &policy(), &TrustedCounter(count)) {
            Err(ContextError::Overflow {
                input_tokens,
                provenance,
            }) => {
                assert_eq!(input_tokens, count);
                assert_eq!(provenance.context_limit_tokens, 1000);
                assert_eq!(provenance.source_digest.len(), 64);
            }
            other => panic!("expected overflow, got {other:?}"),
        }
    }
}

#[test]
fn counter_errors_are_not_replaced_even_in_inspection_mode() {
    let mut policy = policy();
    policy.mode = BudgetMode::Inspect;
    assert!(matches!(
        prepare_context(&request(), &descriptor(), &policy, &FailingCounter),
        Err(ContextError::CounterFailed { .. })
    ));
}

#[test]
fn unknown_or_mismatched_model_limits_fail_and_host_limit_cannot_expand_model() {
    let mut model = descriptor();
    model.context_window = None;
    assert_eq!(
        prepare_context(&request(), &model, &policy(), &TrustedCounter(1)),
        Err(ContextError::UnknownModelLimit)
    );
    model = descriptor();
    model.reference.model = "other".into();
    assert!(prepare_context(&request(), &model, &policy(), &TrustedCounter(1)).is_err());
    let mut restricted = policy();
    restricted.context_limit_tokens = Some(500);
    assert!(matches!(
        prepare_context(&request(), &descriptor(), &restricted, &TrustedCounter(401)),
        Err(ContextError::Overflow { .. })
    ));
    restricted.context_limit_tokens = Some(2000);
    assert!(matches!(
        prepare_context(&request(), &descriptor(), &restricted, &TrustedCounter(901)),
        Err(ContextError::Overflow { .. })
    ));
}

#[test]
fn byte_and_structure_bounds_are_separate_from_tokens() {
    let request = request();
    let bytes = serde_json::to_vec(&request).unwrap().len();
    let mut bound = policy();
    bound.max_serialized_bytes = bytes;
    assert!(prepare_context(&request, &descriptor(), &bound, &TrustedCounter(1)).is_ok());
    bound.max_serialized_bytes -= 1;
    assert_eq!(
        prepare_context(&request, &descriptor(), &bound, &TrustedCounter(1)),
        Err(ContextError::SizeLimit)
    );
    bound = policy();
    bound.max_messages = 1;
    assert!(prepare_context(&paired_request(), &descriptor(), &bound, &TrustedCounter(1)).is_err());
    bound = policy();
    bound.max_content_blocks = 1;
    assert!(prepare_context(&paired_request(), &descriptor(), &bound, &TrustedCounter(1)).is_err());
}

#[test]
fn orphan_duplicate_unfinished_or_interleaved_tool_pairs_are_rejected() {
    let mut variants = Vec::new();
    let mut value = request();
    value.messages[0].content = vec![result("unknown")];
    variants.push(value);
    let mut value = paired_request();
    value.messages.pop();
    variants.push(value);
    let mut value = paired_request();
    value.messages[2].content.push(result("call-1"));
    variants.push(value);
    let mut value = paired_request();
    value.messages[1].content.push(call("call-1"));
    variants.push(value);
    let mut value = paired_request();
    value.messages[2].role = MessageRole::Assistant;
    variants.push(value);
    let mut value = paired_request();
    value.messages[2].content.insert(
        0,
        ContentBlock::Text {
            text: "Unrelated input".into(),
        },
    );
    variants.push(value);
    let mut value = request();
    value.messages[0].content = vec![call("call")];
    variants.push(value);
    for value in variants {
        assert!(matches!(
            prepare_context(&value, &descriptor(), &policy(), &TrustedCounter(1)),
            Err(ContextError::Invalid(_))
        ));
    }
}

#[test]
fn result_batches_can_span_user_messages_and_followed_text_is_preserved() {
    let mut value = paired_request();
    value.messages[2].content = vec![result("call-1")];
    value.messages.push(Message {
        role: MessageRole::User,
        content: vec![
            result("call-2"),
            ContentBlock::Text {
                text: "Continue".into(),
            },
        ],
    });
    assert_eq!(
        prepare_context(&value, &descriptor(), &policy(), &TrustedCounter(1))
            .unwrap()
            .request,
        value
    );
}

#[test]
fn reasoning_is_retained_opaque_and_wrong_role_fails() {
    let mut value = request();
    value.messages[0].content = vec![ContentBlock::Reasoning {
        text: "Wrong role".into(),
        opaque: None,
    }];
    assert!(prepare_context(&value, &descriptor(), &policy(), &TrustedCounter(1)).is_err());
    let source = paired_request();
    assert_eq!(
        prepare_context(&source, &descriptor(), &policy(), &TrustedCounter(1))
            .unwrap()
            .request
            .messages[1]
            .content,
        source.messages[1].content
    );
}

#[test]
fn nonexistent_cache_sections_are_rejected_not_silently_removed() {
    for breakpoint in [
        CacheBreakpoint::System,
        CacheBreakpoint::Tools,
        CacheBreakpoint::Messages,
    ] {
        let mut value = request();
        match breakpoint {
            CacheBreakpoint::System => value.system.clear(),
            CacheBreakpoint::Tools => value.tools.clear(),
            CacheBreakpoint::Messages => value.messages.clear(),
        }
        value.prompt_cache = Some(PromptCacheConfig {
            key: None,
            retention: None,
            breakpoints: vec![breakpoint],
        });
        assert!(prepare_context(&value, &descriptor(), &policy(), &TrustedCounter(1)).is_err());
    }
}

#[test]
fn invalid_policy_and_output_reserve_are_explicit_failures() {
    let mut bad = policy();
    bad.output_reserve_tokens = 201;
    assert!(prepare_context(&request(), &descriptor(), &bad, &TrustedCounter(1)).is_err());
    bad = policy();
    bad.max_serialized_bytes = 16 * 1024 * 1024 + 1;
    assert!(prepare_context(&request(), &descriptor(), &bad, &TrustedCounter(1)).is_err());
    bad = policy();
    bad.revision.clear();
    assert!(prepare_context(&request(), &descriptor(), &bad, &TrustedCounter(1)).is_err());
    let mut value = request();
    value.max_output_tokens = Some(101);
    assert!(prepare_context(&value, &descriptor(), &policy(), &TrustedCounter(1)).is_err());
}
