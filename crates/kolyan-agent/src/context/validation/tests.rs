use kolyan_model::{Message, ModelRef, ToolCall, ToolChoice, ToolResult};
use serde_json::json;

use super::*;
use crate::context::{BudgetMode, SerializedByteEstimator, prepare_context};

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall {
        call: ToolCall {
            id: id.into(),
            name: "file.read".into(),
            arguments: json!({"path":"input"}),
        },
    }
}

fn result(id: &str) -> ContentBlock {
    ContentBlock::ToolResult {
        result: ToolResult {
            call_id: id.into(),
            content: "observed content".into(),
            is_error: false,
        },
    }
}

fn request(messages: Vec<Message>) -> ModelRequest {
    ModelRequest {
        request_id: "reused-call-id".into(),
        model: ModelRef::new("test", "test"),
        system: Vec::new(),
        messages,
        tools: Vec::new(),
        tool_choice: ToolChoice::Auto,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: Some(100),
        extensions: json!({}),
    }
}

fn policy() -> ContextPolicy {
    ContextPolicy {
        id: "reuse-regression".into(),
        revision: "1".into(),
        mode: BudgetMode::Inspect,
        max_serialized_bytes: 65536,
        max_messages: 100,
        max_content_blocks: 200,
        context_limit_tokens: None,
        output_reserve_tokens: 100,
    }
}

#[test]
fn completed_batches_allow_reused_call_ids_and_preserve_all_context() {
    let mut messages = Vec::new();
    for _ in 0..3 {
        messages.push(Message {
            role: MessageRole::Assistant,
            content: vec![call("reused-1"), call("reused-2")],
        });
        // Results may arrive in separate user messages and in reversed order.
        messages.push(Message {
            role: MessageRole::User,
            content: vec![result("reused-2")],
        });
        messages.push(Message {
            role: MessageRole::User,
            content: vec![result("reused-1")],
        });
    }
    let source = request(messages);
    let descriptor = kolyan_model::ModelDescriptor {
        reference: source.model.clone(),
        context_window: Some(10000),
        max_output_tokens: Some(100),
        features: Default::default(),
    };
    let prepared =
        prepare_context(&source, &descriptor, &policy(), &SerializedByteEstimator).unwrap();
    assert_eq!(prepared.request, source);
    assert_eq!(
        prepared.provenance.source_digest,
        prepared.provenance.prepared_digest
    );
}

#[test]
fn duplicate_active_calls_still_fail_after_an_earlier_batch_completes() {
    let source = request(vec![
        Message {
            role: MessageRole::Assistant,
            content: vec![call("reused")],
        },
        Message {
            role: MessageRole::User,
            content: vec![result("reused")],
        },
        Message {
            role: MessageRole::Assistant,
            content: vec![call("reused"), call("reused")],
        },
        Message {
            role: MessageRole::User,
            content: vec![result("reused")],
        },
    ]);
    assert!(matches!(
        messages(&source, &policy()),
        Err(ContextError::Invalid(_))
    ));
}

#[test]
fn reuse_cannot_advance_the_assistant_while_another_result_is_pending() {
    let source = request(vec![
        Message {
            role: MessageRole::Assistant,
            content: vec![call("reused"), call("pending")],
        },
        Message {
            role: MessageRole::User,
            content: vec![result("reused")],
        },
        Message {
            role: MessageRole::Assistant,
            content: vec![call("reused")],
        },
    ]);
    assert!(matches!(
        messages(&source, &policy()),
        Err(ContextError::Invalid(_))
    ));
}
