use std::io::Write;

use kolyan_model::{ContentBlock, Message, MessageRole, ModelRef, ToolCall, ToolResult};
use serde::Deserialize;
use serde_json::json;

use super::*;
use crate::context::{BudgetMode, BudgetStatus, SerializedByteEstimator};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    retained: Vec<RetainedMessageRange>,
    mutation: String,
    expected: String,
    selected_messages: Option<usize>,
}

#[test]
fn projections_export_before_comparing_source_pairing_and_budget_contracts() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-context-projection-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut output = std::fs::File::create(&path).unwrap();
    println!("CONTEXT_PROJECTION_TRACE={}", path.display());
    for case in cases {
        let mut source = source();
        let mut bounds = policy();
        let mut target = bounds.clone();
        target.mode = BudgetMode::Inspect;
        let mut plan = ContextProjectionPlan {
            policy_id: "completed-history-selection".into(),
            policy_revision: "1".into(),
            expected_source_digest: context_source_digest(&source, &bounds).unwrap(),
            retained_messages: case.retained,
        };
        match case.mutation.as_str() {
            "none" => {}
            "digest" => plan.expected_source_digest = "wrong-source".into(),
            "strict" => target.mode = BudgetMode::Strict,
            "target_size" => target.max_serialized_bytes = 1,
            "source_size" => bounds.max_serialized_bytes = 1,
            "source_messages" => bounds.max_messages = 1,
            "no_anchor" => {
                source.messages = vec![Message {
                    role: MessageRole::Assistant,
                    content: vec![ContentBlock::Text {
                        text: "No objective.".into(),
                    }],
                }];
                plan.expected_source_digest = context_source_digest(&source, &bounds).unwrap();
            }
            "output_missing" => {
                source.max_output_tokens = None;
                plan.expected_source_digest = context_source_digest(&source, &bounds).unwrap();
            }
            "source_dangling" => source.messages[2].content = vec![result("unknown-call")],
            other => panic!("unknown mutation {other}"),
        }
        let original = source.clone();
        let projected = project_context(
            &source,
            &descriptor(),
            &bounds,
            &plan,
            &target,
            &SerializedByteEstimator,
        );
        let observed = match &projected {
            Ok(_) => "passed",
            Err(ContextError::UnknownBudget { .. }) => "unknown_budget",
            Err(ContextError::SizeLimit) => "size_limit",
            Err(ContextError::Invalid(_)) => "invalid",
            Err(error) => panic!("unexpected error {error}"),
        };
        writeln!(output, "{}", json!({"case":case.id,"source":source,"plan":plan,"target_policy":target,"projected":projected.as_ref().ok(),"error":projected.as_ref().err().map(ToString::to_string),"observed":observed,"expected":case.expected})).unwrap();
        output.flush().unwrap();
        assert_eq!(observed, case.expected, "case={}", case.id);
        assert_eq!(source, original, "source mutation in {}", case.id);
        if let Ok(projected) = projected {
            assert_eq!(
                projected.prepared.request.messages.len(),
                case.selected_messages.unwrap()
            );
            assert!(matches!(
                projected.prepared.budget,
                BudgetStatus::Unverified { .. }
            ));
            assert_eq!(projected.prepared.request.system, source.system);
            assert_eq!(projected.prepared.request.tools, source.tools);
            assert_eq!(projected.prepared.request.extensions, source.extensions);
            let reloaded_source: ModelRequest =
                serde_json::from_value(serde_json::to_value(&source).unwrap()).unwrap();
            let reloaded_plan: ContextProjectionPlan =
                serde_json::from_value(serde_json::to_value(&plan).unwrap()).unwrap();
            assert_eq!(
                project_context(
                    &reloaded_source,
                    &descriptor(),
                    &bounds,
                    &reloaded_plan,
                    &target,
                    &SerializedByteEstimator
                )
                .unwrap(),
                projected
            );
            if projected.provenance.omitted_messages.is_empty() {
                assert_eq!(projected.prepared.request, source);
            } else {
                assert!(
                    projected.provenance.selected_serialized_bytes
                        < projected.provenance.source_serialized_bytes
                );
                assert_ne!(
                    projected.provenance.selected_digest,
                    projected.provenance.source_digest
                );
            }
        }
    }
}

#[test]
fn projection_wire_rejects_unknown_plan_and_range_fields() {
    let base = json!({"policy_id":"selection","policy_revision":"1","expected_source_digest":"source","retained_messages":[{"start":0,"end":1}]});
    let mut extra_plan = base.clone();
    extra_plan["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ContextProjectionPlan>(extra_plan).is_err());
    let mut extra_range = base;
    extra_range["retained_messages"][0]["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ContextProjectionPlan>(extra_range).is_err());
}

fn source() -> ModelRequest {
    let mut source: ModelRequest = serde_json::from_value(json!({
        "request_id":"projection-request","model":{"provider":"test","model":"test"},
        "system":[{"text":"Preserve governing instructions.","cache":true}],
        "messages":[{"role":"user","content":[{"type":"text","text":"Original objective."}]}],
        "tools":[{"name":"file.read","description":null,"input_schema":{"type":"object"}}],
        "tool_choice":"auto","max_output_tokens":100,"extensions":{"test":true}
    }))
    .unwrap();
    let message = |role, content| Message { role, content };
    let text = |value: &str| ContentBlock::Text { text: value.into() };
    source.messages.extend([
        message(MessageRole::Assistant, vec![call("reused-id")]),
        message(MessageRole::User, vec![result("reused-id")]),
        message(
            MessageRole::Assistant,
            vec![text("First completed operation.")],
        ),
        message(MessageRole::Assistant, vec![call("reused-id")]),
        message(MessageRole::User, vec![result("reused-id")]),
        message(
            MessageRole::Assistant,
            vec![text("Second completed operation.")],
        ),
        message(
            MessageRole::User,
            vec![text("Continue the original objective.")],
        ),
        message(MessageRole::Assistant, vec![call("current-id")]),
        message(MessageRole::User, vec![result("current-id")]),
    ]);
    source
}

fn descriptor() -> ModelDescriptor {
    ModelDescriptor {
        reference: ModelRef::new("test", "test"),
        context_window: Some(1000),
        max_output_tokens: Some(200),
        features: Default::default(),
    }
}

fn policy() -> ContextPolicy {
    ContextPolicy {
        id: "projection-context".into(),
        revision: "1".into(),
        mode: BudgetMode::Inspect,
        max_serialized_bytes: 65536,
        max_messages: 100,
        max_content_blocks: 200,
        context_limit_tokens: None,
        output_reserve_tokens: 100,
    }
}

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall {
        call: ToolCall {
            id: id.into(),
            name: "file.read".into(),
            arguments: json!({"path":"safe/proof.txt"}),
        },
    }
}

fn result(id: &str) -> ContentBlock {
    ContentBlock::ToolResult {
        result: ToolResult {
            call_id: id.into(),
            content: "Actual completed result.".into(),
            is_error: false,
        },
    }
}
