use super::*;
use crate::{ApprovalMode, Capability, Idempotency, PathScope, ToolManifest};
use kolyan_model::{MessageRole, ToolResult};
use serde_json::json;

fn call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "file.read".into(),
        arguments: json!({"path":"safe/a"}),
    }
}

fn history(outputs: &[&str]) -> Vec<Message> {
    outputs
        .iter()
        .enumerate()
        .flat_map(|(index, output)| {
            let call = call(&index.to_string());
            vec![
                Message {
                    role: MessageRole::Assistant,
                    content: vec![ContentBlock::ToolCall { call: call.clone() }],
                },
                Message {
                    role: MessageRole::User,
                    content: vec![ContentBlock::ToolResult {
                        result: ToolResult {
                            call_id: call.id,
                            content: (*output).into(),
                            is_error: false,
                        },
                    }],
                },
            ]
        })
        .collect()
}

#[test]
fn different_ids_do_not_hide_repetition_and_changed_results_reset_progress() {
    let policy = PolicyEngine::default()
        .with_progress_policy(ProgressPolicy {
            repeat_limit: 2,
            polling_tools: BTreeSet::new(),
        })
        .unwrap();
    for (outputs, stop) in [
        (vec!["a"], false),
        (vec!["a", "a"], true),
        (vec!["a", "b"], false),
        (vec!["a", "b", "b"], true),
    ] {
        assert_eq!(
            policy.has_no_progress(&call("different-id"), &history(&outputs)),
            stop
        );
    }
    let mut changed = call("another");
    changed.arguments = json!({"path":"safe/b"});
    assert!(!policy.has_no_progress(&changed, &history(&["a", "a"])));
}

#[test]
fn polling_is_an_explicit_read_only_policy_not_a_blanket_retry_permission() {
    let mut engine = PolicyEngine::default();
    engine.register(ToolManifest {
        tool_name: "file.read".into(),
        capabilities: [Capability::FilesystemRead].into_iter().collect(),
        effects: [Effect::Read].into_iter().collect(),
        path_scopes: vec![PathScope::new("safe")],
        idempotency: Idempotency::Idempotent,
        approval: ApprovalMode::Never,
    });
    let config = ProgressPolicy {
        repeat_limit: 2,
        polling_tools: ["file.read".into()].into_iter().collect(),
    };
    let engine = engine.with_progress_policy(config).unwrap();
    assert!(!engine.has_no_progress(&call("next"), &history(&["a", "a", "a"])));
    assert!(
        PolicyEngine::default()
            .with_progress_policy(ProgressPolicy {
                repeat_limit: 2,
                polling_tools: ["file.write".into()].into_iter().collect(),
            })
            .is_err()
    );
}
