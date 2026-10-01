use super::*;
use kolyan_core::{StepOutcome, StepResult};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelRef, ModelResponse, StopReason, TokenUsage,
};
use serde_json::json;

#[test]
fn versioned_turn_input_survives_reopen_and_serializes_pending_turns() {
    let root =
        std::env::temp_dir().join(format!("kolyan-session-versioned-{}", std::process::id()));
    let store = FileSessionStore::new(&root).unwrap();
    store.create("s").unwrap();
    let turn = SessionTurn {
        turn_id: "t".into(),
        execution_id: "e".into(),
        status: SessionTurnStatus::Running,
    };
    let input = vec![Message {
        role: MessageRole::User,
        content: vec![ContentBlock::Text {
            text: "original input".into(),
        }],
    }];
    assert!(
        store
            .begin_turn_with_input("s", turn.clone(), 1, input.clone())
            .is_err()
    );
    store
        .begin_turn_with_input("s", turn, 0, input.clone())
        .unwrap();
    store
        .update_turn("s", "t", SessionTurnStatus::Suspended, vec![])
        .unwrap();
    let other = FileSessionStore::new(&root).unwrap();
    assert!(
        other
            .begin_turn_with_input(
                "s",
                SessionTurn {
                    turn_id: "next".into(),
                    execution_id: "next-e".into(),
                    status: SessionTurnStatus::Running
                },
                2,
                vec![]
            )
            .is_err()
    );
    assert_eq!(other.load("s").unwrap().inputs["t"].messages, input);
    let committed = other
        .update_turn("s", "t", SessionTurnStatus::Completed, input.clone())
        .unwrap();
    assert_eq!(
        other
            .update_turn("s", "t", SessionTurnStatus::Completed, input)
            .unwrap(),
        committed
    );
    assert!(
        other
            .update_turn("s", "t", SessionTurnStatus::Failed, vec![])
            .is_err()
    );
    assert_eq!(other.load("s").unwrap(), committed);
    fs::remove_dir_all(root).unwrap();
}

fn request() -> ApprovalRequest {
    ApprovalRequest {
        approval_id: "approval-1".into(),
        turn_id: "turn-1".into(),
        call_id: "call-1".into(),
        tool_name: "file.write".into(),
        reason: "requires approval".into(),
        state: kolyan_core::ApprovalState::Pending,
        expires_at_ms: None,
        continuation: kolyan_core::TurnContinuation {
            continuation_id: "continuation-1".into(),
            approval_id: "approval-1".into(),
            turn_id: "turn-1".into(),
            model_request: serde_json::from_value(json!({
                "request_id":"r", "model":{"provider":"p","model":"m"},
                "system":[], "messages":[], "tools":[], "tool_choice":"auto",
                "output_format":null, "prompt_cache":null, "reasoning":null,
                "max_output_tokens":null, "extensions":{}
            }))
            .unwrap(),
            assistant_content: vec![],
            pending_calls: vec![],
            steps: vec![StepResult {
                step_id: "s".into(),
                response: ModelResponse {
                    id: "r".into(),
                    model: ModelRef {
                        provider: "p".into(),
                        model: "m".into(),
                    },
                    content: vec![],
                    structured_output: None,
                    stop_reason: StopReason::ToolUse,
                    usage: TokenUsage::default(),
                    metadata: json!({}),
                },
                outcome: StepOutcome::ToolCalls,
            }],
            max_steps: 2,
            next_step_index: 1,
            call_id: "call-1".into(),
            tool_name: "file.write".into(),
            args_fingerprint: "fp".into(),
            policy_version: "v1".into(),
            // Storage round-trips this opaque fixture; it is not executable authority.
            prepared_calls: Vec::new(),
            preparation_errors: Vec::new(),
            execution_scope: serde_json::from_value(json!({
                "execution": {"session_id":"session-1", "turn_id":"turn-1", "execution_id":"execution-1"},
                "step_id":"turn-1-step-0", "agent_snapshot_digest":null
            })).unwrap(),
            approved_call_ids: Vec::new(),
            max_tool_calls: None,
            tool_calls_used: 0,
            deadline_at_ms: None,
            tool_dispatch: Default::default(),
            tool_timeout_ms: None,
        },
    }
}

#[test]
fn file_store_round_trips_and_deletes() {
    let root = std::env::temp_dir().join(format!("kolyan-storage-{}", std::process::id()));
    let store = FileApprovalStore::new(&root).unwrap();
    let value = request();
    store.save(&value).unwrap();
    assert_eq!(store.load("approval-1").unwrap(), value);
    assert_eq!(store.claim("approval-1").unwrap(), value);
    assert!(matches!(
        store.claim("approval-1"),
        Err(StorageError::NotFound(_))
    ));
    store.delete("approval-1").unwrap();
    assert!(matches!(
        store.load("approval-1"),
        Err(StorageError::NotFound(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn file_store_rejects_path_traversal() {
    let store =
        FileApprovalStore::new(std::env::temp_dir().join("kolyan-storage-invalid")).unwrap();
    assert!(matches!(
        store.load("../escape"),
        Err(StorageError::InvalidId)
    ));
}

#[test]
fn file_store_rejects_corrupt_checkpoint() {
    let root = std::env::temp_dir().join(format!("kolyan-storage-corrupt-{}", std::process::id()));
    let store = FileApprovalStore::new(&root).unwrap();
    fs::write(root.join("approval-corrupt.json"), b"{not-json")
        .expect("corrupt fixture should be written");
    assert!(matches!(
        store.load("approval-corrupt"),
        Err(StorageError::Serialization(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn session_store_reopens_and_preserves_ordered_turns_and_messages() {
    let root = std::env::temp_dir().join(format!("kolyan-session-{}", std::process::id()));
    let first = FileSessionStore::new(&root).unwrap();
    assert_eq!(first.create("session-1").unwrap().version, 0);
    let message_one = Message {
        role: MessageRole::User,
        content: vec![ContentBlock::Text {
            text: "first turn".into(),
        }],
    };
    let message_two = Message {
        role: MessageRole::Assistant,
        content: vec![ContentBlock::Text {
            text: "first answer".into(),
        }],
    };
    let after_first = first
        .append_turn(
            "session-1",
            SessionTurn {
                turn_id: "turn-1".into(),
                execution_id: "execution-1".into(),
                status: SessionTurnStatus::Completed,
            },
            vec![message_one.clone(), message_two.clone()],
        )
        .unwrap();
    assert_eq!(after_first.version, 1);
    let message_three = Message {
        role: MessageRole::User,
        content: vec![ContentBlock::Text {
            text: "second turn".into(),
        }],
    };
    first
        .append_turn(
            "session-1",
            SessionTurn {
                turn_id: "turn-2".into(),
                execution_id: "execution-2".into(),
                status: SessionTurnStatus::Running,
            },
            vec![message_three.clone()],
        )
        .unwrap();
    drop(first);

    let reopened = FileSessionStore::new(&root).unwrap();
    let record = reopened.load("session-1").unwrap();
    assert_eq!(record.version, 2);
    assert_eq!(
        record
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-1", "turn-2"]
    );
    assert_eq!(
        record.messages,
        vec![message_one, message_two, message_three]
    );
    assert!(matches!(
        reopened.append_turn(
            "session-1",
            SessionTurn {
                turn_id: "turn-1".into(),
                execution_id: "execution-duplicate".into(),
                status: SessionTurnStatus::Completed,
            },
            Vec::new(),
        ),
        Err(StorageError::Conflict(_))
    ));
    fs::remove_dir_all(root).unwrap();
}
