use kolyan_model::{ContentBlock, Message, MessageRole};
use kolyan_storage::{FileSessionStore, SessionStore, SessionTurn, SessionTurnStatus};
use std::fs;

#[test]
fn two_independent_turns_reopen_with_ordered_context() {
    let root =
        std::env::temp_dir().join(format!("kolyan-session-integration-{}", std::process::id()));
    let store = FileSessionStore::new(&root).unwrap();
    store.create("session-integration").unwrap();
    store
        .append_turn(
            "session-integration",
            SessionTurn {
                turn_id: "turn-1".into(),
                execution_id: "execution-1".into(),
                status: SessionTurnStatus::Completed,
            },
            vec![Message {
                role: MessageRole::User,
                content: vec![ContentBlock::Text {
                    text: "first request".into(),
                }],
            }],
        )
        .unwrap();
    store
        .append_turn(
            "session-integration",
            SessionTurn {
                turn_id: "turn-2".into(),
                execution_id: "execution-2".into(),
                status: SessionTurnStatus::Completed,
            },
            vec![Message {
                role: MessageRole::Assistant,
                content: vec![ContentBlock::Text {
                    text: "second answer".into(),
                }],
            }],
        )
        .unwrap();
    drop(store);

    let reopened = FileSessionStore::new(&root).unwrap();
    let session = reopened.load("session-integration").unwrap();
    assert_eq!(session.version, 2);
    assert_eq!(session.turns.len(), 2);
    assert_eq!(session.turns[0].turn_id, "turn-1");
    assert_eq!(session.turns[1].turn_id, "turn-2");
    assert_eq!(session.messages.len(), 2);
    fs::remove_dir_all(root).unwrap();
}
