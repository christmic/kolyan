use super::*;
use kolyan_model::{ContentBlock, MessageRole};
use std::sync::Barrier;

fn initial(label: &str) -> SessionInitialization {
    SessionInitialization {
        binding_digest: "a".repeat(64),
        messages: vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text { text: label.into() }],
        }],
    }
}

#[test]
fn initialization_commits_complete_context_and_reopens_exactly() {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    let input = initial("selected parent context, not unrelated history");
    let saved = store.initialize("child", &input).unwrap();
    assert_eq!(saved.initialization, Some(input.clone()));
    assert_eq!(saved.messages, input.messages);
    assert_eq!(saved.context_messages, saved.messages);
    assert!(saved.turns.is_empty() && saved.inputs.is_empty());
    assert_eq!(saved.version, 0);
    assert_eq!(
        FileSessionStore::new(root.path())
            .unwrap()
            .load("child")
            .unwrap(),
        saved
    );
}

#[test]
fn retry_preserves_later_turns_and_original_initialization() {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    let input = initial("selected context");
    store.initialize("child", &input).unwrap();
    let saved = store
        .append_turn(
            "child",
            SessionTurn {
                turn_id: "t".into(),
                execution_id: "e".into(),
                status: SessionTurnStatus::Completed,
            },
            initial("child result").messages,
        )
        .unwrap();
    let reopened = FileSessionStore::new(root.path()).unwrap();
    assert_eq!(reopened.initialize("child", &input).unwrap(), saved);
    assert_eq!(saved.messages.len(), 2);
    assert_eq!(saved.version, 1);
    let before = fs::read(root.path().join("child.json")).unwrap();
    let mut changed = input.clone();
    changed.binding_digest = "b".repeat(64);
    assert!(reopened.initialize("child", &changed).is_err());
    assert!(
        reopened
            .initialize("child", &initial("foreign projection"))
            .is_err()
    );
    assert_eq!(fs::read(root.path().join("child.json")).unwrap(), before);
}

#[test]
fn ordinary_session_cannot_be_adopted_as_a_private_context() {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    let saved = store.create("child").unwrap();
    assert!(store.initialize("child", &initial("private")).is_err());
    assert_eq!(store.load("child").unwrap(), saved);
}

#[test]
fn independent_handles_serialize_identical_and_conflicting_initializers() {
    for conflict in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let gate = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|index| {
                let path = root.path().to_owned();
                let gate = gate.clone();
                std::thread::spawn(move || {
                    let store = FileSessionStore::new(path).unwrap();
                    let input = initial(if conflict && index == 1 {
                        "different"
                    } else {
                        "same"
                    });
                    gate.wait();
                    store.initialize("child", &input)
                })
            })
            .collect();
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            if conflict { 1 } else { 2 }
        );
        let saved = FileSessionStore::new(root.path())
            .unwrap()
            .load("child")
            .unwrap();
        for result in results.into_iter().filter_map(Result::ok) {
            assert_eq!(result, saved);
        }
    }
}

#[test]
fn corrupt_committed_snapshot_is_not_replaced_and_stale_temp_is_not_adopted() {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    let path = root.path().join("child.json");
    fs::write(&path, b"{truncated").unwrap();
    assert!(matches!(
        store.initialize("child", &initial("private")),
        Err(StorageError::Serialization(_))
    ));
    assert_eq!(fs::read(&path).unwrap(), b"{truncated");
    fs::write(
        root.path().join("other.json.tmp"),
        b"stale unpublished context",
    )
    .unwrap();
    let saved = store
        .initialize("other", &initial("actual committed projection"))
        .unwrap();
    assert_eq!(store.load("other").unwrap(), saved);
}

#[test]
fn initialization_framing_rejects_missing_and_unknown_critical_fields() {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    let record = store.create("root").unwrap();
    let mut encoded = serde_json::to_value(record).unwrap();
    encoded.as_object_mut().unwrap().remove("initialization");
    assert!(serde_json::from_value::<SessionRecord>(encoded).is_err());
    let mut encoded = serde_json::to_value(initial("context")).unwrap();
    encoded["grant"] = serde_json::json!("unauthorized");
    assert!(serde_json::from_value::<SessionInitialization>(encoded).is_err());
    for digest in ["", "../foreign", &"A".repeat(64), &"g".repeat(64)] {
        let mut bad = initial("context");
        bad.binding_digest = digest.into();
        assert!(store.initialize("invalid", &bad).is_err());
    }
    assert!(!root.path().join("invalid.json").exists());
}

#[test]
fn oversized_initialization_is_rejected_before_snapshot_publication() {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    let input = initial(&"x".repeat(MAX_INITIALIZATION_BYTES));
    assert!(store.initialize("large", &input).is_err());
    assert!(!root.path().join("large.json").exists());
}
