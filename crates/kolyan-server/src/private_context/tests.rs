use super::*;
mod verified;
use kolyan_ledger::{MemoryFactJournal, SqliteFactJournal};
use kolyan_model::{ContentBlock, MessageRole};
use kolyan_storage::FileSessionStore;

struct ExactOwner(Arc<dyn FactJournal>);
impl PrivateContextOwnershipVerifier for ExactOwner {
    fn verify_owner(&self, owner: &PrivateContextOwner, fact: &FactRef) -> Result<(), ServerError> {
        let after = fact
            .position
            .checked_sub(1)
            .ok_or_else(|| conflict("zero ownership position"))?;
        let rows = self
            .0
            .read(&fact.stream_id, after, 1)
            .map_err(|error| conflict(error.to_string()))?;
        if rows.len() != 1
            || rows[0].stream_id != fact.stream_id
            || rows[0].position != fact.position
            || rows[0].draft.fact_id != fact.fact_id
            || rows[0].draft.kind != "test.exact-owner"
            || rows[0].draft.schema_version != 1
            || rows[0].draft.payload != json!(owner)
        {
            return Err(conflict("ownership source differs"));
        }
        Ok(())
    }
}
fn owner() -> PrivateContextOwner {
    PrivateContextOwner {
        logical_session_id: "logical".into(),
        task_id: "task".into(),
        invocation_id: "child".into(),
        private_session_id: "private".into(),
        snapshot_digest: "a".repeat(64),
    }
}
fn messages(value: &str) -> Vec<Message> {
    vec![Message {
        role: MessageRole::User,
        content: vec![ContentBlock::Text { text: value.into() }],
    }]
}
fn source(journal: &dyn FactJournal, owner: &PrivateContextOwner) -> FactRef {
    journal
        .append(
            "owner",
            0,
            vec![FactDraft {
                fact_id: "ownership".into(),
                subject: FactSubject {
                    kind: "test.owner".into(),
                    id: "child".into(),
                },
                kind: "test.exact-owner".into(),
                schema_version: 1,
                critical: true,
                causes: vec![],
                payload: json!(owner),
            }],
        )
        .unwrap();
    FactRef {
        stream_id: "owner".into(),
        position: 1,
        fact_id: "ownership".into(),
    }
}
fn check(journal: Arc<dyn FactJournal>) {
    let directory = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(directory.path()).unwrap();
    let owner = owner();
    let reference = source(journal.as_ref(), &owner);
    let make = || {
        PrivateContextService::new(
            SessionService::new(store.clone()),
            journal.clone(),
            Arc::new(ExactOwner(journal.clone())),
        )
    };
    let initial = make()
        .initialize(&owner, &reference, messages("selected input"))
        .unwrap();
    assert_eq!(initial.version, 0);
    assert!(initial.turns.is_empty());
    assert_eq!(initial.messages, messages("selected input"));
    assert_eq!(initial.context_messages, initial.messages);
    assert_eq!(
        make()
            .initialize(&owner, &reference, messages("selected input"))
            .unwrap(),
        initial
    );
    assert!(
        make()
            .initialize(&owner, &reference, messages("changed input"))
            .is_err()
    );
    let mut foreign = owner.clone();
    foreign.snapshot_digest = "b".repeat(64);
    assert!(
        make()
            .initialize(&foreign, &reference, messages("selected input"))
            .is_err()
    );
    assert_eq!(store.load("private").unwrap(), initial);
    // Journal survives an independent service reconstruction; no second snapshot.
    let rows = journal
        .read(
            &format!(
                "server.private-context.{}",
                digest(&json!([
                    "kolyan.server.private-context.owner.v1",
                    "private"
                ]))
                .unwrap()
            ),
            0,
            2,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].draft.payload.get("messages").is_none());
    assert!(rows[0].draft.payload.get("snapshot").is_none());
}

#[test]
fn memory_and_sqlite_exact_projection_initialization_is_nooverwrite() {
    check(Arc::new(MemoryFactJournal::default()));
    let directory = tempfile::tempdir().unwrap();
    check(Arc::new(
        SqliteFactJournal::open(directory.path().join("facts.sqlite")).unwrap(),
    ));
}

#[test]
fn existing_unowned_context_cannot_be_adopted_and_bad_ownership_never_creates_context() {
    let journal = Arc::new(MemoryFactJournal::default());
    let directory = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(directory.path()).unwrap();
    store.create("private").unwrap();
    let owner = owner();
    let reference = source(journal.as_ref(), &owner);
    let service = PrivateContextService::new(
        SessionService::new(store.clone()),
        journal.clone(),
        Arc::new(ExactOwner(journal)),
    );
    let legacy = store.load("private").unwrap();
    assert!(
        service
            .initialize(&owner, &reference, messages("input"))
            .is_err()
    );
    assert_eq!(store.load("private").unwrap(), legacy);
    let mut foreign = owner;
    foreign.private_session_id = "foreign".into();
    assert!(
        service
            .initialize(&foreign, &reference, messages("input"))
            .is_err()
    );
    assert!(store.load("foreign").is_err());
}
