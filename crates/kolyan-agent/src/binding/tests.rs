use kolyan_ledger::{MemoryFactJournal, SqliteFactJournal};
use kolyan_model::ModelRef;
use tempfile::TempDir;

use super::*;
use crate::{AgentDefinition, AgentDefinitionInput, AgentKey, AgentPermissions, EnvironmentTool};

fn binding() -> AgentInvocationBinding {
    let permissions = AgentPermissions {
        tools: [EnvironmentTool::Read, EnvironmentTool::Write].into(),
        ..Default::default()
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "reader".into(),
        revision: "1".into(),
        display_name: None,
        model: ModelRef::new("provider", "model"),
        instructions: "Follow host authority.".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    AgentInvocationBinding {
        task_id: "task".into(),
        invocation_id: "invocation".into(),
        logical_session_id: "logical".into(),
        private_session_id: child_private_session_id("logical", "task", "invocation").unwrap(),
        context_kind: BindingContextKind::Child,
        snapshot: AgentSnapshot::new(definition, "instance".into(), permissions).unwrap(),
    }
}

fn backends() -> (TempDir, Vec<Arc<dyn FactJournal>>) {
    let dir = tempfile::tempdir().unwrap();
    let stores: Vec<Arc<dyn FactJournal>> = vec![
        Arc::new(MemoryFactJournal::default()),
        Arc::new(SqliteFactJournal::open(dir.path().join("facts.db")).unwrap()),
    ];
    (dir, stores)
}

fn draft(value: &AgentInvocationBinding) -> FactDraft {
    FactDraft {
        fact_id: fact_id(&value.task_id, &value.invocation_id).unwrap(),
        subject: FactSubject {
            kind: SUBJECT_KIND.into(),
            id: value.invocation_id.clone(),
        },
        kind: FACT_KIND.into(),
        schema_version: 1,
        critical: true,
        causes: vec![],
        payload: encode(value).unwrap(),
    }
}

#[test]
fn memory_and_sqlite_exact_roundtrip_and_idempotency() {
    let (_dir, journals) = backends();
    for journal in journals {
        let store = AgentInvocationBindingStore::new(journal.clone());
        let value = binding();
        assert_eq!(store.load("task", "invocation", "logical").unwrap(), None);
        let saved = store.save(&value).unwrap();
        assert_eq!(store.save(&value).unwrap(), saved);
        assert_eq!(
            AgentInvocationBindingStore::new(journal.clone())
                .load("task", "invocation", "logical")
                .unwrap(),
            Some(value)
        );
        assert_eq!(journal.read(&saved.stream_id, 0, 2).unwrap().len(), 1);
    }
}

#[test]
fn sqlite_reopens_without_catalog_or_retained_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("facts.db");
    let value = binding();
    let coordinate = {
        let store =
            AgentInvocationBindingStore::new(Arc::new(SqliteFactJournal::open(&path).unwrap()));
        store.save(&value).unwrap()
    };
    let store = AgentInvocationBindingStore::new(Arc::new(SqliteFactJournal::open(&path).unwrap()));
    assert_eq!(
        store.load("task", "invocation", "logical").unwrap(),
        Some(value.clone())
    );
    assert_eq!(store.save(&value).unwrap(), coordinate);
    assert!(matches!(
        store.load("task", "invocation", "foreign"),
        Err(BindingError::ForeignOwner)
    ));
}

#[test]
fn changed_snapshot_permissions_revision_and_context_conflict() {
    let (_dir, journals) = backends();
    for journal in journals {
        let store = AgentInvocationBindingStore::new(journal);
        let original = binding();
        store.save(&original).unwrap();
        let mut context = original.clone();
        context.private_session_id = "different".into();
        let mut owner = original.clone();
        owner.logical_session_id = "foreign".into();
        owner.private_session_id = child_private_session_id(
            &owner.logical_session_id,
            &owner.task_id,
            &owner.invocation_id,
        )
        .unwrap();
        let mut permissions = original.clone();
        permissions.snapshot = AgentSnapshot::new(
            original.snapshot.definition().clone(),
            "instance".into(),
            AgentPermissions::default(),
        )
        .unwrap();
        let mut revision = original.clone();
        let mut input: AgentDefinitionInput = original.snapshot.definition().clone().into();
        input.revision = "2".into();
        revision.snapshot = AgentSnapshot::new(
            AgentDefinition::new(input).unwrap(),
            "instance".into(),
            original.snapshot.permissions().clone(),
        )
        .unwrap();
        assert!(matches!(
            store.save(&context),
            Err(BindingError::Invalid(_))
        ));
        for changed in [permissions, revision] {
            assert!(matches!(store.save(&changed), Err(BindingError::Conflict)));
        }
        assert!(matches!(
            store.save(&owner),
            Err(BindingError::ForeignOwner)
        ));
        assert_eq!(
            store.load("task", "invocation", "logical").unwrap(),
            Some(original)
        );
    }
}

#[test]
fn context_kind_and_identifier_validation_are_explicit() {
    let store = AgentInvocationBindingStore::new(Arc::new(MemoryFactJournal::default()));
    let mut value = binding();
    value.private_session_id = value.logical_session_id.clone();
    assert!(matches!(store.save(&value), Err(BindingError::Invalid(_))));
    value.context_kind = BindingContextKind::Root;
    store.save(&value).unwrap();
    for invalid in ["", " ", "path/escape", "control\n", &"a".repeat(257)] {
        assert!(matches!(
            store.load(invalid, "invocation", "logical"),
            Err(BindingError::Invalid(_))
        ));
    }
}

#[test]
fn long_ids_have_bounded_unambiguous_keys() {
    let (_dir, journals) = backends();
    for journal in journals {
        let store = AgentInvocationBindingStore::new(journal);
        let mut value = binding();
        value.task_id = "t".repeat(256);
        value.invocation_id = "i".repeat(256);
        value.logical_session_id = "l".repeat(256);
        value.private_session_id = child_private_session_id(
            &value.logical_session_id,
            &value.task_id,
            &value.invocation_id,
        )
        .unwrap();
        assert!(value.private_session_id.len() <= 256);
        let saved = store.save(&value).unwrap();
        assert!(saved.stream_id.len() <= 256 && saved.fact_id.len() <= 256);
        assert_eq!(
            store
                .load(
                    &value.task_id,
                    &value.invocation_id,
                    &value.logical_session_id
                )
                .unwrap(),
            Some(value)
        );
        assert_ne!(
            stream_id("a:b", "c").unwrap(),
            stream_id("a", "b:c").unwrap()
        );
    }
}

#[test]
fn oversized_definition_is_rejected_before_any_fact_append() {
    let (_dir, journals) = backends();
    for journal in journals {
        let mut value = binding();
        let mut input: AgentDefinitionInput = value.snapshot.definition().clone().into();
        input.instructions = "x".repeat(65536);
        for index in 0..256 {
            input.permissions.delegation.named_targets.insert(
                AgentKey::new(format!("{index:03}{}", "d".repeat(253)), "r".repeat(256)).unwrap(),
            );
        }
        let permissions = input.permissions.clone();
        value.snapshot = AgentSnapshot::new(
            AgentDefinition::new(input).unwrap(),
            "instance".into(),
            permissions,
        )
        .unwrap();
        let store = AgentInvocationBindingStore::new(journal.clone());
        assert!(matches!(
            store.save(&value),
            Err(BindingError::Oversized {
                limit: MAX_BINDING_PAYLOAD_BYTES,
                ..
            })
        ));
        assert!(
            journal
                .read(&stream_id("task", "invocation").unwrap(), 0, 2)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn corrupt_version_subject_scope_digest_and_unknown_fields_fail_closed() {
    for mutation in 0..8 {
        let (dir, journals) = backends();
        for (index, journal) in journals.into_iter().enumerate() {
            let value = binding();
            let mut event = draft(&value);
            match mutation {
                0 => event.schema_version = 2,
                1 => event.kind = "agent.unknown_critical".into(),
                2 => event.subject.id = "foreign".into(),
                3 => event.payload["task_id"] = "foreign".into(),
                4 => event.payload["snapshot"]["digest"] = "tampered".into(),
                5 => event.payload["unexpected"] = true.into(),
                6 => event.critical = false,
                _ => event.payload["private_session_id"] = "logical".into(),
            }
            let stream = stream_id("task", "invocation").unwrap();
            journal.append(&stream, 0, vec![event]).unwrap();
            assert!(matches!(
                AgentInvocationBindingStore::new(journal.clone()).load(
                    "task",
                    "invocation",
                    "logical"
                ),
                Err(BindingError::Corrupt(_))
            ));
            drop(journal);
            // Actual reopen proves invalid facts are rejected after reconstruction.
            if index == 1 {
                let store = AgentInvocationBindingStore::new(Arc::new(
                    SqliteFactJournal::open(dir.path().join("facts.db")).unwrap(),
                ));
                assert!(matches!(
                    store.load("task", "invocation", "logical"),
                    Err(BindingError::Corrupt(_))
                ));
            }
        }
    }
}

#[test]
fn extra_unknown_fact_blocks_reads_and_idempotent_save() {
    let (_dir, journals) = backends();
    for journal in journals {
        let store = AgentInvocationBindingStore::new(journal.clone());
        let value = binding();
        let saved = store.save(&value).unwrap();
        let mut unknown = draft(&value);
        unknown.fact_id = format!("{}.extra", saved.fact_id);
        unknown.kind = "agent.unknown_critical".into();
        journal.append(&saved.stream_id, 1, vec![unknown]).unwrap();
        assert!(matches!(
            store.load("task", "invocation", "logical"),
            Err(BindingError::Corrupt(_))
        ));
        assert!(matches!(store.save(&value), Err(BindingError::Corrupt(_))));
    }
}

#[test]
fn concurrent_identical_and_conflicting_admissions_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let memory = MemoryFactJournal::default();
    let sqlite_path = dir.path().join("concurrent.db");
    let pairs: Vec<(Arc<dyn FactJournal>, Arc<dyn FactJournal>)> = vec![
        (Arc::new(memory.clone()), Arc::new(memory)),
        (
            Arc::new(SqliteFactJournal::open(&sqlite_path).unwrap()),
            Arc::new(SqliteFactJournal::open(&sqlite_path).unwrap()),
        ),
    ];
    for (first, second) in pairs {
        for conflicting in [false, true] {
            let mut value = binding();
            value.invocation_id = if conflicting {
                "concurrent-conflicting"
            } else {
                "concurrent-identical"
            }
            .into();
            value.private_session_id = child_private_session_id(
                &value.logical_session_id,
                &value.task_id,
                &value.invocation_id,
            )
            .unwrap();
            let mut other = value.clone();
            if conflicting {
                other.snapshot = AgentSnapshot::new(
                    value.snapshot.definition().clone(),
                    "another-instance".into(),
                    value.snapshot.permissions().clone(),
                )
                .unwrap();
            }
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let workers =
                [(first.clone(), value), (second.clone(), other)].map(|(journal, value)| {
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        AgentInvocationBindingStore::new(journal).save(&value)
                    })
                });
            barrier.wait();
            let [a, b] = workers.map(|worker| worker.join().unwrap());
            if conflicting {
                assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
                let loser = if a.is_err() { a } else { b };
                assert!(matches!(loser, Err(BindingError::Conflict)));
            } else {
                assert_eq!(a.unwrap(), b.unwrap());
            }
        }
    }
}

#[test]
fn children_cannot_alias_another_invocation_context() {
    let (_dir, journals) = backends();
    for journal in journals {
        let store = AgentInvocationBindingStore::new(journal.clone());
        let first = binding();
        store.save(&first).unwrap();
        for scope in 0..3 {
            let mut child = first.clone();
            match scope {
                0 => child.invocation_id = "sibling".into(),
                1 => child.task_id = "another-task".into(),
                _ => child.logical_session_id = "another-owner".into(),
            }
            // An otherwise valid context ID belonging to another scope is not accepted.
            assert!(matches!(store.save(&child), Err(BindingError::Invalid(_))));
            if scope != 2 {
                assert!(
                    journal
                        .read(
                            &stream_id(&child.task_id, &child.invocation_id).unwrap(),
                            0,
                            2
                        )
                        .unwrap()
                        .is_empty()
                );
            }
            let derived = child_private_session_id(
                &child.logical_session_id,
                &child.task_id,
                &child.invocation_id,
            )
            .unwrap();
            assert_ne!(derived, first.private_session_id);
            child.private_session_id = derived;
            if scope != 2 {
                store.save(&child).unwrap();
            } else {
                assert!(matches!(
                    store.save(&child),
                    Err(BindingError::ForeignOwner)
                ));
            }
        }
        assert_eq!(
            child_private_session_id("a:b", "c", "d").unwrap(),
            child_private_session_id("a:b", "c", "d").unwrap()
        );
        assert_ne!(
            child_private_session_id("a:b", "c", "d").unwrap(),
            child_private_session_id("a", "b:c", "d").unwrap()
        );
    }
}

#[test]
fn independent_root_turns_intentionally_share_the_logical_session() {
    let (_dir, journals) = backends();
    for journal in journals {
        let store = AgentInvocationBindingStore::new(journal);
        for invocation in ["root-turn-1", "root-turn-2"] {
            let mut root = binding();
            root.invocation_id = invocation.into();
            root.context_kind = BindingContextKind::Root;
            root.private_session_id = root.logical_session_id.clone();
            store.save(&root).unwrap();
            assert_eq!(
                store.load("task", invocation, "logical").unwrap(),
                Some(root.clone())
            );
            root.private_session_id = "changed-root-context".into();
            assert!(matches!(store.save(&root), Err(BindingError::Invalid(_))));
        }
    }
}

#[test]
fn root_cannot_borrow_a_foreign_or_child_physical_session() {
    let (_dir, journals) = backends();
    for journal in journals {
        let store = AgentInvocationBindingStore::new(journal.clone());
        let mut root = binding();
        root.context_kind = BindingContextKind::Root;
        for borrowed in [
            "foreign-logical-session".to_string(),
            child_private_session_id("logical", "task", "other-child").unwrap(),
        ] {
            root.private_session_id = borrowed;
            assert!(matches!(store.save(&root), Err(BindingError::Invalid(_))));
            assert!(
                journal
                    .read(&stream_id("task", "invocation").unwrap(), 0, 2)
                    .unwrap()
                    .is_empty()
            );
        }
        // A corrupt persisted Root is rejected too, not only construction input.
        journal
            .append(
                &stream_id("task", "invocation").unwrap(),
                0,
                vec![draft(&root)],
            )
            .unwrap();
        assert!(matches!(
            store.load("task", "invocation", "logical"),
            Err(BindingError::Corrupt(_))
        ));
    }
}
