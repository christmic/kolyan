//! Real Journal/ArtifactStore foundation matrix; no model, grants or Skill tool execution.

use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    sync::{Arc, Mutex},
};

use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, MemoryFactJournal, SqliteFactJournal,
};
use kolyan_model::ModelRef;
use kolyan_trace::ArtifactStore;
use serde::Deserialize;
use serde_json::{Value, json};

use super::super::*;
use crate::{
    AgentDefinition, AgentDefinitionInput, AgentInvocationBinding, AgentInvocationBindingStore,
    AgentPermissions, AgentSnapshot, BindingContextKind, child_private_session_id,
};

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Retry,
    BodyConflict,
    MetadataConflict,
    Revision,
    Restore,
    Revoke,
    Deny,
    Namespace,
    Scope,
    Owner,
    Acl,
    Capacity,
    BodyCapacity,
    AdvertisementCapacity,
    MetadataCapacity,
    Schema,
    Payload,
    Digest,
    Causes,
    BindingSchema,
    BindingCauses,
    Unselected,
    MetadataOnly,
    Artifact,
    ArtifactCorrupt,
    Child,
    RevokeForeign,
    Operation,
    Limits,
    ExactBodyCap,
    MetadataControl,
    BindingRefUnknown,
    Critical,
    Subject,
    Position,
    Kind,
    Storage,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    action: Action,
    expected: String,
}

// Read mutation is explicit fault injection, not an alternate production backend.
// Every underlying record and append still uses the real Memory/SQLite Journal.
struct FaultJournal {
    inner: Arc<dyn FactJournal>,
    mutation: Mutex<Option<Action>>,
    streams: Mutex<std::collections::BTreeSet<String>>,
}

impl FactJournal for FaultJournal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.streams.lock().unwrap().insert(stream.into());
        let mut records = self.inner.read(stream, after, limit)?;
        if let Some(action) = *self.mutation.lock().unwrap() {
            if matches!(action, Action::Storage) {
                return Err(FactError::Storage(
                    "injected storage failure after real durable publication".into(),
                ));
            }
            for record in &mut records {
                match action {
                    Action::Schema if record.draft.kind == "agent.skill.registered" => {
                        record.draft.schema_version = 2
                    }
                    Action::Payload if record.draft.kind == "agent.skill.registered" => {
                        record.draft.payload["unknown"] = json!(true);
                    }
                    Action::Digest if record.draft.kind == "agent.skill.registered" => {
                        record.draft.payload["metadata"]["content_digest"] = json!("0".repeat(64));
                    }
                    Action::Causes if record.draft.kind == "agent.skill.registered" => {
                        record
                            .draft
                            .causes
                            .push(super::super::catalog::reference(record));
                    }
                    Action::BindingSchema if record.draft.kind == "agent.skill.bound" => {
                        record.draft.schema_version = 2
                    }
                    Action::BindingCauses if record.draft.kind == "agent.skill.bound" => {
                        record.draft.causes.clear()
                    }
                    Action::BindingRefUnknown if record.draft.kind == "agent.skill.bound" => {
                        record.draft.payload["ownership"]["unknown"] = json!(true);
                    }
                    Action::Critical if record.draft.kind == "agent.skill.registered" => {
                        record.draft.critical = false
                    }
                    Action::Subject if record.draft.kind == "agent.skill.registered" => {
                        record.draft.subject.id = "foreign".into()
                    }
                    Action::Position if record.draft.kind == "agent.skill.registered" => {
                        record.position += 1
                    }
                    Action::Kind if record.draft.kind == "agent.skill.registered" => {
                        record.draft.kind = "unknown.fact".into()
                    }
                    _ => {}
                }
            }
        }
        Ok(records)
    }
    fn append(
        &self,
        stream: &str,
        expected: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        self.streams.lock().unwrap().insert(stream.into());
        self.inner.append(stream, expected, batch)
    }
}

fn owner(invocation: &str, child: bool) -> AgentInvocationBinding {
    let permissions = AgentPermissions::default();
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "agent".into(),
        revision: "1".into(),
        display_name: None,
        model: ModelRef {
            provider: "fixture".into(),
            model: "synthetic-no-model-run".into(),
        },
        instructions: "fixture ownership".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    AgentInvocationBinding {
        task_id: "task".into(),
        invocation_id: invocation.into(),
        logical_session_id: "session".into(),
        private_session_id: if child {
            child_private_session_id("session", "task", invocation).unwrap()
        } else {
            "session".into()
        },
        context_kind: if child {
            BindingContextKind::Child
        } else {
            BindingContextKind::Root
        },
        snapshot: AgentSnapshot::new(definition, format!("instance-{invocation}"), permissions)
            .unwrap(),
    }
}

fn descriptor(revision: &str) -> SkillDescriptorInput {
    SkillDescriptorInput {
        key: SkillKey::new("guide".into(), revision.into()).unwrap(),
        title: "Guide".into(),
        description: "Host knowledge only".into(),
    }
}

fn policy(binding: &AgentInvocationBinding, keys: Vec<SkillKey>) -> SkillAccessPolicy {
    SkillAccessPolicy::new(
        "host.acl".into(),
        "1".into(),
        vec![SkillAccessRuleInput {
            agent: binding.snapshot.definition().key(),
            logical_session_id: binding.logical_session_id.clone(),
            task_id: Some(binding.task_id.clone()),
            invocation_id: Some(binding.invocation_id.clone()),
            skills: keys.into_iter().collect(),
        }],
    )
    .unwrap()
}

fn classify<T>(errors: &mut Vec<Value>, result: Result<T, SkillError>) -> String {
    if let Err(error) = &result {
        errors.push(json!({"error": error.to_string()}));
    }
    match result {
        Ok(_) => "ok",
        Err(SkillError::Invalid(_)) => "invalid",
        Err(SkillError::Capacity) => "capacity",
        Err(SkillError::Conflict) => "conflict",
        Err(SkillError::Revoked) => "revoked",
        Err(SkillError::Permission) => "permission",
        Err(SkillError::Provenance(_)) => "provenance",
        Err(SkillError::Integrity(_)) => "integrity",
        Err(SkillError::Journal(_)) => "journal",
        Err(SkillError::Artifact(_)) => "artifact",
    }
    .into()
}

fn records(journal: &FaultJournal, mutated: bool) -> Value {
    let streams = journal.streams.lock().unwrap().clone();
    let mut all = Vec::new();
    for stream in streams {
        all.extend(if mutated {
            journal.read(&stream, 0, 1024).unwrap()
        } else {
            journal.inner.read(&stream, 0, 1024).unwrap()
        });
    }
    json!(all)
}

fn run(row: &Row, backend: &str, root: &std::path::Path) -> Value {
    fs::create_dir_all(root).unwrap();
    let database = root.join("facts.sqlite");
    let inner: Arc<dyn FactJournal> = if backend == "sqlite" {
        Arc::new(SqliteFactJournal::open(&database).unwrap())
    } else {
        Arc::new(MemoryFactJournal::default())
    };
    let journal = Arc::new(FaultJournal {
        inner,
        mutation: Mutex::new(None),
        streams: Mutex::new(Default::default()),
    });
    let artifacts_path = root.join("artifacts");
    let artifacts = Arc::new(ArtifactStore::new(&artifacts_path, MAX_BODY_BYTES as u64).unwrap());
    let limits = match row.action {
        Action::Capacity => SkillLimits::new(1, 16, MAX_METADATA_BYTES, MAX_BODY_BYTES).unwrap(),
        Action::AdvertisementCapacity => {
            SkillLimits::new(128, 1, MAX_METADATA_BYTES, MAX_BODY_BYTES).unwrap()
        }
        Action::MetadataCapacity => SkillLimits::new(128, 16, 1, MAX_BODY_BYTES).unwrap(),
        _ => SkillLimits::default(),
    };
    let catalog = SkillCatalog::new(
        journal.clone(),
        artifacts.clone(),
        "host.fixture".into(),
        limits,
    )
    .unwrap();
    let body = "真实知识\nkeep permissions unchanged\n\n";
    let registered = catalog.register(descriptor("1"), body).unwrap();
    let binding = owner("root", false);
    let store = AgentInvocationBindingStore::new(journal.clone());
    let ownership = store.save(&binding).unwrap();
    let scope = SkillScope::from_binding(&binding).unwrap();
    let mut keys = vec![registered.metadata().descriptor().key.clone()];
    if matches!(row.action, Action::AdvertisementCapacity) {
        keys.push(
            catalog
                .register(descriptor("2"), "other")
                .unwrap()
                .metadata()
                .descriptor()
                .key
                .clone(),
        );
    }
    let runtime = SkillRuntime::new(catalog.clone(), policy(&binding, keys));
    let before = records(&journal, false);
    let ad_result = runtime.discover(&binding.snapshot, &scope);
    let mut details = json!({"body":body,"limits":limits,"owner":binding,"ownership":ownership,
        "scope":scope,"registered":registered,"advertisement":ad_result.as_ref().ok(),"before":before});
    let mut errors = Vec::new();
    let outcome = if matches!(
        row.action,
        Action::AdvertisementCapacity | Action::MetadataCapacity
    ) {
        classify(&mut errors, ad_result)
    } else {
        let advertisement = ad_result.unwrap();
        let bound = runtime.bind(&advertisement, &ownership).unwrap();
        details["binding"] = json!(bound);
        match row.action {
            Action::Retry => {
                let retry = catalog.register(descriptor("1"), body).unwrap();
                details["retry"] = json!(retry);
                if retry == registered {
                    "ok".into()
                } else {
                    "different_retry".into()
                }
            }
            Action::BodyConflict => {
                classify(&mut errors, catalog.register(descriptor("1"), "changed"))
            }
            Action::MetadataConflict => {
                let mut changed = descriptor("1");
                changed.description = "different".into();
                classify(&mut errors, catalog.register(changed, body))
            }
            Action::Revision => classify(&mut errors, catalog.register(descriptor("2"), "changed")),
            Action::Restore => {
                let reloaded: Arc<dyn FactJournal> = if backend == "sqlite" {
                    Arc::new(SqliteFactJournal::open(&database).unwrap())
                } else {
                    journal.inner.clone()
                };
                let reopened_artifacts =
                    Arc::new(ArtifactStore::new(&artifacts_path, MAX_BODY_BYTES as u64).unwrap());
                let reopened = SkillRuntime::new(
                    SkillCatalog::new(reloaded, reopened_artifacts, "host.fixture".into(), limits)
                        .unwrap(),
                    policy(&binding, vec![descriptor("1").key]),
                );
                let restored = reopened.restore_binding(bound.reference(), &scope);
                details["reconstructed"] = json!(restored.as_ref().ok());
                classify(
                    &mut errors,
                    restored.and_then(|value| {
                        if value == bound {
                            Ok(())
                        } else {
                            Err(SkillError::Conflict)
                        }
                    }),
                )
            }
            Action::Revoke => {
                let fact = catalog
                    .revoke(
                        &descriptor("1").key,
                        registered.reference(),
                        "revoke.1",
                        "host revoked",
                    )
                    .unwrap();
                let retry = catalog
                    .revoke(
                        &descriptor("1").key,
                        registered.reference(),
                        "revoke.1",
                        "host revoked",
                    )
                    .unwrap();
                details["revoke"] = json!({"first":fact,"retry":retry,"historical":runtime.restore_binding(bound.reference(), &scope).unwrap(),
                    "new_advertisement":runtime.discover(&binding.snapshot, &scope).unwrap()});
                classify(&mut errors, runtime.validate_current(&bound))
            }
            Action::Deny => {
                let denied = SkillRuntime::new(catalog.clone(), SkillAccessPolicy::deny_all())
                    .discover(&binding.snapshot, &scope)
                    .unwrap();
                details["denied"] = json!(denied);
                if denied.skills().is_empty() {
                    "empty".into()
                } else {
                    "nonempty".into()
                }
            }
            Action::Namespace => {
                let other = SkillRuntime::new(
                    SkillCatalog::new(
                        journal.clone(),
                        artifacts.clone(),
                        "other.host".into(),
                        limits,
                    )
                    .unwrap(),
                    policy(&binding, vec![]),
                );
                classify(
                    &mut errors,
                    other.restore_binding(bound.reference(), &scope),
                )
            }
            Action::Scope => classify(
                &mut errors,
                runtime.restore_binding(
                    bound.reference(),
                    &SkillScope::from_binding(&owner("other", false)).unwrap(),
                ),
            ),
            Action::Owner => {
                let mut foreign = ownership.clone();
                foreign.position += 1;
                classify(&mut errors, runtime.bind(&advertisement, &foreign))
            }
            Action::Acl => classify(
                &mut errors,
                SkillRuntime::new(catalog.clone(), SkillAccessPolicy::deny_all())
                    .validate_current(&bound),
            ),
            Action::Capacity => classify(&mut errors, catalog.register(descriptor("2"), body)),
            Action::BodyCapacity => classify(
                &mut errors,
                catalog.register(descriptor("2"), &"x".repeat(MAX_BODY_BYTES + 1)),
            ),
            Action::Schema
            | Action::Payload
            | Action::Digest
            | Action::Causes
            | Action::BindingSchema
            | Action::BindingCauses
            | Action::BindingRefUnknown
            | Action::Critical
            | Action::Subject
            | Action::Position
            | Action::Kind => {
                *journal.mutation.lock().unwrap() = Some(row.action);
                details["fault_view"] = records(&journal, true);
                classify(
                    &mut errors,
                    runtime.restore_binding(bound.reference(), &scope),
                )
            }
            Action::Unselected | Action::MetadataOnly => {
                // Removing host-owned content proves metadata paths never read it;
                // Required storage itself rightly forbids removing it through its API.
                let removed = if matches!(row.action, Action::Unselected) {
                    catalog.register(descriptor("2"), "unselected").unwrap()
                } else {
                    registered.clone()
                };
                fs::remove_file(artifacts_path.join(&removed.metadata().body().digest)).unwrap();
                details["removed_body"] = json!(removed.metadata().body());
                let discovered = runtime.discover(&binding.snapshot, &scope);
                details["after_removal_advertisement"] = json!(discovered.as_ref().ok());
                classify(
                    &mut errors,
                    discovered.and_then(|_| runtime.validate_current(&bound)),
                )
            }
            Action::Artifact | Action::ArtifactCorrupt => {
                if matches!(row.action, Action::ArtifactCorrupt) {
                    fs::write(
                        artifacts_path.join(&registered.metadata().body().digest),
                        b"damaged",
                    )
                    .unwrap();
                }
                let actual = artifacts.read(registered.metadata().body(), MAX_BODY_BYTES as u64);
                details["artifact_result"] = match &actual {
                    Ok(bytes) => json!({"bytes":bytes}),
                    Err(error) => json!({"error":error.to_string()}),
                };
                classify(
                    &mut errors,
                    actual.map_err(SkillError::from).and_then(|bytes| {
                        if bytes == body.as_bytes() {
                            Ok(())
                        } else {
                            Err(SkillError::Integrity("bytes differ".into()))
                        }
                    }),
                )
            }
            Action::Child => {
                let child = owner("child", true);
                let child_ref = store.save(&child).unwrap();
                let child_scope = SkillScope::from_binding(&child).unwrap();
                let ad = runtime.discover(&child.snapshot, &child_scope).unwrap();
                let child_bound = runtime.bind(&ad, &child_ref).unwrap();
                details["child"] = json!({"owner":child,"advertisement":ad,"binding":child_bound});
                if ad.skills().is_empty() && child_bound.reference() != bound.reference() {
                    "empty".into()
                } else {
                    "inherited".into()
                }
            }
            Action::RevokeForeign => {
                let mut foreign = registered.reference().clone();
                foreign.fact_id = "foreign.fact".into();
                classify(
                    &mut errors,
                    catalog.revoke(&descriptor("1").key, &foreign, "revoke.1", "host revoked"),
                )
            }
            Action::Operation => {
                catalog
                    .revoke(
                        &descriptor("1").key,
                        registered.reference(),
                        "revoke.1",
                        "host revoked",
                    )
                    .unwrap();
                classify(
                    &mut errors,
                    catalog.revoke(
                        &descriptor("1").key,
                        registered.reference(),
                        "revoke.1",
                        "changed reason",
                    ),
                )
            }
            Action::Limits => classify(
                &mut errors,
                SkillLimits::new(129, 16, MAX_METADATA_BYTES, MAX_BODY_BYTES),
            ),
            Action::ExactBodyCap => {
                let input = "x".repeat(MAX_BODY_BYTES);
                let saved = catalog.register(descriptor("2"), &input).unwrap();
                let bytes = artifacts
                    .read(saved.metadata().body(), MAX_BODY_BYTES as u64)
                    .unwrap();
                details["exact_cap"] = json!({"input":input,"returned":bytes,"registration":saved});
                "ok".into()
            }
            Action::MetadataControl => {
                let mut input = descriptor("2");
                input.title = "bad\nmetadata".into();
                classify(&mut errors, catalog.register(input, body))
            }
            Action::Storage => {
                *journal.mutation.lock().unwrap() = Some(row.action);
                classify(&mut errors, runtime.validate_current(&bound))
            }
            Action::AdvertisementCapacity | Action::MetadataCapacity => unreachable!(),
        }
    };
    details["errors"] = json!(errors);
    details["after"] = records(&journal, false);
    json!({"id":row.id,"backend":backend,"outcome":outcome,"observations":details})
}

#[test]
fn governed_skills_memory_sqlite_data_matrix() {
    let rows: Vec<Row> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-skills-a-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut file = File::create(&path).unwrap();
    for backend in ["memory", "sqlite"] {
        for row in &rows {
            let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run(row, backend, &root.join(format!("{backend}-{}", row.id)))
            }))
            .unwrap_or_else(|_| json!({"id":row.id,"backend":backend,"outcome":"harness_failure"}));
            serde_json::to_writer(&mut file, &actual).unwrap();
            writeln!(file).unwrap();
        }
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    println!("SKILLS_A_ACTUAL={}", path.display());
    let physical: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    assert_eq!(physical.len(), rows.len() * 2);
    for (index, actual) in physical.iter().enumerate() {
        let row = &rows[index % rows.len()];
        assert_eq!(actual["id"], row.id);
        assert_eq!(
            actual["outcome"], row.expected,
            "{} {}",
            actual["backend"], row.id
        );
        if matches!(row.action, Action::Retry) {
            assert_eq!(
                actual["observations"]["retry"],
                actual["observations"]["registered"]
            );
        }
        if matches!(row.action, Action::Revoke) {
            assert_eq!(
                actual["observations"]["revoke"]["first"],
                actual["observations"]["revoke"]["retry"]
            );
            assert_eq!(
                actual["observations"]["revoke"]["new_advertisement"]["skills"],
                json!([])
            );
        }
        if matches!(row.action, Action::ExactBodyCap) {
            let input = actual["observations"]["exact_cap"]["input"]
                .as_str()
                .unwrap();
            let bytes: Vec<u8> =
                serde_json::from_value(actual["observations"]["exact_cap"]["returned"].clone())
                    .unwrap();
            assert_eq!(input.len(), MAX_BODY_BYTES);
            assert_eq!(bytes, input.as_bytes());
            assert_eq!(
                actual["observations"]["exact_cap"]["registration"]["metadata"]["body"]["byte_length"],
                MAX_BODY_BYTES
            );
        }
    }
}
