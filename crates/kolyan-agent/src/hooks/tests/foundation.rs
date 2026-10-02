use super::super::*;
use crate::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentInvocationBinding,
    AgentInvocationBindingStore, AgentPermissions, AgentSelector, BindingContextKind,
};
use kolyan_ledger::{FactJournal, MemoryFactJournal, SqliteFactJournal};
use kolyan_model::ModelRef;
use kolyan_policy::{Capability, Effect};
use kolyan_trace::ArtifactStore;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufRead, BufReader, Write},
    sync::Arc,
};

#[derive(Deserialize)]
struct Case {
    id: String,
    action: String,
    expected: String,
}

pub(super) fn manifest() -> HookManifest {
    HookManifest {
        key: HookKey {
            id: "fixture".into(),
            revision: "1".into(),
        },
        phases: BTreeSet::from([HookPhase::BeforeModel]),
        tool_names: BTreeSet::new(),
        capabilities: BTreeSet::from([Capability::ProcessExecute, Capability::FilesystemRead]),
        effects: BTreeSet::from([Effect::Execute, Effect::Read]),
        timeout_ms: 1000,
        max_input_bytes: 4096,
        max_output_bytes: 4096,
    }
}

pub(super) fn setup(
    journal: Arc<dyn FactJournal>,
) -> (HookScope, kolyan_ledger::FactRef, HookAccessPolicy) {
    let permissions = AgentPermissions::default();
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "agent".into(),
        revision: "1".into(),
        display_name: None,
        model: ModelRef {
            provider: "fixture".into(),
            model: "no-network".into(),
        },
        instructions: "No model execution".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let catalog = AgentCatalog::new(1).unwrap();
    let snapshot = catalog
        .resolve(
            &AgentSelector::Inline(definition),
            "instance",
            &permissions,
            &permissions,
        )
        .unwrap();
    let owner = AgentInvocationBinding {
        task_id: "task".into(),
        invocation_id: "root".into(),
        logical_session_id: "logical".into(),
        private_session_id: "logical".into(),
        context_kind: BindingContextKind::Root,
        snapshot,
    };
    let reference = AgentInvocationBindingStore::new(journal)
        .save(&owner)
        .unwrap();
    let scope = HookScope::from_binding(&owner, "execution".into(), "turn".into()).unwrap();
    let policy = HookAccessPolicy::new(
        "1".into(),
        vec![HookAccessRule {
            agent: owner.snapshot.definition().key(),
            logical_session_id: "logical".into(),
            task_id: Some("task".into()),
            invocation_id: Some("root".into()),
            hooks: BTreeSet::from([manifest().key]),
            phases: BTreeSet::from([HookPhase::BeforeModel]),
        }],
    )
    .unwrap();
    (scope, reference, policy)
}

fn category(result: Result<(), HookError>) -> (String, Option<String>) {
    match result {
        Ok(()) => ("ok".into(), None),
        Err(e) => (
            match e {
                HookError::Permission => "permission",
                HookError::Revoked => "revoked",
                HookError::Conflict => "conflict",
                HookError::Integrity(_) => "integrity",
                HookError::Invalid(_) => "invalid",
                HookError::Capacity => "capacity",
                _ => "other",
            }
            .into(),
            Some(e.to_string()),
        ),
    }
}

#[test]
fn catalog_and_binding_data_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("foundation.json")).unwrap();
    let proof = tempfile::Builder::new()
        .prefix("kolyan-hook-foundation-")
        .tempdir()
        .unwrap()
        .keep();
    let path = proof.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    for backend in ["memory", "sqlite"] {
        for case in &cases {
            let root = tempfile::tempdir().unwrap();
            let journal: Arc<dyn FactJournal> = if backend == "memory" {
                Arc::new(MemoryFactJournal::default())
            } else {
                Arc::new(SqliteFactJournal::open(root.path().join("journal.sqlite")).unwrap())
            };
            let artifacts =
                Arc::new(ArtifactStore::new(root.path().join("artifacts"), 1024 * 1024).unwrap());
            let catalog =
                HookCatalog::new(journal.clone(), artifacts.clone(), "fixture".into()).unwrap();
            let (scope, ownership, policy) = setup(journal.clone());
            let registration = catalog.register(manifest(), "printf fixture").unwrap();
            let binding = catalog
                .bind(
                    scope.clone(),
                    ownership.clone(),
                    vec![manifest().key],
                    &policy,
                    "a".repeat(64),
                )
                .unwrap();
            let action = (|| -> Result<(), HookError> {
                match case.action.as_str() {
                    "retry" => {
                        if catalog.register(manifest(), "printf fixture")? != registration {
                            return Err(HookError::Conflict);
                        }
                    }
                    "conflict" => {
                        catalog.register(manifest(), "printf changed")?;
                    }
                    "metadata" => {
                        let mut m = manifest();
                        m.timeout_ms += 1;
                        catalog.register(m, "printf fixture")?;
                    }
                    "revision" => {
                        let mut m = manifest();
                        m.key.revision = "2".into();
                        catalog.register(m, "printf fixture")?;
                    }
                    "restore" => {
                        let reopened: Arc<dyn FactJournal> = if backend == "sqlite" {
                            Arc::new(
                                SqliteFactJournal::open(root.path().join("journal.sqlite"))
                                    .unwrap(),
                            )
                        } else {
                            journal.clone()
                        };
                        let rebuilt =
                            HookCatalog::new(reopened, artifacts.clone(), "fixture".into())?;
                        if rebuilt.restore_binding(binding.reference(), &scope)? != binding {
                            return Err(HookError::Conflict);
                        }
                        rebuilt.validate_current(&binding, &policy, &"a".repeat(64))?;
                    }
                    "revoke" => {
                        catalog.revoke(
                            &manifest().key,
                            &registration.reference,
                            "revoke-1".into(),
                            "host removed".into(),
                        )?;
                        catalog.restore_binding(binding.reference(), &scope)?;
                        catalog.validate_current(&binding, &policy, &"a".repeat(64))?;
                    }
                    "deny" => {
                        catalog.bind(
                            scope.clone(),
                            ownership.clone(),
                            vec![manifest().key],
                            &HookAccessPolicy::deny_all(),
                            "a".repeat(64),
                        )?;
                    }
                    "policy" => catalog.validate_current(
                        &binding,
                        &HookAccessPolicy::deny_all(),
                        &"a".repeat(64),
                    )?,
                    "native" => catalog.validate_current(&binding, &policy, &"b".repeat(64))?,
                    "owner" => {
                        let mut other = ownership.clone();
                        other.fact_id = "foreign".into();
                        catalog.bind(
                            scope.clone(),
                            other,
                            vec![manifest().key],
                            &policy,
                            "a".repeat(64),
                        )?;
                    }
                    "scope" => {
                        let mut other = scope.clone();
                        other.private_session_id = "foreign".into();
                        catalog.restore_binding(binding.reference(), &other)?;
                    }
                    "namespace" => {
                        HookCatalog::new(journal.clone(), artifacts, "foreign".into())?
                            .restore_binding(binding.reference(), &scope)?;
                    }
                    "duplicate" => {
                        catalog.bind(
                            scope.clone(),
                            ownership,
                            vec![manifest().key, manifest().key],
                            &policy,
                            "a".repeat(64),
                        )?;
                    }
                    "missing" => {
                        let mut key = manifest().key;
                        key.revision = "missing".into();
                        catalog.bind(
                            scope.clone(),
                            ownership,
                            vec![key],
                            &policy,
                            "a".repeat(64),
                        )?;
                    }
                    "write" | "network" => {
                        let mut m = manifest();
                        m.capabilities.insert(if case.action == "write" {
                            Capability::FilesystemWrite
                        } else {
                            Capability::NetworkConnect
                        });
                        catalog.register(m, "printf fixture")?;
                    }
                    "oversized" => {
                        catalog.register(manifest(), &"x".repeat(32769))?;
                    }
                    "foreign_revoke" => {
                        let mut other = registration.reference.clone();
                        other.position += 1;
                        catalog.revoke(&manifest().key, &other, "revoke".into(), "deny".into())?;
                    }
                    _ => return Err(HookError::Invalid("unknown dataset action".into())),
                }
                Ok(())
            })();
            let (actual, error) = category(action);
            writeln!(output, "{}", json!({"backend":backend,"id":case.id,"manifest":manifest(),"scope":scope,"registration":registration,"binding":binding,"actual":actual,"error":error,"expected":case.expected,"catalog":journal.read(catalog.stream_id(),0,257).unwrap()})).unwrap();
        }
    }
    output.flush().unwrap();
    output.sync_all().unwrap();
    drop(output);
    println!("HOOK_FOUNDATION_ACTUAL {}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len() * 2);
    for row in rows {
        assert_eq!(row["actual"], row["expected"], "{row}");
    }
}
