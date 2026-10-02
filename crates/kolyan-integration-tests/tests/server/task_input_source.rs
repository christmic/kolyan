//! Exact Server-host request provenance, not an Agent-private input DTO or grant.
use kolyan_ledger::{FactJournal, FactRecord, FactRef, SqliteFactJournal};
use kolyan_model::ModelRequest;
use kolyan_server::{
    InvocationInputEnvelope, InvocationInputKind, InvocationInputScope, InvocationInputSource,
    InvocationRole, TaskCoordinator,
};
use serde_json::{Value, json};
use std::{io::Write, path::Path};

fn reference(record: &FactRecord) -> FactRef {
    FactRef {
        stream_id: record.stream_id.clone(),
        position: record.position,
        fact_id: record.draft.fact_id.clone(),
    }
}

/// Publish once before admission; later bindings read the admitted source instead
/// of republishing changed agent/scope/request data on a negative test path.
pub(super) fn publish(
    coordinator: &TaskCoordinator<SqliteFactJournal>,
    task: &str,
    definition: &Value,
    request: ModelRequest,
) -> InvocationInputSource {
    let role: InvocationRole = serde_json::from_value(definition["role"].clone()).unwrap();
    let kind = if role == InvocationRole::Root {
        InvocationInputKind::Standalone
    } else {
        InvocationInputKind::Derived
    };
    let records = coordinator.journal().read(task, 0, 1024).unwrap();
    let mut causes = vec![reference(records.first().unwrap())];
    if let Some(parent) = definition["parent_invocation_id"].as_str() {
        let admitted = records
            .iter()
            .find(|record| {
                record.draft.kind == "task.invocation_admitted" && record.draft.subject.id == parent
            })
            .unwrap();
        causes.push(reference(admitted));
        if role == InvocationRole::Continuation {
            causes.push(
                coordinator.snapshot(task).unwrap().invocations[parent]
                    .completion_fact
                    .clone()
                    .unwrap(),
            );
        }
    }
    let saved = coordinator
        .publish_invocation_input_source(
            InvocationInputEnvelope {
                kind,
                scope: InvocationInputScope {
                    task_id: task.into(),
                    invocation_id: definition["invocation_id"].as_str().unwrap().into(),
                    agent: serde_json::from_value(definition["agent"].clone()).unwrap(),
                    constraints_digest: definition["constraints_digest"].as_str().unwrap().into(),
                },
                body: json!({"host_definition":definition,"model_request":request}),
            },
            causes,
        )
        .unwrap();
    let source = match kind {
        InvocationInputKind::Standalone => InvocationInputSource::Standalone {
            fact: saved.reference.clone(),
        },
        InvocationInputKind::Derived => InvocationInputSource::Derived {
            fact: saved.reference.clone(),
        },
    };
    // Reconstruction is a read-only exact proof check, not an admission fallback.
    let reopened = TaskCoordinator::new(coordinator.journal().clone());
    assert_eq!(reopened.load_verified_invocation_input_source(
        &source, &saved.envelope.scope, 16 * 1024 * 1024,
    ).unwrap(), saved);
    source
}

pub(super) fn export(
    coordinator: &TaskCoordinator<SqliteFactJournal>,
    task: &str,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = coordinator.snapshot(task)?;
    let mut file = std::fs::File::create(path)?;
    for invocation in snapshot.invocations.values() {
        let source = invocation.definition.input_source.fact();
        let records = coordinator.journal().read(&source.stream_id, 0, 2)?;
        for record in records {
            writeln!(file, "{}", serde_json::to_string(&record)?)?;
        }
    }
    file.sync_all()?;
    Ok(())
}
