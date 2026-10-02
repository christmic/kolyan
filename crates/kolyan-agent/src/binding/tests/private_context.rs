//! Actual Server/Storage initialization uses the production binding verifier.

use std::fs::File;
use std::io::{BufWriter, Write};

use kolyan_model::{ContentBlock, Message, MessageRole};
use kolyan_server::{PrivateContextOwner, PrivateContextService, SessionService};
use kolyan_storage::{FileSessionStore, SessionStore};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    allowed: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    Stream,
    FactId,
    Position,
    ZeroPosition,
    Task,
    Invocation,
    Logical,
    Private,
    Snapshot,
    Root,
    ExtraFact,
    Missing,
}

fn projection(text: &str) -> Vec<Message> {
    vec![Message {
        role: MessageRole::User,
        content: vec![ContentBlock::Text { text: text.into() }],
    }]
}

#[test]
fn exact_binding_initialization_matrix_exports_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("private_context.json")).unwrap();
    let report = tempfile::Builder::new()
        .prefix("kolyan-agent-private-context-")
        .tempdir()
        .unwrap()
        .keep();
    let mut rows = Vec::new();
    for case in &cases {
        let (_directory, journals) = backends();
        for (backend, journal) in ["memory", "sqlite"].into_iter().zip(journals) {
            let sessions_directory = tempfile::tempdir().unwrap();
            let sessions = FileSessionStore::new(sessions_directory.path()).unwrap();
            let mut value = binding();
            if matches!(case.mutation, Mutation::Root) {
                value.context_kind = BindingContextKind::Root;
                value.private_session_id = value.logical_session_id.clone();
            }
            let bindings = AgentInvocationBindingStore::new(journal.clone());
            let saved = if matches!(case.mutation, Mutation::Missing) {
                coordinate(&value.task_id, &value.invocation_id).unwrap()
            } else {
                bindings.save(&value).unwrap()
            };
            let loaded = bindings
                .load_with_reference(
                    &value.task_id,
                    &value.invocation_id,
                    &value.logical_session_id,
                )
                .unwrap();
            let mut reference = saved.clone();
            let mut owner = PrivateContextOwner {
                task_id: value.task_id.clone(),
                invocation_id: value.invocation_id.clone(),
                logical_session_id: value.logical_session_id.clone(),
                private_session_id: value.private_session_id.clone(),
                snapshot_digest: value.snapshot.digest().into(),
            };
            match case.mutation {
                Mutation::None | Mutation::Root | Mutation::Missing => {}
                Mutation::Stream => reference.stream_id = "foreign-stream".into(),
                Mutation::FactId => reference.fact_id = "foreign-fact".into(),
                Mutation::Position => reference.position = 2,
                Mutation::ZeroPosition => reference.position = 0,
                Mutation::Task => owner.task_id = "foreign-task".into(),
                Mutation::Invocation => owner.invocation_id = "foreign-invocation".into(),
                Mutation::Logical => owner.logical_session_id = "foreign-logical".into(),
                Mutation::Private => owner.private_session_id = "foreign-private".into(),
                Mutation::Snapshot => owner.snapshot_digest = "b".repeat(64),
                Mutation::ExtraFact => {
                    let mut extra = draft(&value);
                    extra.fact_id = "extra-critical-binding".into();
                    journal.append(&saved.stream_id, 1, vec![extra]).unwrap();
                }
            }
            let before = journal.read(&saved.stream_id, 0, 3).unwrap();
            let make = || {
                PrivateContextService::new(
                    SessionService::new(sessions.clone()),
                    journal.clone(),
                    Arc::new(AgentInvocationBindingStore::new(journal.clone())),
                )
            };
            let result = make().initialize(&owner, &reference, projection("selected child input"));
            let repeated = result
                .as_ref()
                .ok()
                .map(|_| make().initialize(&owner, &reference, projection("selected child input")));
            let changed = result
                .as_ref()
                .ok()
                .map(|_| make().initialize(&owner, &reference, projection("changed child input")));
            let after = journal.read(&saved.stream_id, 0, 3).unwrap();
            let persisted = match sessions.load(&owner.private_session_id) {
                Ok(record) => json!({"record":record}),
                Err(error) => json!({"error":error.to_string()}),
            };
            let outcome = match result {
                Ok(record) => json!({"allowed":true,"record":record}),
                Err(error) => json!({"allowed":false,"error":error.to_string()}),
            };
            rows.push(json!({
                "fixture_id":case.id,"backend":backend,"owner":owner,"reference":reference,
                "saved_reference":saved,"loaded":loaded,"binding_before":before,"binding_after":after,
                "outcome":outcome,"repeat":repeated.map(|result| match result {
                    Ok(record) => json!({"record":record}),
                    Err(error) => json!({"error":error.to_string()}),
                }),
                "changed_refused":changed.map(|result| result.is_err()),
                "persisted":persisted
            }));
        }
    }
    let path = report.join("actual.jsonl");
    let mut output = BufWriter::new(File::create(&path).unwrap());
    for row in &rows {
        serde_json::to_writer(&mut output, row).unwrap();
        writeln!(output).unwrap();
    }
    output.flush().unwrap();
    println!("AGENT_PRIVATE_CONTEXT_TRACE={}", path.display());
    let actual: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual, rows);
    assert_eq!(actual.len(), cases.len() * 2);
    let (pairs, remainder) = actual.as_chunks::<2>();
    assert!(remainder.is_empty());
    for (case, pair) in cases.iter().zip(pairs) {
        for row in pair {
            assert_eq!(
                row["outcome"]["allowed"], case.allowed,
                "{}: {row}",
                case.id
            );
            assert_eq!(row["binding_before"], row["binding_after"]);
            if case.allowed {
                assert_eq!(row["loaded"][1], row["saved_reference"]);
                assert_eq!(row["repeat"]["record"], row["outcome"]["record"]);
                assert_eq!(row["persisted"]["record"], row["outcome"]["record"]);
                assert_eq!(row["changed_refused"], true);
                assert_eq!(
                    row["persisted"]["record"]["messages"],
                    json!(projection("selected child input"))
                );
                assert_eq!(
                    row["persisted"]["record"]["context_messages"],
                    row["persisted"]["record"]["messages"]
                );
                assert_eq!(row["persisted"]["record"]["turns"], json!([]));
            } else {
                assert!(
                    row["persisted"]["error"].is_string(),
                    "refusal created a context: {row}"
                );
            }
        }
    }
}
