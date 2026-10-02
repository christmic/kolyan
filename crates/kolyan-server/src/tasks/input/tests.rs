//! Generic evidence fixtures are not Agent source authorization or body validation.
mod admission;
mod causal;
use super::*;
use kolyan_ledger::{FactError, MemoryFactJournal, SqliteFactJournal};
use std::io::Write;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum Mutation {
    Read,
    Schema,
    Critical,
    Subject,
    Scope,
    Kind,
    Unknown,
    Missing,
    Cycle,
    Duplicate,
    Foreign,
    ChangedBody,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum Cap {
    Maximum,
    Zero,
    AboveMaximum,
    Exact,
    BelowExact,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    cap: Cap,
    accepted: bool,
}
#[derive(Clone)]
struct ReadOnly<J> {
    journal: J,
    mutation: Mutation,
}
impl<J: FactJournal> FactJournal for ReadOnly<J> {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        let mut rows = self.journal.read(stream, after, limit)?;
        for row in &mut rows {
            if row.draft.kind == KIND {
                match self.mutation {
                    Mutation::Schema => row.draft.schema_version = 2,
                    Mutation::Critical => row.draft.critical = false,
                    Mutation::Subject => row.draft.subject.id = "foreign".into(),
                    Mutation::Scope => row.draft.payload["scope"]["task_id"] = json!("foreign"),
                    Mutation::Kind => row.draft.payload["kind"] = json!("Derived"),
                    Mutation::Unknown => row.draft.payload["unknown"] = json!(true),
                    Mutation::Missing => {
                        row.draft.payload.as_object_mut().unwrap().remove("kind");
                    }
                    Mutation::Cycle => {
                        row.draft.causes = vec![FactRef {
                            stream_id: stream.into(),
                            position: 1,
                            fact_id: stream.into(),
                        }]
                    }
                    Mutation::Duplicate => row.draft.causes.push(row.draft.causes[0].clone()),
                    _ => {}
                }
            }
        }
        Ok(rows)
    }
    fn append(&self, _: &str, _: u64, _: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        panic!("read port writes")
    }
}
fn scope() -> InvocationInputScope {
    InvocationInputScope {
        task_id: "task".into(),
        invocation_id: "root".into(),
        agent: AgentIdentity {
            definition_id: "agent".into(),
            revision: "r1".into(),
            instance_id: "instance".into(),
        },
        constraints_digest: "a".repeat(64),
    }
}
fn setup<J: FactJournal>(journal: &J) -> FactRef {
    journal
        .append(
            "owner",
            0,
            vec![FactDraft {
                fact_id: "owner".into(),
                subject: FactSubject {
                    kind: "fixture.owner".into(),
                    id: "root".into(),
                },
                kind: "fixture.owner".into(),
                schema_version: 1,
                critical: true,
                causes: vec![],
                payload: json!({"meaning":"generic Server fixture, not Agent authorization"}),
            }],
        )
        .unwrap();
    FactRef {
        stream_id: "owner".into(),
        position: 1,
        fact_id: "owner".into(),
    }
}
fn observations<J: FactJournal + Clone>(
    journal: J,
    output: &mut std::fs::File,
    backend: &str,
) -> Vec<(Case, Result<VerifiedInvocationInputSource, TaskError>, bool)> {
    let cause = setup(&journal);
    let coordinator = TaskCoordinator::new(journal.clone());
    let envelope = InvocationInputEnvelope {
        kind: InvocationInputKind::Standalone,
        scope: scope(),
        body: json!({"fixture":"opaque body deliberately not interpreted by Server"}),
    };
    let saved = coordinator
        .publish_invocation_input_source(envelope.clone(), vec![cause.clone()])
        .unwrap();
    let source = InvocationInputSource::Standalone {
        fact: saved.reference.clone(),
    };
    let before = journal.read(&saved.reference.stream_id, 0, 2).unwrap();
    let exact = serde_json::to_vec(&saved)
        .unwrap()
        .len()
        .max(serde_json::to_vec(&before[0]).unwrap().len());
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let mut results = Vec::new();
    for case in cases {
        let cap = match case.cap {
            Cap::Maximum => MAX_BYTES,
            Cap::Zero => 0,
            Cap::AboveMaximum => MAX_BYTES + 1,
            Cap::Exact => exact,
            Cap::BelowExact => exact - 1,
        };
        let mut requested = source.clone();
        if matches!(case.mutation, Mutation::Foreign) {
            requested = InvocationInputSource::Standalone {
                fact: cause.clone(),
            };
        }
        let result = if matches!(case.mutation, Mutation::ChangedBody) {
            let mut changed = envelope.clone();
            changed.body = json!({"changed":true});
            coordinator.publish_invocation_input_source(changed, vec![cause.clone()])
        } else {
            TaskCoordinator::new(ReadOnly {
                journal: journal.clone(),
                mutation: case.mutation,
            })
            .load_verified_invocation_input_source(&requested, &scope(), cap)
        };
        let after = journal.read(&saved.reference.stream_id, 0, 2).unwrap();
        writeln!(output,"{}",json!({"backend":backend,"case":case,"cap":cap,"exact_cap":exact,"before":before,"result":result.as_ref().map_err(ToString::to_string),"after":after})).unwrap();
        results.push((case, result, before == after));
    }
    results
}
#[test]
fn invocation_input_source_read_data_exports_all_backends_before_compare() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-input-source-")
        .tempdir()
        .unwrap()
        .keep();
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    println!("INPUT_SOURCE_TRACE={}", root.join("actual.jsonl").display());
    let mut rows = observations(MemoryFactJournal::default(), &mut output, "memory");
    rows.extend(observations(
        SqliteFactJournal::open(root.join("facts.sqlite")).unwrap(),
        &mut output,
        "sqlite",
    ));
    output.sync_all().unwrap();
    for (case, actual, unchanged) in rows {
        assert_eq!(actual.is_ok(), case.accepted, "{}: {actual:?}", case.id);
        assert!(unchanged, "{}", case.id);
    }
}

#[test]
fn invocation_input_source_concurrent_publication_rebuild_and_large_body() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-input-source-cas-")
        .tempdir()
        .unwrap()
        .keep();
    let journal = SqliteFactJournal::open(root.join("facts.sqlite")).unwrap();
    let cause = setup(&journal);
    let envelope = InvocationInputEnvelope {
        kind: InvocationInputKind::Standalone,
        scope: scope(),
        body: json!({"large_opaque_fixture": "x".repeat(80*1024),"meaning":"not an Agent authorization fixture"}),
    };
    let results = std::thread::scope(|threads| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let path = root.join("facts.sqlite");
                let envelope = envelope.clone();
                let cause = cause.clone();
                threads.spawn(move || {
                    TaskCoordinator::new(SqliteFactJournal::open(path).unwrap())
                        .publish_invocation_input_source(envelope, vec![cause])
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    let restored =
        TaskCoordinator::new(SqliteFactJournal::open(root.join("facts.sqlite")).unwrap())
            .publish_invocation_input_source(envelope.clone(), vec![cause.clone()]);
    let mut changed = envelope;
    changed.scope.agent.instance_id = "foreign".into();
    let conflict =
        TaskCoordinator::new(journal.clone()).publish_invocation_input_source(changed, vec![cause]);
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    println!(
        "INPUT_SOURCE_CAS_TRACE={}",
        root.join("actual.jsonl").display()
    );
    writeln!(output,"{}",json!({"results":results.iter().map(|r| r.as_ref().map_err(ToString::to_string)).collect::<Vec<_>>(),"restored":restored.as_ref().map_err(ToString::to_string),"changed_identity":conflict.as_ref().map_err(ToString::to_string),"source_records":journal.read(&coordinate(&scope()).unwrap(),0,2).unwrap()})).unwrap();
    output.sync_all().unwrap();
    let saved = restored.unwrap();
    for result in results {
        assert_eq!(result.unwrap(), saved);
    }
    assert!(conflict.is_err());
    assert_eq!(
        journal
            .read(&saved.reference.stream_id, 0, 2)
            .unwrap()
            .len(),
        1
    );
}
