//! Atomic typed source admission/replay; no Agent body authorization is claimed.
mod race;

use super::*;
use crate::{
    AttemptBinding, CancellationPolicy, CompletionCriterion, InvocationDefinition, InvocationRole,
    TaskDefinition, TaskLimits,
};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum Mutation {
    Read,
    WrongAttemptSource,
    MissingDefinitionSource,
    MissingAttemptSource,
    MissingAdmissionCause,
    MissingAttemptCause,
    WrongRole,
    Cancelled,
    ChangedBody,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    accepted: bool,
}
#[derive(Clone)]
struct Corrupt<J> {
    journal: J,
    mutation: Mutation,
    source: FactRef,
}
impl<J: FactJournal> FactJournal for Corrupt<J> {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        let mut rows = self.journal.read(stream, after, limit)?;
        for row in &mut rows {
            match (&self.mutation, row.draft.kind.as_str()) {
                (Mutation::MissingDefinitionSource, "task.invocation_admitted") => {
                    row.draft.payload["InvocationAdmitted"]
                        .as_object_mut()
                        .unwrap()
                        .remove("input_source");
                }
                (Mutation::MissingAttemptSource, "task.attempt_started") => {
                    row.draft.payload["AttemptStarted"]
                        .as_object_mut()
                        .unwrap()
                        .remove("input_source");
                }
                (Mutation::MissingAdmissionCause, "task.invocation_admitted")
                | (Mutation::MissingAttemptCause, "task.attempt_started") => {
                    row.draft.causes.retain(|cause| cause != &self.source)
                }
                _ => {}
            }
        }
        Ok(rows)
    }
    fn append(&self, _: &str, _: u64, _: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        panic!("replay writes");
    }
}
fn registered<J: FactJournal>(journal: J) -> TaskCoordinator<J> {
    let coordinator = TaskCoordinator::new(journal);
    let scope = scope();
    coordinator
        .register_task(
            "registered",
            TaskDefinition {
                task_id: scope.task_id,
                objective: "typed source binding fixture".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: scope.agent,
                constraints_digest: scope.constraints_digest,
                limits: TaskLimits {
                    max_depth: 2,
                    max_invocations: 3,
                    max_attempts: 3,
                    max_tokens: None,
                    max_steps_per_turn: 3,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    coordinator
}
fn prepare<J: FactJournal>(
    coordinator: &TaskCoordinator<J>,
) -> (InvocationInputEnvelope, InvocationInputSource) {
    let envelope = InvocationInputEnvelope {
        kind: InvocationInputKind::Standalone,
        scope: scope(),
        body: json!({"meaning":"generic source fixture, not Agent body verification"}),
    };
    let saved = coordinator
        .publish_invocation_input_source(
            envelope.clone(),
            vec![FactRef {
                stream_id: "task".into(),
                position: 1,
                fact_id: "registered".into(),
            }],
        )
        .unwrap();
    (
        envelope,
        InvocationInputSource::Standalone {
            fact: saved.reference,
        },
    )
}
fn definition(input_source: InvocationInputSource) -> InvocationDefinition {
    let scope = scope();
    InvocationDefinition {
        invocation_id: scope.invocation_id,
        agent: scope.agent,
        constraints_digest: scope.constraints_digest,
        role: InvocationRole::Root,
        parent_invocation_id: None,
        dependencies: vec![],
        input_source,
    }
}
fn binding(input_source: InvocationInputSource) -> AttemptBinding {
    let scope = scope();
    AttemptBinding {
        attempt_id: "attempt".into(),
        invocation_id: scope.invocation_id,
        agent: scope.agent,
        constraints_digest: scope.constraints_digest,
        input_source,
        execution: crate::ExecutionRef {
            session_id: "session".into(),
            turn_id: "turn".into(),
            execution_id: "execution".into(),
        },
    }
}
fn run<J: FactJournal + Clone>(
    journal: J,
    case: &Case,
    output: &mut std::fs::File,
    backend: &str,
) -> (bool, bool) {
    let coordinator = registered(journal.clone());
    let (envelope, source) = prepare(&coordinator);
    let mut proposed = definition(source.clone());
    let before = journal.read("task", 0, 32).unwrap();
    let result = match case.mutation {
        Mutation::WrongRole => {
            proposed.input_source = InvocationInputSource::Derived {
                fact: source.fact().clone(),
            };
            coordinator.admit_invocation("task", "admitted", proposed)
        }
        Mutation::Cancelled => {
            coordinator
                .cancel_task("task", "cancelled", "fixture cancel")
                .unwrap();
            coordinator.admit_invocation("task", "admitted", proposed)
        }
        Mutation::ChangedBody => {
            coordinator
                .admit_invocation("task", "admitted", proposed)
                .unwrap();
            let mut changed = envelope;
            changed.body = json!({"late_changed_candidate":true});
            coordinator
                .publish_invocation_input_source(
                    changed,
                    vec![FactRef {
                        stream_id: "task".into(),
                        position: 1,
                        fact_id: "registered".into(),
                    }],
                )
                .and_then(|_| coordinator.snapshot("task"))
        }
        _ => {
            coordinator
                .admit_invocation("task", "admitted", proposed)
                .unwrap();
            let mut attempt = binding(source.clone());
            if matches!(case.mutation, Mutation::WrongAttemptSource) {
                attempt.input_source = InvocationInputSource::Derived {
                    fact: source.fact().clone(),
                };
                coordinator.start_attempt("task", "started", attempt)
            } else {
                coordinator
                    .start_attempt("task", "started", attempt)
                    .unwrap();
                TaskCoordinator::new(Corrupt {
                    journal: journal.clone(),
                    mutation: case.mutation,
                    source: source.fact().clone(),
                })
                .snapshot("task")
            }
        }
    };
    let after = journal.read("task", 0, 32).unwrap();
    let sources = journal.read(&source.fact().stream_id, 0, 2).unwrap();
    writeln!(output,"{}",json!({"backend":backend,"case":case,"before":before,"source":sources,"result":result.as_ref().map_err(ToString::to_string),"after":after})).unwrap();
    let bound = after
        .iter()
        .filter(|record| {
            matches!(
                record.draft.kind.as_str(),
                "task.invocation_admitted" | "task.attempt_started"
            )
        })
        .all(|record| record.draft.causes.contains(source.fact()));
    (result.is_ok(), bound)
}
#[test]
fn invocation_input_source_atomic_admission_exact_attempt_and_replay_data() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-input-admission-")
        .tempdir()
        .unwrap()
        .keep();
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    println!(
        "INPUT_ADMISSION_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let cases: Vec<Case> = serde_json::from_str(include_str!("admission_cases.json")).unwrap();
    let mut observations = Vec::new();
    for case in cases {
        let memory = run(MemoryFactJournal::default(), &case, &mut output, "memory");
        let sqlite = run(
            SqliteFactJournal::open(root.join(format!("{}.sqlite", case.id))).unwrap(),
            &case,
            &mut output,
            "sqlite",
        );
        observations.push((case, memory, sqlite));
    }
    output.sync_all().unwrap();
    for (case, memory, sqlite) in observations {
        for (accepted, bound) in [memory, sqlite] {
            assert_eq!(accepted, case.accepted, "{}", case.id);
            assert!(bound, "{}", case.id);
        }
    }
}

#[test]
fn invocation_input_source_atomic_admission_concurrent_exact_retry_rebuild() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-input-admission-cas-")
        .tempdir()
        .unwrap()
        .keep();
    let journal = SqliteFactJournal::open(root.join("facts.sqlite")).unwrap();
    let coordinator = registered(journal.clone());
    let (_, source) = prepare(&coordinator);
    let results = std::thread::scope(|threads| {
        let handles: Vec<_> =
            (0..4)
                .map(|_| {
                    let path = root.join("facts.sqlite");
                    let source = source.clone();
                    threads.spawn(move || {
                        TaskCoordinator::new(SqliteFactJournal::open(path).unwrap())
                            .admit_invocation("task", "admitted", definition(source))
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
            .admit_invocation("task", "admitted", definition(source));
    let records = journal.read("task", 0, 32).unwrap();
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    println!(
        "INPUT_ADMISSION_CAS_TRACE={}",
        root.join("actual.jsonl").display()
    );
    writeln!(output,"{}",json!({"results":results.iter().map(|result|result.as_ref().map_err(ToString::to_string)).collect::<Vec<_>>(),"restored":restored.as_ref().map_err(ToString::to_string),"records":records})).unwrap();
    output.sync_all().unwrap();
    let expected = restored.unwrap();
    for result in results {
        assert_eq!(result.unwrap(), expected);
    }
    assert_eq!(records.len(), 2);
}
