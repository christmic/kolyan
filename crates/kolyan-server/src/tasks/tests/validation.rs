//! Admission/replay validation and exact retry/CAS semantics.

use crate::input_fixture::SourceFixtureAdmission;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use kolyan_ledger::{FactDraft, FactError, FactRecord, FactSubject};
use serde_json::json;

use super::*;

fn observational(fact_id: &str) -> FactDraft {
    FactDraft {
        fact_id: fact_id.into(),
        subject: FactSubject {
            kind: "diagnostic.observer".into(),
            id: "observer".into(),
        },
        kind: "diagnostic.note".into(),
        schema_version: 47,
        critical: false,
        causes: Vec::new(),
        payload: json!({"note": "has no governance authority"}),
    }
}

#[test]
fn exact_command_retries_survive_later_appends_but_changed_content_fails() {
    let coordinator = setup(MemoryFactJournal::default());
    let before = coordinator.snapshot("task").unwrap();
    assert_eq!(
        coordinator.register_task("register", definition()).unwrap(),
        before
    );
    assert_eq!(
        coordinator
            .admit_fixture(
                "task",
                "admit-root",
                invocation("root", None, InvocationRole::Root)
            )
            .unwrap(),
        before
    );
    let mut changed = definition();
    changed.objective = "changed".into();
    assert!(coordinator.register_task("register", changed).is_err());
    assert_eq!(coordinator.snapshot("task").unwrap(), before);
    let attempt = binding("root", "a1");
    coordinator
        .start_attempt("task", "start", attempt.clone())
        .unwrap();
    let observed = observation(
        &attempt,
        4,
        AttemptOutcome::Completed {
            evidence: evidence(&attempt),
        },
    );
    let state = coordinator
        .observe_attempt("task", "observe", observed.clone())
        .unwrap();
    assert_eq!(
        coordinator
            .observe_attempt("task", "observe", observed.clone())
            .unwrap(),
        state
    );
    assert!(
        coordinator
            .observe_attempt("task", "different-identity", observed.clone())
            .is_err()
    );
    let mut altered = observed;
    altered.usage.output_tokens = 999;
    assert!(
        coordinator
            .observe_attempt("task", "observe", altered)
            .is_err()
    );
    assert_eq!(
        coordinator.snapshot("task").unwrap().usage.total(),
        Some(15)
    );
}

#[test]
fn unknown_observations_do_not_change_state_critical_semantics_block_replay() {
    let coordinator = setup(MemoryFactJournal::default());
    let before = coordinator.snapshot("task").unwrap();
    coordinator
        .journal()
        .append("task", before.position, vec![observational("note")])
        .unwrap();
    let after = coordinator.snapshot("task").unwrap();
    assert_eq!(after.state, before.state);
    assert_eq!(after.usage, before.usage);
    assert_eq!(after.position, before.position + 1);
    coordinator
        .start_attempt("task", "start", binding("root", "a1"))
        .unwrap();
    let head = coordinator.snapshot("task").unwrap().position;
    let mut critical = observational("unsupported");
    critical.critical = true;
    coordinator
        .journal()
        .append("task", head, vec![critical])
        .unwrap();
    assert!(coordinator.snapshot("task").is_err());
    assert!(
        coordinator
            .cancel_task("task", "unsafe-cancel", "user request")
            .is_err()
    );
}

#[test]
fn critical_version_subject_payload_and_causal_chain_are_checked_on_replay() {
    for mutation in [
        "version",
        "subject",
        "payload",
        "critical",
        "cause",
        "stream-task",
    ] {
        let coordinator = setup(MemoryFactJournal::default());
        let source = coordinator.journal().read("task", 0, 10).unwrap();
        let destination = MemoryFactJournal::default();
        let mut copied: Vec<_> = source.iter().map(|record| record.draft.clone()).collect();
        match mutation {
            "version" => copied[1].schema_version = 2,
            "subject" => copied[1].subject.id = "forged-invocation".into(),
            "payload" => copied[1].payload = json!({"unexpected": "field"}),
            "critical" => copied[1].critical = false,
            "cause" => copied[1].causes.clear(),
            _ => {
                let mut task = definition();
                task.task_id = "foreign-task".into();
                copied[0].payload =
                    serde_json::to_value(types::TaskEvent::Registered(task)).unwrap();
            }
        }
        // Preserve the exact source closure in the reconstructed journal. The
        // deliberately mutated Task drafts are still checked by production replay.
        let registration = copied.remove(0);
        destination.append("task", 0, vec![registration]).unwrap();
        for cause in &source[1].draft.causes {
            if cause.stream_id != "task" {
                let records = coordinator.journal().read(&cause.stream_id, 0, 2).unwrap();
                destination
                    .append(
                        &cause.stream_id,
                        0,
                        records.into_iter().map(|record| record.draft).collect(),
                    )
                    .unwrap();
            }
        }
        destination.append("task", 1, copied).unwrap();
        assert!(
            TaskCoordinator::new(destination).snapshot("task").is_err(),
            "{mutation}"
        );
    }
}

#[test]
fn identity_collision_and_changed_admission_are_rejected_before_append() {
    let coordinator = setup(MemoryFactJournal::default());
    let before = coordinator.snapshot("task").unwrap();
    for id in ["task", "root"] {
        assert!(
            coordinator
                .admit_fixture(
                    "task",
                    &format!("collision-{id}"),
                    invocation(id, Some("root"), InvocationRole::SelfCall)
                )
                .is_err()
        );
    }
    let mut wrong = binding("root", "a1");
    wrong.constraints_digest = "e".repeat(64);
    assert!(
        coordinator
            .start_attempt("task", "changed-constraints", wrong)
            .is_err()
    );
    let mut wrong = binding("root", "a1");
    wrong.agent.revision = "r2".into();
    assert!(
        coordinator
            .start_attempt("task", "changed-revision", wrong)
            .is_err()
    );
    assert_eq!(coordinator.snapshot("task").unwrap(), before);
}

#[derive(Clone)]
struct RacingJournal {
    inner: MemoryFactJournal,
    armed: Arc<AtomicBool>,
}

impl FactJournal for RacingJournal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.inner.read(stream, after, limit)
    }

    fn append(
        &self,
        stream: &str,
        expected: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.inner
                .append(stream, expected, vec![observational("concurrent-note")])?;
        }
        self.inner.append(stream, expected, batch)
    }
}

#[test]
fn competing_writer_invalidates_validation_without_partial_domain_commit() {
    let journal = RacingJournal {
        inner: MemoryFactJournal::default(),
        armed: Arc::new(AtomicBool::new(false)),
    };
    let coordinator = setup(journal.clone());
    journal.armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        coordinator.start_attempt("task", "start", binding("root", "a1")),
        Err(TaskError::Journal(_))
    ));
    let state = coordinator.snapshot("task").unwrap();
    assert!(state.attempts.is_empty());
    assert_eq!(state.invocations["root"].state, InvocationState::Admitted);
    assert_eq!(state.position, 3);
    assert_eq!(
        coordinator
            .start_attempt("task", "start", binding("root", "a1"))
            .unwrap()
            .attempts
            .len(),
        1
    );
}

#[test]
fn artifact_criterion_needs_matching_digest_and_actual_bound_source() {
    let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
    let mut task = definition();
    task.criteria = vec![CompletionCriterion::ArtifactDigest {
        id: "file".into(),
        invocation_id: "root".into(),
        sha256: "a".repeat(64),
    }];
    coordinator.register_task("register", task).unwrap();
    coordinator
        .admit_fixture(
            "task",
            "root",
            invocation("root", None, InvocationRole::Root),
        )
        .unwrap();
    let attempt = binding("root", "a1");
    coordinator
        .start_attempt("task", "start", attempt.clone())
        .unwrap();
    let proof = CompletionEvidence::VerifiedArtifact {
        criterion_id: "file".into(),
        source: source(&attempt, 3),
        sha256: "a".repeat(64),
        byte_len: 17,
    };
    let mut wrong = proof.clone();
    if let CompletionEvidence::VerifiedArtifact { sha256, .. } = &mut wrong {
        *sha256 = "b".repeat(64);
    }
    assert!(
        coordinator
            .observe_attempt(
                "task",
                "wrong-digest",
                observation(
                    &attempt,
                    4,
                    AttemptOutcome::Completed {
                        evidence: vec![wrong]
                    }
                )
            )
            .is_err()
    );
    coordinator
        .observe_attempt(
            "task",
            "verified",
            observation(
                &attempt,
                4,
                AttemptOutcome::Completed {
                    evidence: vec![proof.clone()],
                },
            ),
        )
        .unwrap();
    assert_eq!(
        coordinator
            .complete_task("task", "done", vec![proof])
            .unwrap()
            .state,
        TaskState::Completed
    );
}
