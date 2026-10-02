//! Concrete Service, pre-apply and replay gates; all rows precede comparisons.
use super::backends::{BackendJournal, inventory};
use super::support::{harness_with_child, registry, verifier};
use crate::*;
use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, LedgerEventKind, LedgerStore, MemoryFactJournal,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;
use std::sync::Arc;

struct FailedChecker(GoalCheckerKey);
impl GoalChecker for FailedChecker {
    fn key(&self) -> &GoalCheckerKey {
        &self.0
    }
    fn validate_predicate(&self, _: &GoalCriterion) -> Result<(), TaskError> {
        Ok(())
    }
    fn assess(
        &self,
        _: &GoalCriterion,
        _: &VerifiedGoalSource,
    ) -> Result<ComputedGoalDecision, TaskError> {
        Err(TaskError::Invalid("injected computation fault".into()))
    }
}
struct FailedRead;
impl LedgerStore for FailedRead {
    fn append(
        &self,
        _: kolyan_ledger::LedgerEvent,
    ) -> Result<kolyan_ledger::LedgerEvent, kolyan_ledger::LedgerError> {
        panic!("no writes")
    }
    fn claim(&self, _: &str) -> Result<bool, kolyan_ledger::LedgerError> {
        panic!("no claims")
    }
    fn events_after(
        &self,
        _: u64,
    ) -> Result<Vec<kolyan_ledger::LedgerEvent>, kolyan_ledger::LedgerError> {
        panic!("no history")
    }
    fn query(
        &self,
        _: &kolyan_ledger::LedgerQuery,
    ) -> Result<Vec<kolyan_ledger::LedgerEvent>, kolyan_ledger::LedgerError> {
        Err(kolyan_ledger::LedgerError::Storage(
            "injected source storage fault".into(),
        ))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    verdict: GoalVerdict,
    mode: String,
    expected: String,
    assessment_rows: usize,
}
struct CorruptReplay(BackendJournal);
impl FactJournal for CorruptReplay {
    fn read(&self, s: &str, a: u64, l: usize) -> Result<Vec<FactRecord>, FactError> {
        let mut rows = self.0.read(s, a, l)?;
        for row in &mut rows {
            if row.draft.kind == "task.goal_assessed" {
                row.draft.payload["GoalAssessed"]["verdict"] = json!("Unsatisfied");
            }
        }
        Ok(rows)
    }
    fn append(&self, _: &str, _: u64, _: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        panic!("replay must not write")
    }
}

#[tokio::test]
async fn goal_assessment_admission_replay_and_public_completion_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("assessments.json")).unwrap();
    let directory = tempfile::tempdir().unwrap().keep();
    let path = directory.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    let backends = inventory();
    for backend in &backends {
        for case in &cases {
            let h = harness_with_child(
                case.verdict,
                false,
                case.mode.starts_with("child_"),
                *backend,
            )
            .await;
            let coordinator = h.service.coordinator();
            let before = coordinator.snapshot("task").unwrap();
            let original_events = h.ledger.events_after(0).unwrap();
            let original_requests = h.requests.lock().unwrap().clone();
            let mut submitted = Value::Null;
            let result: Result<TaskSnapshot, String> = match case.mode.as_str() {
                mode if mode.starts_with("claim_") => {
                    let mut claim = coordinator
                        .compute_goal_assessment(&before, "goal")
                        .unwrap();
                    match mode {
                        "claim_predicate" => claim.predicate_digest = "b".repeat(64),
                        "claim_revision" => claim.checker.revision = "unknown-v2".into(),
                        "claim_source" => claim.source.cursor += 1,
                        "claim_proof_bound" => claim.proof = json!("x".repeat(65536)),
                        "claim_reason_bound" => claim.reason = "界".repeat(2731),
                        "claim_unknown" => {}
                        _ => panic!("unknown fixture mode"),
                    }
                    submitted = serde_json::to_value(&claim).unwrap();
                    if mode == "claim_unknown" {
                        submitted["unknown"] = json!(true);
                    }
                    serde_json::from_value::<GoalAssessment>(submitted.clone())
                        .map_err(|e| e.to_string())
                        .and_then(|a| {
                            coordinator
                                .assess_goal("task", "assessment", a)
                                .map_err(|e| e.to_string())
                        })
                }
                "cancelled" => {
                    coordinator
                        .cancel_task("task", "cancel", "host cancelled")
                        .unwrap();
                    h.service
                        .assess_goal("task", "assessment", "goal")
                        .map_err(|e| e.to_string())
                }
                "changed_role" => {
                    let mut forged = before.clone();
                    forged.invocations.get_mut("root").unwrap().definition.role =
                        InvocationRole::Continuation;
                    coordinator
                        .compute_goal_assessment(&forged, "goal")
                        .map_err(|e| e.to_string())
                        .and_then(|a| {
                            coordinator
                                .assess_goal("task", "assessment", a)
                                .map_err(|e| e.to_string())
                        })
                }
                "tight_budget" => {
                    let port = LedgerTaskGoalVerifier::new(
                        h.ledger.clone(),
                        registry(case.verdict),
                        GoalSourceLimits {
                            max_rows: 1,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                    let narrow = TaskCoordinator::new(coordinator.journal().clone())
                        .with_goal_verifier(Arc::new(port));
                    narrow
                        .compute_goal_assessment(&before, "goal")
                        .map_err(|e| e.to_string())
                        .and_then(|a| {
                            narrow
                                .assess_goal("task", "assessment", a)
                                .map_err(|e| e.to_string())
                        })
                }
                "bypass" => coordinator
                    .complete_task("task", "complete", vec![])
                    .map_err(|e| e.to_string()),
                "missing_verifier" => TaskCoordinator::new(MemoryFactJournal::default())
                    .register_task("register", before.definition.clone())
                    .map_err(|e| e.to_string()),
                "computation_fault" | "storage_fault" => {
                    let port: Arc<dyn TaskGoalVerifier> = if case.mode == "storage_fault" {
                        Arc::new(
                            LedgerTaskGoalVerifier::new(
                                FailedRead,
                                registry(case.verdict),
                                GoalSourceLimits::default(),
                            )
                            .unwrap(),
                        )
                    } else {
                        Arc::new(
                            LedgerTaskGoalVerifier::new(
                                h.ledger.clone(),
                                GoalCheckerRegistry::new(vec![Arc::new(FailedChecker(
                                    GoalCheckerKey {
                                        kind: "fixture.text".into(),
                                        revision: "v1".into(),
                                    },
                                ))])
                                .unwrap(),
                                GoalSourceLimits::default(),
                            )
                            .unwrap(),
                        )
                    };
                    let isolated = TaskCoordinator::new(coordinator.journal().clone())
                        .with_goal_verifier(port);
                    isolated
                        .compute_goal_assessment(&before, "goal")
                        .map_err(|e| e.to_string())
                        .and_then(|a| {
                            isolated
                                .assess_goal("task", "assessment", a)
                                .map_err(|e| e.to_string())
                        })
                }
                "late_effect" => {
                    let mut late = original_events
                        .iter()
                        .find(|e| e.kind == LedgerEventKind::StepCompleted)
                        .unwrap()
                        .clone();
                    late.cursor = 0;
                    late.event_id = "execution/late-step".into();
                    late.idempotency_key = late.event_id.clone();
                    h.ledger.append(late).unwrap();
                    h.service
                        .assess_goal("task", "assessment", "goal")
                        .map_err(|e| e.to_string())
                }
                "forged_verdict" | "forged_proof" => {
                    let mut claim = coordinator
                        .compute_goal_assessment(&before, "goal")
                        .unwrap();
                    if case.mode == "forged_verdict" {
                        claim.verdict = GoalVerdict::Satisfied;
                    } else {
                        claim.proof = json!({"fabricated":true});
                    }
                    coordinator
                        .assess_goal("task", "assessment", claim)
                        .map_err(|e| e.to_string())
                }
                _ => {
                    let first = h.service.assess_goal("task", "assessment", "goal");
                    match first {
                        Err(e) => Err(e.to_string()),
                        Ok(_) if case.mode == "retry_new" => h
                            .service
                            .assess_goal("task", "second-assessment", "goal")
                            .map_err(|e| e.to_string()),
                        Ok(_) if case.mode == "corrupt_replay" => {
                            TaskCoordinator::new(CorruptReplay(coordinator.journal().clone()))
                                .with_goal_verifier(verifier(h.ledger.clone(), case.verdict))
                                .snapshot("task")
                                .map_err(|e| e.to_string())
                        }
                        Ok(_) => {
                            if case.mode == "retry_same" {
                                h.service.assess_goal("task", "assessment", "goal").unwrap();
                            }
                            if case.mode == "child_late_step" {
                                let mut late = original_events
                                    .iter()
                                    .find(|e| {
                                        e.execution_id == "child-execution"
                                            && e.kind == LedgerEventKind::StepCompleted
                                    })
                                    .unwrap()
                                    .clone();
                                late.cursor = 0;
                                late.event_id = "child-execution/late-step".into();
                                late.idempotency_key = late.event_id.clone();
                                h.ledger.append(late).unwrap();
                            }
                            // Rebuild the coordinator for replay/public completion. The
                            // original-service branches below do not prove service restart;
                            // reconstruction.rs exercises that separate boundary.
                            let (reopened_ledger, reopened_journal) =
                                h.backend
                                    .reopen(h.root.path(), &h.ledger, coordinator.journal());
                            let rebuilt = TaskCoordinator::new(reopened_journal)
                                .with_goal_verifier(verifier(reopened_ledger, case.verdict));
                            let reconstructed = rebuilt.snapshot("task").unwrap();
                            if case.mode == "service" {
                                h.service
                                    .complete("task", "complete")
                                    .map_err(|e| e.to_string())
                            } else if case.verdict == GoalVerdict::Satisfied {
                                let saved = &reconstructed.goal_assessments[0];
                                rebuilt
                                    .complete_task(
                                        "task",
                                        "complete",
                                        vec![CompletionEvidence::GoalSatisfied {
                                            criterion_id: "goal".into(),
                                            source: saved.assessment.source.clone(),
                                            assessment: saved.reference.clone(),
                                            assessment_digest: saved.assessment_digest.clone(),
                                        }],
                                    )
                                    .map_err(|e| e.to_string())
                            } else {
                                h.service
                                    .complete("task", "complete")
                                    .map_err(|e| e.to_string())
                            }
                        }
                    }
                }
            };
            let facts = coordinator.journal().read("task", 0, 1024).unwrap();
            let assessment_rows = facts
                .iter()
                .filter(|r| r.draft.kind == "task.goal_assessed")
                .count();
            let actual = match result {
                Ok(snapshot) => json!({"status":snapshot.state,"snapshot":snapshot}),
                Err(error) => json!({"status":"Refused","error":error}),
            };
            writeln!(export,"{}",json!({"backend":backend,"case":case.id,"actual":actual,"submitted":submitted,"before":before,"facts":facts,"assessment_rows":assessment_rows,
            "events":h.ledger.events_after(0).unwrap(),"requests":*h.requests.lock().unwrap(),"original_requests":original_requests,
            "tool_calls":h.tool_calls.load(std::sync::atomic::Ordering::SeqCst)})).unwrap();
        }
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("goal_assessment matrix {}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len() * backends.len());
    for (index, row) in rows.iter().enumerate() {
        let case = &cases[index % cases.len()];
        assert_eq!(
            row["backend"],
            serde_json::to_value(backends[index / cases.len()]).unwrap()
        );
        assert_eq!(
            row["actual"]["status"], case.expected,
            "{}: {}",
            case.id, row
        );
        assert_eq!(row["assessment_rows"], case.assessment_rows, "{}", case.id);
        assert_eq!(
            row["requests"], row["original_requests"],
            "no model reentry {}",
            case.id
        );
        assert_eq!(row["tool_calls"], 0);
        assert_eq!(row["before"]["state"], "Waiting");
    }
}
