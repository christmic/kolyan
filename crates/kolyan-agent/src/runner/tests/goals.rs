//! Actual Runner/Service goal lifecycle, with an explicitly synthetic checker.
//! Native FileWriteCommitted acceptance belongs in separate process integration.
use super::super::*;
use super::support::*;
use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, LedgerStore, MemoryFactJournal,
};
use kolyan_server::{
    ComputedGoalDecision, GoalChecker, GoalCheckerKey, GoalCheckerRegistry, GoalCriterion,
    GoalSourceLimits, GoalVerdict, LedgerTaskGoalVerifier, TaskCoordinator, TaskError,
    VerifiedGoalSource,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

struct Checker {
    key: GoalCheckerKey,
    verdict: GoalVerdict,
    operational: bool,
}
impl GoalChecker for Checker {
    fn key(&self) -> &GoalCheckerKey {
        &self.key
    }
    fn validate_predicate(&self, c: &GoalCriterion) -> Result<(), TaskError> {
        if c.predicate != json!({"expected":"Actual root answer"}) {
            return Err(TaskError::Invalid("fixture goal predicate differs".into()));
        }
        Ok(())
    }
    fn assess(
        &self,
        _: &GoalCriterion,
        s: &VerifiedGoalSource,
    ) -> Result<ComputedGoalDecision, TaskError> {
        if self.operational {
            return Err(TaskError::Invalid(
                "actual injected checker read failure".into(),
            ));
        }
        Ok(ComputedGoalDecision {
            verdict: self.verdict,
            reason: "synthetic lifecycle decision from actual verified source".into(),
            proof: json!({"terminal":s.terminal(),"response":s.response()}),
        })
    }
}
#[derive(Clone)]
struct Journal {
    inner: MemoryFactJournal,
    fail_complete: Arc<AtomicBool>,
}
impl FactJournal for Journal {
    fn read(&self, s: &str, a: u64, l: usize) -> Result<Vec<FactRecord>, FactError> {
        self.inner.read(s, a, l)
    }
    fn append(&self, s: &str, p: u64, b: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        if self.fail_complete.load(Ordering::SeqCst) && b.iter().any(|f| f.kind == "task.completed")
        {
            return Err(FactError::Storage(
                "injected crash before completed append".into(),
            ));
        }
        self.inner.append(s, p, b)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: String,
    verdict: GoalVerdict,
    expected: String,
    assessments: usize,
    requests: usize,
}

#[tokio::test]
async fn root_goals_admission_and_proof_only_finalization_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("goals.json")).unwrap();
    let root = tempfile::tempdir().unwrap().keep();
    let path = root.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let h = Harness::new();
        let ledger = h
            .service
            .sessions()
            .execution()
            .server()
            .coordinator()
            .ledger()
            .clone();
        let fault = Arc::new(AtomicBool::new(case.mode == "crash"));
        let journal = Journal {
            inner: h.service.coordinator().journal().clone(),
            fail_complete: fault.clone(),
        };
        let mut coordinator = TaskCoordinator::new(journal.clone());
        let key = GoalCheckerKey {
            kind: "fixture.root-goal".into(),
            revision: "v1".into(),
        };
        if case.mode != "missing_checker" {
            coordinator = coordinator.with_goal_verifier(Arc::new(
                LedgerTaskGoalVerifier::new(
                    ledger.clone(),
                    GoalCheckerRegistry::new(vec![Arc::new(Checker {
                        key: key.clone(),
                        verdict: case.verdict,
                        operational: case.mode == "operational",
                    })])
                    .unwrap(),
                    GoalSourceLimits::default(),
                )
                .unwrap(),
            ));
        }
        let service = Arc::new(TaskExecutionService::new(
            coordinator,
            h.service.sessions().clone(),
        ));
        let runner = Arc::new(
            AgentRunner::new(
                service.clone(),
                h.runner.instances.clone(),
                h.bindings.clone(),
                h.runner.catalog.clone(),
                h.runner.host.clone(),
                (
                    Providers {
                        observations: h.observations.clone(),
                        fail: false,
                        reject_context: false,
                        call_tool: false,
                    },
                    Tools(h.observations.clone(), false),
                ),
                h.runner.input_artifacts.clone(),
            )
            .unwrap(),
        );
        let mut input = h.request(&case.id, true);
        let mut goal = GoalCriterion::new(
            "goal".into(),
            "root".into(),
            key,
            json!({"expected":"Actual root answer"}),
        )
        .unwrap();
        match case.mode.as_str() {
            "unknown_revision" => goal.checker.revision = "unknown".into(),
            "wrong_owner" => goal.invocation_id = "other".into(),
            "execution_id" => goal.id = "root-final-answer".into(),
            "digest" => goal.predicate_digest = "b".repeat(64),
            "predicate" => {
                goal = GoalCriterion::new(
                    goal.id,
                    goal.invocation_id,
                    goal.checker,
                    json!({"unknown":true}),
                )
                .unwrap();
            }
            _ => {}
        }
        input.goals = vec![goal.clone()];
        if case.mode == "duplicate" {
            input.goals.push(goal.clone());
        }
        if case.mode == "too_many" {
            input.goals = (0..128)
                .map(|i| {
                    let mut c = goal.clone();
                    c.id = format!("goal-{i}");
                    c
                })
                .collect();
        }
        let declared = json!(input.goals);
        let result = runner.start(input).await;
        let observed = |r: Result<TaskSnapshot, RunnerError>| match r {
            Ok(t) => json!({"status":t.state,"task":t}),
            Err(e) => json!({"status":"Refused","error":e.to_string()}),
        };
        let first = observed(result.map(|r| r.task));
        let before_facts = journal.read(&case.id, 0, 1024).unwrap();
        let before_events = ledger.events_after(0).unwrap();
        let before_requests = h.observations.requests.lock().unwrap().clone();
        fault.store(false, Ordering::SeqCst);
        // Rebuild Runner over the actual saved bindings and same host service;
        // crash rows retain an assessment committed before failed completion.
        let restored = Arc::new(
            AgentRunner::new(
                service.clone(),
                runner.instances.clone(),
                h.bindings.clone(),
                AgentCatalog::new(8).unwrap(),
                runner.host.clone(),
                (
                    Providers {
                        observations: h.observations.clone(),
                        fail: false,
                        reject_context: false,
                        call_tool: false,
                    },
                    Tools(h.observations.clone(), false),
                ),
                h.runner.input_artifacts.clone(),
            )
            .unwrap(),
        );
        let request = TaskFinalizationRequest {
            task_id: case.id.clone(),
            logical_session_id: "session".into(),
            root_invocation_id: "root".into(),
            root_attempt_id: "attempt".into(),
            policy: TaskFinalizationPolicy::AllInvocationsSuccessful,
        };
        let finish = if case.requests > 0 {
            Some(observed(restored.finalize_task(request.clone()).await))
        } else {
            None
        };
        let after_first = journal.read(&case.id, 0, 1024).unwrap();
        let repeat = if case.requests > 0 {
            Some(observed(restored.finalize_task(request).await))
        } else {
            None
        };
        writeln!(export,"{}",json!({"case":case.id,"declared":declared,"first":first,"final":finish,"repeat":repeat,
            "before_facts":before_facts,"after_first":after_first,"facts":journal.read(&case.id,0,1024).unwrap(),"before_events":before_events,"events":ledger.events_after(0).unwrap(),
            "before_requests":before_requests,"requests":*h.observations.requests.lock().unwrap(),"effects":*h.observations.effects.lock().unwrap()})).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("RUNNER_ROOT_GOALS_TRACE={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        let actual = if case.requests > 0 {
            &row["final"]
        } else {
            &row["first"]
        };
        assert_eq!(actual["status"], case.expected, "{}: {row}", case.id);
        assert_eq!(
            row["facts"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|f| f["draft"]["kind"] == "task.goal_assessed")
                .count(),
            case.assessments
        );
        assert_eq!(row["requests"].as_array().unwrap().len(), case.requests);
        assert_eq!(row["effects"], json!([]));
        assert_eq!(row["requests"], row["before_requests"]);
        assert_eq!(row["events"], row["before_events"]);
        assert_eq!(row["facts"], row["after_first"]);
        if case.requests > 0 {
            assert_eq!(row["final"], row["repeat"]);
        }
        if case.mode == "crash" {
            assert_eq!(row["first"]["status"], "Refused");
            assert_eq!(
                row["before_facts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|f| f["draft"]["kind"] == "task.goal_assessed")
                    .count(),
                1
            );
        }
        if case.mode == "operational" {
            assert!(
                row["first"]["error"]
                    .as_str()
                    .unwrap()
                    .contains("actual injected checker read failure")
            );
        }
    }
}
