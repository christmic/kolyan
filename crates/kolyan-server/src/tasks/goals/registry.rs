//! Exact-key deterministic checkers and a concrete Ledger-backed enforcing port.

use std::collections::BTreeMap;
use std::sync::Arc;

use kolyan_ledger::LedgerStore;
use serde_json::json;

use super::*;
use crate::{
    AttemptOutcome, CompletionCriterion, CompletionEvidence, GoalSourceCoverage, GoalSourceLimits,
    GoalSourceReader, InvocationState, TaskError, TaskSnapshot, VerifiedGoalSource,
};

pub trait GoalChecker: Send + Sync {
    fn key(&self) -> &GoalCheckerKey;
    fn validate_predicate(&self, criterion: &GoalCriterion) -> Result<(), TaskError>;
    fn assess(
        &self,
        criterion: &GoalCriterion,
        source: &VerifiedGoalSource,
    ) -> Result<ComputedGoalDecision, TaskError>;
}

pub trait TaskGoalVerifier: Send + Sync {
    fn validate_criterion(&self, criterion: &GoalCriterion) -> Result<(), TaskError>;
    fn compute_assessment(
        &self,
        prefix: &TaskSnapshot,
        criterion_id: &str,
    ) -> Result<GoalAssessment, TaskError>;
    fn verify_assessment(
        &self,
        prefix: &TaskSnapshot,
        assessment: &GoalAssessment,
    ) -> Result<(), TaskError>;
    fn verify_completion(
        &self,
        prefix: &TaskSnapshot,
        evidence: &[CompletionEvidence],
    ) -> Result<(), TaskError>;
}

pub struct GoalCheckerRegistry {
    entries: BTreeMap<GoalCheckerKey, Arc<dyn GoalChecker>>,
}

impl GoalCheckerRegistry {
    /// Freeze unique, exact host implementations. No model-selected plugins.
    pub fn new(checkers: Vec<Arc<dyn GoalChecker>>) -> Result<Self, TaskError> {
        if checkers.is_empty() || checkers.len() > 128 {
            return Err(bad("checker count must be 1..128"));
        }
        let mut entries = BTreeMap::new();
        for checker in checkers {
            checker.key().validate()?;
            if entries.insert(checker.key().clone(), checker).is_some() {
                return Err(bad("duplicate checker key"));
            }
        }
        Ok(Self { entries })
    }
    fn get(&self, key: &GoalCheckerKey) -> Result<&dyn GoalChecker, TaskError> {
        let checker = self
            .entries
            .get(key)
            .ok_or_else(|| bad("unknown goal checker kind/revision"))?;
        if checker.key() != key {
            return Err(bad("registered checker key changed"));
        }
        Ok(checker.as_ref())
    }
}

pub struct LedgerTaskGoalVerifier<L> {
    reader: GoalSourceReader<L>,
    registry: GoalCheckerRegistry,
    limits: GoalSourceLimits,
}

impl<L: LedgerStore> LedgerTaskGoalVerifier<L> {
    pub fn new(
        ledger: L,
        registry: GoalCheckerRegistry,
        limits: GoalSourceLimits,
    ) -> Result<Self, TaskError> {
        limits.validate().map_err(|error| bad(&error.to_string()))?;
        Ok(Self {
            reader: GoalSourceReader::new(ledger),
            registry,
            limits,
        })
    }
    fn computed(
        &self,
        prefix: &TaskSnapshot,
        assessment: &GoalAssessment,
    ) -> Result<ComputedGoalDecision, TaskError> {
        let (criterion, _) = super::assessment_owner(prefix, assessment)?;
        self.validate_criterion(criterion)?;
        let source = match self.reader.inspect_stopped(
            prefix,
            &assessment.attempt,
            &assessment.source,
            &self.limits,
        ) {
            Ok(source) => source,
            Err(crate::GoalSourceError::BoundsExhausted) => {
                return Ok(ComputedGoalDecision {
                    verdict: GoalVerdict::Indeterminate,
                    reason: "source budget exhausted before physical identity validation".into(),
                    proof: json!({"coverage":GoalSourceCoverage::Exhausted,"unverified_source":assessment.source}),
                });
            }
            Err(error) => return Err(TaskError::GoalSource(error)),
        };
        let decision = if source.coverage() == GoalSourceCoverage::Complete {
            self.registry
                .get(&criterion.checker)?
                .assess(criterion, &source)?
        } else {
            ComputedGoalDecision {
                verdict: GoalVerdict::Indeterminate,
                reason: "authoritative source coverage is incomplete or uncertain".into(),
                proof: json!({"coverage":source.coverage(),"source":source.terminal()}),
            }
        };
        decision.validate()?;
        Ok(decision)
    }
}

impl<L: LedgerStore> TaskGoalVerifier for LedgerTaskGoalVerifier<L> {
    fn validate_criterion(&self, criterion: &GoalCriterion) -> Result<(), TaskError> {
        criterion.validate()?;
        self.registry
            .get(&criterion.checker)?
            .validate_predicate(criterion)
    }
    fn compute_assessment(
        &self,
        prefix: &TaskSnapshot,
        criterion_id: &str,
    ) -> Result<GoalAssessment, TaskError> {
        let criterion = prefix
            .definition
            .criteria
            .iter()
            .find_map(|criterion| match criterion {
                CompletionCriterion::Goal(goal) if goal.id == criterion_id => Some(goal),
                _ => None,
            })
            .ok_or_else(|| bad("unknown goal criterion"))?;
        let invocation = prefix
            .invocations
            .get(&criterion.invocation_id)
            .ok_or_else(|| bad("goal root not admitted"))?;
        let attempt = invocation
            .attempts
            .last()
            .and_then(|id| prefix.attempts.get(id))
            .ok_or_else(|| bad("goal has no latest attempt"))?;
        let observation = attempt
            .observation
            .as_ref()
            .ok_or_else(|| bad("goal attempt not observed"))?;
        let mut assessment = GoalAssessment {
            criterion_id: criterion.id.clone(),
            predicate_digest: criterion.predicate_digest.clone(),
            checker: criterion.checker.clone(),
            attempt: attempt.binding.clone(),
            source: observation.source.clone(),
            verdict: GoalVerdict::Indeterminate,
            reason: String::new(),
            proof: serde_json::Value::Null,
        };
        let decision = self.computed(prefix, &assessment)?;
        assessment.verdict = decision.verdict;
        assessment.reason = decision.reason;
        assessment.proof = decision.proof;
        Ok(assessment)
    }
    fn verify_assessment(
        &self,
        prefix: &TaskSnapshot,
        assessment: &GoalAssessment,
    ) -> Result<(), TaskError> {
        assessment.digest()?;
        if self.computed(prefix, assessment)? != assessment.decision() {
            return Err(bad(
                "submitted goal decision differs from deterministic recomputation",
            ));
        }
        Ok(())
    }
    fn verify_completion(
        &self,
        prefix: &TaskSnapshot,
        evidence: &[CompletionEvidence],
    ) -> Result<(), TaskError> {
        // Recheck every latest invocation, including children without root criteria.
        for invocation in prefix.invocations.values() {
            if invocation.state != InvocationState::Completed {
                return Err(bad("unfinished physical invocation"));
            }
            let attempt = invocation
                .attempts
                .last()
                .and_then(|id| prefix.attempts.get(id))
                .ok_or_else(|| bad("missing latest physical attempt"))?;
            let observation = attempt
                .observation
                .as_ref()
                .ok_or_else(|| bad("missing stopped observation"))?;
            let AttemptOutcome::Completed { evidence: proofs } = &observation.outcome else {
                return Err(bad("physical observation is not successful"));
            };
            let source = self
                .reader
                .inspect_stopped(prefix, &attempt.binding, &observation.source, &self.limits)
                .map_err(TaskError::GoalSource)?;
            if source.coverage() != GoalSourceCoverage::Complete {
                return Err(bad(
                    "cannot complete with incomplete/uncertain physical proof",
                ));
            }
            let response = source
                .response()
                .ok_or_else(|| bad("missing successful actual response"))?;
            let response_bytes =
                serde_json::to_vec(response).map_err(|error| bad(&error.to_string()))?;
            for proof in proofs {
                if proof.source() != source.terminal() {
                    return Err(bad("physical completion source differs"));
                }
                match proof {
                    CompletionEvidence::ExecutionResult { result_digest, .. }
                        if result_digest
                            == &crate::task_driver::evidence::response_digest(response)? => {}
                    CompletionEvidence::VerifiedArtifact {
                        sha256, byte_len, ..
                    } if sha256
                        == &super::types::goal_value_digest(
                            &json!(response),
                            self.limits.max_response_bytes,
                        )?
                        && *byte_len == response_bytes.len() as u64 => {}
                    _ => return Err(bad("physical completion result differs")),
                }
            }
        }
        for criterion in &prefix.definition.criteria {
            let CompletionCriterion::Goal(goal) = criterion else {
                continue;
            };
            let saved = prefix
                .goal_assessments
                .iter()
                .rev()
                .find(|record| record.assessment.criterion_id == goal.id)
                .ok_or_else(|| bad("missing goal assessment"))?;
            self.verify_assessment(prefix, &saved.assessment)?;
            if saved.assessment.verdict != GoalVerdict::Satisfied
                || saved.assessment_digest != saved.assessment.digest()?
            {
                return Err(bad("latest goal assessment is not verified Satisfied"));
            }
            let required = CompletionEvidence::GoalSatisfied {
                criterion_id: goal.id.clone(),
                source: saved.assessment.source.clone(),
                assessment: saved.reference.clone(),
                assessment_digest: saved.assessment_digest.clone(),
            };
            if !evidence.contains(&required) {
                return Err(bad("missing exact GoalSatisfied reference"));
            }
        }
        Ok(())
    }
}

fn bad(message: &str) -> TaskError {
    TaskError::Invalid(message.into())
}
