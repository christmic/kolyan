//! Bounded immutable goal inputs and derived assessment references.

use std::io::{self, Write};

use kolyan_ledger::FactRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{AttemptBinding, ExecutionEvidence, TaskError};

pub const MAX_GOAL_PREDICATE_BYTES: usize = 64 * 1024;
pub const MAX_GOAL_PROOF_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalCheckerKey {
    pub kind: String,
    pub revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalCriterion {
    pub id: String,
    pub invocation_id: String,
    pub checker: GoalCheckerKey,
    pub predicate: Value,
    pub predicate_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GoalVerdict {
    Satisfied,
    Unsatisfied,
    Indeterminate,
    CheckerFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputedGoalDecision {
    pub verdict: GoalVerdict,
    pub reason: String,
    pub proof: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalAssessment {
    pub criterion_id: String,
    pub predicate_digest: String,
    pub checker: GoalCheckerKey,
    pub attempt: AttemptBinding,
    pub source: ExecutionEvidence,
    pub verdict: GoalVerdict,
    pub reason: String,
    pub proof: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalAssessmentRecord {
    pub assessment: GoalAssessment,
    pub reference: FactRef,
    pub assessment_digest: String,
}

impl GoalCriterion {
    /// Construct a trusted host goal. Registry validation still precedes admission.
    pub fn new(
        id: String,
        invocation_id: String,
        checker: GoalCheckerKey,
        predicate: Value,
    ) -> Result<Self, TaskError> {
        let predicate_digest = goal_value_digest(&predicate, MAX_GOAL_PREDICATE_BYTES)?;
        let result = Self {
            id,
            invocation_id,
            checker,
            predicate,
            predicate_digest,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), TaskError> {
        super::super::reducer::identity(&self.id)?;
        super::super::reducer::identity(&self.invocation_id)?;
        self.checker.validate()?;
        if self.predicate_digest != goal_value_digest(&self.predicate, MAX_GOAL_PREDICATE_BYTES)? {
            return Err(TaskError::Invalid("goal predicate digest differs".into()));
        }
        Ok(())
    }
}

impl GoalCheckerKey {
    pub(crate) fn validate(&self) -> Result<(), TaskError> {
        super::super::reducer::identity(&self.kind)?;
        super::super::reducer::identity(&self.revision)
    }
}

impl ComputedGoalDecision {
    pub(crate) fn validate(&self) -> Result<(), TaskError> {
        if self.reason.trim().is_empty() || self.reason.len() > 8192 {
            return Err(TaskError::Invalid("empty or oversized goal reason".into()));
        }
        bounded_serialized_bytes(&self.proof, MAX_GOAL_PROOF_BYTES)?;
        Ok(())
    }
}

impl GoalAssessment {
    pub fn decision(&self) -> ComputedGoalDecision {
        ComputedGoalDecision {
            verdict: self.verdict,
            reason: self.reason.clone(),
            proof: self.proof.clone(),
        }
    }
    pub fn digest(&self) -> Result<String, TaskError> {
        self.checker.validate()?;
        super::super::reducer::identity(&self.criterion_id)?;
        if self.reason.trim().is_empty() || self.reason.len() > 8192 {
            return Err(TaskError::Invalid("empty or oversized goal reason".into()));
        }
        bounded_serialized_bytes(&self.proof, MAX_GOAL_PROOF_BYTES)?;
        bounded_serialized_bytes(self, 128 * 1024)?;
        // The proof is bounded first; the full typed assessment is a distinct digest.
        let bytes =
            serde_json::to_vec(self).map_err(|error| TaskError::Invalid(error.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

pub(crate) fn goal_value_digest(value: &Value, limit: usize) -> Result<String, TaskError> {
    bounded_serialized_bytes(value, limit)?;
    let bytes = serde_json::to_vec(value).map_err(|error| TaskError::Invalid(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

/// Count before allocating/cloning an untrusted payload; no storage allocation claim.
pub(crate) fn bounded_serialized_bytes<T: Serialize>(
    value: &T,
    limit: usize,
) -> Result<usize, TaskError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("size overflow"))?;
            if self.bytes > self.limit {
                return Err(io::Error::other("goal payload exceeds byte ceiling"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)
        .map_err(|error| TaskError::Invalid(error.to_string()))?;
    Ok(counter.bytes)
}
