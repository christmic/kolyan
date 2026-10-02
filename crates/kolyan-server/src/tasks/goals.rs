//! Authoritative goal contracts and enforcing verification, never model execution.

mod registry;
mod transitions;
pub(crate) mod types;

pub use registry::{GoalChecker, GoalCheckerRegistry, LedgerTaskGoalVerifier, TaskGoalVerifier};
pub use types::*;

pub(super) use transitions::{
    apply_assessment, assessment_owner, goal_causes, refresh_goals, verify_event,
};
