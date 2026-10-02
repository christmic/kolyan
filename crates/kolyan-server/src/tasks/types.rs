//! Typed task identities, commands and evidence. Topology never carries grants.

use std::collections::BTreeMap;

use kolyan_ledger::{FactError, FactRef};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::InvocationInputSource;
use super::{GoalAssessment, GoalAssessmentRecord, GoalCriterion, GoalVerdict};
use crate::ExecutionRef;

/// Definition revision and instance are distinct from invocation identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentIdentity {
    pub definition_id: String,
    pub revision: String,
    pub instance_id: String,
}

/// Shared task limits are ceilings, not authorization to perform effects.
/// Token ceilings use observed usage, not reserved/prepriced input; concurrent
/// attempts may overspend and must then stop further admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskLimits {
    pub max_depth: u32,
    pub max_invocations: u64,
    pub max_attempts: u64,
    pub max_tokens: Option<u64>,
    pub max_steps_per_turn: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CompletionCriterion {
    Goal(GoalCriterion),
    ExecutionCompleted {
        id: String,
        invocation_id: String,
    },
    ArtifactDigest {
        id: String,
        invocation_id: String,
        sha256: String,
    },
}

impl CompletionCriterion {
    pub fn id(&self) -> &str {
        match self {
            Self::Goal(goal) => &goal.id,
            Self::ExecutionCompleted { id, .. } | Self::ArtifactDigest { id, .. } => id,
        }
    }

    pub fn invocation_id(&self) -> &str {
        match self {
            Self::Goal(goal) => &goal.invocation_id,
            Self::ExecutionCompleted { invocation_id, .. }
            | Self::ArtifactDigest { invocation_id, .. } => invocation_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancellationPolicy {
    /// Stop admitting work; request cancellation of every nonterminal attempt.
    AllInvocations,
    /// Stop admitting work without propagating into already admitted children.
    RootOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDefinition {
    pub task_id: String,
    pub objective: String,
    pub criteria: Vec<CompletionCriterion>,
    pub agent: AgentIdentity,
    pub constraints_digest: String,
    pub limits: TaskLimits,
    pub cancellation_policy: CancellationPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationRole {
    Root,
    SelfCall,
    Delegation,
    Continuation,
}

/// Each child/continuation receives an explicit identity and constraints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationDefinition {
    pub invocation_id: String,
    pub agent: AgentIdentity,
    pub constraints_digest: String,
    pub role: InvocationRole,
    pub parent_invocation_id: Option<String>,
    pub dependencies: Vec<String>,
    pub input_source: InvocationInputSource,
}

/// Execution identity must be the identity admitted by SessionExecutionService.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptBinding {
    pub attempt_id: String,
    pub invocation_id: String,
    pub execution: ExecutionRef,
    pub agent: AgentIdentity,
    pub constraints_digest: String,
    pub input_source: InvocationInputSource,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Known token portions plus an explicit count of Steps with missing counts.
pub struct TaskUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub unreported_steps: u64,
}

impl TaskUsage {
    /// Sum reported portions, not a complete cost when unreported_steps is nonzero.
    pub fn total(&self) -> Option<u64> {
        self.input_tokens.checked_add(self.output_tokens)
    }
}

/// Links verified host evidence to an execution fact, not to a model assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionEvidence {
    pub execution: ExecutionRef,
    pub event_id: String,
    pub cursor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CompletionEvidence {
    GoalSatisfied {
        criterion_id: String,
        source: ExecutionEvidence,
        assessment: FactRef,
        assessment_digest: String,
    },
    ExecutionResult {
        criterion_id: String,
        source: ExecutionEvidence,
        result_digest: String,
    },
    VerifiedArtifact {
        criterion_id: String,
        source: ExecutionEvidence,
        sha256: String,
        byte_len: u64,
    },
}

impl CompletionEvidence {
    pub fn criterion_id(&self) -> &str {
        match self {
            Self::GoalSatisfied { criterion_id, .. } => criterion_id,
            Self::ExecutionResult { criterion_id, .. }
            | Self::VerifiedArtifact { criterion_id, .. } => criterion_id,
        }
    }

    pub fn source(&self) -> &ExecutionEvidence {
        match self {
            Self::GoalSatisfied { source, .. } => source,
            Self::ExecutionResult { source, .. } | Self::VerifiedArtifact { source, .. } => source,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum WaitingReason {
    GoalAssessment {
        criterion_id: String,
        invocation_id: String,
    },
    GoalUnmet {
        criterion_id: String,
        verdict: GoalVerdict,
        reason: String,
    },
    Suspension(TaskSuspension),
    ChildResults {
        invocation_ids: Vec<String>,
    },
    Recovery {
        reason: String,
    },
}

/// Coordinates of a Runtime checkpoint, not approval or result authority.
/// Both sets may be populated; only Runtime verifies the underlying proofs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSuspension {
    pub checkpoint_id: String,
    pub approval_ids: Vec<String>,
    pub external_wait_ids: Vec<String>,
}

/// Authoritative stopped outcome supplied by the host, never model text alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AttemptOutcome {
    Suspended { waiting: TaskSuspension },
    Completed { evidence: Vec<CompletionEvidence> },
    Failed { reason: String, safe_to_retry: bool },
    Cancelled { reason: String },
    RecoveryRequired { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Usage is an increment since the previous observation, including approval waits.
pub struct AttemptObservation {
    pub attempt_id: String,
    pub execution: ExecutionRef,
    pub source: ExecutionEvidence,
    pub usage: TaskUsage,
    pub outcome: AttemptOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Ready,
    Active,
    Waiting,
    RecoveryRequired,
    Completed,
    Failed,
    Cancelled,
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationState {
    Admitted,
    Running,
    Suspended,
    Completed,
    Failed,
    Cancelled,
    RecoveryRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptSnapshot {
    pub binding: AttemptBinding,
    pub state: InvocationState,
    pub observation: Option<AttemptObservation>,
    pub waiting: Option<WaitingReason>,
    pub cancellation_requested: bool,
    pub retry_authorized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumedResult {
    pub child_invocation_id: String,
    pub completion_fact: FactRef,
    pub evidence: Vec<CompletionEvidence>,
}

/// A terminal result is feedback, not proof that the Task succeeded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum TerminalResultDisposition {
    Completed { evidence: Vec<CompletionEvidence> },
    Failed { reason: String },
    Cancelled { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumedTerminalResult {
    pub parent: AttemptBinding,
    pub child: AttemptBinding,
    pub terminal_fact: FactRef,
    pub source: ExecutionEvidence,
    pub disposition: TerminalResultDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationSnapshot {
    pub definition: InvocationDefinition,
    pub state: InvocationState,
    pub attempts: Vec<String>,
    pub consumed_results: BTreeMap<String, ConsumedResult>,
    pub terminal_consumed_results: BTreeMap<String, ConsumedTerminalResult>,
    pub terminal_fact: Option<FactRef>,
    pub completion_fact: Option<FactRef>,
    pub cancellation_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub goal_assessments: Vec<GoalAssessmentRecord>,
    pub definition: TaskDefinition,
    pub position: u64,
    pub state: TaskState,
    pub usage: TaskUsage,
    pub invocations: BTreeMap<String, InvocationSnapshot>,
    pub attempts: BTreeMap<String, AttemptSnapshot>,
    pub waiting: Vec<WaitingReason>,
    pub success_evidence: Vec<CompletionEvidence>,
}

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("goal source inspection: {0}")]
    GoalSource(#[from] crate::GoalSourceError),
    #[error("coordination journal: {0}")]
    Journal(#[from] FactError),
    #[error("task contract: {0}")]
    Invalid(String),
    #[error("task not registered: {0}")]
    NotFound(String),
    #[error("task transition: {0}")]
    Transition(String),
}

/// Critical typed events are validated identically before append and on replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum TaskEvent {
    GoalAssessed(Box<GoalAssessment>),
    Registered(TaskDefinition),
    InvocationAdmitted(InvocationDefinition),
    DependencyAdmitted {
        invocation_id: String,
        dependency_id: String,
    },
    AttemptStarted(AttemptBinding),
    AttemptObserved(AttemptObservation),
    AttemptResumed {
        binding: AttemptBinding,
        checkpoint_id: String,
    },
    RecoveryNeeded {
        attempt_id: String,
        reason: String,
    },
    RetryAuthorized {
        attempt_id: String,
        source: ExecutionEvidence,
        reason: String,
    },
    ResultConsumed {
        invocation_id: String,
        result: ConsumedResult,
    },
    TerminalResultConsumed {
        invocation_id: String,
        result: Box<ConsumedTerminalResult>,
    },
    Completed {
        evidence: Vec<CompletionEvidence>,
    },
    Cancelled {
        reason: String,
    },
    Failed {
        reason: String,
    },
}
