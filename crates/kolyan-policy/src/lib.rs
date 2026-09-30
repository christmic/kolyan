//! Static tool capability ceilings and per-invocation policy decisions.

mod progress;
pub use progress::ProgressPolicy;
mod prepared;
pub use prepared::{
    ApprovalEvidence, PreparedCall, PreparedError, PreparedGrant, ToolRequirements,
};

use kolyan_model::ToolCall;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    FilesystemRead,
    FilesystemWrite,
    ProcessInspect,
    ProcessExecute,
    NetworkConnect,
    SecretUse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    Read,
    Create,
    Update,
    Delete,
    Execute,
    ExternalNetwork,
    CredentialUse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Idempotency {
    Idempotent,
    NonIdempotent,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    Never,
    OnRisk,
    Always,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathScope {
    pub prefix: String,
}

impl PathScope {
    pub fn new(prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        Self { prefix }
    }

    fn contains(&self, path: &str) -> bool {
        path == self.prefix.trim_end_matches('/') || path.starts_with(&self.prefix)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolManifest {
    pub tool_name: String,
    pub capabilities: BTreeSet<Capability>,
    pub effects: BTreeSet<Effect>,
    pub path_scopes: Vec<PathScope>,
    pub idempotency: Idempotency,
    pub approval: ApprovalMode,
}

impl ToolManifest {
    pub fn new(tool_name: impl Into<String>) -> Self {
        Self {
            tool_name: tool_name.into(),
            capabilities: BTreeSet::new(),
            effects: BTreeSet::new(),
            path_scopes: Vec::new(),
            idempotency: Idempotency::Unknown,
            approval: ApprovalMode::OnRisk,
        }
    }

    pub fn allows_path(&self, path: &str) -> bool {
        self.path_scopes.is_empty() || self.path_scopes.iter().any(|scope| scope.contains(path))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceClaim {
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationClaim {
    pub tool_name: String,
    pub capabilities: BTreeSet<Capability>,
    pub effects: BTreeSet<Effect>,
    pub resource: ResourceClaim,
    pub idempotency: Idempotency,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRecord {
    pub tool_name: String,
    pub effect: Effect,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PolicyContext {
    pub agent_id: Option<String>,
    pub user_id: Option<String>,
    pub task_id: Option<String>,
    pub turn_id: Option<String>,
    pub workspace: Option<String>,
    pub remaining_tool_calls: Option<usize>,
    pub previous_effects: Vec<EffectRecord>,
}

impl InvocationClaim {
    pub fn from_call(call: &ToolCall) -> Self {
        let path = call
            .arguments
            .get("path")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned);
        let (capability, effect, idempotency) = match call.name.as_str() {
            "file.read" => (
                Capability::FilesystemRead,
                Effect::Read,
                Idempotency::Idempotent,
            ),
            "file.write" => (
                Capability::FilesystemWrite,
                Effect::Update,
                Idempotency::NonIdempotent,
            ),
            "shell.query" => (
                Capability::ProcessInspect,
                Effect::Read,
                Idempotency::Idempotent,
            ),
            _ => (
                Capability::ProcessInspect,
                Effect::Execute,
                Idempotency::Unknown,
            ),
        };
        Self {
            tool_name: call.name.clone(),
            capabilities: [capability].into_iter().collect(),
            effects: [effect].into_iter().collect(),
            resource: ResourceClaim { path },
            idempotency,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecisionKind {
    Allow,
    AllowWithConstraints,
    RequireApproval,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionConstraints {
    pub max_output_bytes: Option<u64>,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub kind: PolicyDecisionKind,
    pub reason: String,
    pub policy_version: String,
    pub constraints: ExecutionConstraints,
}

impl PolicyDecision {
    pub fn denied(reason: impl Into<String>) -> Self {
        Self {
            kind: PolicyDecisionKind::Deny,
            reason: reason.into(),
            policy_version: "v1".into(),
            constraints: ExecutionConstraints {
                max_output_bytes: None,
                timeout_ms: None,
            },
        }
    }

    pub fn into_grant(self, call: &ToolCall) -> Result<ExecutionGrant, PolicyError> {
        match self.kind {
            PolicyDecisionKind::Allow | PolicyDecisionKind::AllowWithConstraints => {
                Ok(ExecutionGrant {
                    call_id: call.id.clone(),
                    tool_name: call.name.clone(),
                    policy_version: self.policy_version,
                    constraints: self.constraints,
                })
            }
            PolicyDecisionKind::RequireApproval => Err(PolicyError::ApprovalRequired {
                reason: self.reason,
            }),
            PolicyDecisionKind::Deny => Err(PolicyError::Denied {
                reason: self.reason,
            }),
        }
    }

    pub fn into_approved_grant(self, call: &ToolCall) -> Result<ExecutionGrant, PolicyError> {
        if self.kind != PolicyDecisionKind::RequireApproval {
            return self.into_grant(call);
        }
        Ok(ExecutionGrant {
            call_id: call.id.clone(),
            tool_name: call.name.clone(),
            policy_version: self.policy_version,
            constraints: self.constraints,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionGrant {
    pub call_id: String,
    pub tool_name: String,
    pub policy_version: String,
    pub constraints: ExecutionConstraints,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchCallDecision {
    pub call_id: String,
    pub claim: InvocationClaim,
    pub decision: PolicyDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchExecutionPlan {
    pub decisions: Vec<BatchCallDecision>,
    pub stages: Vec<Vec<String>>,
}

impl BatchExecutionPlan {
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    /// Schedule granted approvals using the same resource ordering as allows.
    /// This only plans execution; callers must still validate and issue grants.
    pub fn stages_with_approvals(&self, approved_call_ids: &[String]) -> Vec<Vec<String>> {
        execution_stages(&self.decisions, approved_call_ids)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PolicyError {
    #[error("policy denied: {reason}")]
    Denied { reason: String },
    #[error("approval required: {reason}")]
    ApprovalRequired { reason: String },
}

pub trait PolicyResolver: Send + Sync {
    fn decide(&self, call: &ToolCall) -> PolicyDecision;
}

#[derive(Debug, Clone, Default)]
pub struct PolicyEngine {
    manifests: HashMap<String, ToolManifest>,
    denied_tools: BTreeSet<String>,
    workspace: Option<PathScope>,
    progress: Option<ProgressPolicy>,
}

impl PolicyEngine {
    pub fn register(&mut self, manifest: ToolManifest) {
        self.manifests.insert(manifest.tool_name.clone(), manifest);
    }
    pub fn deny_tool(&mut self, name: impl Into<String>) {
        self.denied_tools.insert(name.into());
    }
    pub fn restrict_workspace(&mut self, prefix: impl Into<String>) {
        self.workspace = Some(PathScope::new(prefix));
    }

    pub fn decide(&self, call: &ToolCall) -> PolicyDecision {
        self.decide_with_context(call, &PolicyContext::default())
    }

    pub fn decide_with_context(&self, call: &ToolCall, context: &PolicyContext) -> PolicyDecision {
        self.decide_claim(&InvocationClaim::from_call(call), context)
    }

    /// Evaluate validated adapter-derived semantics, not tool-name heuristics.
    /// Invalid persisted preparation is denied before any authority is issued.
    pub fn decide_prepared(&self, call: &PreparedCall, context: &PolicyContext) -> PolicyDecision {
        if let Err(error) = call.validate() {
            return PolicyDecision::denied(error.to_string());
        }
        self.decide_claim(call.claim(), context)
    }

    fn decide_claim(&self, claim: &InvocationClaim, context: &PolicyContext) -> PolicyDecision {
        if context.remaining_tool_calls == Some(0) {
            return PolicyDecision::denied("tool-call budget is exhausted");
        }
        if self.denied_tools.contains(&claim.tool_name) {
            return PolicyDecision::denied("tool is denied by runtime policy");
        }
        let Some(manifest) = self.manifests.get(&claim.tool_name) else {
            return PolicyDecision::denied("tool has no trusted manifest");
        };
        if !claim.capabilities.is_subset(&manifest.capabilities)
            || !claim.effects.is_subset(&manifest.effects)
        {
            return PolicyDecision::denied("invocation exceeds the tool capability ceiling");
        }
        let workspace = context
            .workspace
            .as_deref()
            .map(PathScope::new)
            .or_else(|| self.workspace.clone());
        if let Some(path) = claim.resource.path.as_deref()
            && (!manifest.allows_path(path)
                || workspace.as_ref().is_some_and(|s| !s.contains(path)))
        {
            return PolicyDecision::denied("resource is outside the allowed path scope");
        }
        let kind = match manifest.approval {
            ApprovalMode::Always => PolicyDecisionKind::RequireApproval,
            ApprovalMode::OnRisk if claim.effects.contains(&Effect::Delete) => {
                PolicyDecisionKind::RequireApproval
            }
            _ => PolicyDecisionKind::Allow,
        };
        PolicyDecision {
            kind,
            reason: "manifest ceiling and runtime policy allow the invocation".into(),
            policy_version: "v1".into(),
            constraints: ExecutionConstraints {
                max_output_bytes: Some(1024 * 1024),
                timeout_ms: Some(30_000),
            },
        }
    }

    pub fn resolve_batch(&self, context: &PolicyContext, calls: &[ToolCall]) -> BatchExecutionPlan {
        let decisions = calls
            .iter()
            .map(|call| BatchCallDecision {
                call_id: call.id.clone(),
                claim: InvocationClaim::from_call(call),
                decision: self.decide_with_context(call, context),
            })
            .collect::<Vec<_>>();
        let stages = execution_stages(&decisions, &[]);
        BatchExecutionPlan { decisions, stages }
    }
}

fn execution_stages(decisions: &[BatchCallDecision], approved: &[String]) -> Vec<Vec<String>> {
    let executable = |item: &BatchCallDecision| {
        matches!(
            item.decision.kind,
            PolicyDecisionKind::Allow | PolicyDecisionKind::AllowWithConstraints
        ) || (item.decision.kind == PolicyDecisionKind::RequireApproval
            && approved.contains(&item.call_id))
    };
    let mut stages: Vec<Vec<String>> = Vec::new();
    for (index, current) in decisions.iter().enumerate() {
        if !executable(current) {
            continue;
        }
        let mut stage_index = 0;
        for previous in &decisions[..index] {
            if executable(previous)
                && claims_conflict(&previous.claim, &current.claim)
                && let Some(previous_stage) = stages
                    .iter()
                    .position(|stage| stage.contains(&previous.call_id))
            {
                stage_index = stage_index.max(previous_stage + 1);
            }
        }
        while stages.len() <= stage_index {
            stages.push(Vec::new());
        }
        stages[stage_index].push(current.call_id.clone());
    }
    stages
}

fn claims_conflict(left: &InvocationClaim, right: &InvocationClaim) -> bool {
    let writes = |claim: &InvocationClaim| {
        claim.effects.iter().any(|effect| {
            matches!(
                effect,
                Effect::Create | Effect::Update | Effect::Delete | Effect::Execute
            )
        })
    };
    if !writes(left) && !writes(right) {
        return false;
    }
    match (&left.resource.path, &right.resource.path) {
        (Some(left), Some(right)) => {
            left == right
                || left.starts_with(&format!("{right}/"))
                || right.starts_with(&format!("{left}/"))
        }
        _ => true,
    }
}

impl PolicyResolver for PolicyEngine {
    fn decide(&self, call: &ToolCall) -> PolicyDecision {
        self.decide(call)
    }
}

#[cfg(test)]
mod tests;
