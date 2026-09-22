//! Static tool capability ceilings and per-invocation policy decisions.

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionGrant {
    pub call_id: String,
    pub tool_name: String,
    pub policy_version: String,
    pub constraints: ExecutionConstraints,
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
        if self.denied_tools.contains(&call.name) {
            return PolicyDecision::denied("tool is denied by runtime policy");
        }
        let Some(manifest) = self.manifests.get(&call.name) else {
            return PolicyDecision::denied("tool has no trusted manifest");
        };
        let claim = InvocationClaim::from_call(call);
        if !claim.capabilities.is_subset(&manifest.capabilities)
            || !claim.effects.is_subset(&manifest.effects)
        {
            return PolicyDecision::denied("invocation exceeds the tool capability ceiling");
        }
        if let Some(path) = claim.resource.path.as_deref()
            && (!manifest.allows_path(path)
                || self.workspace.as_ref().is_some_and(|s| !s.contains(path)))
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
}

impl PolicyResolver for PolicyEngine {
    fn decide(&self, call: &ToolCall) -> PolicyDecision {
        self.decide(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn write_manifest() -> ToolManifest {
        ToolManifest {
            tool_name: "file.write".into(),
            capabilities: [Capability::FilesystemWrite].into_iter().collect(),
            effects: [Effect::Update].into_iter().collect(),
            path_scopes: vec![PathScope::new("/workspace/src")],
            idempotency: Idempotency::NonIdempotent,
            approval: ApprovalMode::Never,
        }
    }
    fn call(path: &str) -> ToolCall {
        ToolCall {
            id: "call-1".into(),
            name: "file.write".into(),
            arguments: json!({"path": path, "content": "ok"}),
        }
    }

    #[test]
    fn static_manifest_and_dynamic_workspace_scope_are_intersected() {
        let mut engine = PolicyEngine::default();
        engine.register(write_manifest());
        engine.restrict_workspace("/workspace");
        assert_eq!(
            engine.decide(&call("/workspace/src/main.rs")).kind,
            PolicyDecisionKind::Allow
        );
        assert_eq!(
            engine.decide(&call("/workspace/docs/readme.md")).kind,
            PolicyDecisionKind::Deny
        );
    }

    #[test]
    fn unknown_or_denied_tools_fail_closed() {
        let mut engine = PolicyEngine::default();
        let unknown = ToolCall {
            id: "x".into(),
            name: "shell.exec".into(),
            arguments: json!({}),
        };
        assert_eq!(engine.decide(&unknown).kind, PolicyDecisionKind::Deny);
        engine.register(write_manifest());
        engine.deny_tool("file.write");
        assert_eq!(
            engine.decide(&call("/workspace/src/main.rs")).kind,
            PolicyDecisionKind::Deny
        );
    }

    #[test]
    fn approval_is_not_an_allow() {
        let mut manifest = write_manifest();
        manifest.approval = ApprovalMode::Always;
        let mut engine = PolicyEngine::default();
        engine.register(manifest);
        let call = call("/workspace/src/main.rs");
        let decision = engine.decide(&call);
        assert_eq!(decision.kind, PolicyDecisionKind::RequireApproval);
        assert!(decision.into_grant(&call).is_err());
    }
}
