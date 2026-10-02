//! Strict, effect-free Agent invocation preparation. No registry, journal, Session,
//! grant issuance, child execution or external-wait publication happens here.

mod input;

use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PreparedCall, ResourceClaim,
    ToolManifest, ToolRequirements,
};
use kolyan_server::{ExecutionRef, InvocationRole};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AgentCatalog, AgentDefinition, AgentError, AgentKey, AgentPermissions, AgentSelector,
    binding::AgentInvocationBinding, catalog::resolve_self_definition, digest,
};

pub const AGENT_INVOKE_NAME: &str = "agent.invoke";
const MAX_PREPARED_BYTES: usize = 64 * 1024;

/// Self selects the exact saved parent definition, not mutable catalog content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum InvocationTarget {
    SelfCall,
    Named(AgentKey),
    Inline(#[serde(deserialize_with = "deserialize_inline")] AgentDefinition),
}

/// A private child starts from this explicit input; no implicit history sharing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildInvocationInput {
    pub target: InvocationTarget,
    pub input: String,
    pub permissions: AgentPermissions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInvokeInput {
    pub children: Vec<ChildInvocationInput>,
    /// A scheduling request, not permission to bypass the host concurrency ceiling.
    pub parallel: bool,
}

/// Host ceilings are bound into preparation, including the complete byte limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvokePrepareLimits {
    pub max_children: usize,
    pub max_parallel: usize,
    pub max_child_input_bytes: usize,
    pub max_output_bytes: u64,
    pub admission_timeout_ms: u64,
}

/// Immutable content selected before admission. No child identity exists yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedChildIntent {
    pub definition: AgentDefinition,
    pub permissions: AgentPermissions,
    pub input: String,
    pub role: InvocationRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvokePlan {
    schema_version: u32,
    task_id: String,
    parent_invocation_id: String,
    parent_snapshot_digest: String,
    execution: ExecutionRef,
    limits: InvokePrepareLimits,
    parallel: bool,
    children: Vec<ResolvedChildIntent>,
}

/// Only this constructor produces a validated plan. Serialized claims are not
/// trusted admission or authority; execution must reprepare against host context.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedAgentInvocation {
    prepared: PreparedCall,
    children: Vec<ResolvedChildIntent>,
    parallel: bool,
}

#[derive(Debug, Error)]
pub enum InvokePrepareError {
    #[error("invalid invocation input: {0}")]
    Invalid(String),
    #[error("Agent resolution: {0}")]
    Agent(#[from] AgentError),
    #[error("prepared invocation: {0}")]
    Prepared(#[from] kolyan_policy::PreparedError),
}

impl PreparedAgentInvocation {
    pub fn prepared(&self) -> &PreparedCall {
        &self.prepared
    }
    pub fn children(&self) -> &[ResolvedChildIntent] {
        &self.children
    }
    pub fn parallel(&self) -> bool {
        self.parallel
    }
}

impl InvokePrepareLimits {
    pub fn validate(&self) -> Result<(), InvokePrepareError> {
        if !(1..=8).contains(&self.max_children)
            || !(1..=8).contains(&self.max_parallel)
            || self.max_child_input_bytes == 0
            || self.max_child_input_bytes > 16 * 1024
            || self.max_output_bytes == 0
            || self.max_output_bytes > 1024 * 1024
            || self.admission_timeout_ms == 0
            || self.admission_timeout_ms > 30_000
        {
            return Err(invalid("invalid bounded host limits"));
        }
        Ok(())
    }
}

/// Static delegation ceiling; dynamic decisions and grant issuance remain Policy's
/// job. Unknown-resource Delegate claims stay conservatively batch-conflicting.
pub fn agent_invoke_manifest(approval: ApprovalMode) -> ToolManifest {
    ToolManifest {
        tool_name: AGENT_INVOKE_NAME.into(),
        capabilities: [Capability::AgentDelegate].into_iter().collect(),
        effects: [Effect::Delegate].into_iter().collect(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval,
    }
}

/// The binding, execution and host allowance must originate from authenticated
/// host state. This pure function checks their consistency, not journal provenance.
/// All targets are resolved before any effect, without allocating preview IDs.
pub fn prepare_agent_invocation(
    call: ToolCall,
    parent: &AgentInvocationBinding,
    execution: &ExecutionRef,
    catalog: &AgentCatalog,
    host: &AgentPermissions,
    limits: &InvokePrepareLimits,
) -> Result<PreparedAgentInvocation, InvokePrepareError> {
    limits.validate()?;
    parent.validate().map_err(invalid)?;
    for id in [
        &execution.session_id,
        &execution.turn_id,
        &execution.execution_id,
    ] {
        crate::identity(id)?;
    }
    if call.name != AGENT_INVOKE_NAME || parent.private_session_id != execution.session_id {
        return Err(invalid("tool name or parent execution ownership differs"));
    }
    if serde_json::to_vec(&call.arguments).map_err(invalid)?.len() > MAX_PREPARED_BYTES {
        return Err(invalid("encoded invocation input exceeds byte limit"));
    }
    let input = input::decode(call.arguments.clone())?;
    if input.children.is_empty() || input.children.len() > limits.max_children {
        return Err(invalid("child count exceeds host ceiling"));
    }
    let mut children = Vec::with_capacity(input.children.len());
    for child in &input.children {
        if child.input.trim().is_empty() || child.input.len() > limits.max_child_input_bytes {
            return Err(invalid("empty or oversized child input"));
        }
        let definition = match &child.target {
            InvocationTarget::SelfCall => {
                resolve_self_definition(&parent.snapshot, host, &child.permissions)
            }
            InvocationTarget::Named(key) => catalog.resolve_child_definition(
                &parent.snapshot,
                &AgentSelector::Named(key.clone()),
                host,
                &child.permissions,
            ),
            InvocationTarget::Inline(definition) => catalog.resolve_child_definition(
                &parent.snapshot,
                &AgentSelector::Inline(definition.clone()),
                host,
                &child.permissions,
            ),
        }?;
        let role = if definition.key() == parent.snapshot.definition().key() {
            InvocationRole::SelfCall
        } else {
            InvocationRole::Delegation
        };
        children.push(ResolvedChildIntent {
            definition,
            permissions: child.permissions.clone(),
            input: child.input.clone(),
            role,
        });
    }
    let plan = InvokePlan {
        schema_version: 1,
        task_id: parent.task_id.clone(),
        parent_invocation_id: parent.invocation_id.clone(),
        parent_snapshot_digest: parent.snapshot.digest().into(),
        execution: execution.clone(),
        limits: limits.clone(),
        parallel: input.parallel,
        children: children.clone(),
    };
    let prepared = PreparedCall::new(
        call,
        format!("kolyan-agent-invoke-v1/{}", digest(limits)?),
        InvocationClaim {
            tool_name: AGENT_INVOKE_NAME.into(),
            capabilities: [Capability::AgentDelegate].into_iter().collect(),
            effects: [Effect::Delegate].into_iter().collect(),
            resource: ResourceClaim { path: None },
            idempotency: Idempotency::NonIdempotent,
        },
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: limits.max_output_bytes,
            timeout_ms: limits.admission_timeout_ms,
        },
    )?
    .with_execution_binding(serde_json::to_value(plan).map_err(invalid)?)?;
    if serde_json::to_vec(&prepared).map_err(invalid)?.len() > MAX_PREPARED_BYTES {
        return Err(invalid("complete prepared invocation exceeds byte limit"));
    }
    Ok(PreparedAgentInvocation {
        prepared,
        children,
        parallel: input.parallel,
    })
}

fn invalid(error: impl std::fmt::Display) -> InvokePrepareError {
    InvokePrepareError::Invalid(error.to_string())
}

/// Revalidate historical intent from its frozen definitions, never the current
/// mutable catalog. The normal preparation path remains the permission SSOT.
pub(crate) fn verify_saved_agent_invocation(
    prepared: &PreparedCall,
    parent: &AgentInvocationBinding,
    execution: &ExecutionRef,
    host: &AgentPermissions,
) -> Result<PreparedAgentInvocation, InvokePrepareError> {
    let plan: InvokePlan =
        serde_json::from_value(prepared.execution_binding().clone()).map_err(invalid)?;
    if plan.children.is_empty() || plan.children.len() > 8 {
        return Err(invalid("saved invoke plan has an invalid child count"));
    }
    let mut catalog = AgentCatalog::new(8)?;
    for child in &plan.children {
        catalog.register(child.definition.clone())?;
    }
    let actual = prepare_agent_invocation(
        prepared.call().clone(),
        parent,
        execution,
        &catalog,
        host,
        &plan.limits,
    )?;
    if actual.prepared() != prepared {
        return Err(invalid(
            "saved invoke target/role/definition binding differs",
        ));
    }
    Ok(actual)
}

fn deserialize_inline<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<AgentDefinition, D::Error> {
    use serde::de::Error;

    let value = serde_json::Value::deserialize(deserializer)?;
    // ModelRef is shared with open Provider extension contracts. The invocation
    // boundary is strict without changing that shared model schema globally.
    if let Some(model) = value.get("model").and_then(serde_json::Value::as_object)
        && model.keys().any(|key| key != "provider" && key != "model")
    {
        return Err(D::Error::custom("unknown inline model field"));
    }
    serde_path_to_error::deserialize(value).map_err(D::Error::custom)
}

#[cfg(test)]
mod tests;
