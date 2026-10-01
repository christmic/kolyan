//! Serializable loop state and pure result merging. No database, model, tool or
//! policy issuance occurs here. Hosts persist merged state before driving it.

use std::collections::HashSet;

use kolyan_model::{ContentBlock, MessageRole, ModelRequest, ToolCall, ToolResult};
use kolyan_policy::{PreparedCall, PreparedGrant, ToolExecutionScope};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::outcome::{ExternalResolution, ExternalWait};
use super::{StepOutcome, StepResult, ToolDispatchPolicy};

pub const TURN_CHECKPOINT_SCHEMA: u32 = 1;
/// Maximum complete JSON encoding, including model history and all authority.
/// Oversized histories fail explicitly; Core never truncates source context.
pub const MAX_TURN_CHECKPOINT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("invalid checkpoint: {0}")]
    Invalid(String),
    #[error("checkpoint scope differs from admitted scope")]
    ForeignScope,
    #[error("external result exceeds issued output limit")]
    OutputLimit,
    #[error("complete checkpoint exceeds 16 MiB encoded limit")]
    CheckpointLimit,
    #[error("checkpoint JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// Historical authority, never permission to perform a new effect. Its saved
/// policy revision is used for integrity validation, not fresh authorization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssuedToolAuthority {
    #[serde(deserialize_with = "exact")]
    pub prepared: PreparedCall,
    #[serde(deserialize_with = "exact")]
    pub grant: PreparedGrant,
    #[serde(deserialize_with = "exact")]
    pub scope: ToolExecutionScope,
    pub policy_revision: String,
}

impl IssuedToolAuthority {
    pub fn validate(&self, expected_scope: &ToolExecutionScope) -> Result<(), CheckpointError> {
        if self.policy_revision.trim().is_empty() {
            return invalid("empty policy revision");
        }
        if self.scope != *expected_scope {
            return Err(CheckpointError::ForeignScope);
        }
        self.grant
            .validate(&self.prepared, &self.policy_revision, expected_scope)
            .map_err(|error| CheckpointError::Invalid(error.to_string()))
    }

    /// Check exact call pairing and the complete serialized ToolResult against
    /// the actually issued grant's output ceiling. Oversized results fail; they
    /// are never truncated. Hosts must separately validate this authority against
    /// independently admitted scope and verify trusted receipt provenance.
    pub fn validate_result(&self, result: &ToolResult) -> Result<(), CheckpointError> {
        if result.call_id != self.prepared.call().id {
            return invalid("result call differs from issued preparation");
        }
        let limit = self
            .grant
            .constraints()
            .max_output_bytes
            .ok_or_else(|| CheckpointError::Invalid("issued output limit is missing".into()))?;
        if serde_json::to_vec(result)?.len() as u64 > limit {
            return Err(CheckpointError::OutputLimit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointCallState {
    Ready,
    Completed {
        #[serde(deserialize_with = "exact")]
        result: ToolResult,
        #[serde(deserialize_with = "required_option")]
        issued: Option<IssuedToolAuthority>,
    },
    AwaitingExternal {
        wait: ExternalWait,
        issued: IssuedToolAuthority,
    },
}

/// Original model call order is preserved by the enclosing vector. Ready
/// preparations are observations only and must be refreshed before execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointCall {
    #[serde(deserialize_with = "exact")]
    pub call: ToolCall,
    #[serde(deserialize_with = "required_option")]
    pub prepared: Option<PreparedCall>,
    pub charged: bool,
    pub state: CheckpointCallState,
}

/// Approval evidence is separate from call progress. A changed Ready preparation
/// invalidates this binding; it cannot be carried into a newly issued grant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointApproval {
    pub approval_id: String,
    #[serde(deserialize_with = "exact")]
    pub prepared: PreparedCall,
    #[serde(deserialize_with = "exact")]
    pub scope: ToolExecutionScope,
    pub policy_revision: String,
    #[serde(deserialize_with = "required_option")]
    pub evidence_id: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub expires_at_ms: Option<u64>,
}

/// Absolute limits survive suspension. Explicit null means no ceiling; missing
/// fields are errors, including Option fields which serde otherwise defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointBudget {
    pub max_steps: usize,
    #[serde(deserialize_with = "required_option")]
    pub max_tool_calls: Option<usize>,
    #[serde(deserialize_with = "required_option")]
    pub deadline_at_ms: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub tool_timeout_ms: Option<u64>,
    pub prior_tool_calls_used: usize,
    pub tool_calls_used: usize,
}

/// A checkpoint exists at a completed model Step's tool batch boundary. Opaque
/// model metadata is retained unchanged; authority fields are strict and bounded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnCheckpoint {
    pub schema_version: u32,
    pub checkpoint_id: String,
    #[serde(deserialize_with = "exact")]
    pub scope: ToolExecutionScope,
    pub model_request: ModelRequest,
    pub input_message_count: usize,
    pub steps: Vec<StepResult>,
    pub next_step_index: usize,
    pub calls: Vec<CheckpointCall>,
    pub stages: Vec<Vec<String>>,
    pub stage_index: usize,
    pub approvals: Vec<CheckpointApproval>,
    pub budget: CheckpointBudget,
    pub dispatch: ToolDispatchPolicy,
}

impl TurnCheckpoint {
    /// Bounded decoding followed by structural validation. The bytes must come
    /// from trusted persistence: scope equality and accounting consistency do
    /// not authenticate original budgets or approval decisions.
    pub fn from_json(
        bytes: &[u8],
        expected_scope: &ToolExecutionScope,
    ) -> Result<Self, CheckpointError> {
        if bytes.len() > MAX_TURN_CHECKPOINT_BYTES {
            return Err(CheckpointError::CheckpointLimit);
        }
        let checkpoint: Self = serde_json::from_slice(bytes)?;
        checkpoint.validate(expected_scope)?;
        Ok(checkpoint)
    }

    /// Validate against independently admitted coordinates. Deserialization is
    /// not trust establishment; callers must invoke this before driving state.
    pub fn validate(&self, expected_scope: &ToolExecutionScope) -> Result<(), CheckpointError> {
        let mut size = EncodedLimit(0);
        if let Err(error) = serde_json::to_writer(&mut size, self) {
            if size.0 > MAX_TURN_CHECKPOINT_BYTES {
                return Err(CheckpointError::CheckpointLimit);
            }
            return Err(CheckpointError::Json(error));
        }
        expected_scope
            .validate()
            .map_err(|e| CheckpointError::Invalid(e.to_string()))?;
        if self.scope != *expected_scope {
            return Err(CheckpointError::ForeignScope);
        }
        identifier(&self.checkpoint_id)?;
        if self.schema_version != TURN_CHECKPOINT_SCHEMA
            || self.steps.is_empty()
            || self.next_step_index != self.steps.len()
            || self.steps.len() > self.budget.max_steps
            || self.input_message_count > self.model_request.messages.len()
            || self.calls.is_empty()
            || self.stages.is_empty()
            || self.stage_index > self.stages.len()
            || self.budget.tool_timeout_ms == Some(0)
        {
            return invalid("invalid schema, boundary, stage or limits");
        }
        let last = self.steps.last().expect("checked nonempty");
        if last.step_id != self.scope.step_id || last.outcome != StepOutcome::ToolCalls {
            return invalid("pending batch is not the scoped completed Step");
        }
        for (index, step) in self.steps.iter().enumerate() {
            if step.step_id != format!("{}-step-{index}", self.scope.execution.turn_id) {
                return invalid("Step sequence is not exact");
            }
        }
        let original: Vec<_> = last
            .response
            .content
            .iter()
            .filter_map(|block| {
                if let ContentBlock::ToolCall { call } = block {
                    Some(call)
                } else {
                    None
                }
            })
            .collect();
        if original != self.calls.iter().map(|item| &item.call).collect::<Vec<_>>() {
            return invalid("original tool call order changed");
        }
        let mut ids = HashSet::new();
        let mut waits = HashSet::new();
        for item in &self.calls {
            // Model IDs are opaque, unlike host coordinates. Match PreparedCall's
            // identity contract even for preparation-error feedback with no plan.
            for identity in [&item.call.id, &item.call.name] {
                if identity.trim().is_empty() || identity.len() > 1024 {
                    return invalid("empty or oversized tool identity");
                }
            }
            if !ids.insert(item.call.id.as_str()) {
                return invalid("duplicate call identity");
            }
            if let Some(prepared) = &item.prepared {
                prepared
                    .validate()
                    .map_err(|e| CheckpointError::Invalid(e.to_string()))?;
                if prepared.call() != &item.call {
                    return invalid("preparation differs from original call");
                }
            }
            match &item.state {
                CheckpointCallState::Ready => {
                    if item.charged || item.prepared.is_none() {
                        return invalid("invalid Ready accounting or preparation");
                    }
                }
                CheckpointCallState::Completed { result, issued } => {
                    if !item.charged || result.call_id != item.call.id {
                        return invalid("invalid completed result or accounting");
                    }
                    if let Some(authority) = issued {
                        self.validate_authority(item, authority)?;
                        authority.validate_result(result)?;
                    } else if !result.is_error {
                        return invalid("successful result lacks issued authority");
                    }
                }
                CheckpointCallState::AwaitingExternal { wait, issued } => {
                    if !item.charged {
                        return invalid("unaccounted external wait");
                    }
                    wait.validate()?;
                    if !waits.insert(&wait.wait_id) {
                        return invalid("duplicate wait identity");
                    }
                    self.validate_authority(item, issued)?;
                }
            }
        }
        let flattened: Vec<_> = self.stages.iter().flatten().collect();
        let stage_ids: HashSet<_> = flattened.iter().map(|id| id.as_str()).collect();
        if self.stages.iter().any(Vec::is_empty)
            || flattened.len() != self.calls.len()
            || stage_ids != ids
        {
            return invalid("stages do not partition original calls");
        }
        for (index, stage) in self.stages.iter().enumerate() {
            for id in stage {
                let item = self
                    .calls
                    .iter()
                    .find(|item| item.call.id == *id)
                    .expect("checked partition");
                if index < self.stage_index
                    && !matches!(item.state, CheckpointCallState::Completed { .. })
                    || index > self.stage_index && !matches!(item.state, CheckpointCallState::Ready)
                {
                    return invalid("call progress contradicts stage cursor");
                }
            }
        }
        let mut boundary = self.input_message_count;
        let mut historical_calls = 0usize;
        for step in &self.steps[..self.steps.len() - 1] {
            if step.outcome != StepOutcome::ToolCalls {
                return invalid("historical Step did not produce the completed tool batch");
            }
            let Some(message) = self.model_request.messages.get(boundary) else {
                return invalid("missing historical assistant message");
            };
            if message.role != MessageRole::Assistant || message.content != step.response.content {
                return invalid("historical assistant boundary changed");
            }
            boundary += 1;
            let historical_batch = super::ToolCallBatch::try_from(
                step.response
                    .content
                    .iter()
                    .filter_map(|block| {
                        if let ContentBlock::ToolCall { call } = block {
                            Some(call.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
            )
            .map_err(|error| CheckpointError::Invalid(error.to_string()))?;
            for call in historical_batch.calls() {
                historical_calls = historical_calls
                    .checked_add(1)
                    .ok_or_else(|| CheckpointError::Invalid("historical usage overflow".into()))?;
                let Some(message) = self.model_request.messages.get(boundary) else {
                    return invalid("missing historical tool result");
                };
                if message.role != MessageRole::User
                    || message.content.len() != 1
                    || !matches!(&message.content[0], ContentBlock::ToolResult { result } if result.call_id == call.id)
                {
                    return invalid("historical tool pairing changed");
                }
                boundary += 1;
            }
        }
        if boundary != self.model_request.messages.len() {
            return invalid("original message boundary differs from historical Steps");
        }
        if historical_calls != self.budget.prior_tool_calls_used {
            return invalid("prior charged usage differs from completed Turn Steps");
        }
        let charged = self.calls.iter().filter(|item| item.charged).count();
        if self.budget.prior_tool_calls_used.checked_add(charged)
            != Some(self.budget.tool_calls_used)
            || self
                .budget
                .max_tool_calls
                .is_some_and(|max| self.budget.tool_calls_used > max)
        {
            return invalid("charged usage differs from budget");
        }
        let mut approvals = HashSet::new();
        let mut approval_calls = HashSet::new();
        for approval in &self.approvals {
            identifier(&approval.approval_id)?;
            if !approvals.insert(&approval.approval_id)
                || !approval_calls.insert(&approval.prepared.call().id)
                || approval.scope != self.scope
                || approval.policy_revision.trim().is_empty()
            {
                return invalid("invalid approval binding");
            }
            let item = self
                .calls
                .iter()
                .find(|item| item.call.id == approval.prepared.call().id)
                .ok_or_else(|| {
                    CheckpointError::Invalid("approval references unknown call".into())
                })?;
            if item.prepared.as_ref() != Some(&approval.prepared) {
                return invalid("stale approval preparation");
            }
            approval
                .prepared
                .validate()
                .map_err(|e| CheckpointError::Invalid(e.to_string()))?;
            if let Some(id) = &approval.evidence_id {
                identifier(id)?;
            }
        }
        Ok(())
    }

    /// Pure, all-or-nothing merge. Partial stages remain suspended. Duplicate,
    /// foreign, oversized or mismatched results are rejected rather than trimmed.
    /// Replaying a resolution for a Completed call is rejected; hosts own durable
    /// idempotent consumption and must recover the already merged checkpoint.
    pub fn merge_external(
        &self,
        resolutions: &[ExternalResolution],
        expected_scope: &ToolExecutionScope,
    ) -> Result<Self, CheckpointError> {
        self.validate(expected_scope)?;
        let mut merged = self.clone();
        let mut seen = HashSet::new();
        for resolution in resolutions {
            if !seen.insert(&resolution.call_id) {
                return invalid("duplicate resolution");
            }
            let item = merged
                .calls
                .iter_mut()
                .find(|item| item.call.id == resolution.call_id)
                .ok_or_else(|| {
                    CheckpointError::Invalid("resolution references unknown call".into())
                })?;
            let CheckpointCallState::AwaitingExternal { wait, issued } = &item.state else {
                return invalid("resolution does not reference an awaiting call");
            };
            if wait != &resolution.wait {
                return invalid("external binding changed");
            }
            issued.validate_result(&resolution.result)?;
            item.state = CheckpointCallState::Completed {
                result: resolution.result.clone(),
                issued: Some(issued.clone()),
            };
        }
        merged.validate(expected_scope)?;
        Ok(merged)
    }

    fn validate_authority(
        &self,
        item: &CheckpointCall,
        authority: &IssuedToolAuthority,
    ) -> Result<(), CheckpointError> {
        authority.validate(&self.scope)?;
        if item.prepared.as_ref() != Some(&authority.prepared) {
            return invalid("issued preparation differs from saved call");
        }
        Ok(())
    }
}

// Count streamed serialization without allocating a second complete history.
struct EncodedLimit(usize);

impl std::io::Write for EncodedLimit {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        if self.0 > MAX_TURN_CHECKPOINT_BYTES {
            return Err(std::io::Error::other(
                "checkpoint encoding exceeds hard limit",
            ));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn identifier(value: &str) -> Result<(), CheckpointError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
    {
        return invalid("identity must be 1..256 ASCII identifier bytes");
    }
    Ok(())
}

fn invalid<T>(message: &str) -> Result<T, CheckpointError> {
    Err(CheckpointError::Invalid(message.into()))
}

// A field deserializer disables serde's missing-Option shortcut.
fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned + Serialize,
{
    exact(deserializer)
}

/// Authority envelopes must retain all fields of their imported policy values.
/// This check is deliberately not used for ModelRequest or StepResult: their
/// provider projections may have different optional-field serialization rules.
pub(super) fn exact<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned + Serialize,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let decoded: T = serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
    let encoded = serde_json::to_value(&decoded).map_err(serde::de::Error::custom)?;
    if encoded != value {
        return Err(serde::de::Error::custom(
            "missing or unknown serialized contract field",
        ));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests;
