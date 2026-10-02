//! Digest-only event projection and strict script output. No request mutation.

use serde::{Deserialize, Serialize};

use kolyan_policy::ToolExecutionScope;

use super::{HookError, HookPhase, HookScope, is_digest};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookPayload {
    BeforeModel {
        opening: u64,
        source_digest: String,
    },
    BeforeTool {
        tool_name: String,
        prepared_digest: String,
        argument_digest: String,
        scope: ToolExecutionScope,
    },
    AfterTool {
        tool_name: String,
        prepared_digest: String,
        scope: ToolExecutionScope,
        receipt_event_id: String,
        receipt_cursor: u64,
        result_digest: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookEvent {
    pub schema_version: u32,
    pub scope: HookScope,
    pub payload: HookPayload,
}
impl HookEvent {
    pub fn phase(&self) -> HookPhase {
        match &self.payload {
            HookPayload::BeforeModel { .. } => HookPhase::BeforeModel,
            HookPayload::BeforeTool { .. } => HookPhase::BeforeTool,
            HookPayload::AfterTool { .. } => HookPhase::AfterTool,
        }
    }
    pub fn tool_name(&self) -> Option<&str> {
        match &self.payload {
            HookPayload::BeforeModel { .. } => None,
            HookPayload::BeforeTool { tool_name, .. }
            | HookPayload::AfterTool { tool_name, .. } => Some(tool_name),
        }
    }
    pub(super) fn validate(&self) -> Result<(), HookError> {
        self.scope.validate()?;
        if self.schema_version != 1 {
            return Err(HookError::Invalid("unknown event schema".into()));
        }
        match &self.payload {
            HookPayload::BeforeModel {
                opening,
                source_digest,
            } => {
                if *opening == 0 || !is_digest(source_digest) {
                    return Err(HookError::Invalid("invalid opening identity".into()));
                }
            }
            HookPayload::BeforeTool {
                tool_name,
                prepared_digest,
                argument_digest,
                scope,
            } => {
                if !is_digest(argument_digest) {
                    return Err(HookError::Invalid("invalid argument digest".into()));
                }
                self.validate_tool(tool_name, prepared_digest, scope)?;
            }
            HookPayload::AfterTool {
                tool_name,
                prepared_digest,
                scope,
                receipt_event_id,
                receipt_cursor,
                result_digest,
            } => {
                if *receipt_cursor == 0
                    || receipt_event_id.is_empty()
                    || receipt_event_id.len() > 4096
                    || !is_digest(result_digest)
                {
                    return Err(HookError::Invalid(
                        "invalid committed receipt coordinates".into(),
                    ));
                }
                self.validate_tool(tool_name, prepared_digest, scope)?;
            }
        }
        Ok(())
    }
    fn validate_tool(
        &self,
        name: &str,
        digest: &str,
        scope: &ToolExecutionScope,
    ) -> Result<(), HookError> {
        super::id(name)?;
        scope
            .validate()
            .map_err(|e| HookError::Invalid(e.to_string()))?;
        if !is_digest(digest)
            || scope.execution.session_id != self.scope.private_session_id
            || scope.execution.execution_id != self.scope.execution_id
            || scope.execution.turn_id != self.scope.turn_id
            || scope.agent_snapshot_digest.as_deref() != Some(&self.scope.agent_snapshot_digest)
        {
            return Err(HookError::Integrity("foreign tool scope or digest".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookDecision {
    Continue,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookReply {
    pub schema_version: u32,
    pub decision: HookDecision,
    pub reason: String,
}
impl HookReply {
    pub fn parse(stdout: &[u8], phase: HookPhase) -> Result<Self, HookError> {
        if stdout.len() > 64 * 1024 {
            return Err(HookError::Capacity);
        }
        let reply: Self =
            serde_json::from_slice(stdout).map_err(|e| HookError::Protocol(e.to_string()))?;
        if reply.schema_version != 1
            || reply.reason.len() > 1024
            || (reply.decision == HookDecision::Deny
                && (phase == HookPhase::AfterTool || reply.reason.trim().is_empty()))
        {
            return Err(HookError::Protocol("invalid hook reply".into()));
        }
        Ok(reply)
    }
}
