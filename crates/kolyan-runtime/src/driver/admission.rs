//! Immutable execution input and ceilings. A checkpoint cannot authenticate its
//! own initial history, Agent snapshot or remaining budget after a restart.

use kolyan_core::{ToolDispatchPolicy, TurnCheckpoint, TurnDeadline, TurnRequest};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ModelRequest;
use kolyan_policy::ToolExecutionScope;
use serde::{Deserialize, Serialize};

use super::{RuntimeTurnKey, append_once};
use crate::RuntimeError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InputAdmission {
    pub schema_version: u32,
    pub key: RuntimeTurnKey,
    pub model_request: ModelRequest,
    pub max_steps: usize,
    #[serde(deserialize_with = "required_option")]
    pub max_tool_calls: Option<usize>,
    #[serde(deserialize_with = "required_option")]
    pub deadline_at_ms: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub tool_timeout_ms: Option<u64>,
    pub dispatch: ToolDispatchPolicy,
    #[serde(deserialize_with = "required_option")]
    pub agent_snapshot_digest: Option<String>,
}

impl InputAdmission {
    pub fn new(
        key: RuntimeTurnKey,
        request: &TurnRequest,
        dispatch: ToolDispatchPolicy,
        tool_timeout_ms: Option<u64>,
        agent_snapshot_digest: Option<String>,
        deadline: &TurnDeadline,
    ) -> Result<Self, RuntimeError> {
        deadline.validate_duration(request.config.deadline)?;
        let deadline_at_ms = deadline.deadline_at_ms();
        let value = Self {
            schema_version: 1,
            key,
            model_request: request.model_request.clone(),
            max_steps: request.config.max_steps,
            max_tool_calls: request.config.max_tool_calls,
            deadline_at_ms,
            tool_timeout_ms,
            dispatch,
            agent_snapshot_digest,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn persist<L: LedgerStore>(&self, ledger: &L) -> Result<(), RuntimeError> {
        self.validate()?;
        append_once(
            ledger,
            &self.key.execution_id,
            &self.key.turn_id,
            "input-admitted",
            LedgerEventKind::ExecutionInputAdmitted,
            serde_json::to_value(self).map_err(invalid)?,
        )?;
        Ok(())
    }

    pub fn load<L: LedgerStore>(ledger: &L, key: &RuntimeTurnKey) -> Result<Self, RuntimeError> {
        Self::load_record(ledger, key).map(|(value, _)| value)
    }

    pub(super) fn load_record<L: LedgerStore>(
        ledger: &L,
        key: &RuntimeTurnKey,
    ) -> Result<(Self, LedgerEvent), RuntimeError> {
        let id = format!("{}/input-admitted", key.execution_id);
        let event = ledger
            .event_by_id(&id)?
            .ok_or_else(|| invalid("missing admitted execution input"))?;
        if event.kind != LedgerEventKind::ExecutionInputAdmitted
            || event.execution_id != key.execution_id
            || event.turn_id != key.turn_id
            || event.event_id != id
            || event.idempotency_key != id
        {
            return Err(invalid("foreign admitted execution input"));
        }
        let value: Self = serde_json::from_value(event.payload.clone()).map_err(invalid)?;
        value.validate()?;
        if value.key != *key || serde_json::to_value(&value).map_err(invalid)? != event.payload {
            return Err(invalid(
                "admitted execution input differs or has unknown fields",
            ));
        }
        Ok((value, event))
    }

    pub fn expected_scope(
        &self,
        checkpoint: &TurnCheckpoint,
    ) -> Result<ToolExecutionScope, RuntimeError> {
        let index = checkpoint
            .steps
            .len()
            .checked_sub(1)
            .ok_or_else(|| invalid("empty suspended Step history"))?;
        Ok(ToolExecutionScope {
            execution: self.key.clone(),
            step_id: format!("{}-step-{index}", self.key.turn_id),
            agent_snapshot_digest: self.agent_snapshot_digest.clone(),
        })
    }

    pub fn validate_checkpoint(
        &self,
        checkpoint: &TurnCheckpoint,
    ) -> Result<ToolExecutionScope, RuntimeError> {
        let scope = self.expected_scope(checkpoint)?;
        checkpoint.validate(&scope).map_err(invalid)?;
        let mut fixed = checkpoint.model_request.clone();
        fixed.messages = self.model_request.messages.clone();
        // Core derives the dispatch request ID from the completed Step. It is
        // not a model-selected change to the immutable request configuration.
        if fixed.request_id != scope.step_id {
            return Err(invalid("checkpoint request identity differs from its Step"));
        }
        fixed.request_id = self.model_request.request_id.clone();
        if fixed != self.model_request
            || checkpoint.input_message_count != self.model_request.messages.len()
            || !checkpoint
                .model_request
                .messages
                .starts_with(&self.model_request.messages)
            || checkpoint.dispatch != self.dispatch
            || checkpoint.budget.max_steps > self.max_steps
            || !within(checkpoint.budget.max_tool_calls, self.max_tool_calls)
            || !within(checkpoint.budget.deadline_at_ms, self.deadline_at_ms)
            || !within(checkpoint.budget.tool_timeout_ms, self.tool_timeout_ms)
        {
            return Err(invalid(
                "checkpoint changes admitted context, snapshot or ceilings",
            ));
        }
        Ok(scope)
    }

    fn validate(&self) -> Result<(), RuntimeError> {
        if self.schema_version != 1 || self.max_steps == 0 {
            return Err(invalid("invalid input admission schema or Step ceiling"));
        }
        ToolExecutionScope {
            execution: self.key.clone(),
            step_id: "admission".into(),
            agent_snapshot_digest: self.agent_snapshot_digest.clone(),
        }
        .validate()
        .map_err(invalid)
    }
}

fn within<T: PartialOrd>(saved: Option<T>, admitted: Option<T>) -> bool {
    match (saved, admitted) {
        (_, None) => true,
        (Some(saved), Some(admitted)) => saved <= admitted,
        (None, Some(_)) => false,
    }
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn invalid(error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Driver(error.to_string())
}
