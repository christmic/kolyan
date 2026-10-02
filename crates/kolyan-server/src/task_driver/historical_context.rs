//! Frozen completed predecessor context, never the latest mutable Session history.
use super::*;
use crate::{ExecutionEvidence, ExecutionRef};
use kolyan_core::StepResult;
use kolyan_ledger::{FactRef, LedgerError, LedgerQuery};
use kolyan_model::{ContentBlock, Message, MessageRole, ToolResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_EVENTS: usize = 65_536;

#[derive(Debug, Clone)]
pub struct HistoricalContextRequest {
    pub binding: AttemptBinding,
    pub terminal_fact: FactRef,
    pub prepared: ExecutionEvidence,
    pub committed: ExecutionEvidence,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VerifiedHistoricalContext {
    pub binding: AttemptBinding,
    pub terminal_fact: FactRef,
    pub admission: kolyan_runtime::VerifiedExecutionInput,
    pub prepared: ExecutionEvidence,
    pub committed: ExecutionEvidence,
    pub messages: Vec<Message>,
    pub context_digest: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Prepared {
    session_id: String,
    status: kolyan_storage::SessionTurnStatus,
    messages: Vec<Message>,
    context_messages: Vec<Message>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Committed {
    session_id: String,
    status: kolyan_storage::SessionTurnStatus,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Step {
    step_id: String,
    outcome: kolyan_core::StepOutcome,
    step: StepResult,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Tool {
    call_id: String,
    is_error: bool,
    result: ToolResult,
}

impl<J, L, S, SS> TaskExecutionService<J, L, S, SS>
where
    J: FactJournal,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
{
    /// Frozen proof read only. Does not reconcile Session, consume, admit or run.
    pub fn load_verified_historical_context(
        &self,
        task_id: &str,
        request: &HistoricalContextRequest,
    ) -> Result<VerifiedHistoricalContext, TaskExecutionError> {
        if request.max_bytes == 0 || request.max_bytes > MAX_BYTES {
            return Err(bad("invalid historical context host ceiling").into());
        }
        let binding = &request.binding;
        if self
            .execution
            .execution()
            .server()
            .coordinator()
            .is_active(&binding.execution.execution_id)
        {
            return Err(bad("predecessor execution is active").into());
        }
        if request.prepared.execution != binding.execution
            || request.committed.execution != binding.execution
            || request.prepared.cursor >= request.committed.cursor
        {
            return Err(bad("foreign or unordered context endpoints").into());
        }
        let frozen = Frozen::read(self.ledger(), &binding.execution, request.committed.cursor)?;
        let physical = result::load_for(
            &self.coordinator,
            &frozen,
            task_id,
            binding,
            1024 * 1024,
            result::ResultRead::Historical,
        )?;
        if physical.terminal_fact != request.terminal_fact
            || !matches!(physical.outcome, VerifiedTaskOutcome::Completed { .. })
        {
            return Err(bad("predecessor terminal fact differs").into());
        }
        let observed = inspect(&frozen, binding, 0)?;
        if observed.source.cursor >= request.prepared.cursor {
            return Err(bad("terminal must precede context commit").into());
        }
        let prep = frozen.endpoint(
            &request.prepared,
            LedgerEventKind::SessionCommitPrepared,
            "Completed/prepared",
        )?;
        let commit = frozen.endpoint(
            &request.committed,
            LedgerEventKind::SessionCommitted,
            "Completed/committed",
        )?;
        let prepared: Prepared = decode(&prep.payload)?;
        let committed: Committed = decode(&commit.payload)?;
        if prepared.session_id != binding.execution.session_id
            || committed.session_id != prepared.session_id
            || prepared.status != kolyan_storage::SessionTurnStatus::Completed
            || committed.status != prepared.status
        {
            return Err(bad("foreign or noncompleted context payload").into());
        }
        let key = kolyan_runtime::ExecutionKey {
            session_id: binding.execution.session_id.clone(),
            turn_id: binding.execution.turn_id.clone(),
            execution_id: binding.execution.execution_id.clone(),
        };
        let admission = kolyan_runtime::verified_execution_input(&frozen, &key, MAX_BYTES)
            .map_err(|error| bad(error.to_string()))?;
        if admission.agent_snapshot_digest.as_deref() != Some(binding.constraints_digest.as_str())
            || admission.cursor >= observed.source.cursor
        {
            return Err(bad("historical admission scope differs").into());
        }
        let session = self
            .execution
            .sessions()
            .store
            .load(&binding.execution.session_id)
            .map_err(ServerError::from)?;
        if session.session_id != binding.execution.session_id {
            return Err(bad("foreign Session input record").into());
        }
        let input = session
            .inputs
            .get(&binding.execution.turn_id)
            .ok_or_else(|| bad("missing original Session input"))?;
        if admission.model_request.messages.get(input.history_len..)
            != Some(input.messages.as_slice())
        {
            return Err(bad("Session input does not match immutable admission range").into());
        }
        let (delta, final_content) = delta(
            &frozen.events,
            &binding.execution,
            admission.cursor,
            observed.source.cursor,
        )?;
        if frozen.events.iter().any(|event| {
            event.cursor >= observed.source.cursor
                && matches!(
                    event.kind,
                    LedgerEventKind::StepCompleted | LedgerEventKind::ToolExecutionCompleted
                )
        }) {
            return Err(bad("turn content published after terminal").into());
        }
        let mut expected_delta = input.messages.clone();
        expected_delta.extend(delta.clone());
        let mut expected_conversation = input.messages.clone();
        expected_conversation.push(Message {
            role: MessageRole::Assistant,
            content: final_content,
        });
        if prepared.context_messages != expected_delta || prepared.messages != expected_conversation
        {
            return Err(bad("completed context delta differs from physical events").into());
        }
        let mut messages = admission.model_request.messages.clone();
        messages.extend(delta);
        let context_digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&messages).map_err(|error| bad(error.to_string()))?)
        );
        let value = VerifiedHistoricalContext {
            binding: binding.clone(),
            terminal_fact: physical.terminal_fact,
            admission,
            prepared: request.prepared.clone(),
            committed: request.committed.clone(),
            messages,
            context_digest,
        };
        if serde_json::to_vec(&value)
            .map_err(|error| bad(error.to_string()))?
            .len()
            > request.max_bytes
        {
            return Err(bad("historical context envelope exceeds host ceiling").into());
        }
        Ok(value)
    }
}

fn decode<T: serde::de::DeserializeOwned + Serialize>(
    value: &serde_json::Value,
) -> Result<T, TaskError> {
    let parsed: T =
        serde_json::from_value(value.clone()).map_err(|error| bad(error.to_string()))?;
    if serde_json::to_value(&parsed).map_err(|error| bad(error.to_string()))? != *value {
        return Err(bad("unknown or noncanonical context payload"));
    }
    Ok(parsed)
}
fn bad(message: impl Into<String>) -> TaskError {
    TaskError::Invalid(message.into())
}

struct Frozen {
    events: Vec<LedgerEvent>,
}
impl Frozen {
    fn read<L: LedgerStore>(
        ledger: &L,
        execution: &ExecutionRef,
        through: u64,
    ) -> Result<Self, TaskError> {
        let mut query = LedgerQuery {
            execution_id: Some(execution.execution_id.clone()),
            event_id: None,
            after: 0,
            through: Some(through),
            limit: 32,
        };
        let mut events = vec![];
        let mut bytes = 0usize;
        loop {
            let page = ledger
                .query(&query)
                .map_err(|error| bad(error.to_string()))?;
            if page.len() > query.limit {
                return Err(bad("oversized context query page"));
            }
            let mut after = query.after;
            for event in &page {
                if event.execution_id != execution.execution_id
                    || event.turn_id != execution.turn_id
                    || event.cursor <= after
                    || event.cursor > through
                {
                    return Err(bad("foreign or unordered context query page"));
                }
                bytes = bytes
                    .checked_add(
                        serde_json::to_vec(event)
                            .map_err(|error| bad(error.to_string()))?
                            .len(),
                    )
                    .ok_or_else(|| bad("context scan overflow"))?;
                if bytes > MAX_BYTES || events.len() + page.len() > MAX_EVENTS {
                    return Err(bad("historical context scan ceiling exceeded"));
                }
                after = event.cursor;
            }
            let count = page.len();
            events.extend(page);
            query.after = after;
            if count < query.limit || after == through {
                break;
            }
        }
        Ok(Self { events })
    }
    fn endpoint(
        &self,
        source: &ExecutionEvidence,
        kind: LedgerEventKind,
        suffix: &str,
    ) -> Result<&LedgerEvent, TaskError> {
        let id = format!("{}/session/{suffix}", source.execution.execution_id);
        let event = self
            .events
            .iter()
            .find(|event| event.event_id == id)
            .ok_or_else(|| bad("missing exact context endpoint"))?;
        if source.event_id != id
            || source.cursor != event.cursor
            || event.kind != kind
            || event.idempotency_key != id
        {
            return Err(bad("context endpoint identity differs"));
        }
        Ok(event)
    }
}
impl LedgerStore for Frozen {
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        query.validate()?;
        Ok(self
            .events
            .iter()
            .filter(|event| {
                event.cursor > query.after
                    && query.through.is_none_or(|through| event.cursor <= through)
                    && query
                        .execution_id
                        .as_ref()
                        .is_none_or(|id| id == &event.execution_id)
                    && query
                        .event_id
                        .as_ref()
                        .is_none_or(|id| id == &event.event_id)
            })
            .take(query.limit)
            .cloned()
            .collect())
    }
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        Err(LedgerError::Storage("context snapshot is read only".into()))
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        Err(LedgerError::Storage(
            "context audit fallback forbidden".into(),
        ))
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        Err(LedgerError::Storage("context snapshot cannot claim".into()))
    }
}

fn delta(
    events: &[LedgerEvent],
    execution: &ExecutionRef,
    after: u64,
    through: u64,
) -> Result<(Vec<Message>, Vec<ContentBlock>), TaskError> {
    let prefix = format!("{}/turn-event/", execution.execution_id);
    let mut messages = vec![];
    let mut pending = BTreeMap::new();
    let mut steps = BTreeSet::new();
    let mut final_content = None;
    for event in events.iter().filter(|event| {
        event.event_id.starts_with(&prefix) && event.cursor > after && event.cursor < through
    }) {
        if event.execution_id != execution.execution_id
            || event.turn_id != execution.turn_id
            || event.idempotency_key != event.event_id
        {
            return Err(bad("foreign or noncanonical turn-event identity"));
        }
        match event.kind {
            LedgerEventKind::StepCompleted => {
                if final_content.is_some() {
                    return Err(bad("Step appended after final answer"));
                }
                let value: Step = decode(&event.payload)?;
                if value.step_id != value.step.step_id
                    || value.outcome != value.step.outcome
                    || value.step_id != format!("{}-step-{}", execution.turn_id, steps.len())
                    || !steps.insert(value.step_id)
                    || !pending.is_empty()
                {
                    return Err(bad("invalid Step ordering or unpaired tool calls"));
                }
                for block in &value.step.response.content {
                    if let ContentBlock::ToolCall { call } = block
                        && pending.insert(call.id.clone(), call.name.clone()).is_some()
                    {
                        return Err(bad("duplicate call in context Step"));
                    }
                }
                if value.step.outcome == kolyan_core::StepOutcome::FinalAnswer {
                    if !pending.is_empty() {
                        return Err(bad("final answer contains unresolved tool calls"));
                    }
                    final_content = Some(value.step.response.content.clone());
                } else if value.step.outcome != kolyan_core::StepOutcome::ToolCalls
                    || pending.is_empty()
                {
                    return Err(bad("nonfinal Step does not contain admitted tool calls"));
                }
                messages.push(Message {
                    role: MessageRole::Assistant,
                    content: value.step.response.content,
                });
            }
            LedgerEventKind::ToolExecutionCompleted => {
                if final_content.is_some() {
                    return Err(bad("tool content appended after final answer"));
                }
                let value: Tool = decode(&event.payload)?;
                if value.call_id != value.result.call_id
                    || value.is_error != value.result.is_error
                    || pending.remove(&value.call_id).is_none()
                {
                    return Err(bad("context tool result pairing differs"));
                }
                messages.push(Message {
                    role: MessageRole::User,
                    content: vec![ContentBlock::ToolResult {
                        result: value.result,
                    }],
                });
            }
            _ => {}
        }
    }
    if !pending.is_empty() {
        return Err(bad("incomplete committed tool pairs"));
    }
    Ok((
        messages,
        final_content.ok_or_else(|| bad("missing committed final Step"))?,
    ))
}

#[cfg(test)]
#[path = "historical_context/tests.rs"]
mod tests;
