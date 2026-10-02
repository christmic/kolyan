//! Generic immutable input evidence. Agent alone authenticates opaque body semantics.
use super::types::{InvocationRole, TaskEvent, TaskSnapshot};
use super::{AgentIdentity, TaskCoordinator, TaskError};
use kolyan_ledger::{FactDraft, FactJournal, FactRecord, FactRef, FactSubject};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_CAUSAL_RECORDS: usize = 1024;
const KIND: &str = "task.invocation_input_source";
pub const INVOCATION_INPUT_SOURCE_SUBJECT_KIND: &str = "task.invocation-input-source";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationInputKind {
    Standalone,
    Derived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum InvocationInputSource {
    Standalone { fact: FactRef },
    Derived { fact: FactRef },
}
impl InvocationInputSource {
    pub fn fact(&self) -> &FactRef {
        match self {
            Self::Standalone { fact } | Self::Derived { fact } => fact,
        }
    }
    pub fn kind(&self) -> InvocationInputKind {
        match self {
            Self::Standalone { .. } => InvocationInputKind::Standalone,
            Self::Derived { .. } => InvocationInputKind::Derived,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationInputScope {
    pub task_id: String,
    pub invocation_id: String,
    pub agent: AgentIdentity,
    pub constraints_digest: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationInputEnvelope {
    pub kind: InvocationInputKind,
    pub scope: InvocationInputScope,
    pub body: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VerifiedInvocationInputSource {
    pub reference: FactRef,
    pub envelope: InvocationInputEnvelope,
    pub causes: Vec<FactRef>,
}

impl<J: FactJournal> TaskCoordinator<J> {
    pub(super) fn verify_input_event(
        &self,
        task_id: &str,
        event: &TaskEvent,
        record: &FactRecord,
        snapshot: Option<&TaskSnapshot>,
    ) -> Result<(), TaskError> {
        let Some(source) = event_source(event) else {
            return Ok(());
        };
        let (id, agent, digest) = match event {
            TaskEvent::InvocationAdmitted(definition) => (
                &definition.invocation_id,
                &definition.agent,
                &definition.constraints_digest,
            ),
            TaskEvent::AttemptStarted(binding)
            | TaskEvent::BudgetedAttemptStarted { binding, .. } => (
                &binding.invocation_id,
                &binding.agent,
                &binding.constraints_digest,
            ),
            _ => return Err(bad("unsupported source event")),
        };
        if !record.draft.causes.contains(source.fact()) {
            return Err(bad("Task input source causal binding is missing"));
        }
        let verified = self.load_verified_invocation_input_source(
            source,
            &InvocationInputScope {
                task_id: task_id.into(),
                invocation_id: id.clone(),
                agent: agent.clone(),
                constraints_digest: digest.clone(),
            },
            MAX_BYTES,
        )?;
        let state = snapshot.ok_or_else(|| bad("input source has no Task state"))?;
        let invocation = state
            .invocations
            .get(id)
            .ok_or_else(|| bad("source invocation is absent"))?;
        if invocation.definition.input_source != *source {
            return Err(bad("attempt source differs from admitted source"));
        }
        if invocation.definition.role == InvocationRole::Continuation {
            let parent = invocation
                .definition
                .parent_invocation_id
                .as_ref()
                .and_then(|id| state.invocations.get(id))
                .ok_or_else(|| bad("source predecessor is absent"))?;
            let completed = parent
                .completion_fact
                .as_ref()
                .ok_or_else(|| bad("source predecessor completion is absent"))?;
            if !verified.causes.contains(completed) {
                return Err(bad(
                    "Continuation source lacks exact predecessor completion cause",
                ));
            }
        }
        Ok(())
    }
    /// Publish once per Task/invocation. Exact retries retain the reference.
    /// Changed identity/body/causes conflict; this does not admit or execute work.
    /// Large Agent source bodies must use required artifacts under Ledger bounds.
    pub fn publish_invocation_input_source(
        &self,
        envelope: InvocationInputEnvelope,
        causes: Vec<FactRef>,
    ) -> Result<VerifiedInvocationInputSource, TaskError> {
        validate_scope(&envelope.scope)?;
        let stream = coordinate(&envelope.scope)?;
        validate_causes(self.journal(), &causes, &stream)?;
        let draft = FactDraft {
            fact_id: stream.clone(),
            subject: FactSubject {
                kind: INVOCATION_INPUT_SOURCE_SUBJECT_KIND.into(),
                id: envelope.scope.invocation_id.clone(),
            },
            kind: KIND.into(),
            schema_version: 1,
            critical: true,
            causes,
            payload: serde_json::to_value(&envelope).map_err(|error| bad(error.to_string()))?,
        };
        bound(
            &FactRecord {
                stream_id: stream.clone(),
                position: 1,
                draft: draft.clone(),
            },
            MAX_BYTES,
        )?;
        let mut rows = self.journal().read(&stream, 0, 2)?;
        if rows.is_empty() {
            // One CAS, then read the exact winner. No retry-selection or overwrite.
            let append = self.journal().append(&stream, 0, vec![draft.clone()]);
            rows = self.journal().read(&stream, 0, 2)?;
            if rows.is_empty() {
                append?;
                return Err(bad("source append has no durable record"));
            }
        }
        if rows.len() != 1
            || rows[0]
                != (FactRecord {
                    stream_id: stream.clone(),
                    position: 1,
                    draft,
                })
        {
            return Err(bad("invocation input source content conflicts"));
        }
        let fact = FactRef {
            stream_id: stream.clone(),
            position: 1,
            fact_id: stream,
        };
        let source = match envelope.kind {
            InvocationInputKind::Standalone => InvocationInputSource::Standalone { fact },
            InvocationInputKind::Derived => InvocationInputSource::Derived { fact },
        };
        self.load_verified_invocation_input_source(&source, &envelope.scope, MAX_BYTES)
    }

    /// Read-only generic envelope/causal closure; no Agent body interpretation,
    /// admission, allocation, reconciliation, grants or model/tool execution.
    /// `max_bytes` bounds the source record and complete returned envelope.
    /// The causal scan independently allows at most 1,024 unique records and
    /// 16 MiB cumulative serialized bytes. Shared DAG nodes count only once.
    pub fn load_verified_invocation_input_source(
        &self,
        source: &InvocationInputSource,
        expected: &InvocationInputScope,
        max_bytes: usize,
    ) -> Result<VerifiedInvocationInputSource, TaskError> {
        if max_bytes == 0 || max_bytes > MAX_BYTES {
            return Err(bad("invalid input source host ceiling"));
        }
        validate_scope(expected)?;
        let stream = coordinate(expected)?;
        if source.fact()
            != &(FactRef {
                stream_id: stream.clone(),
                position: 1,
                fact_id: stream.clone(),
            })
        {
            return Err(bad("foreign input source coordinate"));
        }
        let rows = self.journal().read(&stream, 0, 2)?;
        if rows.len() != 1 {
            return Err(bad("missing or ambiguous input source"));
        }
        let record = &rows[0];
        bound(record, max_bytes)?;
        if record.stream_id != stream
            || record.position != 1
            || record.draft.fact_id != stream
            || record.draft.kind != KIND
            || record.draft.schema_version != 1
            || !record.draft.critical
            || record.draft.subject
                != (FactSubject {
                    kind: INVOCATION_INPUT_SOURCE_SUBJECT_KIND.into(),
                    id: expected.invocation_id.clone(),
                })
        {
            return Err(bad("invalid input source fact envelope"));
        }
        let envelope: InvocationInputEnvelope =
            serde_json::from_value(record.draft.payload.clone())
                .map_err(|error| bad(error.to_string()))?;
        if envelope.scope != *expected
            || envelope.kind != source.kind()
            || serde_json::to_value(&envelope).map_err(|error| bad(error.to_string()))?
                != record.draft.payload
        {
            return Err(bad("input source scope/kind or known fields differ"));
        }
        validate_causes(self.journal(), &record.draft.causes, &stream)?;
        let value = VerifiedInvocationInputSource {
            reference: source.fact().clone(),
            envelope,
            causes: record.draft.causes.clone(),
        };
        bound(&value, max_bytes)?;
        Ok(value)
    }
}

pub(super) fn event_source(event: &TaskEvent) -> Option<&InvocationInputSource> {
    match event {
        TaskEvent::InvocationAdmitted(definition) => Some(&definition.input_source),
        TaskEvent::AttemptStarted(binding) | TaskEvent::BudgetedAttemptStarted { binding, .. } => {
            Some(&binding.input_source)
        }
        _ => None,
    }
}

fn coordinate(scope: &InvocationInputScope) -> Result<String, TaskError> {
    let bytes = serde_json::to_vec(&json!([
        "kolyan.server.invocation-input-source.v1",
        scope.task_id,
        scope.invocation_id
    ]))
    .map_err(|error| bad(error.to_string()))?;
    Ok(format!(
        "task.invocation-input-source.{:x}",
        Sha256::digest(bytes)
    ))
}
fn validate_scope(scope: &InvocationInputScope) -> Result<(), TaskError> {
    for id in [
        &scope.task_id,
        &scope.invocation_id,
        &scope.agent.definition_id,
        &scope.agent.revision,
        &scope.agent.instance_id,
    ] {
        super::reducer::identity(id)?;
    }
    if scope.constraints_digest.len() != 64
        || !scope
            .constraints_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(bad("invalid input constraints digest"));
    }
    Ok(())
}
fn bound<T: Serialize>(value: &T, max_bytes: usize) -> Result<(), TaskError> {
    if serde_json::to_vec(value)
        .map_err(|error| bad(error.to_string()))?
        .len()
        > max_bytes
    {
        return Err(bad("input source evidence exceeds host ceiling"));
    }
    Ok(())
}
type CausalKey = (String, u64, String);
enum Walk {
    Enter(FactRef),
    Exit(CausalKey),
}
fn validate_causes<J: FactJournal>(
    journal: &J,
    causes: &[FactRef],
    stream: &str,
) -> Result<(), TaskError> {
    if causes.is_empty() || causes.len() > 32 {
        return Err(bad("missing or oversized input causes"));
    }
    for (index, cause) in causes.iter().enumerate() {
        if causes[..index].contains(cause) {
            return Err(bad("duplicate input cause"));
        }
    }
    let mut black = BTreeSet::new();
    let mut gray = BTreeSet::new();
    let mut bytes = 0usize;
    let mut stack = causes
        .iter()
        .rev()
        .cloned()
        .map(Walk::Enter)
        .collect::<Vec<_>>();
    while let Some(action) = stack.pop() {
        let reference = match action {
            Walk::Exit(key) => {
                gray.remove(&key);
                black.insert(key);
                continue;
            }
            Walk::Enter(reference) => reference,
        };
        if reference.position == 0 || reference.stream_id == stream {
            return Err(bad("cyclic or nonhistorical input cause"));
        }
        super::reducer::identity(&reference.stream_id)?;
        super::reducer::identity(&reference.fact_id)?;
        let key = (
            reference.stream_id.clone(),
            reference.position,
            reference.fact_id.clone(),
        );
        if gray.contains(&key) {
            return Err(bad("cyclic input cause"));
        }
        if black.contains(&key) {
            continue;
        }
        if black.len() + gray.len() >= MAX_CAUSAL_RECORDS {
            return Err(bad("input causal scan count exceeded"));
        }
        let rows = journal.read(&reference.stream_id, reference.position - 1, 1)?;
        let [record] = rows.as_slice() else {
            return Err(bad("missing or ambiguous input cause"));
        };
        if record.stream_id != reference.stream_id
            || record.position != reference.position
            || record.draft.fact_id != reference.fact_id
            || !record.draft.critical
            || record.draft.causes.len() > 32
        {
            return Err(bad("foreign or noncritical input cause"));
        }
        let size = serde_json::to_vec(record)
            .map_err(|error| bad(error.to_string()))?
            .len();
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| bad("input causal byte overflow"))?;
        if bytes > MAX_BYTES {
            return Err(bad("input causal scan byte ceiling exceeded"));
        }
        for (index, cause) in record.draft.causes.iter().enumerate() {
            if record.draft.causes[..index].contains(cause)
                || (cause.stream_id == record.stream_id && cause.position >= record.position)
            {
                return Err(bad("duplicate or nonhistorical causal reference"));
            }
        }
        gray.insert(key.clone());
        stack.push(Walk::Exit(key));
        stack.extend(record.draft.causes.iter().rev().cloned().map(Walk::Enter));
    }
    Ok(())
}
fn bad(message: impl Into<String>) -> TaskError {
    TaskError::Invalid(message.into())
}

#[cfg(test)]
mod tests;
