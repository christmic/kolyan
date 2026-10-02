//! A successor inherits verified predecessor context, not a delegated ToolResult.
//! All readers are historical and bounded; this module never initializes a
//! context, reconciles execution, consumes a result or authorizes another run.

pub(super) mod projection;

use super::*;
use kolyan_server::{
    ExecutionEvidence, HistoricalContextRequest, PrivateContextOwner, PrivateContextService,
};

const MAX_CONTEXT_BYTES: usize = 16 * 1024 * 1024;

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    pub(super) fn verify_continuation_context(
        &self,
        request: &TaskFinalizationRequest,
        task: &TaskSnapshot,
        invocation_id: &str,
        terminals: &BTreeMap<String, VerifiedTaskResult>,
    ) -> Result<(), RunnerError> {
        let invocation = &task.invocations[invocation_id];
        let predecessor_id = invocation
            .definition
            .parent_invocation_id
            .as_ref()
            .ok_or_else(|| invalid("Continuation has no predecessor"))?;
        if !invocation.definition.dependencies.contains(predecessor_id) {
            return Err(invalid(
                "Continuation predecessor is not an explicit dependency",
            ));
        }
        let predecessor = terminals
            .get(predecessor_id)
            .ok_or_else(|| invalid("Continuation predecessor terminal is absent"))?;
        if !matches!(predecessor.outcome, VerifiedTaskOutcome::Completed { .. }) {
            return Err(invalid("Continuation predecessor did not complete"));
        }
        let (saved, ownership) = self
            .bindings
            .load_with_reference(&request.task_id, invocation_id, &request.logical_session_id)?
            .ok_or_else(|| invalid("Continuation owner is absent"))?;
        if saved.context_kind != BindingContextKind::Child {
            return Err(invalid("Continuation does not own a private context"));
        }
        let owner = PrivateContextOwner {
            logical_session_id: saved.logical_session_id.clone(),
            task_id: saved.task_id.clone(),
            invocation_id: saved.invocation_id.clone(),
            private_session_id: saved.private_session_id.clone(),
            snapshot_digest: saved.snapshot.digest().into(),
        };
        let contexts = PrivateContextService::new(
            self.service.sessions().sessions().clone(),
            Arc::new(ContextJournal(self.service.clone())),
            Arc::new(self.bindings.clone()),
        );
        let initialized = contexts
            .load_verified_initialization(&owner, &ownership, MAX_CONTEXT_BYTES)
            .map_err(|error| invalid(error.to_string()))?;
        let history = self.service.load_verified_historical_context(
            &request.task_id,
            &HistoricalContextRequest {
                binding: predecessor.binding.clone(),
                terminal_fact: predecessor.terminal_fact.clone(),
                prepared: self.context_endpoint(&predecessor.binding.execution, "prepared")?,
                committed: self.context_endpoint(&predecessor.binding.execution, "committed")?,
                max_bytes: MAX_CONTEXT_BYTES,
            },
        )?;
        self.verify_continuation_projection(
            request,
            invocation_id,
            &initialized,
            &history,
            &terminals[invocation_id],
        )?;
        Ok(())
    }

    // Read coordinates only. Server verifies exact kind, payload, scope and
    // frozen cursor endpoints; Agent never decodes a shadow commit schema.
    fn context_endpoint(
        &self,
        execution: &kolyan_server::ExecutionRef,
        suffix: &str,
    ) -> Result<ExecutionEvidence, RunnerError> {
        let id = format!("{}/session/Completed/{suffix}", execution.execution_id);
        let event = self
            .service
            .sessions()
            .execution()
            .server()
            .coordinator()
            .ledger()
            .event_by_id(&id)
            .map_err(|error| invalid(error.to_string()))?
            .ok_or_else(|| invalid("predecessor context endpoint is absent"))?;
        Ok(ExecutionEvidence {
            execution: execution.clone(),
            cursor: event.cursor,
            event_id: id,
        })
    }
}

// Reuse the existing Task journal; no extra storage or duplicated definitions.
pub(in crate::runner) struct ContextJournal<J, L, S, SS>(
    pub(in crate::runner) Arc<kolyan_server::TaskExecutionService<J, L, S, SS>>,
);
impl<J, L, S, SS> FactJournal for ContextJournal<J, L, S, SS>
where
    J: FactJournal,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
{
    fn read(
        &self,
        stream: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        self.0.coordinator().journal().read(stream, after, limit)
    }
    fn append(
        &self,
        _stream: &str,
        _expected: u64,
        _facts: Vec<kolyan_ledger::FactDraft>,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        Err(kolyan_ledger::FactError::Conflict(
            "finalization context journal is read only".into(),
        ))
    }
}

#[cfg(test)]
mod tests;
