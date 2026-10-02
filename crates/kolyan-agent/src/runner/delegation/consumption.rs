//! Typed terminal join and exact consumption through Server's physical proof port.
//! Failure/cancellation is feedback, never substituted successful child evidence.

use kolyan_model::ToolResult;
use kolyan_server::{ConsumedTerminalResult, TerminalResultDisposition, VerifiedTaskOutcome};
use serde_json::json;

use super::*;

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    /// Join only physically verified terminal children, recording every disposition
    /// idempotently through Server. This fixed feedback policy returns typed failures
    /// to the parent with `is_error`; it never declares failed children successful.
    /// The complete result must fit the original grant before any consumption write.
    /// Partial consumption is repaired from exact existing facts without reexecuting
    /// children. Parent cancellation/terminal state blocks both fresh and recovered use.
    pub async fn consume_agent_children(
        self: &Arc<Self>,
        owner: DelegationOwner,
        issued: IssuedToolAuthority,
        supplied: ExternalWait,
    ) -> Result<ToolResult, ToolError> {
        let children = self
            .verify_agent_child_wait(owner.clone(), issued.clone(), supplied)
            .await?;
        let runner = self.clone();
        tokio::task::spawn_blocking(move || {
            runner.validate_child_owner(&owner)?;
            let ceiling = issued.grant.constraints().max_output_bytes.ok_or_else(|| denied("missing original output ceiling"))?.min(1024 * 1024) as usize;
            let mut terminals = Vec::with_capacity(children.len());
            for child in &children {
                let terminal = runner.service.load_verified_result(&owner.task_id, &child.attempt, ceiling).map_err(denied)?;
                terminals.push(terminal);
            }
            let result = ToolResult {
                call_id: issued.prepared.call().id.clone(),
                is_error: terminals.iter().any(|terminal| !matches!(terminal.outcome, VerifiedTaskOutcome::Completed { .. })),
                content: serde_json::to_string(&json!({"schema_version":1,"admission":admission_coordinate(&owner,&issued)?,"children":terminals})).map_err(denied)?,
            };
            issued.validate_result(&result).map_err(denied)?;
            for (index, child) in children.iter().enumerate() {
                let terminal = &terminals[index];
                if let Some(saved) = runner.service.load_verified_consumed_result(&owner.task_id, &owner.parent, &child.attempt, 1024 * 1024).map_err(denied)? {
                    if saved.child != *terminal { return Err(denied("existing consumed terminal proof differs")); }
                    continue;
                }
                let state = runner.service.coordinator().snapshot(&owner.task_id).map_err(denied)?;
                let observation = state.attempts.get(&child.attempt.attempt_id).and_then(|attempt| attempt.observation.as_ref()).ok_or_else(|| denied("child terminal observation is absent"))?;
                let disposition = match (&terminal.outcome, &observation.outcome) {
                    (VerifiedTaskOutcome::Completed{..}, kolyan_server::AttemptOutcome::Completed{evidence}) => TerminalResultDisposition::Completed{evidence:evidence.clone()},
                    (VerifiedTaskOutcome::Failed{reason}, kolyan_server::AttemptOutcome::Failed{reason: observed,..}) if reason == observed => TerminalResultDisposition::Failed{reason:reason.clone()},
                    (VerifiedTaskOutcome::Cancelled{reason}, kolyan_server::AttemptOutcome::Cancelled{reason:observed}) if reason == observed => TerminalResultDisposition::Cancelled{reason:reason.clone()},
                    _ => return Err(denied("terminal feedback differs from admitted observation")),
                };
                let coordinate = admission_coordinate(&owner,&issued)?;
                runner.service.coordinator().consume_terminal_result(&owner.task_id, &format!("{}/child/{index}/consumed",coordinate.fact_id), &owner.parent.invocation_id, ConsumedTerminalResult {
                    parent:owner.parent.clone(),child:child.attempt.clone(),terminal_fact:terminal.terminal_fact.clone(),source:observation.source.clone(),disposition,
                }).map_err(uncertain)?;
                let saved = runner.service.load_verified_consumed_result(&owner.task_id,&owner.parent,&child.attempt,1024 * 1024).map_err(uncertain)?.ok_or_else(|| uncertain("terminal consumption fact is absent"))?;
                if saved.child != *terminal { return Err(uncertain("committed consumed terminal proof differs")); }
            }
            Ok(result)
        }).await.map_err(uncertain)?
    }
}
