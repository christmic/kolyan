//! Read-only Runtime host proof port, attached once without an Arc ownership cycle.

use std::sync::{OnceLock, Weak};

use kolyan_model::ToolResult;
use kolyan_runtime::{
    ExternalRecoveryFuture, ExternalVerificationFuture, ExternalWaitContext, ExternalWaitVerifier,
};
use kolyan_server::VerifiedTaskOutcome;

use super::*;

type WeakHost<J, L, S, SS, P, T> = Weak<AgentRunner<J, L, S, SS, P, T>>;
type Host<J, L, S, SS, P, T> = Arc<AgentRunner<J, L, S, SS, P, T>>;

/// Construct before ExecutionService, install there, then attach the resulting
/// Runner once. A missing or dropped host rejects every wait, never authorizes it.
pub struct AgentChildWaitVerifier<J, L, S, SS, P, T> {
    runner: OnceLock<WeakHost<J, L, S, SS, P, T>>,
}

impl<J, L, S, SS, P, T> Default for AgentChildWaitVerifier<J, L, S, SS, P, T> {
    fn default() -> Self {
        Self {
            runner: OnceLock::new(),
        }
    }
}

impl<J, L, S, SS, P, T> AgentChildWaitVerifier<J, L, S, SS, P, T> {
    pub fn attach(&self, runner: &Arc<AgentRunner<J, L, S, SS, P, T>>) -> Result<(), ToolError> {
        self.runner
            .set(Arc::downgrade(runner))
            .map_err(|_| denied("Agent wait verifier already attached"))
    }

    fn host(&self) -> Result<Host<J, L, S, SS, P, T>, ToolError> {
        self.runner
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| denied("Agent wait verifier has no active host"))
    }
}

impl<J, L, S, SS, P, T> ExternalWaitVerifier for AgentChildWaitVerifier<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    fn verify_wait(&self, context: ExternalWaitContext) -> ExternalVerificationFuture<'_> {
        Box::pin(async move {
            let runner = self.host()?;
            tokio::task::spawn_blocking(move || {
                let admission = runner
                    .admission_for_authority(&context.issued)?
                    .ok_or_else(|| denied("Agent child admission is absent"))?;
                context.validate(&admission.owner.scope)?;
                if context.wait != wait(admission_coordinate(&admission.owner, &context.issued)?)? {
                    return Err(denied("wait differs from committed Agent admission"));
                }
                runner.verify_child_admission(&admission.owner, &context.issued, &admission)
            })
            .await
            .map_err(uncertain)?
        })
    }

    fn verify_result(
        &self,
        context: ExternalWaitContext,
        result: ToolResult,
    ) -> ExternalVerificationFuture<'_> {
        Box::pin(async move {
            let runner = self.host()?;
            tokio::task::spawn_blocking(move || {
                let admission = runner.admission_for_authority(&context.issued)?
                    .ok_or_else(|| denied("Agent child admission is absent"))?;
                context.validate_result(&admission.owner.scope, &result)?;
                runner.verify_child_admission(&admission.owner, &context.issued, &admission)?;
                let coordinate = admission_coordinate(&admission.owner, &context.issued)?;
                if context.wait != wait(coordinate.clone())? {
                    return Err(denied("result wait differs from committed Agent admission"));
                }
                let mut terminals = Vec::with_capacity(admission.children.len());
                for child in &admission.children {
                    let consumed = runner.service.load_verified_consumed_result(&admission.owner.task_id,
                        &admission.owner.parent, &child.attempt, 1024 * 1024).map_err(denied)?
                        .ok_or_else(|| denied("child has no verified consumption proof"))?;
                    terminals.push(consumed.child);
                }
                let expected = ToolResult {
                    call_id: context.issued.prepared.call().id.clone(),
                    is_error: terminals.iter().any(|terminal| !matches!(terminal.outcome, VerifiedTaskOutcome::Completed { .. })),
                    content: serde_json::to_string(&serde_json::json!({"schema_version":1,"admission":coordinate,"children":terminals})).map_err(denied)?,
                };
                if result != expected { return Err(denied("result differs from verified consumed child terminals")); }
                Ok(())
            }).await.map_err(uncertain)?
        })
    }

    fn recover_wait(&self, issued: IssuedToolAuthority) -> ExternalRecoveryFuture<'_> {
        Box::pin(async move {
            let runner = self.host()?;
            tokio::task::spawn_blocking(move || {
                let Some(admission) = runner.admission_for_authority(&issued)? else {
                    return Ok(None);
                };
                runner.verify_child_admission(&admission.owner, &issued, &admission)?;
                wait(admission_coordinate(&admission.owner, &issued)?).map(Some)
            })
            .await
            .map_err(uncertain)?
        })
    }
}

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    fn admission_for_authority(
        &self,
        issued: &IssuedToolAuthority,
    ) -> Result<Option<Admission>, ToolError> {
        if issued.prepared.call().name != crate::AGENT_INVOKE_NAME {
            return Err(denied("unsupported external tool"));
        }
        // Serialized plan coordinates are lookup hints only. The admitted graph
        // and exact committed fact supply independent ownership before acceptance.
        let plan = issued.prepared.execution_binding();
        let task_id = plan["task_id"]
            .as_str()
            .ok_or_else(|| denied("missing task lookup coordinate"))?;
        let invocation_id = plan["parent_invocation_id"]
            .as_str()
            .ok_or_else(|| denied("missing parent lookup coordinate"))?;
        crate::identity(task_id).map_err(denied)?;
        crate::identity(invocation_id).map_err(denied)?;
        let graph = self
            .service
            .coordinator()
            .snapshot(task_id)
            .map_err(denied)?;
        let key = &issued.scope.execution;
        let mut matching = graph.attempts.values().filter(|attempt| {
            let binding = &attempt.binding;
            binding.invocation_id == invocation_id
                && binding.execution.session_id == key.session_id
                && binding.execution.turn_id == key.turn_id
                && binding.execution.execution_id == key.execution_id
        });
        let parent = matching
            .next()
            .ok_or_else(|| denied("no admitted parent attempt matches authority"))?
            .binding
            .clone();
        if matching.next().is_some() {
            return Err(denied("ambiguous parent execution authority"));
        }
        // Logical Session is not part of the lookup hash; it must be recovered
        // from the trusted receipt and checked by BindingStore before use.
        let lookup = DelegationOwner {
            task_id: task_id.into(),
            logical_session_id: String::new(),
            parent,
            scope: issued.scope.clone(),
        };
        let coordinate = admission_coordinate(&lookup, issued)?;
        let admission = self.load_child_admission(&coordinate)?;
        if let Some(saved) = &admission
            && (admission_coordinate(&saved.owner, issued)? != coordinate
                || saved.issued != *issued)
        {
            return Err(denied("authority does not match exact committed admission"));
        }
        Ok(admission)
    }
}
