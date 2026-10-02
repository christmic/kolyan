//! Shared immutable owner validation for root approval acceptance and denial.

use super::*;
use crate::{AgentInvocationBinding, BindingContextKind};
use kolyan_server::AttemptBinding;

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    pub(super) fn load_root_approval_owner(
        &self,
        request: &RootApprovalResumeRequest,
    ) -> Result<(AgentInvocationBinding, AttemptBinding), RunnerError> {
        let (saved, binding) = self.inspect_root_approval_owner(request)?;
        let ceiling = self
            .host
            .intersection(saved.snapshot.definition().permissions())?;
        saved.snapshot.permissions().require_subset_of(&ceiling)?;
        Ok((saved, binding))
    }

    pub(super) fn inspect_root_approval_owner(
        &self,
        request: &RootApprovalResumeRequest,
    ) -> Result<(AgentInvocationBinding, AttemptBinding), RunnerError> {
        for id in [
            &request.task_id,
            &request.invocation_id,
            &request.logical_session_id,
            &request.attempt_id,
            &request.approval_id,
        ] {
            crate::identity(id)?;
        }
        let saved = self
            .bindings
            .load(
                &request.task_id,
                &request.invocation_id,
                &request.logical_session_id,
            )?
            .ok_or_else(|| RunnerError::Host("saved root binding is absent".into()))?;
        if saved.context_kind != BindingContextKind::Root {
            return Err(RunnerError::Host(
                "root approval cannot borrow a child context".into(),
            ));
        }
        let task = self.service.coordinator().snapshot(&request.task_id)?;
        let attempt = task
            .attempts
            .get(&request.attempt_id)
            .ok_or_else(|| RunnerError::Host("saved attempt is absent".into()))?;
        let binding = attempt.binding.clone();
        self.verify_root_attempt_source(&saved, &binding)?;
        if task.state.is_terminal()
            || attempt.cancellation_requested
            || binding.invocation_id != saved.invocation_id
            || binding.execution.session_id != saved.private_session_id
            || binding.agent != *saved.snapshot.identity()
            || binding.constraints_digest != saved.snapshot.digest()
        {
            return Err(RunnerError::Host(
                "saved attempt and Agent ownership differ or are terminal".into(),
            ));
        }
        Ok((saved, binding))
    }
}
