//! Restore exact charged ceilings without minting new slots or a fresh deadline.

use super::*;

impl<J, L, S, SS> TaskExecutionService<J, L, S, SS>
where
    J: FactJournal,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
{
    pub(super) fn budget_resume_executor<P: ModelProvider, T: ToolExecutor>(
        &self,
        task_id: &str,
        binding: &AttemptBinding,
        checkpoint: &kolyan_core::TurnCheckpoint,
        executor: TurnExecutor<P, T>,
    ) -> Result<TurnExecutor<P, T>, TaskExecutionError> {
        let snapshot = self.coordinator.snapshot(task_id)?;
        let Some(budget) = snapshot.execution_budget else {
            return Ok(executor);
        };
        let reservation = budget
            .reservations
            .get(&binding.attempt_id)
            .ok_or_else(|| invalid("configured Task resume has no exact Step reservation"))?;
        if reservation.binding != *binding
            || checkpoint.budget.max_steps > reservation.max_steps as usize
            || checkpoint
                .budget
                .deadline_at_ms
                .is_none_or(|cutoff| cutoff > budget.policy.deadline_at_ms)
        {
            return Err(
                invalid("resume checkpoint exceeds its saved Task execution budget").into(),
            );
        }
        Ok(executor.with_absolute_deadline_at_ms(budget.policy.deadline_at_ms))
    }
}
