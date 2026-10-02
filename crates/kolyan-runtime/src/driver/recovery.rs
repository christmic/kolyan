//! Rehydrate only committed effects. No preparation, policy issuance, model
//! request or child admission occurs while repairing a checkpoint commit gap.

use kolyan_core::{CheckpointCallState, ToolError, ToolErrorPolicy, TurnCheckpoint, TurnError};
use kolyan_ledger::{LedgerEventKind, LedgerStore};
use kolyan_model::ToolResult;
use kolyan_trace::TraceSink;

use super::suspension::invalid;
use super::{DurableTurnDriver, InputAdmission, RuntimeTurnKey};
use crate::reconciliation::receipt::{PreparedEvidence, check_event};
use crate::{ExternalWaitContext, RuntimeError};

impl<L: LedgerStore + Clone + 'static, S: TraceSink> DurableTurnDriver<L, S> {
    pub(super) async fn hydrate_checkpoint(
        &self,
        key: &RuntimeTurnKey,
        mut checkpoint: TurnCheckpoint,
    ) -> Result<TurnCheckpoint, RuntimeError> {
        self.check_resumable(key)?;
        let admission = InputAdmission::load(&self.ledger, key)?;
        admission.validate_checkpoint(&checkpoint)?;
        for call in &mut checkpoint.calls {
            if matches!(call.state, CheckpointCallState::Completed { .. }) {
                continue;
            }
            let effect_id = format!("{}/{}", checkpoint.scope.step_id, call.call.id);
            let prefix = format!("{}/effect/{effect_id}", key.execution_id);
            let Some(started) = self.ledger.event_by_id(&format!("{prefix}/started"))? else {
                continue;
            };
            check_event(
                &started,
                key,
                &format!("{prefix}/started"),
                LedgerEventKind::EffectStarted,
            )
            .map_err(invalid)?;
            let prepared_id = format!("{prefix}/prepared");
            let authorized_id = format!("{prefix}/authorized");
            let prepared = self
                .ledger
                .event_by_id(&prepared_id)?
                .ok_or_else(|| invalid("entered effect lacks preparation"))?;
            let authorized = self
                .ledger
                .event_by_id(&authorized_id)?
                .ok_or_else(|| invalid("entered effect lacks authorization"))?;
            check_event(
                &prepared,
                key,
                &prepared_id,
                LedgerEventKind::EffectPrepared,
            )
            .map_err(invalid)?;
            check_event(
                &authorized,
                key,
                &authorized_id,
                LedgerEventKind::EffectAuthorized,
            )
            .map_err(invalid)?;
            let bound = PreparedEvidence::from_facts(
                key,
                &effect_id,
                &prepared.payload,
                &authorized.payload,
            )
            .map_err(invalid)?;
            if bound.scope != checkpoint.scope
                || bound.prepared.call() != &call.call
                || call.prepared.as_ref() != Some(&bound.prepared)
            {
                return Err(invalid("recovered authority differs from the saved call"));
            }
            self.validate_effect_authority(&bound, key)?;
            let issued = bound.issued();
            if let CheckpointCallState::AwaitingExternal { issued: saved, .. } = &call.state
                && saved != &issued
            {
                return Err(invalid("recovered external authority differs"));
            }
            let receipt_id = format!("{prefix}/receipt");
            let state = if let Some(receipt) = self.ledger.event_by_id(&receipt_id)? {
                check_event(&receipt, key, &receipt_id, LedgerEventKind::EffectReceipt)
                    .map_err(invalid)?;
                let (result, kind) =
                    match bound.validate_receipt(&receipt.payload).map_err(invalid)? {
                        Ok(result) => (result, LedgerEventKind::EffectCompleted),
                        Err(error) => {
                            self.atomic_publication(
                                key,
                                &format!("{prefix}/failed"),
                                LedgerEventKind::EffectFailed,
                                receipt.payload.clone(),
                            )?;
                            if checkpoint.dispatch.on_error == ToolErrorPolicy::FailTurn
                                || matches!(
                                    error,
                                    ToolError::Cancelled
                                        | ToolError::TimedOut
                                        | ToolError::Uncertain { .. }
                                )
                            {
                                return Err(RuntimeError::Turn(TurnError::Tool(error)));
                            }
                            (
                                ToolResult {
                                    call_id: call.call.id.clone(),
                                    content: error.to_string(),
                                    is_error: true,
                                },
                                LedgerEventKind::EffectFailed,
                            )
                        }
                    };
                issued.validate_result(&result).map_err(invalid)?;
                self.atomic_publication(
                    key,
                    &format!(
                        "{prefix}/{}",
                        if kind == LedgerEventKind::EffectCompleted {
                            "completed"
                        } else {
                            "failed"
                        }
                    ),
                    kind,
                    receipt.payload,
                )?;
                CheckpointCallState::Completed {
                    result,
                    issued: Some(issued),
                }
            } else {
                let waiting_id = format!("{prefix}/waiting");
                let wait = if let Some(event) = self.ledger.event_by_id(&waiting_id)? {
                    check_event(
                        &event,
                        key,
                        &waiting_id,
                        LedgerEventKind::EffectAwaitingExternal,
                    )
                    .map_err(invalid)?;
                    bound.validate_wait(&event.payload).map_err(invalid)?
                } else {
                    self.verifier.recover_wait(issued.clone()).await.map_err(invalid)?
                        .ok_or_else(|| RuntimeError::Turn(TurnError::Tool(ToolError::Uncertain {
                            message:"entered effect has no receipt or proven external admission; reconciliation required".into(),
                        })))?
                };
                let context = ExternalWaitContext {
                    issued: issued.clone(),
                    wait: wait.clone(),
                };
                context.validate(&checkpoint.scope).map_err(invalid)?;
                self.verifier.verify_wait(context).await.map_err(invalid)?;
                self.atomic_publication(
                    key,
                    &waiting_id,
                    LedgerEventKind::EffectAwaitingExternal,
                    bound.wait_payload(&wait).map_err(invalid)?,
                )?;
                CheckpointCallState::AwaitingExternal { wait, issued }
            };
            if !call.charged {
                checkpoint.budget.tool_calls_used = checkpoint
                    .budget
                    .tool_calls_used
                    .checked_add(1)
                    .ok_or_else(|| invalid("recovered tool usage overflow"))?;
                call.charged = true;
            }
            call.state = state;
        }
        checkpoint.approvals.retain(|approval| {
            checkpoint.calls.iter().any(|call| {
                call.call.id == approval.prepared.call().id
                    && matches!(call.state, CheckpointCallState::Ready)
            })
        });
        admission.validate_checkpoint(&checkpoint)?;
        self.validate_checkpoint_effects(key, &checkpoint)?;
        Ok(checkpoint)
    }
}

#[cfg(test)]
mod tests;
