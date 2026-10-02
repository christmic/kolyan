//! Verify host evidence, merge purely, commit receipts and merged state, then
//! drive. No permanent resume claim can strand a committed continuation.

use std::sync::Arc;

use kolyan_core::{
    ApprovalConfirmation, CheckpointCallState, ExternalResolution, ResumableTurn, ResumeInput,
    ToolExecutor, TurnCheckpoint, TurnControl, TurnDeadline, TurnError, TurnExecutor,
};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ModelProvider;
use kolyan_trace::TraceSink;
use serde_json::{Value, json};

use super::suspension::{content_digest, invalid};
use super::{
    DurableTools, DurableTurnDriver, DurableTurnResult, InputAdmission, LedgerBoundaryControl,
    LedgerRecorder, RuntimeTurnKey,
};
use crate::reconciliation::receipt::{PreparedEvidence, check_event};
use crate::{ExternalWaitContext, RuntimeError, approval_decision_payload};

impl<L: LedgerStore + Clone + 'static, S: TraceSink> DurableTurnDriver<L, S> {
    /// Approval convenience reads an already persisted affirmative host decision.
    /// Calling Runtime is not itself evidence that a human approved the call.
    pub async fn resume_approval<P: ModelProvider, T: ToolExecutor>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        approval_id: &str,
    ) -> Result<DurableTurnResult, RuntimeError> {
        let execution_id = execution_id.into();
        let suspension = self.load_current_suspension(&execution_id)?;
        let event = self
            .ledger
            .execution_events_after(&execution_id, 0)?
            .into_iter()
            .rev()
            .find(|event| {
                event.kind == LedgerEventKind::ApprovalResolved
                    && event.payload["approval_id"] == approval_id
                    && event.payload["schema_version"] == 1
                    && event.payload["evidence_id"].is_string()
            })
            .ok_or_else(|| invalid("approval has no durable host decision"))?;
        let confirmation:ApprovalConfirmation=serde_json::from_value(json!({
            "approval_id":event.payload["approval_id"],"prepared_digest":event.payload["prepared_digest"],
            "policy_revision":event.payload["policy_revision"],"scope":event.payload["scope"],"evidence_id":event.payload["evidence_id"],
        })).map_err(invalid)?;
        self.resume(
            executor,
            session_id,
            execution_id,
            &suspension.checkpoint.checkpoint_id,
            ResumeInput::ApprovalConfirmed(confirmation),
        )
        .await
    }

    pub async fn resume<P: ModelProvider, T: ToolExecutor>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        checkpoint_id: &str,
        input: ResumeInput,
    ) -> Result<DurableTurnResult, RuntimeError> {
        let execution_id = execution_id.into();
        let key = self.execution_key(&execution_id)?;
        if key.session_id != session_id.into() {
            return Err(invalid("foreign resume Session"));
        }
        self.check_resumable(&key)?;
        let executor = executor.with_execution_key(key.clone());
        let admission = InputAdmission::load(&self.ledger, &key)?;
        if executor.agent_snapshot_digest() != admission.agent_snapshot_digest.as_deref() {
            return Err(invalid(
                "resume executor differs from admitted Agent snapshot",
            ));
        }
        let suspension = self.load_suspension(&execution_id, checkpoint_id)?;
        let scope = admission.validate_checkpoint(&suspension.checkpoint)?;
        let deadline = TurnDeadline::restore(suspension.checkpoint.budget.deadline_at_ms)?;
        match &input {
            ResumeInput::ApprovalConfirmed(confirmation) => {
                let event = self
                    .ledger
                    .event_by_id(&confirmation.evidence_id)?
                    .ok_or_else(|| invalid("missing affirmative approval evidence"))?;
                check_event(
                    &event,
                    &key,
                    &confirmation.evidence_id,
                    LedgerEventKind::ApprovalResolved,
                )
                .map_err(invalid)?;
                if event.payload
                    != approval_decision_payload(checkpoint_id, confirmation, "approve")
                {
                    return Err(invalid(
                        "approval evidence is not an exact affirmative decision",
                    ));
                }
            }
            ResumeInput::ExternalResolved(resolutions) => {
                for resolution in resolutions {
                    self.verify_resolution(&key, &suspension.checkpoint, resolution)
                        .await?;
                }
            }
        }
        for item in &suspension.checkpoint.calls {
            if let CheckpointCallState::AwaitingExternal { wait, issued } = &item.state {
                self.verifier
                    .verify_wait(ExternalWaitContext {
                        issued: issued.clone(),
                        wait: wait.clone(),
                    })
                    .await
                    .map_err(invalid)?;
            }
        }
        let merged = match executor.merge_resume_with_control_and_deadline(
            suspension,
            input.clone(),
            scope,
            TurnControl::default(),
            deadline.clone(),
        ) {
            Ok(merged) => merged,
            Err(error @ TurnError::TimedOut) => {
                // The real checked Core merge entered this Runtime resume attempt.
                // Preserve its timeout so Session projection can close/reconcile.
                self.persist_error(&key, &error)?;
                return Err(RuntimeError::Turn(error));
            }
            Err(error) => return Err(RuntimeError::Turn(error)),
        };
        admission.validate_checkpoint(&merged)?;
        if let ResumeInput::ExternalResolved(resolutions) = &input {
            for resolution in resolutions {
                self.commit_resolution(&key, &merged, resolution)?;
            }
        }
        self.commit_merged(&key, &merged)?;
        self.drive_checkpoint(executor, &key, merged, deadline)
            .await
    }

    /// Resume an already committed pure merge after a crash, without consuming
    /// external proof again. Original authority and receipts are still checked.
    pub async fn resume_committed<P: ModelProvider, T: ToolExecutor>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        checkpoint_id: &str,
    ) -> Result<DurableTurnResult, RuntimeError> {
        let execution_id = execution_id.into();
        let key = self.execution_key(&execution_id)?;
        if key.session_id != session_id.into() {
            return Err(invalid("foreign committed resume Session"));
        }
        self.check_resumable(&key)?;
        let current = self.load_suspension(&execution_id, checkpoint_id)?;
        let deadline = TurnDeadline::restore(current.checkpoint.budget.deadline_at_ms)?;
        let checkpoint = self.hydrate_checkpoint(&key, current.checkpoint).await?;
        self.commit_merged(&key, &checkpoint)?;
        self.drive_checkpoint(executor, &key, checkpoint, deadline)
            .await
    }

    pub(super) fn check_resumable(&self, key: &RuntimeTurnKey) -> Result<(), RuntimeError> {
        let events = self.ledger.execution_events_after(&key.execution_id, 0)?;
        if events.iter().any(|event| {
            matches!(
                event.kind,
                LedgerEventKind::ExecutionCancelled | LedgerEventKind::TurnCancelled
            )
        }) {
            return Err(RuntimeError::Turn(TurnError::Cancelled));
        }
        if events.iter().any(|event| {
            matches!(
                event.kind,
                LedgerEventKind::TurnCompleted
                    | LedgerEventKind::TurnFailed
                    | LedgerEventKind::TurnTimedOut
            )
        }) {
            return Err(invalid("terminal execution cannot resume"));
        }
        Ok(())
    }

    async fn verify_resolution(
        &self,
        key: &RuntimeTurnKey,
        checkpoint: &TurnCheckpoint,
        resolution: &ExternalResolution,
    ) -> Result<(), RuntimeError> {
        let call = checkpoint
            .calls
            .iter()
            .find(|call| call.call.id == resolution.call_id)
            .ok_or_else(|| invalid("resolution references unknown call"))?;
        let CheckpointCallState::AwaitingExternal { wait, issued } = &call.state else {
            return Err(invalid("resolution call is not awaiting external work"));
        };
        if *wait != resolution.wait {
            return Err(invalid("resolution differs from saved wait"));
        }
        let context = ExternalWaitContext {
            issued: issued.clone(),
            wait: wait.clone(),
        };
        context
            .validate_result(&checkpoint.scope, &resolution.result)
            .map_err(invalid)?;
        let bound = PreparedEvidence::new(
            issued.prepared.clone(),
            issued.grant.clone(),
            issued.scope.clone(),
            &issued.policy_revision,
            key,
        )
        .map_err(invalid)?;
        self.validate_effect_authority(&bound, key)?;
        self.verifier
            .verify_result(context, resolution.result.clone())
            .await
            .map_err(invalid)
    }

    fn commit_resolution(
        &self,
        key: &RuntimeTurnKey,
        merged: &TurnCheckpoint,
        resolution: &ExternalResolution,
    ) -> Result<(), RuntimeError> {
        let call = merged
            .calls
            .iter()
            .find(|call| call.call.id == resolution.call_id)
            .ok_or_else(|| invalid("merged call missing"))?;
        let CheckpointCallState::Completed {
            result,
            issued: Some(issued),
        } = &call.state
        else {
            return Err(invalid("merged resolution lacks authority"));
        };
        let bound = PreparedEvidence::new(
            issued.prepared.clone(),
            issued.grant.clone(),
            issued.scope.clone(),
            &issued.policy_revision,
            key,
        )
        .map_err(invalid)?;
        let payload = bound
            .receipt_payload(&Ok(result.clone()))
            .map_err(invalid)?;
        self.atomic_publication(
            key,
            &format!("{}/receipt", bound.prefix()),
            LedgerEventKind::EffectReceipt,
            payload.clone(),
        )?;
        self.atomic_publication(
            key,
            &format!("{}/completed", bound.prefix()),
            LedgerEventKind::EffectCompleted,
            payload,
        )?;
        Ok(())
    }

    pub(super) fn commit_merged(
        &self,
        key: &RuntimeTurnKey,
        checkpoint: &TurnCheckpoint,
    ) -> Result<(), RuntimeError> {
        InputAdmission::load(&self.ledger, key)?.validate_checkpoint(checkpoint)?;
        self.validate_checkpoint_effects(key, checkpoint)?;
        let publication = self
            .ledger
            .execution_events_after(&key.execution_id, 0)?
            .last()
            .map_or(0, |event| event.cursor);
        let payload =
            json!({"schema_version":1,"publication_cursor":publication,"checkpoint":checkpoint});
        let id = format!(
            "{}/checkpoint/merged/{}",
            key.execution_id,
            content_digest(&payload)?
        );
        self.atomic_publication(key, &id, LedgerEventKind::TurnCheckpointMerged, payload)
    }

    pub(super) fn atomic_publication(
        &self,
        key: &RuntimeTurnKey,
        id: &str,
        kind: LedgerEventKind,
        payload: Value,
    ) -> Result<(), RuntimeError> {
        self.check_resumable(key)?;
        if let Some(event) = self.ledger.event_by_id(id)? {
            check_event(&event, key, id, kind).map_err(invalid)?;
            if event.payload != payload {
                return Err(invalid("conflicting result publication"));
            }
        } else {
            self.ledger
                .append_unless_cancelled(LedgerEvent {
                    event_id: id.into(),
                    idempotency_key: id.into(),
                    execution_id: key.execution_id.clone(),
                    turn_id: key.turn_id.clone(),
                    cursor: 0,
                    kind,
                    payload,
                })
                .map_err(|error| match error {
                    kolyan_ledger::LedgerError::Cancelled(_) => {
                        RuntimeError::Turn(TurnError::Cancelled)
                    }
                    error => RuntimeError::Ledger(error),
                })?;
        }
        Ok(())
    }

    async fn drive_checkpoint<P: ModelProvider, T: ToolExecutor>(
        &self,
        executor: TurnExecutor<P, T>,
        key: &RuntimeTurnKey,
        checkpoint: TurnCheckpoint,
        deadline: TurnDeadline,
    ) -> Result<DurableTurnResult, RuntimeError> {
        self.check_resumable(key)?;
        let admission = InputAdmission::load(&self.ledger, key)?;
        let scope = admission.validate_checkpoint(&checkpoint)?;
        if executor.agent_snapshot_digest() != admission.agent_snapshot_digest.as_deref() {
            return Err(invalid("resume executor snapshot differs"));
        }
        let cursor = self
            .ledger
            .execution_events_after(&key.execution_id, 0)?
            .last()
            .map_or(0, |event| event.cursor);
        let recorder = Arc::new(LedgerRecorder::new(
            self.ledger.clone(),
            key.clone(),
            format!("resume/{}/{cursor}", checkpoint.checkpoint_id),
        ));
        let executor = executor
            .with_execution_key(key.clone())
            .map_tool_executor(|inner| {
                let tools = DurableTools::new(self.ledger.clone(), key.clone(), inner)
                    .with_snapshot_digest(admission.agent_snapshot_digest.clone())
                    .with_wait_verifier(self.verifier.clone());
                match &self.effect_hooks {
                    Some(hooks) => tools.with_effect_hooks(hooks.clone()),
                    None => tools,
                }
            })
            .with_boundary_control(Arc::new(LedgerBoundaryControl::for_resume(
                self.ledger.clone(),
                key.clone(),
                cursor,
            )))
            .with_event_recorder(recorder.clone())
            .with_step_event_recorder(recorder);
        match executor
            .resume_checkpoint_with_control_and_deadline(
                checkpoint,
                scope,
                TurnControl::default(),
                deadline,
            )
            .await
        {
            Ok(ResumableTurn::Completed(execution)) => self.completed(key, *execution),
            Ok(ResumableTurn::Suspended(suspension)) => self.suspended(key, *suspension),
            Err(error) => {
                self.persist_error(key, &error)?;
                Err(RuntimeError::Turn(error))
            }
        }
    }
}
