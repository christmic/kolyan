//! Compound snapshots are committed atomically with their wait summaries.
//! Display approval facts do not replace the execution input or effect evidence.

use kolyan_core::{ApprovalRequest, CheckpointCallState, TurnCheckpoint, TurnSuspension};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_trace::TraceSink;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{DurableTurnDriver, DurableTurnResult, InputAdmission, RuntimeTurnKey, append_once};
use crate::RuntimeError;
use crate::reconciliation::receipt::{PreparedEvidence, check_event};

impl<L: LedgerStore + Clone + 'static, S: TraceSink> DurableTurnDriver<L, S> {
    pub(super) fn execution_key(&self, execution_id: &str) -> Result<RuntimeTurnKey, RuntimeError> {
        let id = format!("{execution_id}/execution-started");
        let event = self
            .ledger
            .event_by_id(&id)?
            .ok_or_else(|| invalid("missing execution identity"))?;
        let key: RuntimeTurnKey = serde_json::from_value(event.payload.clone()).map_err(invalid)?;
        if event.kind != LedgerEventKind::ExecutionStarted
            || event.event_id != id
            || event.idempotency_key != id
            || event.execution_id != execution_id
            || key.execution_id != execution_id
            || event.turn_id != key.turn_id
            || serde_json::to_value(&key).map_err(invalid)? != event.payload
        {
            return Err(invalid("foreign or malformed execution identity"));
        }
        InputAdmission::load(&self.ledger, &key)?;
        Ok(key)
    }

    pub fn load_current_suspension(
        &self,
        execution_id: &str,
    ) -> Result<TurnSuspension, RuntimeError> {
        let key = self.execution_key(execution_id)?;
        let admission = InputAdmission::load(&self.ledger, &key)?;
        let event = self
            .ledger
            .execution_events_after(execution_id, 0)?
            .into_iter()
            .rev()
            .find(|event| {
                matches!(
                    event.kind,
                    LedgerEventKind::ExecutionSuspended
                        | LedgerEventKind::TurnCheckpointMerged
                        | LedgerEventKind::TurnCheckpointPrepared
                )
            })
            .ok_or_else(|| invalid("missing compound checkpoint"))?;
        if event.execution_id != key.execution_id
            || event.turn_id != key.turn_id
            || event.payload["schema_version"] != 1
        {
            return Err(invalid("foreign or unsupported compound checkpoint"));
        }
        let (checkpoint, suspension, category) =
            if event.kind == LedgerEventKind::ExecutionSuspended {
                let suspension: TurnSuspension =
                    serde_json::from_value(event.payload["suspension"].clone()).map_err(invalid)?;
                let checkpoint = suspension.checkpoint.clone();
                (checkpoint, Some(suspension), "suspended")
            } else {
                let checkpoint: TurnCheckpoint =
                    serde_json::from_value(event.payload["checkpoint"].clone()).map_err(invalid)?;
                (
                    checkpoint,
                    None,
                    if event.kind == LedgerEventKind::TurnCheckpointPrepared {
                        "prepared"
                    } else {
                        "merged"
                    },
                )
            };
        let scope = admission.validate_checkpoint(&checkpoint)?;
        let result = if let Some(value) = suspension {
            value.validate(&scope).map_err(invalid)?;
            value
        } else {
            TurnSuspension::from_checkpoint(checkpoint, &scope).map_err(invalid)?
        };
        let publication = event.payload["publication_cursor"]
            .as_u64()
            .ok_or_else(|| invalid("missing checkpoint publication cursor"))?;
        if publication >= event.cursor {
            return Err(invalid("checkpoint publication does not follow its source"));
        }
        let expected = if category == "suspended" {
            json!({"schema_version":1,"publication_cursor":publication,"suspension":result})
        } else {
            json!({"schema_version":1,"publication_cursor":publication,"checkpoint":result.checkpoint})
        };
        let id = format!(
            "{execution_id}/checkpoint/{category}/{}",
            content_digest(&expected)?
        );
        if event.payload != expected || event.event_id != id || event.idempotency_key != id {
            return Err(invalid("checkpoint content identity differs"));
        }
        self.validate_checkpoint_effects(&key, &result.checkpoint)?;
        Ok(result)
    }

    pub fn load_suspension(
        &self,
        execution_id: &str,
        checkpoint_id: &str,
    ) -> Result<TurnSuspension, RuntimeError> {
        let current = self.load_current_suspension(execution_id)?;
        if current.checkpoint.checkpoint_id != checkpoint_id {
            return Err(invalid("stale or foreign checkpoint identity"));
        }
        Ok(current)
    }

    pub fn load_approval(
        &self,
        execution_id: &str,
        approval_id: &str,
    ) -> Result<ApprovalRequest, RuntimeError> {
        self.load_current_suspension(execution_id)?
            .waiting
            .approvals
            .into_iter()
            .find(|approval| approval.approval_id == approval_id)
            .ok_or_else(|| invalid("approval is not pending in the current checkpoint"))
    }

    pub(super) fn suspended(
        &self,
        key: &RuntimeTurnKey,
        suspension: TurnSuspension,
    ) -> Result<DurableTurnResult, RuntimeError> {
        let admitted = InputAdmission::load(&self.ledger, key)?;
        let scope = admitted.validate_checkpoint(&suspension.checkpoint)?;
        suspension.validate(&scope).map_err(invalid)?;
        self.validate_checkpoint_effects(key, &suspension.checkpoint)?;
        for approval in &suspension.waiting.approvals {
            append_once(
                &self.ledger,
                &key.execution_id,
                &key.turn_id,
                &format!("approval/{}/requested", approval.approval_id),
                LedgerEventKind::ApprovalRequested,
                json!({"schema_version":1,"approval":approval}),
            )?;
        }
        let publication = self
            .ledger
            .execution_events_after(&key.execution_id, 0)?
            .last()
            .map_or(0, |event| event.cursor);
        let payload =
            json!({"schema_version":1,"publication_cursor":publication,"suspension":suspension});
        let id = format!(
            "{}/checkpoint/suspended/{}",
            key.execution_id,
            content_digest(&payload)?
        );
        if let Some(existing) = self.ledger.event_by_id(&id)? {
            if existing.turn_id != key.turn_id
                || existing.execution_id != key.execution_id
                || existing.payload != payload
                || existing.kind != LedgerEventKind::ExecutionSuspended
                || existing.idempotency_key != id
            {
                return Err(invalid("conflicting compound checkpoint publication"));
            }
        } else {
            self.ledger.append_unless_cancelled(LedgerEvent {
                event_id: id.clone(),
                idempotency_key: id,
                execution_id: key.execution_id.clone(),
                turn_id: key.turn_id.clone(),
                cursor: 0,
                kind: LedgerEventKind::ExecutionSuspended,
                payload,
            })?;
        }
        Ok(DurableTurnResult::Suspended {
            suspension: Box::new(suspension),
            trajectory: self.project_events(key)?,
        })
    }

    pub(super) fn validate_checkpoint_effects(
        &self,
        key: &RuntimeTurnKey,
        checkpoint: &TurnCheckpoint,
    ) -> Result<(), RuntimeError> {
        validate_checkpoint_history(&self.ledger, key, checkpoint)?;
        for call in &checkpoint.calls {
            match &call.state {
                CheckpointCallState::Ready
                | CheckpointCallState::Completed { issued: None, .. } => {}
                CheckpointCallState::Completed {
                    result,
                    issued: Some(issued),
                } => {
                    let bound = PreparedEvidence::new(
                        issued.prepared.clone(),
                        issued.grant.clone(),
                        issued.scope.clone(),
                        &issued.policy_revision,
                        key,
                    )
                    .map_err(invalid)?;
                    self.validate_effect_authority(&bound, key)?;
                    let id = format!("{}/receipt", bound.prefix());
                    let event = self
                        .ledger
                        .event_by_id(&id)?
                        .ok_or_else(|| invalid("completed checkpoint call has no receipt"))?;
                    check_event(&event, key, &id, LedgerEventKind::EffectReceipt)
                        .map_err(invalid)?;
                    let saved = match bound.validate_receipt(&event.payload).map_err(invalid)? {
                        Ok(output) => output,
                        Err(error) => kolyan_model::ToolResult {
                            call_id: call.call.id.clone(),
                            content: error.to_string(),
                            is_error: true,
                        },
                    };
                    if saved != *result {
                        return Err(invalid("checkpoint result differs from the exact receipt"));
                    }
                }
                CheckpointCallState::AwaitingExternal { wait, issued } => {
                    let bound = PreparedEvidence::new(
                        issued.prepared.clone(),
                        issued.grant.clone(),
                        issued.scope.clone(),
                        &issued.policy_revision,
                        key,
                    )
                    .map_err(invalid)?;
                    self.validate_effect_authority(&bound, key)?;
                    let id = format!("{}/waiting", bound.prefix());
                    let event = self
                        .ledger
                        .event_by_id(&id)?
                        .ok_or_else(|| invalid("checkpoint wait has no durable effect evidence"))?;
                    check_event(&event, key, &id, LedgerEventKind::EffectAwaitingExternal)
                        .map_err(invalid)?;
                    if bound.validate_wait(&event.payload).map_err(invalid)? != *wait {
                        return Err(invalid("checkpoint wait differs from effect evidence"));
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn validate_effect_authority(
        &self,
        bound: &PreparedEvidence,
        key: &RuntimeTurnKey,
    ) -> Result<(), RuntimeError> {
        for (id, kind, payload) in [
            (
                format!("{}/prepared", bound.prefix()),
                LedgerEventKind::EffectPrepared,
                bound.prepared_payload().map_err(invalid)?,
            ),
            (
                bound.authorization.authorization_id.clone(),
                LedgerEventKind::EffectAuthorized,
                bound.authorized_payload().map_err(invalid)?,
            ),
            (
                format!("{}/started", bound.prefix()),
                LedgerEventKind::EffectStarted,
                bound.started_payload(),
            ),
        ] {
            let event = self
                .ledger
                .event_by_id(&id)?
                .ok_or_else(|| invalid("missing effect authority fact"))?;
            check_event(&event, key, &id, kind).map_err(invalid)?;
            if event.payload != payload {
                return Err(invalid("effect preparation, scope or authority differs"));
            }
        }
        Ok(())
    }
}

pub(super) fn content_digest(value: &Value) -> Result<String, RuntimeError> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(invalid)?)
    ))
}

pub(super) fn invalid(error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Driver(error.to_string())
}

pub(super) fn validate_checkpoint_history<L: LedgerStore>(
    ledger: &L,
    key: &RuntimeTurnKey,
    checkpoint: &TurnCheckpoint,
) -> Result<(), RuntimeError> {
    let source = ledger.execution_events_after(&key.execution_id, 0)?;
    for step in &checkpoint.steps {
        let expected =
            json!({"step_id":step.step_id,"outcome":format!("{:?}",step.outcome),"step":step});
        if !source.iter().any(|event| {
            event.kind == LedgerEventKind::StepCompleted
                && event.execution_id == key.execution_id
                && event.turn_id == key.turn_id
                && event.payload == expected
        }) {
            return Err(invalid(
                "checkpoint Step differs from committed model response",
            ));
        }
    }
    Ok(())
}
