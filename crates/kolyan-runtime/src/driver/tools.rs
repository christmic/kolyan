//! Receipt-backed prepared execution. Started without a receipt remains
//! uncertain, never permission to repeat an external side effect.

use kolyan_core::{ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ToolCall;
use serde_json::{Value, json};

use super::{RuntimeTurnKey, append_once};
use crate::reconciliation::receipt::{PreparedEvidence, check_event, failed};

pub(super) struct DurableTools<L, T> {
    ledger: L,
    key: RuntimeTurnKey,
    inner: T,
}

impl<L, T> DurableTools<L, T> {
    pub(super) fn new(ledger: L, key: RuntimeTurnKey, inner: T) -> Self {
        Self { ledger, key, inner }
    }
}

impl<L: LedgerStore, T: ToolExecutor> ToolExecutor for DurableTools<L, T> {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        self.inner.prepare(call)
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            let bound = PreparedEvidence::new(
                invocation.prepared.clone(),
                invocation.grant.clone(),
                invocation.scope.clone(),
                &invocation.policy_revision,
                &self.key,
            )?;
            if invocation.control.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
            let effect = &bound.request.effect_id;
            let prefix = bound.prefix();
            append_once(
                &self.ledger,
                &self.key.execution_id,
                &self.key.turn_id,
                &format!("effect/{effect}/prepared"),
                LedgerEventKind::EffectPrepared,
                bound.prepared_payload()?,
            )
            .map_err(failed)?;
            if let Some(event) = self
                .ledger
                .event_by_id(&format!("{prefix}/receipt"))
                .map_err(failed)?
            {
                check_event(
                    &event,
                    &self.key,
                    &format!("{prefix}/receipt"),
                    LedgerEventKind::EffectReceipt,
                )?;
                let authorized = self
                    .ledger
                    .event_by_id(&bound.authorization.authorization_id)
                    .map_err(failed)?
                    .ok_or_else(|| failed("receipt has no durable authorization"))?;
                check_event(
                    &authorized,
                    &self.key,
                    &bound.authorization.authorization_id,
                    LedgerEventKind::EffectAuthorized,
                )?;
                if authorized.payload != bound.authorized_payload()? {
                    return Err(failed("receipt authorization mismatch"));
                }
                let started_id = format!("{prefix}/started");
                let started = self
                    .ledger
                    .event_by_id(&started_id)
                    .map_err(failed)?
                    .ok_or_else(|| failed("receipt has no durable start binding"))?;
                check_event(
                    &started,
                    &self.key,
                    &started_id,
                    LedgerEventKind::EffectStarted,
                )?;
                if started.payload != bound.started_payload() {
                    return Err(failed("receipt start binding mismatch"));
                }
                return bound.validate_receipt(&event.payload)?;
            }
            if let Some(started) = self
                .ledger
                .event_by_id(&format!("{prefix}/started"))
                .map_err(failed)?
            {
                check_event(
                    &started,
                    &self.key,
                    &format!("{prefix}/started"),
                    LedgerEventKind::EffectStarted,
                )?;
                append_once(
                    &self.ledger,
                    &self.key.execution_id,
                    &self.key.turn_id,
                    &format!("effect/{effect}/uncertain"),
                    LedgerEventKind::EffectUncertain,
                    json!({"effect_id": effect, "input_digest": bound.request.input_digest,
                        "scope": bound.scope, "evidence": "started invocation has no receipt"}),
                )
                .map_err(failed)?;
                return Err(failed("uncertain tool effect: reconciliation required"));
            }
            append_once(
                &self.ledger,
                &self.key.execution_id,
                &self.key.turn_id,
                &format!("effect/{effect}/authorized"),
                LedgerEventKind::EffectAuthorized,
                bound.authorized_payload()?,
            )
            .map_err(failed)?;
            if invocation.control.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
            let id = format!("{prefix}/started");
            self.ledger
                .append_unless_cancelled(LedgerEvent {
                    event_id: id.clone(),
                    turn_id: self.key.turn_id.clone(),
                    execution_id: self.key.execution_id.clone(),
                    cursor: 0,
                    kind: LedgerEventKind::EffectStarted,
                    idempotency_key: id,
                    payload: bound.started_payload(),
                })
                .map_err(|error| match error {
                    kolyan_ledger::LedgerError::Cancelled(_) => ToolError::Cancelled,
                    error => failed(error),
                })?;
            // Pass the original scope and control intact. Dropping after entry
            // leaves Started without a receipt; no rollback can be inferred.
            let result = self.inner.execute_invocation(invocation).await;
            let payload: Value = bound.receipt_payload(&result)?;
            append_once(
                &self.ledger,
                &self.key.execution_id,
                &self.key.turn_id,
                &format!("effect/{effect}/receipt"),
                LedgerEventKind::EffectReceipt,
                payload.clone(),
            )
            .map_err(failed)?;
            let (suffix, kind) = if result.is_ok() {
                ("completed", LedgerEventKind::EffectCompleted)
            } else {
                ("failed", LedgerEventKind::EffectFailed)
            };
            append_once(
                &self.ledger,
                &self.key.execution_id,
                &self.key.turn_id,
                &format!("effect/{effect}/{suffix}"),
                kind,
                payload,
            )
            .map_err(failed)?;
            result
        })
    }
}

#[cfg(test)]
mod tests;
