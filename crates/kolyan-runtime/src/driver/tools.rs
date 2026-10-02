//! Receipt-backed prepared execution. Started without a receipt remains
//! uncertain, never permission to repeat an external side effect.

use std::sync::Arc;

use kolyan_core::{
    ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture,
};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ToolCall;
use serde_json::{Value, json};

use super::{RuntimeTurnKey, append_once};
use crate::effect_hooks::{
    CommittedEffectReceipt, EffectHookContext, EffectHookDecision, EffectHookError, EffectHookPort,
    entry_error, observation_error,
};
use crate::reconciliation::receipt::{PreparedEvidence, check_event, failed};
use crate::{ExternalWaitContext, ExternalWaitVerifier, RefuseExternalWaits};

pub(super) struct DurableTools<L, T> {
    ledger: L,
    key: RuntimeTurnKey,
    inner: T,
    verifier: Arc<dyn ExternalWaitVerifier>,
    snapshot_digest: Option<String>,
    effect_hooks: Option<Arc<dyn EffectHookPort>>,
}

impl<L, T> DurableTools<L, T> {
    pub(super) fn new(ledger: L, key: RuntimeTurnKey, inner: T) -> Self {
        Self {
            ledger,
            key,
            inner,
            verifier: Arc::new(RefuseExternalWaits),
            snapshot_digest: None,
            effect_hooks: None,
        }
    }

    pub(super) fn with_wait_verifier(mut self, verifier: Arc<dyn ExternalWaitVerifier>) -> Self {
        self.verifier = verifier;
        self
    }

    pub(super) fn with_snapshot_digest(mut self, digest: Option<String>) -> Self {
        self.snapshot_digest = digest;
        self
    }

    /// Only an unconfigured host skips hooks; configured failures never fall back.
    pub(super) fn with_effect_hooks(mut self, hooks: Arc<dyn EffectHookPort>) -> Self {
        self.effect_hooks = Some(hooks);
        self
    }
}

impl<L: LedgerStore, T: ToolExecutor> ToolExecutor for DurableTools<L, T> {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        self.inner.prepare(call)
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            if invocation.scope.agent_snapshot_digest != self.snapshot_digest {
                return Err(failed("tool snapshot differs from Runtime admission"));
            }
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
            let hook_context =
                EffectHookContext::new(&bound, invocation.control.clone(), invocation.window);
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
                )
                .map_err(|error| {
                    self.uncertain(
                        &bound,
                        format!("saved receipt identity is not proven: {error}"),
                    )
                })?;
                check_entered(&self.ledger, &bound, &self.key)
                    .map_err(|error| self.uncertain(&bound, error.to_string()))?;
                let result = bound.validate_receipt(&event.payload).map_err(|error| {
                    self.uncertain(&bound, format!("saved receipt integrity failed: {error}"))
                })?;
                if let Some(hooks) = &self.effect_hooks {
                    hook_context
                        .check_live()
                        .map_err(|error| observation_error("verify_observation", &event, error))?;
                    let receipt =
                        CommittedEffectReceipt::from_ack(&self.ledger, &bound, event.clone())
                            .map_err(|error| {
                                observation_error("verify_observation", &event, error)
                            })?;
                    hook_context
                        .wait(hooks.verify_observation(hook_context.clone(), receipt))
                        .await
                        .map_err(|error| observation_error("verify_observation", &event, error))?;
                }
                return result.map(ToolOutcome::Completed);
            }
            if let Some(event) = self
                .ledger
                .event_by_id(&format!("{prefix}/waiting"))
                .map_err(failed)?
            {
                check_event(
                    &event,
                    &self.key,
                    &format!("{prefix}/waiting"),
                    LedgerEventKind::EffectAwaitingExternal,
                )
                .map_err(|error| {
                    self.uncertain(
                        &bound,
                        format!("saved wait identity is not proven: {error}"),
                    )
                })?;
                check_entered(&self.ledger, &bound, &self.key)
                    .map_err(|error| self.uncertain(&bound, error.to_string()))?;
                let wait = bound.validate_wait(&event.payload).map_err(|error| {
                    self.uncertain(&bound, format!("saved wait integrity failed: {error}"))
                })?;
                self.verifier
                    .verify_wait(ExternalWaitContext {
                        issued: bound.issued(),
                        wait: wait.clone(),
                    })
                    .await
                    .map_err(|error| ToolError::Uncertain {
                        message: format!("saved external admission is not proven: {error}"),
                    })?;
                return Ok(ToolOutcome::AwaitingExternal(wait));
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
                )
                .map_err(|error| {
                    self.uncertain(
                        &bound,
                        format!("entered effect identity is not proven: {error}"),
                    )
                })?;
                check_entered(&self.ledger, &bound, &self.key).map_err(|error| {
                    self.uncertain(
                        &bound,
                        format!("entered effect authority is not proven: {error}"),
                    )
                })?;
                let recovered =
                    self.verifier
                        .recover_wait(bound.issued())
                        .await
                        .map_err(|error| {
                            self.uncertain(
                                &bound,
                                format!("external admission recovery failed: {error}"),
                            )
                        })?;
                if let Some(wait) = recovered {
                    let context = ExternalWaitContext {
                        issued: bound.issued(),
                        wait: wait.clone(),
                    };
                    context
                        .validate(&bound.scope)
                        .map_err(|error| self.uncertain(&bound, error.to_string()))?;
                    self.verifier
                        .verify_wait(context)
                        .await
                        .map_err(|error| self.uncertain(&bound, error.to_string()))?;
                    let id = format!("{prefix}/waiting");
                    self.ledger
                        .append_unless_cancelled(LedgerEvent {
                            event_id: id.clone(),
                            idempotency_key: id,
                            execution_id: self.key.execution_id.clone(),
                            turn_id: self.key.turn_id.clone(),
                            cursor: 0,
                            kind: LedgerEventKind::EffectAwaitingExternal,
                            payload: bound.wait_payload(&wait)?,
                        })
                        .map_err(|error| match error {
                            kolyan_ledger::LedgerError::Cancelled(_) => ToolError::Cancelled,
                            error => self.uncertain(&bound, error.to_string()),
                        })?;
                    return Ok(ToolOutcome::AwaitingExternal(wait));
                }
                return Err(self.uncertain(
                    &bound,
                    "uncertain tool effect: reconciliation required".into(),
                ));
            }
            if let Some(hooks) = &self.effect_hooks {
                hook_context.check_live().map_err(entry_error)?;
                match hook_context
                    .wait(hooks.before_effect(hook_context.clone()))
                    .await
                    .map_err(entry_error)?
                {
                    EffectHookDecision::Continue => {}
                    EffectHookDecision::Denied { reason } => {
                        if reason.trim().is_empty() || reason.len() > 1024 {
                            return Err(entry_error(EffectHookError::Host {
                                message: "invalid hook denial reason".into(),
                            }));
                        }
                        return Err(ToolError::PolicyDenied { message: reason });
                    }
                }
                hook_context.check_live().map_err(entry_error)?;
                // A hook can await while the executor's revision or resource binding changes.
                // Re-read current preparation inside the original window, never a renewed grant.
                let current = hook_context
                    .wait(Box::pin(async {
                        self.inner
                            .prepare(invocation.prepared.call().clone())
                            .await
                            .map_err(|error| EffectHookError::Host {
                                message: format!("post-hook current preparation failed: {error}"),
                            })
                    }))
                    .await
                    .map_err(entry_error)?;
                hook_context.check_live().map_err(entry_error)?;
                if current != invocation.prepared {
                    return Err(entry_error(EffectHookError::Host {
                        message: "post-hook current preparation differs from issued preparation"
                            .into(),
                    }));
                }
                invocation
                    .grant
                    .validate(
                        &invocation.prepared,
                        &invocation.policy_revision,
                        &invocation.scope,
                    )
                    .map_err(|error| {
                        entry_error(EffectHookError::Host {
                            message: format!("post-hook authority differs: {error}"),
                        })
                    })?;
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
                    error => ToolError::Uncertain {
                        message: format!("effect entry outcome is not proven: {error}"),
                    },
                })?;
            // Pass the original scope and control intact. Dropping after entry
            // leaves Started without a receipt; no rollback can be inferred.
            let outcome = self.inner.execute_invocation(invocation).await;
            let result = match outcome {
                Ok(ToolOutcome::Completed(result)) => Ok(result),
                Ok(ToolOutcome::AwaitingExternal(wait)) => {
                    let payload = match bound.wait_payload(&wait) {
                        Ok(payload) => payload,
                        Err(error) => return Err(self.uncertain(&bound, error.to_string())),
                    };
                    let context = ExternalWaitContext {
                        issued: bound.issued(),
                        wait: wait.clone(),
                    };
                    if let Err(error) = self.verifier.verify_wait(context).await {
                        return Err(self.uncertain(
                            &bound,
                            format!("external admission is not proven: {error}"),
                        ));
                    }
                    if let Err(error) = self.ledger.append_unless_cancelled(LedgerEvent {
                        event_id: format!("{prefix}/waiting"),
                        turn_id: self.key.turn_id.clone(),
                        execution_id: self.key.execution_id.clone(),
                        cursor: 0,
                        kind: LedgerEventKind::EffectAwaitingExternal,
                        idempotency_key: format!("{prefix}/waiting"),
                        payload,
                    }) {
                        return Err(self.uncertain(
                            &bound,
                            format!("external wait publication failed: {error}"),
                        ));
                    }
                    return Ok(ToolOutcome::AwaitingExternal(wait));
                }
                Err(error @ ToolError::Uncertain { .. }) => {
                    return Err(self.uncertain(&bound, error.to_string()));
                }
                Err(error) => Err(error),
            };
            let payload: Value = match bound.receipt_payload(&result) {
                Ok(payload) => payload,
                Err(error) => return Err(self.uncertain(&bound, error.to_string())),
            };
            let receipt_event = append_once(
                &self.ledger,
                &self.key.execution_id,
                &self.key.turn_id,
                &format!("effect/{effect}/receipt"),
                LedgerEventKind::EffectReceipt,
                payload.clone(),
            )
            .map_err(|error| {
                self.uncertain(
                    &bound,
                    format!("effect receipt publication failed: {error}"),
                )
            })?;
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
            .map_err(|error| {
                self.uncertain(
                    &bound,
                    format!("effect completion projection failed: {error}"),
                )
            })?;
            if let Some(hooks) = &self.effect_hooks {
                hook_context
                    .check_live()
                    .map_err(|error| observation_error("after_receipt", &receipt_event, error))?;
                let receipt = CommittedEffectReceipt::from_ack(
                    &self.ledger,
                    &bound,
                    receipt_event.clone(),
                )
                .map_err(|error| observation_error("after_receipt", &receipt_event, error))?;
                hook_context
                    .wait(hooks.after_receipt(hook_context.clone(), receipt))
                    .await
                    .map_err(|error| observation_error("after_receipt", &receipt_event, error))?;
            }
            result.map(ToolOutcome::Completed)
        })
    }
}

impl<L: LedgerStore, T> DurableTools<L, T> {
    fn uncertain(&self, bound: &PreparedEvidence, message: String) -> ToolError {
        let persisted = append_once(
            &self.ledger,
            &self.key.execution_id,
            &self.key.turn_id,
            &format!("effect/{}/uncertain", bound.request.effect_id),
            LedgerEventKind::EffectUncertain,
            json!({"effect_id":bound.request.effect_id,"input_digest":bound.request.input_digest,
                "scope":bound.scope,"evidence":message}),
        );
        ToolError::Uncertain {
            message: match persisted {
                Ok(_) => message,
                Err(error) => format!("{message}; uncertainty publication failed: {error}"),
            },
        }
    }
}

fn check_entered<L: LedgerStore>(
    ledger: &L,
    bound: &PreparedEvidence,
    key: &RuntimeTurnKey,
) -> Result<(), ToolError> {
    let authorization = ledger
        .event_by_id(&bound.authorization.authorization_id)
        .map_err(failed)?
        .ok_or_else(|| failed("wait has no durable authorization"))?;
    check_event(
        &authorization,
        key,
        &bound.authorization.authorization_id,
        LedgerEventKind::EffectAuthorized,
    )?;
    if authorization.payload != bound.authorized_payload()? {
        return Err(failed("wait authorization mismatch"));
    }
    let id = format!("{}/started", bound.prefix());
    let started = ledger
        .event_by_id(&id)
        .map_err(failed)?
        .ok_or_else(|| failed("wait has no durable start"))?;
    check_event(&started, key, &id, LedgerEventKind::EffectStarted)?;
    if started.payload != bound.started_payload() {
        return Err(failed("wait start binding mismatch"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod effect_hooks_tests;
