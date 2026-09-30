//! Receipt-backed tool adapter. A started invocation without a durable result
//! is uncertain, never permission to repeat an external side effect.

use kolyan_core::{ToolError, ToolExecutor, ToolFuture};
use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::ToolCall;
use kolyan_policy::ExecutionGrant;
use serde_json::{Value, json};

use super::{RuntimeTurnKey, append_once};
use crate::{EffectGrant, EffectReceipt, EffectRequest, ReceiptStatus};

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
    fn execute(&self, _: ToolCall) -> ToolFuture<'_> {
        Box::pin(async { Err(failed("durable tool requires a step identity")) })
    }

    fn execute_invocation(
        &self,
        step_id: String,
        call: ToolCall,
        grant: Option<ExecutionGrant>,
    ) -> ToolFuture<'_> {
        Box::pin(async move {
            let effect = format!("{step_id}/{}", call.id);
            let prefix = format!("{}/effect/{effect}", self.key.execution_id);
            let input = json!({"name": call.name, "arguments": call.arguments});
            // Canonical JSON fingerprints are stable equality keys, not claims
            // of cryptographic authenticity. The ledger is the trust boundary.
            let input_digest = serde_json::to_string(&input).map_err(failed)?;
            let policy_revision = grant
                .as_ref()
                .map_or("unmanaged-v1", |grant| grant.policy_version.as_str())
                .to_owned();
            let request = EffectRequest {
                effect_id: effect.clone(),
                operation_kind: call.name.clone(),
                input_digest: input_digest.clone(),
                requirements: Vec::new(),
                policy_revision: policy_revision.clone(),
            };
            append_once(
                &self.ledger,
                &self.key.execution_id,
                &self.key.turn_id,
                &format!("effect/{effect}/prepared"),
                LedgerEventKind::EffectPrepared,
                json!(request),
            )
            .map_err(failed)?;
            if let Some(receipt) = self
                .ledger
                .event_by_id(&format!("{prefix}/receipt"))
                .map_err(failed)?
            {
                if receipt.payload["receipt"]["input_digest"] != input_digest {
                    return Err(failed("receipt input mismatch"));
                }
                if let Some(output) = receipt.payload.get("output") {
                    return serde_json::from_value(output.clone()).map_err(failed);
                }
                return Err(failed(
                    receipt.payload["error"]
                        .as_str()
                        .unwrap_or("invalid receipt"),
                ));
            }
            if self
                .ledger
                .event_by_id(&format!("{prefix}/started"))
                .map_err(failed)?
                .is_some()
            {
                append_once(
                    &self.ledger,
                    &self.key.execution_id,
                    &self.key.turn_id,
                    &format!("effect/{effect}/uncertain"),
                    LedgerEventKind::EffectUncertain,
                    json!({"effect_id": effect, "evidence": "started invocation has no receipt"}),
                )
                .map_err(failed)?;
                return Err(failed("uncertain tool effect: reconciliation required"));
            }
            let authorization = match &grant {
                Some(grant) => json!({"call_id": grant.call_id, "tool_name": grant.tool_name,
                    "policy_version": grant.policy_version, "constraints": grant.constraints}),
                None => Value::Null,
            };
            let authorization = EffectGrant {
                authorization_id: format!("{prefix}/authorized"),
                effect_id: effect.clone(),
                input_digest: input_digest.clone(),
                constraints_digest: authorization.to_string(),
                authority_revision: policy_revision,
            };
            append_once(
                &self.ledger,
                &self.key.execution_id,
                &self.key.turn_id,
                &format!("effect/{effect}/authorized"),
                LedgerEventKind::EffectAuthorized,
                json!(authorization),
            )
            .map_err(failed)?;
            let id = format!("{prefix}/started");
            self.ledger
                .append_unless_cancelled(LedgerEvent {
                    event_id: id.clone(),
                    turn_id: self.key.turn_id.clone(),
                    execution_id: self.key.execution_id.clone(),
                    cursor: 0,
                    kind: LedgerEventKind::EffectStarted,
                    idempotency_key: id,
                    payload: json!({"effect_id": effect}),
                })
                .map_err(|error| match error {
                    kolyan_ledger::LedgerError::Cancelled(_) => ToolError::Cancelled,
                    error => failed(error),
                })?;
            // Dropping this future after entry leaves Started without a receipt.
            // Recovery cannot assume that the external operation was rolled back.
            let result = self
                .inner
                .execute_invocation(step_id, call.clone(), grant)
                .await;
            if let Ok(output) = &result
                && output.call_id != call.id
            {
                return Err(failed("tool result identity mismatch; effect is uncertain"));
            }
            let mut payload = match &result {
                Ok(output) => {
                    json!({"effect_id": effect, "input": input, "authorization": authorization, "output": output})
                }
                Err(error) => {
                    json!({"effect_id": effect, "input": input, "authorization": authorization, "error": error.to_string()})
                }
            };
            let receipt = EffectReceipt {
                receipt_id: format!("{prefix}/receipt"),
                effect_id: effect.clone(),
                authorization_id: authorization.authorization_id.clone(),
                input_digest,
                executor_id: format!("tool/{}", call.name),
                executor_revision: env!("CARGO_PKG_VERSION").into(),
                result_digest: payload
                    .get("output")
                    .or_else(|| payload.get("error"))
                    .expect("outcome")
                    .to_string(),
                status: if result.is_ok() {
                    ReceiptStatus::Completed
                } else {
                    ReceiptStatus::Failed
                },
            };
            payload["receipt"] = json!(receipt);
            // Output and receipt are one append: no crash gap between them.
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

fn failed(error: impl std::fmt::Display) -> ToolError {
    ToolError::Failed {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests;
