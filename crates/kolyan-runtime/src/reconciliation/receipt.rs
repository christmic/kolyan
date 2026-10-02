//! One evidence format for prepared execution and historical reconciliation.
//! Hashes prove binding integrity, not authority outside the trusted journal.

use kolyan_core::{ExternalWait, IssuedToolAuthority, ToolError};
use kolyan_ledger::{LedgerEvent, LedgerEventKind};
use kolyan_model::ToolResult;
use kolyan_policy::{PreparedCall, PreparedGrant, ToolExecutionScope};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{ReconciliationRequest, ReconciliationResolution};
use crate::{EffectGrant, EffectReceipt, EffectRequest, ExecutionKey, ReceiptStatus};

pub(crate) struct PreparedEvidence {
    pub(crate) request: EffectRequest,
    pub(crate) authorization: EffectGrant,
    pub(crate) prepared: PreparedCall,
    pub(crate) scope: ToolExecutionScope,
    grant: PreparedGrant,
}

impl PreparedEvidence {
    pub(crate) fn new(
        prepared: PreparedCall,
        grant: PreparedGrant,
        scope: ToolExecutionScope,
        policy_revision: &str,
        execution: &ExecutionKey,
    ) -> Result<Self, ToolError> {
        scope.validate().map_err(failed)?;
        if scope.execution != *execution {
            return Err(failed("tool invocation execution scope mismatch"));
        }
        grant
            .validate(&prepared, policy_revision, &scope)
            .map_err(failed)?;
        let effect_id = format!("{}/{}", scope.step_id, prepared.call().id);
        let input_digest = digest(
            "kolyan.runtime.prepared-input.v1",
            &json!({"prepared_digest": prepared.digest(), "scope": scope}),
        )?;
        let authorization = EffectGrant {
            authorization_id: format!("{}/effect/{effect_id}/authorized", execution.execution_id),
            effect_id: effect_id.clone(),
            input_digest: input_digest.clone(),
            constraints_digest: digest("kolyan.runtime.prepared-grant.v1", &json!(grant))?,
            authority_revision: policy_revision.to_owned(),
        };
        let request = EffectRequest {
            effect_id,
            operation_kind: prepared.call().name.clone(),
            input_digest,
            requirements: vec![
                serde_json::to_string(&canonical(json!(prepared.requirements())))
                    .map_err(failed)?,
            ],
            policy_revision: policy_revision.to_owned(),
        };
        Ok(Self {
            request,
            authorization,
            prepared,
            scope,
            grant,
        })
    }

    /// Validate saved authority against saved preparation and revision. This
    /// does not issue authority or evaluate today's policy for a past effect.
    pub(crate) fn from_facts(
        execution: &ExecutionKey,
        effect_id: &str,
        prepared_payload: &Value,
        authorized_payload: &Value,
    ) -> Result<Self, ToolError> {
        let request: EffectRequest =
            serde_json::from_value(prepared_payload.clone()).map_err(failed)?;
        let prepared = serde_json::from_value(prepared_payload["input"]["prepared"].clone())
            .map_err(failed)?;
        let scope =
            serde_json::from_value(prepared_payload["input"]["scope"].clone()).map_err(failed)?;
        let grant =
            serde_json::from_value(authorized_payload["prepared_grant"].clone()).map_err(failed)?;
        let bound = Self::new(prepared, grant, scope, &request.policy_revision, execution)?;
        if bound.request.effect_id != effect_id
            || bound.prepared_payload()? != *prepared_payload
            || bound.authorized_payload()? != *authorized_payload
        {
            return Err(failed("prepared effect evidence binding mismatch"));
        }
        Ok(bound)
    }

    pub(crate) fn prefix(&self) -> String {
        format!(
            "{}/effect/{}",
            self.scope.execution.execution_id, self.request.effect_id
        )
    }

    pub(crate) fn executor_id(&self) -> String {
        format!("tool/{}", self.prepared.call().name)
    }

    pub(crate) fn issued(&self) -> IssuedToolAuthority {
        IssuedToolAuthority {
            prepared: self.prepared.clone(),
            grant: self.grant.clone(),
            scope: self.scope.clone(),
            policy_revision: self.request.policy_revision.clone(),
        }
    }

    pub(crate) fn wait_payload(&self, wait: &ExternalWait) -> Result<Value, ToolError> {
        crate::ExternalWaitContext {
            issued: self.issued(),
            wait: wait.clone(),
        }
        .validate(&self.scope)?;
        Ok(
            json!({"schema_version":1,"effect_id":self.request.effect_id,
            "input":self.input(),"authorization":self.authorization,
            "prepared_grant":self.grant,"wait":wait}),
        )
    }

    pub(crate) fn validate_wait(&self, payload: &Value) -> Result<ExternalWait, ToolError> {
        let wait: ExternalWait = serde_json::from_value(payload["wait"].clone()).map_err(failed)?;
        if *payload != self.wait_payload(&wait)? {
            return Err(failed("external wait historical authority differs"));
        }
        Ok(wait)
    }

    pub(crate) fn prepared_payload(&self) -> Result<Value, ToolError> {
        let mut payload = serde_json::to_value(&self.request).map_err(failed)?;
        payload["binding_kind"] = json!("prepared_tool_v1");
        payload["input"] = self.input();
        Ok(payload)
    }

    pub(crate) fn authorized_payload(&self) -> Result<Value, ToolError> {
        let mut payload = serde_json::to_value(&self.authorization).map_err(failed)?;
        payload["prepared_grant"] = json!(self.grant);
        Ok(payload)
    }

    pub(crate) fn started_payload(&self) -> Value {
        json!({"effect_id": self.request.effect_id, "input_digest": self.request.input_digest, "scope": self.scope})
    }

    pub(crate) fn receipt_payload(
        &self,
        result: &Result<ToolResult, ToolError>,
    ) -> Result<Value, ToolError> {
        let mut payload = json!({"effect_id": self.request.effect_id, "input": self.input(),
            "authorization": self.authorization, "prepared_grant": self.grant});
        let (status, outcome) = match result {
            Ok(output) => {
                self.issued().validate_result(output).map_err(failed)?;
                payload["output"] = json!(output);
                (ReceiptStatus::Completed, json!({"output": output}))
            }
            Err(error) => {
                if matches!(error, ToolError::Uncertain { .. }) {
                    return Err(failed(
                        "uncertain effect cannot produce a definitive receipt",
                    ));
                }
                let error = encode_error(error);
                payload["error"] = error.clone();
                (ReceiptStatus::Failed, json!({"error": error}))
            }
        };
        payload["receipt"] = json!(EffectReceipt {
            receipt_id: format!("{}/receipt", self.prefix()),
            effect_id: self.request.effect_id.clone(),
            authorization_id: self.authorization.authorization_id.clone(),
            input_digest: self.request.input_digest.clone(),
            executor_id: self.executor_id(),
            executor_revision: self.prepared.tool_revision().to_owned(),
            result_digest: digest("kolyan.runtime.prepared-result.v1", &outcome)?,
            status,
        });
        Ok(payload)
    }

    pub(crate) fn validate_receipt(
        &self,
        payload: &Value,
    ) -> Result<Result<ToolResult, ToolError>, ToolError> {
        let receipt: EffectReceipt =
            serde_json::from_value(payload["receipt"].clone()).map_err(failed)?;
        let result = match receipt.status {
            ReceiptStatus::Completed => {
                Ok(serde_json::from_value(payload["output"].clone()).map_err(failed)?)
            }
            ReceiptStatus::Failed => Err(decode_error(&payload["error"])?),
            ReceiptStatus::Uncertain => {
                return Err(failed("uncertain receipt requires reconciliation"));
            }
        };
        let mut expected = self.receipt_payload(&result)?;
        if let Some(metadata) = payload.get("reconciliation") {
            let request: ReconciliationRequest =
                serde_json::from_value(metadata["request"].clone()).map_err(failed)?;
            let resolution: ReconciliationResolution =
                serde_json::from_value(metadata["resolution"].clone()).map_err(failed)?;
            self.validate_resolution(&request, &resolution)?;
            let ReconciliationResolution::Committed { output, .. } = &resolution else {
                return Err(failed("receipt reconciliation is not committed"));
            };
            if result.as_ref().ok() != Some(output) {
                return Err(failed("reconciliation and receipt results differ"));
            }
            expected["reconciliation"] = json!({"request": request, "resolution": resolution});
        }
        if expected != *payload {
            return Err(failed(
                "receipt preparation, scope, authority or result binding mismatch",
            ));
        }
        Ok(result)
    }

    pub(crate) fn validate_resolution(
        &self,
        request: &ReconciliationRequest,
        resolution: &ReconciliationResolution,
    ) -> Result<(), ToolError> {
        if request.execution != self.scope.execution
            || request.effect_id != self.request.effect_id
            || request.reconciliation_id.is_empty()
            || request.reconciliation_id.len() > 256
            || !request
                .reconciliation_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
        {
            return Err(failed("reconciliation request binding differs"));
        }
        let evidence = match resolution {
            ReconciliationResolution::Committed {
                output,
                executor_id,
                executor_revision,
                evidence,
            } => {
                if output.call_id != self.prepared.call().id
                    || *executor_id != self.executor_id()
                    || executor_revision != self.prepared.tool_revision()
                {
                    return Err(failed("reconciled result or adapter revision differs"));
                }
                evidence
            }
            ReconciliationResolution::NotCommitted { evidence }
            | ReconciliationResolution::Unknown { evidence } => evidence,
        };
        if evidence.trim().is_empty() || evidence.len() > 16_384 {
            return Err(failed("reconciliation requires bounded executor evidence"));
        }
        Ok(())
    }

    fn input(&self) -> Value {
        json!({"prepared": self.prepared, "scope": self.scope})
    }
}

pub(crate) fn check_event(
    event: &LedgerEvent,
    key: &ExecutionKey,
    id: &str,
    kind: LedgerEventKind,
) -> Result<(), ToolError> {
    if event.event_id != id
        || event.execution_id != key.execution_id
        || event.turn_id != key.turn_id
        || event.kind != kind
        || event.idempotency_key != id
    {
        return Err(failed("foreign or malformed effect evidence"));
    }
    Ok(())
}

// Preserve error identity on replay; cancellation is not an ordinary failure.
fn encode_error(error: &ToolError) -> Value {
    match error {
        ToolError::InvalidBatch { message } => json!({"kind": "invalid_batch", "message": message}),
        ToolError::Unavailable { name } => json!({"kind": "unavailable", "name": name}),
        ToolError::Failed { message } => json!({"kind": "failed", "message": message}),
        ToolError::Cancelled => json!({"kind": "cancelled"}),
        ToolError::TimedOut => json!({"kind": "timed_out"}),
        ToolError::PolicyDenied { message } => json!({"kind": "policy_denied", "message": message}),
        ToolError::Uncertain { message } => json!({"kind": "uncertain", "message": message}),
    }
}

fn decode_error(value: &Value) -> Result<ToolError, ToolError> {
    let text = |key| {
        value
            .get(key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| failed("invalid recorded tool error"))
    };
    let error = match value["kind"].as_str() {
        Some("invalid_batch") => ToolError::InvalidBatch {
            message: text("message")?,
        },
        Some("unavailable") => ToolError::Unavailable {
            name: text("name")?,
        },
        Some("failed") => ToolError::Failed {
            message: text("message")?,
        },
        Some("cancelled") => ToolError::Cancelled,
        Some("timed_out") => ToolError::TimedOut,
        Some("policy_denied") => ToolError::PolicyDenied {
            message: text("message")?,
        },
        _ => return Err(failed("unknown recorded tool error")),
    };
    if encode_error(&error) != *value {
        return Err(failed("unexpected recorded tool error fields"));
    }
    Ok(error)
}

fn digest(domain: &str, value: &Value) -> Result<String, ToolError> {
    let bytes = serde_json::to_vec(&canonical(json!([domain, value]))).map_err(failed)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn canonical(value: Value) -> Value {
    match value {
        Value::Object(entries) => Value::Object(
            entries
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .map(|(key, value)| (key, canonical(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        value => value,
    }
}

pub(crate) fn failed(error: impl std::fmt::Display) -> ToolError {
    ToolError::Failed {
        message: error.to_string(),
    }
}
