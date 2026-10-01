use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::marker::PhantomData;
use thiserror::Error;

use crate::reconciliation::receipt::{PreparedEvidence, check_event};

pub use kolyan_types::ExecutionKey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRequest {
    pub effect_id: String,
    pub operation_kind: String,
    pub input_digest: String,
    pub requirements: Vec<String>,
    pub policy_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectGrant {
    pub authorization_id: String,
    pub effect_id: String,
    pub input_digest: String,
    pub constraints_digest: String,
    pub authority_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectReceipt {
    pub receipt_id: String,
    pub effect_id: String,
    pub authorization_id: String,
    pub input_digest: String,
    pub executor_id: String,
    pub executor_revision: String,
    pub result_digest: String,
    pub status: ReceiptStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptStatus {
    Completed,
    Failed,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    Grant(EffectGrant),
    AwaitingDecision { interaction_id: String },
    Deny { code: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EffectOutcome {
    Completed {
        receipt: EffectReceipt,
        output: Value,
    },
    Failed {
        receipt: EffectReceipt,
        code: String,
    },
    Uncertain {
        receipt: EffectReceipt,
        evidence: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EffectDisposition {
    Completed { output: Value },
    Failed { code: String },
    Uncertain { evidence: String },
    Suspended { interaction_id: String },
    Denied { code: String },
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStatus {
    Running,
    Suspended,
    Completed,
    Cancelled,
    Failed,
    Uncertain,
}

#[derive(Debug, Error)]
pub enum RuntimeExecutionError {
    #[error("invalid runtime value: {0}")]
    Invalid(String),
    #[error("ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("admission failed: {0}")]
    Admission(String),
    #[error("effect executor failed: {0}")]
    Executor(String),
}

pub trait AdmissionPort: Send + Sync {
    fn decide(
        &self,
        execution: &ExecutionKey,
        effect: &EffectRequest,
    ) -> Result<AdmissionDecision, RuntimeExecutionError>;
}

pub trait EffectExecutor: Send + Sync {
    fn execute(
        &self,
        execution: &ExecutionKey,
        effect: &EffectRequest,
        grant: &EffectGrant,
    ) -> Result<EffectOutcome, RuntimeExecutionError>;
}

pub struct ExecutionRuntime<L, A, E> {
    ledger: L,
    admission: A,
    executor: E,
    _marker: PhantomData<fn() -> ()>,
}

impl<L, A, E> ExecutionRuntime<L, A, E>
where
    L: LedgerStore,
    A: AdmissionPort,
    E: EffectExecutor,
{
    pub fn new(ledger: L, admission: A, executor: E) -> Self {
        Self {
            ledger,
            admission,
            executor,
            _marker: PhantomData,
        }
    }

    pub fn ledger(&self) -> &L {
        &self.ledger
    }

    pub fn start(&self, key: &ExecutionKey) -> Result<(), RuntimeExecutionError> {
        self.append_once(
            key,
            "execution-started",
            LedgerEventKind::ExecutionStarted,
            json!(key),
        )
        .map(|_| ())
    }

    pub fn cancel(&self, key: &ExecutionKey) -> Result<(), RuntimeExecutionError> {
        self.append_once(
            key,
            "execution-cancelled",
            LedgerEventKind::ExecutionCancelled,
            Value::Null,
        )
        .map(|_| ())
    }

    pub fn apply_effect(
        &self,
        key: &ExecutionKey,
        effect: &EffectRequest,
    ) -> Result<EffectDisposition, RuntimeExecutionError> {
        validate_effect(effect)?;
        if self.status(key)? == Some(ExecutionStatus::Cancelled) {
            return Ok(EffectDisposition::Cancelled);
        }
        let prepared_id = format!("{}/effect/{}/prepared", key.execution_id, effect.effect_id);
        if let Some(saved) = self.ledger.event_by_id(&prepared_id)? {
            check_event(&saved, key, &prepared_id, LedgerEventKind::EffectPrepared)
                .map_err(|error| RuntimeExecutionError::Invalid(error.to_string()))?;
            match saved.payload["binding_kind"].as_str() {
                Some("prepared_tool_v1") => return self.recover_prepared_tool(key, effect, &saved),
                Some("generic_effect_v1") => {}
                _ => {
                    return Err(RuntimeExecutionError::Invalid(
                        "missing or unknown effect binding kind".into(),
                    ));
                }
            }
        } else {
            for suffix in ["authorized", "started", "receipt"] {
                if self
                    .ledger
                    .event_by_id(&format!(
                        "{}/effect/{}/{suffix}",
                        key.execution_id, effect.effect_id,
                    ))?
                    .is_some()
                {
                    return Err(RuntimeExecutionError::Invalid(
                        "effect evidence has no prepared binding".into(),
                    ));
                }
            }
        }
        let mut prepared_payload = json!(effect);
        prepared_payload["binding_kind"] = json!("generic_effect_v1");
        self.append_once(
            key,
            &format!("effect/{}/prepared", effect.effect_id),
            LedgerEventKind::EffectPrepared,
            prepared_payload,
        )?;
        if let Some(existing) = self.terminal_effect(key, effect)? {
            return Ok(existing);
        }
        if self
            .ledger
            .event_by_id(&format!(
                "{}/effect/{}/started",
                key.execution_id, effect.effect_id
            ))?
            .is_some()
        {
            return Ok(EffectDisposition::Uncertain {
                evidence: "effect started without a terminal result; reconciliation required"
                    .into(),
            });
        }
        match self.admission.decide(key, effect)? {
            AdmissionDecision::Deny { code } => {
                self.append_once(
                    key,
                    &format!("effect/{}/denied", effect.effect_id),
                    LedgerEventKind::EffectDenied,
                    json!({"effect_id": effect.effect_id, "code": code}),
                )?;
                Ok(EffectDisposition::Denied { code })
            }
            AdmissionDecision::AwaitingDecision { interaction_id } => {
                self.append_once(
                    key,
                    &format!("effect/{}/awaiting", effect.effect_id),
                    LedgerEventKind::EffectAwaitingDecision,
                    json!({"effect_id": effect.effect_id, "interaction_id": interaction_id}),
                )?;
                self.append_once(
                    key,
                    &format!("execution-suspended/{}", effect.effect_id),
                    LedgerEventKind::ExecutionSuspended,
                    json!({"effect_id": effect.effect_id}),
                )?;
                Ok(EffectDisposition::Suspended { interaction_id })
            }
            AdmissionDecision::Grant(grant) => self.dispatch(key, effect, grant),
        }
    }

    // A prepared tool's evidence belongs to the tool pipeline. Generic admission
    // may observe its exact historical outcome, never replace or re-authorize it.
    fn recover_prepared_tool(
        &self,
        key: &ExecutionKey,
        effect: &EffectRequest,
        saved: &LedgerEvent,
    ) -> Result<EffectDisposition, RuntimeExecutionError> {
        let invalid =
            |error: kolyan_core::ToolError| RuntimeExecutionError::Invalid(error.to_string());
        let identity_id = format!("{}/execution-started", key.execution_id);
        let identity = self
            .ledger
            .event_by_id(&identity_id)?
            .ok_or_else(|| RuntimeExecutionError::Invalid("missing execution identity".into()))?;
        check_event(
            &identity,
            key,
            &identity_id,
            LedgerEventKind::ExecutionStarted,
        )
        .map_err(invalid)?;
        if identity.payload != json!(key) {
            return Err(RuntimeExecutionError::Invalid(
                "execution binding differs".into(),
            ));
        }
        let prefix = format!("{}/effect/{}", key.execution_id, effect.effect_id);
        let authorization_id = format!("{prefix}/authorized");
        let authorized = self.ledger.event_by_id(&authorization_id)?.ok_or_else(|| {
            RuntimeExecutionError::Invalid("prepared tool has no scoped authorization".into())
        })?;
        check_event(
            &authorized,
            key,
            &authorization_id,
            LedgerEventKind::EffectAuthorized,
        )
        .map_err(invalid)?;
        let bound = PreparedEvidence::from_facts(
            key,
            &effect.effect_id,
            &saved.payload,
            &authorized.payload,
        )
        .map_err(invalid)?;
        if bound.request != *effect {
            return Err(RuntimeExecutionError::Invalid(
                "prepared tool request differs".into(),
            ));
        }
        let started_id = format!("{prefix}/started");
        let started = self.ledger.event_by_id(&started_id)?;
        if let Some(event) = &started {
            check_event(event, key, &started_id, LedgerEventKind::EffectStarted)
                .map_err(invalid)?;
            if event.payload != bound.started_payload() {
                return Err(RuntimeExecutionError::Invalid(
                    "started tool binding differs".into(),
                ));
            }
        }
        let receipt_id = format!("{prefix}/receipt");
        if let Some(receipt) = self.ledger.event_by_id(&receipt_id)? {
            check_event(&receipt, key, &receipt_id, LedgerEventKind::EffectReceipt)
                .map_err(invalid)?;
            if started.is_none() {
                return Err(RuntimeExecutionError::Invalid(
                    "tool receipt has no start evidence".into(),
                ));
            }
            return Ok(
                match bound.validate_receipt(&receipt.payload).map_err(invalid)? {
                    Ok(output) => EffectDisposition::Completed {
                        output: json!(output),
                    },
                    Err(error) => EffectDisposition::Failed {
                        code: error.to_string(),
                    },
                },
            );
        }
        Ok(EffectDisposition::Uncertain {
            evidence: "prepared tool has no committed receipt; trusted reconciliation required"
                .into(),
        })
    }

    pub fn status(
        &self,
        key: &ExecutionKey,
    ) -> Result<Option<ExecutionStatus>, RuntimeExecutionError> {
        let events = self.ledger.execution_events_after(&key.execution_id, 0)?;
        let mut status = None;
        for event in events {
            if status == Some(ExecutionStatus::Cancelled) {
                break;
            }
            status = match event.kind {
                LedgerEventKind::ExecutionCancelled => Some(ExecutionStatus::Cancelled),
                LedgerEventKind::ExecutionSuspended => Some(ExecutionStatus::Suspended),
                LedgerEventKind::EffectUncertain => Some(ExecutionStatus::Uncertain),
                LedgerEventKind::EffectFailed => Some(ExecutionStatus::Failed),
                LedgerEventKind::EffectCompleted => Some(ExecutionStatus::Completed),
                LedgerEventKind::ExecutionStarted => Some(ExecutionStatus::Running),
                _ => status,
            };
        }
        Ok(status)
    }

    fn dispatch(
        &self,
        key: &ExecutionKey,
        effect: &EffectRequest,
        grant: EffectGrant,
    ) -> Result<EffectDisposition, RuntimeExecutionError> {
        if grant.effect_id != effect.effect_id || grant.input_digest != effect.input_digest {
            return Err(RuntimeExecutionError::Invalid(
                "grant does not bind effect".into(),
            ));
        }
        self.append_once(
            key,
            &format!("effect/{}/authorized", effect.effect_id),
            LedgerEventKind::EffectAuthorized,
            json!(grant),
        )?;
        // Only the caller that appends the start record may dispatch the effect.
        let event_id = format!("{}/effect/{}/started", key.execution_id, effect.effect_id);
        match self.ledger.append_unless_cancelled(LedgerEvent {
            event_id: event_id.clone(),
            turn_id: key.turn_id.clone(),
            execution_id: key.execution_id.clone(),
            cursor: 0,
            kind: LedgerEventKind::EffectStarted,
            idempotency_key: event_id,
            payload: json!({"effect_id": effect.effect_id}),
        }) {
            Ok(_) => {}
            Err(LedgerError::Cancelled(_)) => return Ok(EffectDisposition::Cancelled),
            Err(LedgerError::Conflict(_)) => {
                return Ok(self.terminal_effect(key, effect)?.unwrap_or(
                    EffectDisposition::Uncertain {
                        evidence: "another dispatcher owns the started effect".into(),
                    },
                ));
            }
            Err(error) => return Err(error.into()),
        }
        match self.executor.execute(key, effect, &grant)? {
            EffectOutcome::Completed { receipt, output } => {
                validate_receipt_status(&receipt, ReceiptStatus::Completed)?;
                validate_receipt(effect, &grant, &receipt)?;
                self.append_receipt(key, effect, json!({"receipt":receipt,"output":output}))?;
                self.append_once(
                    key,
                    &format!("effect/{}/completed", effect.effect_id),
                    LedgerEventKind::EffectCompleted,
                    json!({"effect_id": effect.effect_id, "output": output}),
                )?;
                Ok(EffectDisposition::Completed { output })
            }
            EffectOutcome::Failed { receipt, code } => {
                validate_receipt_status(&receipt, ReceiptStatus::Failed)?;
                validate_receipt(effect, &grant, &receipt)?;
                self.append_receipt(key, effect, json!({"receipt":receipt,"code":code}))?;
                self.append_once(
                    key,
                    &format!("effect/{}/failed", effect.effect_id),
                    LedgerEventKind::EffectFailed,
                    json!({"effect_id": effect.effect_id, "code": code}),
                )?;
                Ok(EffectDisposition::Failed { code })
            }
            EffectOutcome::Uncertain { receipt, evidence } => {
                validate_receipt_status(&receipt, ReceiptStatus::Uncertain)?;
                validate_receipt(effect, &grant, &receipt)?;
                self.append_receipt(key, effect, json!({"receipt":receipt,"evidence":evidence}))?;
                self.append_once(
                    key,
                    &format!("effect/{}/uncertain", effect.effect_id),
                    LedgerEventKind::EffectUncertain,
                    json!({"effect_id": effect.effect_id, "evidence": evidence}),
                )?;
                Ok(EffectDisposition::Uncertain { evidence })
            }
        }
    }

    fn append_receipt(
        &self,
        key: &ExecutionKey,
        effect: &EffectRequest,
        payload: Value,
    ) -> Result<(), RuntimeExecutionError> {
        self.append_once(
            key,
            &format!("effect/{}/receipt", effect.effect_id),
            LedgerEventKind::EffectReceipt,
            payload,
        )
        .map(|_| ())
    }

    fn terminal_effect(
        &self,
        key: &ExecutionKey,
        effect: &EffectRequest,
    ) -> Result<Option<EffectDisposition>, RuntimeExecutionError> {
        for event in self.ledger.execution_events_after(&key.execution_id, 0)? {
            if event.event_id == format!("{}/effect/{}/receipt", key.execution_id, effect.effect_id)
            {
                let receipt: EffectReceipt =
                    serde_json::from_value(event.payload["receipt"].clone())
                        .map_err(|error| RuntimeExecutionError::Invalid(error.to_string()))?;
                if receipt.input_digest != effect.input_digest
                    || receipt.effect_id != effect.effect_id
                {
                    return Err(RuntimeExecutionError::Invalid(
                        "receipt identity mismatch".into(),
                    ));
                }
                return Ok(Some(match receipt.status {
                    ReceiptStatus::Completed => EffectDisposition::Completed {
                        output: event.payload.get("output").cloned().ok_or_else(|| {
                            RuntimeExecutionError::Invalid("receipt has no output".into())
                        })?,
                    },
                    ReceiptStatus::Failed => EffectDisposition::Failed {
                        code: event
                            .payload
                            .get("code")
                            .or_else(|| event.payload.get("error"))
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                RuntimeExecutionError::Invalid("receipt has no failure".into())
                            })?
                            .into(),
                    },
                    ReceiptStatus::Uncertain => EffectDisposition::Uncertain {
                        evidence: event.payload["evidence"]
                            .as_str()
                            .ok_or_else(|| {
                                RuntimeExecutionError::Invalid("receipt has no evidence".into())
                            })?
                            .into(),
                    },
                }));
            }
            if event.event_id
                == format!("{}/effect/{}/completed", key.execution_id, effect.effect_id)
            {
                return Ok(Some(EffectDisposition::Completed {
                    output: event.payload.get("output").cloned().unwrap_or(Value::Null),
                }));
            }
            if event.event_id == format!("{}/effect/{}/failed", key.execution_id, effect.effect_id)
            {
                return Ok(Some(EffectDisposition::Failed {
                    code: event.payload["code"]
                        .as_str()
                        .unwrap_or("failed")
                        .to_owned(),
                }));
            }
            if event.event_id
                == format!("{}/effect/{}/uncertain", key.execution_id, effect.effect_id)
            {
                return Ok(Some(EffectDisposition::Uncertain {
                    evidence: event.payload["evidence"]
                        .as_str()
                        .unwrap_or("uncertain")
                        .to_owned(),
                }));
            }
        }
        Ok(None)
    }

    fn append_once(
        &self,
        key: &ExecutionKey,
        suffix: &str,
        kind: LedgerEventKind,
        payload: Value,
    ) -> Result<LedgerEvent, RuntimeExecutionError> {
        let event_id = format!("{}/{}", key.execution_id, suffix);
        if let Some(existing) = self.ledger.event_by_id(&event_id)? {
            if existing.turn_id != key.turn_id
                || existing.execution_id != key.execution_id
                || existing.kind != kind
                || existing.payload != payload
            {
                return Err(RuntimeExecutionError::Invalid(format!(
                    "conflicting event identity: {event_id}"
                )));
            }
            return Ok(existing);
        }
        Ok(self.ledger.append(LedgerEvent {
            event_id: event_id.clone(),
            turn_id: key.turn_id.clone(),
            execution_id: key.execution_id.clone(),
            cursor: 0,
            kind,
            idempotency_key: event_id,
            payload,
        })?)
    }
}

fn validate_effect(effect: &EffectRequest) -> Result<(), RuntimeExecutionError> {
    if effect.effect_id.is_empty()
        || effect.operation_kind.is_empty()
        || effect.input_digest.is_empty()
        || effect.policy_revision.is_empty()
    {
        return Err(RuntimeExecutionError::Invalid(
            "effect identity and digests are required".into(),
        ));
    }
    Ok(())
}

fn validate_receipt(
    effect: &EffectRequest,
    grant: &EffectGrant,
    receipt: &EffectReceipt,
) -> Result<(), RuntimeExecutionError> {
    if receipt.effect_id != effect.effect_id
        || receipt.authorization_id != grant.authorization_id
        || receipt.input_digest != effect.input_digest
        || receipt.executor_id.is_empty()
        || receipt.executor_revision.is_empty()
        || receipt.result_digest.is_empty()
    {
        return Err(RuntimeExecutionError::Invalid(
            "receipt does not bind effect and grant".into(),
        ));
    }
    Ok(())
}

fn validate_receipt_status(
    receipt: &EffectReceipt,
    expected: ReceiptStatus,
) -> Result<(), RuntimeExecutionError> {
    if receipt.status != expected {
        return Err(RuntimeExecutionError::Invalid(
            "receipt status disagrees with effect outcome".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
