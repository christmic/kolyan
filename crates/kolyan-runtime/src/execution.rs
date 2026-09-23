use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::marker::PhantomData;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionKey {
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
}

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
        if let Some(existing) = self.terminal_effect(key, effect)? {
            return Ok(existing);
        }
        self.append_once(
            key,
            &format!("effect/{}/prepared", effect.effect_id),
            LedgerEventKind::EffectPrepared,
            json!(effect),
        )?;
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
                    "execution-suspended",
                    LedgerEventKind::ExecutionSuspended,
                    json!({"effect_id": effect.effect_id}),
                )?;
                Ok(EffectDisposition::Suspended { interaction_id })
            }
            AdmissionDecision::Grant(grant) => self.dispatch(key, effect, grant),
        }
    }

    pub fn status(
        &self,
        key: &ExecutionKey,
    ) -> Result<Option<ExecutionStatus>, RuntimeExecutionError> {
        let events = self.ledger.events_after(0)?;
        let mut status = None;
        for event in events
            .into_iter()
            .filter(|event| event.execution_id == key.execution_id)
        {
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
        self.append_once(
            key,
            &format!("effect/{}/started", effect.effect_id),
            LedgerEventKind::EffectStarted,
            json!({"effect_id": effect.effect_id}),
        )?;
        match self.executor.execute(key, effect, &grant)? {
            EffectOutcome::Completed { receipt, output } => {
                validate_receipt(effect, &grant, &receipt)?;
                self.append_receipt(key, effect, &receipt)?;
                self.append_once(
                    key,
                    &format!("effect/{}/completed", effect.effect_id),
                    LedgerEventKind::EffectCompleted,
                    json!({"effect_id": effect.effect_id, "output": output}),
                )?;
                Ok(EffectDisposition::Completed { output })
            }
            EffectOutcome::Failed { receipt, code } => {
                validate_receipt(effect, &grant, &receipt)?;
                self.append_receipt(key, effect, &receipt)?;
                self.append_once(
                    key,
                    &format!("effect/{}/failed", effect.effect_id),
                    LedgerEventKind::EffectFailed,
                    json!({"effect_id": effect.effect_id, "code": code}),
                )?;
                Ok(EffectDisposition::Failed { code })
            }
            EffectOutcome::Uncertain { receipt, evidence } => {
                validate_receipt(effect, &grant, &receipt)?;
                self.append_receipt(key, effect, &receipt)?;
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
        receipt: &EffectReceipt,
    ) -> Result<(), RuntimeExecutionError> {
        self.append_once(
            key,
            &format!("effect/{}/receipt", effect.effect_id),
            LedgerEventKind::EffectReceipt,
            json!(receipt),
        )
        .map(|_| ())
    }

    fn terminal_effect(
        &self,
        key: &ExecutionKey,
        effect: &EffectRequest,
    ) -> Result<Option<EffectDisposition>, RuntimeExecutionError> {
        for event in self
            .ledger
            .events_after(0)?
            .into_iter()
            .filter(|event| event.execution_id == key.execution_id)
        {
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
        if let Some(existing) = self
            .ledger
            .events_after(0)?
            .into_iter()
            .find(|event| event.event_id == event_id)
        {
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
