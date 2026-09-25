//! Single-process ownership and ledger-derived lifecycle.

use super::*;

impl<L> ExecutionCoordinator<L>
where
    L: LedgerStore + Clone,
{
    pub fn new(ledger: L) -> Self {
        Self {
            ledger,
            active: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn ledger(&self) -> &L {
        &self.ledger
    }

    pub fn state(&self, execution_id: &str) -> Result<ExecutionState, CoordinatorError> {
        let mut state = ExecutionState::New;
        for event in self
            .ledger
            .events_after(0)?
            .into_iter()
            .filter(|event| event.execution_id == execution_id)
        {
            if is_terminal(state) {
                break;
            }
            state = match event.kind {
                LedgerEventKind::ExecutionStarted => ExecutionState::Running,
                LedgerEventKind::ExecutionSuspended => ExecutionState::Suspended,
                LedgerEventKind::ExecutionCancelled | LedgerEventKind::TurnCancelled => {
                    ExecutionState::Cancelled
                }
                LedgerEventKind::TurnCompleted => ExecutionState::Completed,
                LedgerEventKind::TurnFailed | LedgerEventKind::EffectUncertain => {
                    ExecutionState::Failed
                }
                _ => state,
            };
        }
        Ok(state)
    }

    pub fn start(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        let state = self.state(&execution.execution_id)?;
        match state {
            ExecutionState::New => {
                self.append_once(
                    &execution,
                    "execution-started",
                    LedgerEventKind::ExecutionStarted,
                    serde_json::to_value(&execution).unwrap_or(Value::Null),
                )?;
                self.admit(execution, AdmissionKind::Start)
            }
            ExecutionState::Running if self.is_active(&execution.execution_id) => {
                Err(CoordinatorError::AlreadyActive {
                    execution_id: execution.execution_id,
                })
            }
            ExecutionState::Running => Err(CoordinatorError::NotRecoverable {
                execution_id: execution.execution_id,
                state,
            }),
            _ if is_terminal(state) => Err(CoordinatorError::Terminal {
                execution_id: execution.execution_id,
                state,
            }),
            _ => Err(CoordinatorError::NotRecoverable {
                execution_id: execution.execution_id,
                state,
            }),
        }
    }

    pub fn resume(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        let state = self.state(&execution.execution_id)?;
        if state != ExecutionState::Suspended {
            return if is_terminal(state) {
                Err(CoordinatorError::Terminal {
                    execution_id: execution.execution_id,
                    state,
                })
            } else {
                Err(CoordinatorError::NotSuspended {
                    execution_id: execution.execution_id,
                })
            };
        }
        self.append_once(
            &execution,
            &format!(
                "execution-resumed/{}",
                self.latest_cursor(&execution.execution_id)?
            ),
            LedgerEventKind::ExecutionStarted,
            Value::Null,
        )?;
        self.admit(execution, AdmissionKind::Resume)
    }

    pub fn recover(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        let state = self.state(&execution.execution_id)?;
        if !matches!(state, ExecutionState::Running | ExecutionState::Suspended) {
            return Err(CoordinatorError::NotRecoverable {
                execution_id: execution.execution_id,
                state,
            });
        }
        self.append_once(
            &execution,
            &format!(
                "execution-recovered/{}",
                self.latest_cursor(&execution.execution_id)?
            ),
            LedgerEventKind::ExecutionStarted,
            Value::Null,
        )?;
        self.admit(execution, AdmissionKind::Recover)
    }

    pub fn cancel(&self, execution: &ExecutionRef) -> Result<(), CoordinatorError> {
        let state = self.state(&execution.execution_id)?;
        if is_terminal(state) {
            return Ok(());
        }
        self.append_once(
            execution,
            "execution-cancelled",
            LedgerEventKind::ExecutionCancelled,
            Value::Null,
        )?;
        self.release(&execution.execution_id);
        Ok(())
    }

    pub fn release(&self, execution_id: &str) {
        self.active
            .lock()
            .expect("coordinator lock must not be poisoned")
            .remove(execution_id);
    }

    fn admit(
        &self,
        execution: ExecutionRef,
        kind: AdmissionKind,
    ) -> Result<ExecutionAdmission, CoordinatorError> {
        let mut active = self
            .active
            .lock()
            .expect("coordinator lock must not be poisoned");
        if !active.insert(execution.execution_id.clone()) {
            return Err(CoordinatorError::AlreadyActive {
                execution_id: execution.execution_id,
            });
        }
        drop(active);
        Ok(ExecutionAdmission { execution, kind })
    }

    fn is_active(&self, execution_id: &str) -> bool {
        self.active
            .lock()
            .expect("coordinator lock must not be poisoned")
            .contains(execution_id)
    }

    pub(super) fn append_once(
        &self,
        execution: &ExecutionRef,
        suffix: &str,
        kind: LedgerEventKind,
        payload: Value,
    ) -> Result<(), CoordinatorError> {
        let event_id = format!("{}/{}", execution.execution_id, suffix);
        if let Some(existing) = self
            .ledger
            .events_after(0)?
            .into_iter()
            .find(|event| event.event_id == event_id)
        {
            if existing.turn_id != execution.turn_id
                || existing.execution_id != execution.execution_id
                || existing.kind != kind
                || existing.payload != payload
            {
                return Err(LedgerError::Conflict(event_id).into());
            }
            return Ok(());
        }
        self.ledger.append(LedgerEvent {
            event_id: event_id.clone(),
            turn_id: execution.turn_id.clone(),
            execution_id: execution.execution_id.clone(),
            cursor: 0,
            kind,
            idempotency_key: event_id,
            payload,
        })?;
        Ok(())
    }

    fn latest_cursor(&self, execution_id: &str) -> Result<u64, CoordinatorError> {
        Ok(self
            .ledger
            .events_after(0)?
            .iter()
            .rev()
            .find(|event| event.execution_id == execution_id)
            .map_or(0, |event| event.cursor))
    }
}

fn is_terminal(state: ExecutionState) -> bool {
    matches!(
        state,
        ExecutionState::Completed | ExecutionState::Cancelled | ExecutionState::Failed
    )
}
