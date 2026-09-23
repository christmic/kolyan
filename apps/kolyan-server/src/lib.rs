use kolyan_ledger::{LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Identity routed by Server and consumed by the Runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRef {
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    New,
    Running,
    Suspended,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionKind {
    Start,
    Resume,
    Recover,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionAdmission {
    pub execution: ExecutionRef,
    pub kind: AdmissionKind,
}

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    #[error("execution {execution_id} is already active in this server")]
    AlreadyActive { execution_id: String },
    #[error("execution {execution_id} is not suspended")]
    NotSuspended { execution_id: String },
    #[error("execution {execution_id} is not recoverable from state {state:?}")]
    NotRecoverable {
        execution_id: String,
        state: ExecutionState,
    },
    #[error("execution {execution_id} is terminal in state {state:?}")]
    Terminal {
        execution_id: String,
        state: ExecutionState,
    },
}

/// Single-process execution orchestration owned by Server.
///
/// It deliberately has no lease API. The active set prevents duplicate local
/// work while the Ledger remains authoritative across a process restart.
#[derive(Clone)]
pub struct ExecutionCoordinator<L> {
    ledger: L,
    active: Arc<Mutex<HashSet<String>>>,
}

/// Thin Server facade. Transport adapters should depend on this type instead
/// of reaching into the Coordinator directly.
#[derive(Clone)]
pub struct ExecutionServer<L> {
    coordinator: ExecutionCoordinator<L>,
}

impl<L> ExecutionServer<L>
where
    L: LedgerStore + Clone,
{
    pub fn new(ledger: L) -> Self {
        Self {
            coordinator: ExecutionCoordinator::new(ledger),
        }
    }

    pub fn coordinator(&self) -> &ExecutionCoordinator<L> {
        &self.coordinator
    }

    pub fn start(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        self.coordinator.start(execution)
    }

    pub fn resume(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        self.coordinator.resume(execution)
    }

    pub fn recover(&self, execution: ExecutionRef) -> Result<ExecutionAdmission, CoordinatorError> {
        self.coordinator.recover(execution)
    }

    pub fn cancel(&self, execution: &ExecutionRef) -> Result<(), CoordinatorError> {
        self.coordinator.cancel(execution)
    }

    pub fn state(&self, execution_id: &str) -> Result<ExecutionState, CoordinatorError> {
        self.coordinator.state(execution_id)
    }

    pub fn release(&self, execution_id: &str) {
        self.coordinator.release(execution_id);
    }
}

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
            "execution-resumed",
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
            "execution-recovered",
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

    fn append_once(
        &self,
        execution: &ExecutionRef,
        suffix: &str,
        kind: LedgerEventKind,
        payload: Value,
    ) -> Result<(), CoordinatorError> {
        let event_id = format!("{}/{}", execution.execution_id, suffix);
        if self
            .ledger
            .events_after(0)?
            .iter()
            .any(|event| event.event_id == event_id)
        {
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
}

fn is_terminal(state: ExecutionState) -> bool {
    matches!(
        state,
        ExecutionState::Completed | ExecutionState::Cancelled | ExecutionState::Failed
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use kolyan_ledger::InMemoryLedger;

    fn execution(id: &str) -> ExecutionRef {
        ExecutionRef {
            session_id: "session-1".into(),
            turn_id: format!("turn-{id}"),
            execution_id: id.into(),
        }
    }

    #[test]
    fn coordinator_prevents_duplicate_local_admission_and_releases_after_cancel() {
        let coordinator = ExecutionCoordinator::new(InMemoryLedger::default());
        let execution = execution("execution-1");
        assert_eq!(
            coordinator.state("execution-1").unwrap(),
            ExecutionState::New
        );
        assert_eq!(
            coordinator.start(execution.clone()).unwrap().kind,
            AdmissionKind::Start
        );
        assert!(matches!(
            coordinator.start(execution.clone()),
            Err(CoordinatorError::AlreadyActive { .. })
        ));
        coordinator.cancel(&execution).unwrap();
        assert_eq!(
            coordinator.state("execution-1").unwrap(),
            ExecutionState::Cancelled
        );
        coordinator.release("execution-1");
    }

    #[test]
    fn recovery_is_explicit_and_uses_ledger_state() {
        let ledger = InMemoryLedger::default();
        let first = ExecutionCoordinator::new(ledger.clone());
        let execution = execution("execution-2");
        first.start(execution.clone()).unwrap();
        first.release("execution-2");

        let restarted = ExecutionCoordinator::new(ledger);
        assert!(matches!(
            restarted.recover(execution).unwrap().kind,
            AdmissionKind::Recover
        ));
    }
}
