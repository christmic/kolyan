//! Verified admission and a shared, bounded attempt registry; no historical permit.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use kolyan_core::{TurnControl, TurnDeadline};
use kolyan_ledger::{FactJournal, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{ModelRequest, digest_json};

use super::OpeningAdmissionError as Error;
use crate::{
    ExecutionKey, ModelOpeningEventRef, ModelOpeningInspectionLimits,
    ModelOpeningInspectionRequest, ModelOpeningState, inspect_model_openings,
};

/// Lookup coordinates are untrusted; construction reads the actual strict prefix.
pub struct ModelOpeningAttemptConfig {
    pub execution: ExecutionKey,
    pub input_admission: ModelOpeningEventRef,
    pub deadline: TurnDeadline,
    pub control: TurnControl,
    pub limits: ModelOpeningInspectionLimits,
}

/// Clones share the same states and cannot mint another opening for one request.
#[derive(Clone)]
pub struct ModelOpeningAttempt {
    pub(super) inner: Arc<Inner>,
}

pub(super) struct Inner {
    pub ledger: Arc<dyn LedgerStore>,
    pub facts: Arc<dyn FactJournal>,
    pub config: ModelOpeningAttemptConfig,
    max_steps: usize,
    registry: Mutex<Registry>,
}

#[derive(Default)]
struct Registry {
    requests: HashMap<String, Entry>,
    failure: Option<Arc<Error>>,
}

struct Entry {
    requested: ModelOpeningEventRef,
    opening: Option<ModelOpeningEventRef>,
    digest: String,
    state: State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Requested,
    Preparing,
    Committing,
    Admitted,
    Consumed,
    Completed,
}

/// Recorder sources only. Private fields and no Deserialize prevent model output
/// from replacing actual acknowledged request/opening coordinates.
pub struct ModelOpeningCompletionSources {
    requested: ModelOpeningEventRef,
    opening: ModelOpeningEventRef,
}

impl ModelOpeningCompletionSources {
    pub fn model_requested(&self) -> &ModelOpeningEventRef {
        &self.requested
    }
    pub fn model_opening(&self) -> &ModelOpeningEventRef {
        &self.opening
    }
}

impl ModelOpeningAttempt {
    /// Runtime assembly supplies its actual stores and already-captured anchor.
    /// Neither this constructor nor a restored durable fact grants a GEN permit.
    pub fn from_admission(
        ledger: Arc<dyn LedgerStore>,
        facts: Arc<dyn FactJournal>,
        config: ModelOpeningAttemptConfig,
    ) -> Result<Self, Error> {
        let proof = inspect_model_openings(
            ledger.as_ref(),
            facts.as_ref(),
            &ModelOpeningInspectionRequest {
                execution: config.execution.clone(),
                through: config.input_admission.clone(),
                limits: config.limits.clone(),
            },
        )?;
        if proof.input_admission() != &config.input_admission || !proof.steps().is_empty() {
            return Err(Error::BindingMismatch(
                "not the exact input admission".into(),
            ));
        }
        let event = exact(ledger.as_ref(), &config.execution, &config.input_admission)?;
        let cutoff: Option<u64> = serde_json::from_value(event.payload["deadline_at_ms"].clone())
            .map_err(|e| Error::BindingMismatch(e.to_string()))?;
        if cutoff != config.deadline.deadline_at_ms() {
            return Err(Error::BindingMismatch(
                "anchor differs from durable input cutoff".into(),
            ));
        }
        let max_steps = event.payload["max_steps"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| Error::BindingMismatch("invalid Step bound".into()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                ledger,
                facts,
                config,
                max_steps,
                registry: Mutex::new(Registry::default()),
            }),
        })
    }

    /// Recorder integration: after append acknowledgement, publish the exact event.
    /// A supplied acknowledgement is independently read and cannot fabricate source.
    /// This is not a prepare/open operation and does not return authority to callers.
    pub fn record_requested(&self, event: &LedgerEvent) -> Result<(), Error> {
        self.check_live()?;
        let reference = ModelOpeningEventRef {
            event_id: event.event_id.clone(),
            cursor: event.cursor,
        };
        let actual = exact(
            self.inner.ledger.as_ref(),
            &self.inner.config.execution,
            &reference,
        )?;
        if actual != *event || event.kind != LedgerEventKind::ModelRequested {
            return Err(Error::BindingMismatch(
                "request acknowledgement differs".into(),
            ));
        }
        let proof = inspect_model_openings(
            self.inner.ledger.as_ref(),
            self.inner.facts.as_ref(),
            &self.inspection(reference.clone()),
        )?;
        if proof.input_admission() != &self.inner.config.input_admission || proof.has_uncertain() {
            return Err(Error::InvalidState("unknown opening cannot resume".into()));
        }
        let step = proof
            .steps()
            .last()
            .ok_or_else(|| Error::BindingMismatch("request without Step".into()))?;
        if step.requested() != Some(&reference) || step.state() != ModelOpeningState::NotAdmitted {
            return Err(Error::InvalidState("request is not unopened".into()));
        }
        let request: ModelRequest = serde_json::from_value(event.payload["request"].clone())
            .map_err(|e| Error::BindingMismatch(e.to_string()))?;
        let digest = digest_json(&request)?;
        let mut registry = self.lock()?;
        if registry.requests.contains_key(step.step_id())
            || registry.requests.len() >= self.inner.max_steps
        {
            return Err(Error::InvalidState(
                "duplicate request or Step bound exhausted".into(),
            ));
        }
        registry.requests.insert(
            step.step_id().into(),
            Entry {
                requested: reference,
                opening: None,
                digest,
                state: State::Requested,
            },
        );
        Ok(())
    }

    /// The driver must consume the typed cause when projecting physical closure.
    pub fn failure(&self) -> Result<Option<Arc<Error>>, Error> {
        Ok(self.lock()?.failure.clone())
    }
    pub fn remaining(&self) -> Option<Duration> {
        self.inner.config.deadline.remaining()
    }

    /// Called by the trusted recorder when encoding Core's actual StepCompleted.
    /// This does not itself assert that Core finished or grant another opening.
    pub fn completion_sources(
        &self,
        step_id: &str,
    ) -> Result<ModelOpeningCompletionSources, Error> {
        let registry = self.lock()?;
        let entry = registry
            .requests
            .get(step_id)
            .ok_or_else(|| Error::InvalidState("missing completion Step".into()))?;
        if entry.state != State::Consumed {
            return Err(Error::InvalidState(
                "GEN permission was not consumed".into(),
            ));
        }
        Ok(ModelOpeningCompletionSources {
            requested: entry.requested.clone(),
            opening: entry
                .opening
                .clone()
                .ok_or_else(|| Error::InvalidState("missing admitted coordinate".into()))?,
        })
    }

    /// Publish an actual completion acknowledgement only after bounded strict
    /// prefix verification, including its typed StepResult and exact references.
    pub fn record_completed(&self, event: &LedgerEvent) -> Result<(), Error> {
        let reference = ModelOpeningEventRef {
            event_id: event.event_id.clone(),
            cursor: event.cursor,
        };
        let actual = exact(
            self.inner.ledger.as_ref(),
            &self.inner.config.execution,
            &reference,
        )?;
        if actual != *event || actual.kind != LedgerEventKind::StepCompleted {
            return Err(Error::BindingMismatch(
                "completion acknowledgement differs".into(),
            ));
        }
        let proof = inspect_model_openings(
            self.inner.ledger.as_ref(),
            self.inner.facts.as_ref(),
            &self.inspection(reference.clone()),
        )?;
        let step = proof
            .steps()
            .last()
            .ok_or_else(|| Error::BindingMismatch("completion without Step".into()))?;
        let sources = self.completion_sources(step.step_id())?;
        if proof.input_admission() != &self.inner.config.input_admission
            || step.state() != ModelOpeningState::Completed
            || step.completed() != Some(&reference)
            || step.requested() != Some(sources.model_requested())
            || step.opening() != Some(sources.model_opening())
        {
            return Err(Error::BindingMismatch(
                "completion differs from consumed opening".into(),
            ));
        }
        self.transition(step.step_id(), State::Consumed, State::Completed)
    }

    pub(super) fn inspect_current(&self, through: ModelOpeningEventRef) -> Result<(), Error> {
        let proof = inspect_model_openings(
            self.inner.ledger.as_ref(),
            self.inner.facts.as_ref(),
            &self.inspection(through),
        )?;
        if proof.input_admission() != &self.inner.config.input_admission {
            return Err(Error::BindingMismatch("input source changed".into()));
        }
        Ok(())
    }
    fn inspection(&self, through: ModelOpeningEventRef) -> ModelOpeningInspectionRequest {
        ModelOpeningInspectionRequest {
            execution: self.inner.config.execution.clone(),
            through,
            limits: self.inner.config.limits.clone(),
        }
    }
    pub(super) fn begin(&self, request: &ModelRequest) -> Result<ModelOpeningEventRef, Error> {
        self.check_live()?;
        let digest = digest_json(request)?;
        let mut registry = self.lock()?;
        let entry = registry
            .requests
            .get_mut(&request.request_id)
            .ok_or_else(|| Error::BindingMismatch("no recorder-owned actual request".into()))?;
        if entry.digest != digest || entry.state != State::Requested {
            return Err(Error::InvalidState(
                "request differs or was already consumed".into(),
            ));
        }
        entry.state = State::Preparing;
        Ok(entry.requested.clone())
    }
    pub(super) fn committing(&self, step: &str) -> Result<(), Error> {
        self.transition(step, State::Preparing, State::Committing)
    }
    pub(super) fn admitted(&self, step: &str, opening: ModelOpeningEventRef) -> Result<(), Error> {
        let mut registry = self.lock()?;
        let entry = registry
            .requests
            .get_mut(step)
            .ok_or_else(|| Error::InvalidState("missing Step".into()))?;
        if entry.state != State::Committing {
            return Err(Error::InvalidState("opening already entered".into()));
        }
        entry.opening = Some(opening);
        entry.state = State::Admitted;
        Ok(())
    }
    pub(super) fn consume(&self, step: &str) -> Result<(), Error> {
        self.transition(step, State::Admitted, State::Consumed)
    }
    fn transition(&self, step: &str, before: State, after: State) -> Result<(), Error> {
        let mut registry = self.lock()?;
        let entry = registry
            .requests
            .get_mut(step)
            .ok_or_else(|| Error::InvalidState("missing Step".into()))?;
        if entry.state != before {
            return Err(Error::InvalidState("opening already entered".into()));
        }
        entry.state = after;
        Ok(())
    }
    pub(super) fn check_live(&self) -> Result<(), Error> {
        if self.inner.config.control.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if self.remaining().is_some_and(|n| n.is_zero()) {
            return Err(Error::DeadlineExceeded);
        }
        Ok(())
    }
    pub(super) fn retain(&self, error: Error) -> Arc<Error> {
        let error = Arc::new(error);
        if let Ok(mut registry) = self.inner.registry.lock()
            && registry.failure.is_none()
        {
            registry.failure = Some(error.clone());
        }
        error
    }
    fn lock(&self) -> Result<MutexGuard<'_, Registry>, Error> {
        self.inner
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("registry poisoned".into()))
    }
}

pub(super) fn exact(
    ledger: &dyn LedgerStore,
    execution: &ExecutionKey,
    reference: &ModelOpeningEventRef,
) -> Result<LedgerEvent, Error> {
    let mut rows = ledger.query(&kolyan_ledger::LedgerQuery {
        execution_id: Some(execution.execution_id.clone()),
        event_id: Some(reference.event_id.clone()),
        after: reference.cursor.saturating_sub(1),
        through: Some(reference.cursor),
        limit: 1,
    })?;
    let event = rows
        .pop()
        .ok_or_else(|| Error::BindingMismatch("missing exact source".into()))?;
    if !rows.is_empty()
        || event.event_id != reference.event_id
        || event.cursor != reference.cursor
        || event.execution_id != execution.execution_id
        || event.turn_id != execution.turn_id
    {
        return Err(Error::BindingMismatch("foreign exact source".into()));
    }
    Ok(event)
}
