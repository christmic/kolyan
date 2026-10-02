//! Charge all actual reads, including Runtime's exact effect helper lookups.

use std::sync::Mutex;

use kolyan_ledger::{LedgerError, LedgerEvent, LedgerQuery, LedgerStore};

use super::{GoalSourceError, GoalSourceLimits};
use crate::tasks::goals::types::bounded_serialized_bytes;

pub(super) struct ReadBudget<'a, L> {
    ledger: &'a L,
    limits: &'a GoalSourceLimits,
    state: Mutex<State>,
}
struct State {
    rows: usize,
    bytes: usize,
    exhausted: bool,
}

impl<'a, L: LedgerStore> ReadBudget<'a, L> {
    pub(super) fn new(ledger: &'a L, limits: &'a GoalSourceLimits) -> Self {
        Self {
            ledger,
            limits,
            state: Mutex::new(State {
                rows: 0,
                bytes: 0,
                exhausted: false,
            }),
        }
    }
    pub(super) fn remaining_rows(&self) -> usize {
        self.limits
            .max_rows
            .saturating_sub(self.state.lock().expect("budget lock").rows)
    }
    pub(super) fn exhausted(&self) -> bool {
        self.state.lock().expect("budget lock").exhausted
    }
    pub(super) fn page(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, GoalSourceError> {
        let page = self.query(query);
        match page {
            Ok(page) => Ok(page),
            Err(_) if self.exhausted() => Err(GoalSourceError::BoundsExhausted),
            Err(error) => Err(GoalSourceError::Storage(error)),
        }
    }
    pub(super) fn exact(&self, id: &str) -> Result<LedgerEvent, GoalSourceError> {
        self.page(&LedgerQuery {
            execution_id: None,
            event_id: Some(id.into()),
            after: 0,
            through: None,
            limit: 1,
        })?
        .into_iter()
        .next()
        .ok_or_else(|| GoalSourceError::Invalid("missing exact source identity".into()))
    }
}

impl<L: LedgerStore> LedgerStore for ReadBudget<'_, L> {
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        Err(read_only())
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        Err(read_only())
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        Err(read_only())
    }
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        let remaining = self.remaining_rows();
        if remaining == 0 || query.limit > remaining {
            self.state.lock().expect("budget lock").exhausted = true;
            return Err(read_only());
        }
        query.validate()?;
        let page = self.ledger.query(query)?;
        if page.len() > query.limit {
            return Err(LedgerError::Storage(
                "oversized goal source query page".into(),
            ));
        }
        let mut previous = query.after;
        for event in &page {
            if event.cursor <= previous
                || query.through.is_some_and(|through| event.cursor > through)
                || query
                    .execution_id
                    .as_ref()
                    .is_some_and(|id| id != &event.execution_id)
                || query
                    .event_id
                    .as_ref()
                    .is_some_and(|id| id != &event.event_id)
            {
                return Err(LedgerError::Storage(
                    "foreign or nonascending goal source page".into(),
                ));
            }
            previous = event.cursor;
        }
        let mut state = self.state.lock().expect("budget lock");
        for event in &page {
            let size = match bounded_serialized_bytes(event, self.limits.max_event_bytes) {
                Ok(size) => size,
                Err(_) => {
                    state.exhausted = true;
                    return Err(read_only());
                }
            };
            state.rows += 1;
            state.bytes = state.bytes.checked_add(size).ok_or_else(read_only)?;
            if state.rows > self.limits.max_rows || state.bytes > self.limits.max_total_bytes {
                state.exhausted = true;
                return Err(read_only());
            }
        }
        Ok(page)
    }
}
fn read_only() -> LedgerError {
    LedgerError::Storage("read-only source budget exhausted or mutation attempted".into())
}
