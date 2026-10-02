//! Bounded physical reads; no global audit fallback and no negative cache.

use std::io::{self, Write};

use kolyan_ledger::{FactJournal, FactRecord, FactRef, LedgerEvent, LedgerQuery, LedgerStore};
use kolyan_policy::ToolExecutionScope;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{ModelOpeningEventRef, ModelOpeningInspectionRequest, ModelOpeningProofError as Error};

pub(super) struct Budget {
    payload: usize,
    remaining: usize,
}

impl Budget {
    pub(super) fn new(request: &ModelOpeningInspectionRequest) -> Result<Self, Error> {
        ToolExecutionScope {
            execution: request.execution.clone(),
            step_id: "opening-proof".into(),
            agent_snapshot_digest: None,
        }
        .validate()
        .map_err(|error| Error::InvalidRequest(error.to_string()))?;
        if request.through.event_id.is_empty()
            || request.through.cursor == 0
            || request.limits.max_events == 0
            || request.limits.max_total_bytes == 0
            || request.limits.max_payload_bytes == 0
        {
            return Err(Error::InvalidRequest(
                "nonzero coordinates and bounded positive limits required".into(),
            ));
        }
        Ok(Self {
            payload: request.limits.max_payload_bytes,
            remaining: request.limits.max_total_bytes,
        })
    }

    pub(super) fn row(&mut self, row: &impl Serialize, payload: &Value) -> Result<(), Error> {
        measure(payload, self.payload, "single payload bytes")?;
        let used = measure(row, self.remaining, "total evidence bytes")?;
        self.remaining -= used;
        Ok(())
    }
}

pub(super) fn prefix<L: LedgerStore + ?Sized>(
    ledger: &L,
    request: &ModelOpeningInspectionRequest,
    budget: &mut Budget,
) -> Result<Vec<LedgerEvent>, Error> {
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let limit = request
            .limits
            .max_events
            .saturating_sub(events.len())
            .saturating_add(1)
            .min(512);
        let page = ledger
            .query(&LedgerQuery {
                execution_id: Some(request.execution.execution_id.clone()),
                event_id: None,
                after,
                through: Some(request.through.cursor),
                limit,
            })
            .map_err(Error::LedgerStorage)?;
        if page.len() > limit {
            return Err(Error::BindingMismatch {
                source_id: request.through.event_id.clone(),
                reason: "adapter exceeded query limit".into(),
            });
        }
        if page.is_empty() {
            break;
        }
        for event in page {
            if event.execution_id != request.execution.execution_id
                || event.turn_id != request.execution.turn_id
                || event.cursor <= after
                || event.cursor > request.through.cursor
                || event.event_id.is_empty()
                || event.idempotency_key != event.event_id
            {
                return Err(binding(
                    &event.event_id,
                    "out-of-scope, unordered or noncanonical Ledger row",
                ));
            }
            if events.len() == request.limits.max_events {
                return Err(Error::BoundsExceeded {
                    bound: "event count",
                });
            }
            budget.row(&event, &event.payload)?;
            after = event.cursor;
            events.push(event);
        }
        if after == request.through.cursor {
            break;
        }
    }
    if events.last().map(coordinate).as_ref() != Some(&request.through) {
        return Err(Error::MissingEvidence {
            source_id: request.through.event_id.clone(),
        });
    }
    let mut ids = std::collections::HashSet::new();
    if events.iter().any(|event| !ids.insert(&event.event_id)) {
        return Err(Error::OrderingMismatch(
            "duplicate Ledger event identity".into(),
        ));
    }
    Ok(events)
}

pub(super) fn fact<F: FactJournal + ?Sized>(
    facts: &F,
    reference: &FactRef,
    budget: &mut Budget,
) -> Result<FactRecord, Error> {
    if reference.position == 0
        || reference.position > i64::MAX as u64
        || [&reference.stream_id, &reference.fact_id]
            .iter()
            .any(|id| id.is_empty() || id.len() > 256)
    {
        return Err(binding(&reference.fact_id, "invalid Fact reference"));
    }
    let mut rows = facts
        .read(&reference.stream_id, reference.position - 1, 1)
        .map_err(Error::FactStorage)?;
    if rows.len() > 1 {
        return Err(binding(
            &reference.fact_id,
            "Fact adapter exceeded exact lookup limit",
        ));
    }
    let row = rows.pop().ok_or_else(|| Error::MissingEvidence {
        source_id: reference.fact_id.clone(),
    })?;
    if row.stream_id != reference.stream_id
        || row.position != reference.position
        || row.draft.fact_id != reference.fact_id
    {
        return Err(binding(
            &reference.fact_id,
            "Fact reference does not resolve exactly",
        ));
    }
    measure(&row.draft.payload, 128 * 1024, "Fact payload bytes")?;
    budget.row(&row, &row.draft.payload)?;
    Ok(row)
}

/// Round-trip equality also rejects nested unknown fields/defaulted missing fields
/// in existing Model/ExecutionKey types without adding public Deserialize APIs.
pub(super) fn decode<T: DeserializeOwned + Serialize>(
    value: &Value,
    source: &str,
) -> Result<T, Error> {
    let decoded: T =
        serde_json::from_value(value.clone()).map_err(|error| invalid(source, error))?;
    if serde_json::to_value(&decoded).map_err(|error| invalid(source, error))? != *value {
        return Err(invalid(
            source,
            "unknown fields or noncanonical/missing fields",
        ));
    }
    Ok(decoded)
}

pub(super) fn coordinate(event: &LedgerEvent) -> ModelOpeningEventRef {
    ModelOpeningEventRef {
        event_id: event.event_id.clone(),
        cursor: event.cursor,
    }
}

pub(super) fn binding(source: &str, reason: impl ToString) -> Error {
    Error::BindingMismatch {
        source_id: source.into(),
        reason: reason.to_string(),
    }
}

pub(super) fn invalid(source: &str, reason: impl ToString) -> Error {
    Error::InvalidPayload {
        source_id: source.into(),
        reason: reason.to_string(),
    }
}

pub(super) fn model_source(value: &Value) -> Result<(), Error> {
    measure(
        value,
        kolyan_model::MAX_CONTEXT_JSON_BYTES as usize,
        "neutral source bytes",
    )?;
    Ok(())
}

fn measure(value: &impl Serialize, limit: usize, bound: &'static str) -> Result<usize, Error> {
    let mut writer = LimitedWriter {
        remaining: limit,
        used: 0,
        exceeded: false,
    };
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        return Err(if writer.exceeded {
            Error::BoundsExceeded { bound }
        } else {
            invalid(bound, error)
        });
    }
    Ok(writer.used)
}

struct LimitedWriter {
    remaining: usize,
    used: usize,
    exceeded: bool,
}

impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            self.exceeded = true;
            return Err(io::Error::other("opening proof byte ceiling exceeded"));
        }
        self.remaining -= bytes.len();
        self.used += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
