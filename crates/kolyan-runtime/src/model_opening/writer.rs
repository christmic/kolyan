//! Exact critical preparation readback and cancellation-atomic opening publication.

use std::io::{self, Write};

use kolyan_ledger::{FactDraft, FactRef, FactSubject, LedgerEvent, LedgerEventKind, LedgerQuery};
use kolyan_model::{ContextProtocol, ModelRequest, PreparedContextWire};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{ModelOpeningAttempt, OpeningAdmissionError as Error};
use crate::{
    ModelContextPrepared, ModelOpeningAccounting, ModelOpeningAdmitted, ModelOpeningEventRef,
    ModelOpeningMappingIdentity, ModelOpeningProtocol,
};

/// Private, neither Clone nor Deserialize; historical events never construct it.
pub(super) struct Permit {
    step_id: String,
}

impl ModelOpeningAttempt {
    pub(super) fn prepare_and_admit(
        &self,
        request: &ModelRequest,
        requested: ModelOpeningEventRef,
        wire: &PreparedContextWire,
        accounting: ModelOpeningAccounting,
    ) -> Result<Permit, Error> {
        self.check_live()?;
        self.inspect_current(requested.clone())?;
        let identity = wire.identity();
        bounded(wire.coverage().unsupported_fields(), 128 * 1024)?;
        let prepared = ModelContextPrepared {
            opening_protocol: 1,
            execution: self.inner.config.execution.clone(),
            step_id: request.request_id.clone(),
            model_requested: requested.clone(),
            neutral_digest: wire.neutral_digest().into(),
            mapping_identity: ModelOpeningMappingIdentity {
                endpoint_id: identity.endpoint_id().into(),
                protocol: match identity.protocol() {
                    ContextProtocol::OpenAiResponses => ModelOpeningProtocol::OpenAiResponses,
                    ContextProtocol::AnthropicMessages => ModelOpeningProtocol::AnthropicMessages,
                },
                model: identity.model().clone(),
                mapping_revision: identity.mapping_revision().into(),
                coverage_revision: identity.coverage_revision().into(),
            },
            count_profile_digest: wire.count_profile_digest()?,
            generation_wire_digest: wire.generation_wire_digest().into(),
            generation_wire_bytes: wire.generation_wire_bytes(),
            count_input_digest: wire.count_input_digest().into(),
            unsupported_count_fields: wire.coverage().unsupported_fields().to_vec(),
            accounting,
        };
        let bytes = bounded(&prepared, 128 * 1024)?;
        let payload =
            serde_json::from_slice(&bytes).map_err(|e| Error::BindingMismatch(e.to_string()))?;
        let coordinate = bounded(
            &(&self.inner.config.execution, &request.request_id),
            128 * 1024,
        )?;
        let digest = format!("{:x}", Sha256::digest(coordinate));
        let stream = format!("model-preparation/{digest}");
        let draft = FactDraft {
            fact_id: format!("model-prepared/{digest}"),
            subject: FactSubject {
                kind: "runtime.execution".into(),
                id: self.inner.config.execution.execution_id.clone(),
            },
            kind: "model.context_prepared".into(),
            schema_version: 1,
            critical: true,
            causes: Vec::new(),
            payload,
        };
        self.check_live()?;
        let rows = self.inner.facts.append(&stream, 0, vec![draft.clone()])?;
        let reread = self.inner.facts.read(&stream, 0, 1)?;
        if rows.len() != 1 || reread.len() != 1 {
            return Err(Error::BindingMismatch(
                "preparation row count differs".into(),
            ));
        }
        for record in rows.iter().chain(&reread) {
            bounded(&record.draft.payload, 128 * 1024)?;
            bounded(record, self.inner.config.limits.max_payload_bytes)?;
        }
        if rows.len() != 1
            || reread.len() != 1
            || rows != reread
            || rows[0].stream_id != stream
            || rows[0].position != 1
            || rows[0].draft != draft
        {
            return Err(Error::BindingMismatch(
                "preparation acknowledgement/readback differs".into(),
            ));
        }
        let preparation = FactRef {
            stream_id: stream,
            position: 1,
            fact_id: draft.fact_id,
        };
        self.committing(&request.request_id)?;
        self.check_live()?;
        let opening = ModelOpeningAdmitted {
            schema_version: 1,
            opening_protocol: 1,
            execution: prepared.execution,
            step_id: prepared.step_id,
            model_requested: requested,
            preparation,
            neutral_digest: prepared.neutral_digest,
            generation_wire_digest: prepared.generation_wire_digest,
            generation_wire_bytes: prepared.generation_wire_bytes,
            count_profile_digest: prepared.count_profile_digest,
            deadline_at_ms: self.inner.config.deadline.deadline_at_ms(),
        };
        let payload = serde_json::from_slice(&bounded(&opening, 128 * 1024)?)
            .map_err(|e| Error::BindingMismatch(e.to_string()))?;
        let id = format!(
            "{}/model-opening/{}",
            self.inner.config.execution.execution_id, request.request_id
        );
        let event = LedgerEvent {
            event_id: id.clone(),
            idempotency_key: id,
            execution_id: self.inner.config.execution.execution_id.clone(),
            turn_id: self.inner.config.execution.turn_id.clone(),
            cursor: 0,
            kind: LedgerEventKind::ModelOpeningAdmitted,
            payload,
        };
        let actual = self
            .inner
            .ledger
            .append_unless_cancelled(event.clone())
            .map_err(|e| match e {
                kolyan_ledger::LedgerError::Cancelled(_) => Error::Cancelled,
                e => Error::Ledger(e),
            })?;
        let mut expected = event;
        expected.cursor = actual.cursor;
        if actual.cursor == 0 || actual != expected {
            return Err(Error::BindingMismatch(
                "opening acknowledgement differs".into(),
            ));
        }
        // Re-read the full frozen chain, including this admission and its exact fact.
        let reference = ModelOpeningEventRef {
            event_id: actual.event_id,
            cursor: actual.cursor,
        };
        self.inspect_current(reference.clone())?;
        self.admitted(&request.request_id, reference)?;
        Ok(Permit {
            step_id: request.request_id.clone(),
        })
    }

    pub(super) fn consume_permit(&self, permit: Permit) -> Result<(), Error> {
        // Bounded cancellation read before consumption; atomic admission remains the
        // cancellation linearization point, not a promise about later network races.
        self.check_live()?;
        let mut after = 0;
        let mut loaded = 0usize;
        let mut total = 0usize;
        loop {
            let limit = self
                .inner
                .config
                .limits
                .max_events
                .saturating_sub(loaded)
                .saturating_add(1)
                .min(512);
            let rows = self.inner.ledger.query(&LedgerQuery {
                execution_id: Some(self.inner.config.execution.execution_id.clone()),
                event_id: None,
                after,
                through: None,
                limit,
            })?;
            let count = rows.len();
            if count > limit {
                return Err(Error::BindingMismatch(
                    "cancellation page exceeds requested bound".into(),
                ));
            }
            for event in rows {
                loaded = loaded
                    .checked_add(1)
                    .ok_or_else(|| Error::InvalidState("row overflow".into()))?;
                if loaded > self.inner.config.limits.max_events {
                    return Err(Error::InvalidState(
                        "cancellation prefix exceeds row bound".into(),
                    ));
                }
                let measured = bounded(
                    &event,
                    self.inner
                        .config
                        .limits
                        .max_total_bytes
                        .saturating_sub(total),
                )?;
                total += measured.len();
                bounded(&event.payload, self.inner.config.limits.max_payload_bytes)?;
                if event.execution_id != self.inner.config.execution.execution_id
                    || event.turn_id != self.inner.config.execution.turn_id
                    || event.cursor <= after
                {
                    return Err(Error::BindingMismatch(
                        "unordered/foreign cancellation prefix".into(),
                    ));
                }
                after = event.cursor;
                if event.kind == LedgerEventKind::ExecutionCancelled {
                    return Err(Error::Cancelled);
                }
                if matches!(
                    event.kind,
                    LedgerEventKind::TurnCancelled
                        | LedgerEventKind::TurnFailed
                        | LedgerEventKind::TurnTimedOut
                        | LedgerEventKind::TurnCompleted
                ) {
                    return Err(Error::InvalidState("physical terminal before GEN".into()));
                }
            }
            if count < limit {
                break;
            }
        }
        self.check_live()?;
        self.consume(&permit.step_id)
    }
}

fn bounded(value: &(impl Serialize + ?Sized), limit: usize) -> Result<Vec<u8>, Error> {
    struct Buffer {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(io::Error::other("opening payload exceeds byte bound"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut buffer = Buffer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut buffer, value).map_err(|e| Error::BindingMismatch(e.to_string()))?;
    Ok(buffer.bytes)
}
