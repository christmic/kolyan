//! One strict Step chain per request, tied to admission and physical ordering.

use kolyan_core::StepOutcome;
use kolyan_ledger::{FactJournal, LedgerEvent, LedgerEventKind as Kind};
use kolyan_model::{ModelRequest, StopReason, digest_json};
use kolyan_policy::ToolExecutionScope;

use super::dto::{Admission, Completed, Requested, StepStarted};
use super::prefix::execution_prefix;
use super::reader::{Budget, binding, coordinate, decode, fact, invalid, model_source};
use super::{
    ModelContextPrepared, ModelOpeningAccounting, ModelOpeningAdmitted,
    ModelOpeningInspectionRequest, ModelOpeningProofError as Error, ModelOpeningState,
    VerifiedModelOpeningStep, VerifiedModelOpenings,
};

pub(super) fn verify<F: FactJournal + ?Sized>(
    facts: &F,
    request: &ModelOpeningInspectionRequest,
    events: &[LedgerEvent],
    budget: &mut Budget,
) -> Result<VerifiedModelOpenings, Error> {
    let admission_id = format!("{}/input-admitted", request.execution.execution_id);
    let admission_event = events
        .iter()
        .find(|event| event.event_id == admission_id)
        .ok_or_else(|| Error::MissingEvidence {
            source_id: admission_id.clone(),
        })?;
    if admission_event.kind != Kind::ExecutionInputAdmitted {
        return Err(binding(&admission_id, "input admission kind differs"));
    }
    protocol(&admission_event.payload, &admission_id, true)?;
    model_source(&admission_event.payload["model_request"])?;
    let admission: Admission = decode(&admission_event.payload, &admission_id)?;
    if admission.key != request.execution || admission.max_steps == 0 {
        return Err(binding(
            &admission_id,
            "admission key or Step ceiling differs",
        ));
    }
    ToolExecutionScope {
        execution: admission.key.clone(),
        step_id: "opening-proof".into(),
        agent_snapshot_digest: admission.agent_snapshot_digest.clone(),
    }
    .validate()
    .map_err(|error| invalid(&admission_id, error))?;
    let started_id = format!("{}/execution-started", request.execution.execution_id);
    let started = events
        .iter()
        .find(|event| event.event_id == started_id)
        .ok_or_else(|| Error::MissingEvidence {
            source_id: started_id.clone(),
        })?;
    if started.event_id != started_id
        || started.kind != Kind::ExecutionStarted
        || started.cursor >= admission_event.cursor
        || decode::<crate::ExecutionKey>(&started.payload, &started_id)? != request.execution
    {
        return Err(binding(
            &started_id,
            "missing canonical execution identity before admission",
        ));
    }
    execution_prefix(events, started)?;

    let mut steps: Vec<VerifiedModelOpeningStep> = Vec::new();
    let mut current_request: Option<ModelRequest> = None;
    let mut last_outcome: Option<StepOutcome> = None;
    let mut terminal = false;
    let mut cancelled = false;
    for event in events {
        let model_fact = matches!(
            event.kind,
            Kind::StepStarted
                | Kind::ModelRequested
                | Kind::ModelOpeningAdmitted
                | Kind::ModelStreamEvent
                | Kind::StepCompleted
        );
        if model_fact && (terminal || event.cursor <= admission_event.cursor) {
            return Err(Error::OrderingMismatch(
                "model fact before admission or after physical terminal".into(),
            ));
        }
        match event.kind {
            Kind::ExecutionInputAdmitted if event.event_id != admission_id => {
                return Err(binding(&event.event_id, "duplicate input admission"));
            }
            Kind::ExecutionStarted if event.event_id != started_id => {
                return Err(binding(&event.event_id, "duplicate execution identity"));
            }
            Kind::StepStarted => {
                let value: StepStarted = decode(&event.payload, &event.event_id)?;
                if steps.len() >= admission.max_steps
                    || value.step_id
                        != format!("{}-step-{}", request.execution.turn_id, steps.len())
                    || steps
                        .last()
                        .is_some_and(|step| step.state != ModelOpeningState::Completed)
                    || (!steps.is_empty() && last_outcome != Some(StepOutcome::ToolCalls))
                {
                    return Err(Error::OrderingMismatch(
                        "Step identity, ceiling or preceding completion differs".into(),
                    ));
                }
                ToolExecutionScope {
                    execution: request.execution.clone(),
                    step_id: value.step_id.clone(),
                    agent_snapshot_digest: admission.agent_snapshot_digest.clone(),
                }
                .validate()
                .map_err(|error| invalid(&event.event_id, error))?;
                steps.push(VerifiedModelOpeningStep {
                    step_id: value.step_id,
                    state: ModelOpeningState::NotAdmitted,
                    started: coordinate(event),
                    requested: None,
                    preparation: None,
                    opening: None,
                    completed: None,
                });
                current_request = None;
            }
            Kind::ModelRequested => {
                let step = steps
                    .last_mut()
                    .ok_or_else(|| Error::OrderingMismatch("request without StepStarted".into()))?;
                if step.requested.is_some() {
                    return Err(Error::OrderingMismatch("duplicate Step request".into()));
                }
                model_source(&event.payload["request"])?;
                let value: Requested = decode(&event.payload, &event.event_id)?;
                if value.request.request_id != step.step_id {
                    return Err(binding(&event.event_id, "request_id differs from Step"));
                }
                let mut fixed = value.request.clone();
                fixed.request_id = admission.model_request.request_id.clone();
                fixed.messages = admission.model_request.messages.clone();
                if fixed != admission.model_request
                    || !value
                        .request
                        .messages
                        .starts_with(&admission.model_request.messages)
                {
                    return Err(binding(
                        &event.event_id,
                        "request changes admitted configuration or source prefix",
                    ));
                }
                step.requested = Some(coordinate(event));
                current_request = Some(value.request);
            }
            Kind::ModelOpeningAdmitted => {
                if cancelled {
                    return Err(Error::OrderingMismatch("opening after cancellation".into()));
                }
                protocol(&event.payload, &event.event_id, true)?;
                let opening: ModelOpeningAdmitted = decode(&event.payload, &event.event_id)?;
                let step = steps
                    .last_mut()
                    .ok_or_else(|| Error::OrderingMismatch("opening without Step".into()))?;
                let model_request = current_request.as_ref().ok_or_else(|| {
                    Error::OrderingMismatch("opening without actual request".into())
                })?;
                if step.opening.is_some() || step.completed.is_some() {
                    return Err(Error::OrderingMismatch(
                        "duplicate opening or opening after completion".into(),
                    ));
                }
                if opening.execution != request.execution
                    || opening.step_id != step.step_id
                    || step.requested.as_ref() != Some(&opening.model_requested)
                    || event.event_id
                        != format!(
                            "{}/model-opening/{}",
                            request.execution.execution_id, step.step_id
                        )
                    || opening.deadline_at_ms != admission.deadline_at_ms
                {
                    return Err(binding(
                        &event.event_id,
                        "opening execution, request, identity or deadline differs",
                    ));
                }
                let record = fact(facts, &opening.preparation, budget)?;
                let draft = &record.draft;
                if draft.kind != "model.context_prepared" || draft.schema_version != 1 {
                    return Err(Error::UnsupportedProtocol {
                        source_id: draft.fact_id.clone(),
                    });
                }
                if !draft.critical
                    || draft.subject.kind != "runtime.execution"
                    || draft.subject.id != request.execution.execution_id
                {
                    return Err(binding(
                        &draft.fact_id,
                        "preparation subject or critical flag differs",
                    ));
                }
                protocol(&draft.payload, &draft.fact_id, false)?;
                let prepared: ModelContextPrepared = decode(&draft.payload, &draft.fact_id)?;
                preparation(&prepared, &opening, model_request, &draft.fact_id)?;
                step.preparation = Some(opening.preparation);
                step.opening = Some(coordinate(event));
                step.state = ModelOpeningState::Uncertain;
            }
            Kind::ModelStreamEvent => {
                let step = steps
                    .last()
                    .ok_or_else(|| Error::OrderingMismatch("stream event without Step".into()))?;
                if step.opening.is_none()
                    || step.completed.is_some()
                    || event
                        .payload
                        .get("step_id")
                        .and_then(|value| value.as_str())
                        != Some(&step.step_id)
                {
                    return Err(Error::OrderingMismatch(
                        "stream event outside admitted active Step".into(),
                    ));
                }
            }
            Kind::StepCompleted => {
                protocol(&event.payload, &event.event_id, false)?;
                let value: Completed = decode(&event.payload, &event.event_id)?;
                let step = steps
                    .last_mut()
                    .ok_or_else(|| Error::OrderingMismatch("completion without Step".into()))?;
                if step.completed.is_some() || step.opening.is_none() {
                    return Err(Error::OrderingMismatch(
                        "duplicate or unadmitted completion".into(),
                    ));
                }
                let model_request = current_request
                    .as_ref()
                    .ok_or_else(|| Error::OrderingMismatch("completion without request".into()))?;
                if value.step_id != step.step_id
                    || value.step.step_id != step.step_id
                    || step.requested.as_ref() != Some(&value.model_requested)
                    || step.opening.as_ref() != Some(&value.model_opening)
                    || value.step.response.model != model_request.model
                    || value.outcome != format!("{:?}", value.step.outcome)
                    || value.step.outcome != outcome(&value.step.response.stop_reason)
                {
                    return Err(binding(
                        &event.event_id,
                        "typed completion binding or outcome differs",
                    ));
                }
                step.completed = Some(coordinate(event));
                step.state = ModelOpeningState::Completed;
                last_outcome = Some(value.step.outcome);
            }
            Kind::ExecutionCancelled => {
                cancelled = true;
            }
            Kind::TurnFailed | Kind::TurnTimedOut | Kind::TurnCancelled | Kind::TurnCompleted => {
                if terminal || event.cursor <= admission_event.cursor {
                    return Err(Error::OrderingMismatch(
                        "physical terminal before input admission or multiple physical terminals"
                            .into(),
                    ));
                }
                terminal = true;
                if event.kind == Kind::TurnCancelled {
                    cancelled = true;
                }
            }
            _ => {}
        }
    }
    Ok(VerifiedModelOpenings {
        execution: request.execution.clone(),
        input_admission: coordinate(admission_event),
        inspected_through: request.through.clone(),
        steps,
    })
}

fn protocol(value: &serde_json::Value, source: &str, schema: bool) -> Result<(), Error> {
    if value
        .get("opening_protocol")
        .and_then(|value| value.as_u64())
        != Some(1)
        || (schema && value.get("schema_version").and_then(|value| value.as_u64()) != Some(1))
    {
        return Err(Error::UnsupportedProtocol {
            source_id: source.into(),
        });
    }
    Ok(())
}

fn preparation(
    prepared: &ModelContextPrepared,
    opening: &ModelOpeningAdmitted,
    request: &ModelRequest,
    source: &str,
) -> Result<(), Error> {
    if prepared.execution != opening.execution
        || prepared.step_id != opening.step_id
        || prepared.model_requested != opening.model_requested
        || prepared.neutral_digest != opening.neutral_digest
        || prepared.neutral_digest
            != digest_json(request).map_err(|error| invalid(source, error))?
        || prepared.mapping_identity.model != request.model
        || prepared.count_profile_digest != opening.count_profile_digest
        || prepared.generation_wire_digest != opening.generation_wire_digest
        || prepared.generation_wire_bytes != opening.generation_wire_bytes
    {
        return Err(binding(
            source,
            "preparation does not bind the actual request/opening",
        ));
    }
    for digest in [
        &prepared.neutral_digest,
        &prepared.mapping_identity.endpoint_id,
        &prepared.count_profile_digest,
        &prepared.generation_wire_digest,
        &prepared.count_input_digest,
    ] {
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(invalid(source, "expected 64-hex digest/endpoint identity"));
        }
    }
    let identity = &prepared.mapping_identity;
    for (text, max) in [
        (&identity.model.provider, 128),
        (&identity.model.model, 512),
        (&identity.mapping_revision, 128),
        (&identity.coverage_revision, 128),
    ] {
        text_valid(text, max, source)?;
    }
    if prepared.generation_wire_bytes == 0
        || prepared.generation_wire_bytes > kolyan_model::MAX_CONTEXT_JSON_BYTES
    {
        return Err(invalid(source, "generation bytes outside Model ceiling"));
    }
    if prepared
        .unsupported_count_fields
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid(source, "coverage fields must be sorted and unique"));
    }
    for field in &prepared.unsupported_count_fields {
        text_valid(field, 128 * 1024, source)?;
    }
    match &prepared.accounting {
        ModelOpeningAccounting::ProviderReported {
            counter_revision,
            input_tokens,
            max_input_tokens,
        } => {
            text_valid(counter_revision, 128, source)?;
            if !prepared.unsupported_count_fields.is_empty() || input_tokens > max_input_tokens {
                return Err(invalid(
                    source,
                    "reported accounting lacks coverage or exceeds budget",
                ));
            }
        }
        ModelOpeningAccounting::WireBytes {
            policy_revision,
            max_generation_wire_bytes,
        } => {
            text_valid(policy_revision, 128, source)?;
            if prepared.generation_wire_bytes > *max_generation_wire_bytes {
                return Err(invalid(source, "generation exceeds explicit byte policy"));
            }
        }
    }
    Ok(())
}

fn text_valid(text: &str, max: usize, source: &str) -> Result<(), Error> {
    if text.trim().is_empty() || text.len() > max || text.chars().any(char::is_control) {
        return Err(invalid(source, "invalid identity text"));
    }
    Ok(())
}

fn outcome(reason: &StopReason) -> StepOutcome {
    match reason {
        StopReason::EndTurn => StepOutcome::FinalAnswer,
        StopReason::ToolUse => StepOutcome::ToolCalls,
        StopReason::Refusal => StepOutcome::Refused,
        StopReason::MaxOutputTokens | StopReason::Other(_) => StepOutcome::Incomplete,
    }
}
