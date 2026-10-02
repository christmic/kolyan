//! Inventory all potential effects; preparation alone is not absence proof.

use std::collections::BTreeMap;

use kolyan_core::{StepOutcome, StepResult};
use kolyan_ledger::{LedgerEvent, LedgerEventKind};
use kolyan_model::ContentBlock;
use kolyan_runtime::VerifiedEffectProof;

use super::{GoalSourceError, bad};

pub(super) struct EffectInventory<'a> {
    pub started: Vec<&'a LedgerEvent>,
    pub unresolved: bool,
    pub count: usize,
}

pub(super) fn inventory(events: &[LedgerEvent]) -> Result<EffectInventory<'_>, GoalSourceError> {
    let mut effects = BTreeMap::<&str, [Option<&LedgerEvent>; 5]>::new();
    for event in events {
        let slot = match event.kind {
            LedgerEventKind::EffectPrepared => 0,
            LedgerEventKind::EffectAuthorized => 1,
            LedgerEventKind::EffectStarted => 2,
            LedgerEventKind::EffectReceipt => 3,
            LedgerEventKind::EffectUncertain => 4,
            _ => continue,
        };
        let id = event.payload["effect_id"]
            .as_str()
            .filter(|id| !id.is_empty() && id.len() <= 1281)
            .ok_or_else(|| bad("effect fact lacks a bounded exact identity"))?;
        if effects.entry(id).or_default()[slot]
            .replace(event)
            .is_some()
        {
            return Err(bad("duplicate physical effect stage"));
        }
    }
    let mut result = EffectInventory {
        started: Vec::new(),
        unresolved: false,
        count: effects.len(),
    };
    for stages in effects.values() {
        let [prepared, authorized, started, receipt, uncertain] = *stages;
        if receipt.is_some() && started.is_none() {
            return Err(bad("effect receipt has no actual entry"));
        }
        if let Some(started) = started {
            if prepared.is_none() || authorized.is_none() {
                return Err(bad("effect entry lacks preparation/authorization"));
            }
            result.started.push(started);
            if let Some(uncertain) = uncertain {
                if uncertain.cursor <= started.cursor {
                    return Err(bad("uncertainty predates physical entry"));
                }
                result.unresolved |=
                    receipt.is_none_or(|receipt| receipt.cursor <= uncertain.cursor);
            }
        } else {
            // Runtime publishes Prepared only after a grant has been issued.
            // Ordinary policy denial never enters this effect inventory.
            result.unresolved |= prepared.is_some() || authorized.is_some() || uncertain.is_some();
        }
    }
    Ok(result)
}

pub(super) fn verify_model_call(
    events: &[LedgerEvent],
    proof: &VerifiedEffectProof,
) -> Result<(), GoalSourceError> {
    let step_event = events
        .iter()
        .find(|event| {
            event.kind == LedgerEventKind::StepCompleted
                && event.payload["step_id"] == proof.scope().step_id
                && event.cursor < proof.sources().started.cursor
        })
        .ok_or_else(|| bad("effect has no actual prior completed model Step"))?;
    let step: StepResult = serde_json::from_value(step_event.payload["step"].clone())
        .map_err(|e| bad(&e.to_string()))?;
    if step.step_id!=proof.scope().step_id || step.outcome!=StepOutcome::ToolCalls
        || !step.response.content.iter().any(|content|matches!(content,ContentBlock::ToolCall {call} if call==proof.prepared().call())) {
        return Err(bad("prepared effect differs from actual model tool call"));
    }
    if !events.iter().any(|e| {
        e.kind == LedgerEventKind::ModelRequested
            && e.payload["request"]["request_id"] == step.step_id
            && e.cursor < step_event.cursor
            && e.cursor > proof.sources().admission.cursor
    }) {
        return Err(bad("effect Step has no actual admitted model request"));
    }
    Ok(())
}
