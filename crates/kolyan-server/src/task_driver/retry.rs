//! Host-verified retry admission. Proof persistence never executes an attempt.
//!
//! Runtime's trusted reconciliation writer owns external-effect inspection. This
//! facade consumes its exact immutable facts and never calls an executor inspector.
//! Hosts must serialize interrupted-execution recovery with retry authorization;
//! checking an active worker is not a distributed fencing or reservation protocol.

use std::collections::BTreeMap;

use kolyan_runtime::{EffectGrant, EffectRequest, ReconciliationRequest, ReconciliationResolution};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::*;
use crate::ExecutionEvidence;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryProof {
    task_id: String,
    fact_id: String,
    binding: AttemptBinding,
    reason: String,
    observed_source: Option<ExecutionEvidence>,
    inspected_through: ExecutionEvidence,
    reconciliations: Vec<ExecutionEvidence>,
}

impl<J, L, S, SS> TaskExecutionService<J, L, S, SS>
where
    J: FactJournal,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
{
    /// Verify a stopped execution before admitting a fresh attempt. Every started
    /// effect requires exact trusted NotCommitted evidence, and any receipt blocks
    /// whole-attempt retry. Unknown/missing evidence is not interpreted as absence.
    /// Identical fact retries repair the proof → domain crash boundary; changed
    /// reasons or later physical execution evidence fail closed. No grants return.
    pub fn authorize_retry(
        &self,
        task_id: &str,
        fact_id: &str,
        attempt_id: &str,
        reason: &str,
    ) -> Result<TaskSnapshot, TaskExecutionError> {
        if fact_id.trim().is_empty()
            || fact_id.len() > 256
            || reason.trim().is_empty()
            || reason.len() > 8192
        {
            return Err(invalid("retry requires bounded fact identity and reason").into());
        }
        let snapshot = self.coordinator.snapshot(task_id)?;
        let attempt = snapshot
            .attempts
            .get(attempt_id)
            .ok_or_else(|| invalid("unknown attempt"))?;
        if snapshot.state.is_terminal()
            || attempt.cancellation_requested
            || !matches!(
                attempt.state,
                InvocationState::Failed | InvocationState::RecoveryRequired
            )
        {
            return Err(
                invalid("retry requires a nonterminal task and a failed/recovery attempt").into(),
            );
        }
        let binding = &attempt.binding;
        self.require_idle(binding)?;
        // Hash the identity tuple so long valid IDs still produce bounded evidence IDs.
        let key = serde_json::to_vec(&(task_id, fact_id, attempt_id))
            .map_err(|error| invalid(error.to_string()))?;
        let proof_id = format!("retry/{:x}", Sha256::digest(key));
        let all = self
            .ledger()
            .execution_events_after(&binding.execution.execution_id, 0)
            .map_err(|error| invalid(error.to_string()))?;
        let existing = all.iter().find(|event| event.event_id == proof_id);
        if attempt.retry_authorized && existing.is_none() {
            return Err(invalid("attempt already has another retry decision").into());
        }
        let facts: Vec<_> = all
            .iter()
            .filter(|event| event.event_id != proof_id)
            .collect();
        validate_membership(task_id, binding, &facts)?;
        if let Some(observation) = &attempt.observation
            && !facts
                .iter()
                .any(|event| evidence::source_ref(binding, event) == observation.source)
        {
            return Err(
                invalid("original observation is not backed by actual execution evidence").into(),
            );
        }
        let inspected_through = facts
            .last()
            .map(|event| evidence::source_ref(binding, event))
            .ok_or_else(|| invalid("execution has no inspection evidence"))?;
        let reconciliations = validate_effects(binding, &facts)?;
        let proof = RetryProof {
            task_id: task_id.into(),
            fact_id: fact_id.into(),
            binding: binding.clone(),
            reason: reason.into(),
            observed_source: attempt
                .observation
                .as_ref()
                .map(|observation| observation.source.clone()),
            inspected_through,
            reconciliations,
        };
        let candidate = LedgerEvent {
            event_id: proof_id.clone(),
            idempotency_key: proof_id.clone(),
            cursor: 0,
            execution_id: binding.execution.execution_id.clone(),
            turn_id: binding.execution.turn_id.clone(),
            kind: LedgerEventKind::ExecutionRetryAuthorized,
            payload: json!({"proof": proof}),
        };
        let committed = if let Some(stored) = existing {
            let mut actual = (*stored).clone();
            actual.cursor = 0;
            if actual != candidate {
                return Err(invalid("retry proof changed source, binding or reason").into());
            }
            (*stored).clone()
        } else {
            self.require_idle(binding)?;
            match self.ledger().append_unless_cancelled(candidate.clone()) {
                Ok(event) => event,
                Err(kolyan_ledger::LedgerError::Conflict(_)) => {
                    let stored = self
                        .ledger()
                        .event_by_id(&proof_id)
                        .map_err(|error| invalid(error.to_string()))?
                        .ok_or_else(|| invalid("conflicting retry proof unavailable"))?;
                    let mut actual = stored.clone();
                    actual.cursor = 0;
                    if actual != candidate {
                        return Err(invalid("conflicting retry proof differs").into());
                    }
                    stored
                }
                Err(error) => return Err(invalid(error.to_string()).into()),
            }
        };
        self.require_idle(binding)?;
        // A cancellation, receipt or further execution fact committed during the
        // cross-journal boundary invalidates this proof instead of granting retry.
        let current = self
            .ledger()
            .execution_events_after(&binding.execution.execution_id, 0)
            .map_err(|error| invalid(error.to_string()))?;
        let expected: Vec<_> = facts
            .into_iter()
            .cloned()
            .chain(std::iter::once(committed.clone()))
            .collect();
        if current != expected {
            return Err(invalid("execution changed while authorizing retry").into());
        }
        Ok(self.coordinator.authorize_retry(
            task_id,
            fact_id,
            attempt_id,
            evidence::source_ref(binding, &committed),
            reason,
        )?)
    }

    fn require_idle(&self, binding: &AttemptBinding) -> Result<(), TaskError> {
        if self
            .execution
            .execution()
            .server()
            .coordinator()
            .is_active(&binding.execution.execution_id)
        {
            return Err(invalid("active execution cannot authorize retry"));
        }
        Ok(())
    }
}

fn validate_membership(
    task_id: &str,
    binding: &AttemptBinding,
    facts: &[&LedgerEvent],
) -> Result<(), TaskError> {
    if facts.iter().any(|event| {
        event.execution_id != binding.execution.execution_id
            || event.turn_id != binding.execution.turn_id
    }) {
        return Err(invalid("foreign execution evidence"));
    }
    let started_id = format!("{}/execution-started", binding.execution.execution_id);
    if !facts.iter().any(|event| {
        event.event_id == started_id
            && event.kind == LedgerEventKind::ExecutionStarted
            && event.payload == json!(binding.execution)
    }) {
        return Err(invalid("missing exact admitted execution identity"));
    }
    let link = ExecutionBinding {
        task_id: task_id.into(),
        invocation_id: binding.invocation_id.clone(),
        attempt_id: binding.attempt_id.clone(),
        session_id: binding.execution.session_id.clone(),
        turn_id: binding.execution.turn_id.clone(),
        execution_id: binding.execution.execution_id.clone(),
    };
    let bound_id = format!("{}/task-binding", binding.execution.execution_id);
    if !facts.iter().any(|event| {
        event.event_id == bound_id
            && event.kind == LedgerEventKind::ExecutionBound
            && event.payload == json!({"binding": link})
    }) {
        return Err(invalid(
            "execution is not bound to this task/invocation/attempt",
        ));
    }
    if facts.iter().any(|event| {
        matches!(
            event.kind,
            LedgerEventKind::TurnCancelled | LedgerEventKind::ExecutionCancelled
        ) || (event.kind == LedgerEventKind::TurnCompleted
            && event.payload["reason"] == "FinalAnswer")
    }) {
        return Err(invalid(
            "cancelled or successfully completed execution cannot retry",
        ));
    }
    Ok(())
}

fn validate_effects(
    binding: &AttemptBinding,
    facts: &[&LedgerEvent],
) -> Result<Vec<ExecutionEvidence>, TaskError> {
    if facts
        .iter()
        .any(|event| event.kind == LedgerEventKind::EffectReceipt)
    {
        return Err(invalid(
            "a committed effect receipt blocks whole-attempt retry",
        ));
    }
    let mut started = BTreeMap::new();
    for event in facts
        .iter()
        .filter(|event| event.kind == LedgerEventKind::EffectStarted)
    {
        let effect_id = event.payload["effect_id"]
            .as_str()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| invalid("started effect lacks identity"))?;
        if started.insert(effect_id, *event).is_some() {
            return Err(invalid("duplicate started effect"));
        }
    }
    let mut proofs = Vec::new();
    for (effect_id, start) in started {
        let prefix = format!("{}/effect/{effect_id}", binding.execution.execution_id);
        if start.event_id != format!("{prefix}/started") {
            return Err(invalid("noncanonical started effect identity"));
        }
        let prepared: EffectRequest = serde_json::from_value(
            required(
                facts,
                &format!("{prefix}/prepared"),
                LedgerEventKind::EffectPrepared,
            )?
            .payload
            .clone(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        let grant: EffectGrant = serde_json::from_value(
            required(
                facts,
                &format!("{prefix}/authorized"),
                LedgerEventKind::EffectAuthorized,
            )?
            .payload
            .clone(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        if prepared.effect_id != effect_id
            || grant.effect_id != effect_id
            || prepared.input_digest != grant.input_digest
            || prepared.policy_revision != grant.authority_revision
            || grant.authorization_id != format!("{prefix}/authorized")
        {
            return Err(invalid(
                "reconciliation has no exact prepared/authorized effect binding",
            ));
        }
        let decision = facts
            .iter()
            .rev()
            .find(|event| {
                event.kind == LedgerEventKind::EffectReconciled
                    && event.payload["request"]["effect_id"] == effect_id
            })
            .ok_or_else(|| invalid("started effect has no trusted inspection"))?;
        let request: ReconciliationRequest =
            serde_json::from_value(decision.payload["request"].clone())
                .map_err(|error| invalid(error.to_string()))?;
        let resolution: ReconciliationResolution =
            serde_json::from_value(decision.payload["resolution"].clone())
                .map_err(|error| invalid(error.to_string()))?;
        if json!(request.execution) != json!(binding.execution)
            || request.effect_id != effect_id
            || request.reconciliation_id.trim().is_empty()
            || decision.event_id != format!("{prefix}/reconciliation/{}", request.reconciliation_id)
            || decision.cursor <= start.cursor
        {
            return Err(invalid("foreign or stale reconciliation evidence"));
        }
        let ReconciliationResolution::NotCommitted { evidence } = resolution else {
            return Err(invalid("inspection does not prove NotCommitted"));
        };
        if evidence.trim().is_empty() || evidence.len() > 16_384 {
            return Err(invalid("missing or oversized executor inspection evidence"));
        }
        proofs.push(evidence::source_ref(binding, decision));
    }
    Ok(proofs)
}

fn required<'a>(
    facts: &[&'a LedgerEvent],
    id: &str,
    kind: LedgerEventKind,
) -> Result<&'a LedgerEvent, TaskError> {
    facts
        .iter()
        .find(|event| event.event_id == id && event.kind == kind)
        .copied()
        .ok_or_else(|| invalid(format!("missing effect fact {id}")))
}

#[cfg(test)]
mod tests;
