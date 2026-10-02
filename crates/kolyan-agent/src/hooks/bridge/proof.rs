//! Exact ownership and immutable bridge publication. Reads never repair facts.

use kolyan_ledger::{FactDraft, FactRef, FactSubject, LedgerEventKind};
use kolyan_runtime::ExecutionBinding;
use kolyan_server::InvocationRole;
use serde_json::json;

use super::*;
use crate::AgentInvocationBindingStore;
use crate::hooks::{HookPhase, HookScope, encode, reference};

pub(super) fn operation(
    context: &EffectHookContext,
    phase: HookPhase,
) -> Result<String, HookError> {
    hash(&(
        "kolyan.hooks.effect.operation.v1",
        context.effect_id(),
        context.input_digest(),
        &context.issued().scope,
        phase,
    ))
}

impl<L, J> AgentEffectHookBridge<L, J>
where
    L: LedgerStore + Clone + Send + Sync + 'static,
    J: FactJournal + Clone + Send + Sync + 'static,
{
    pub(super) fn resolve(
        &self,
        context: &EffectHookContext,
    ) -> Result<VerifiedHookBinding, HookError> {
        let key = &context.issued().scope.execution;
        context
            .issued()
            .grant
            .validate(
                &context.issued().prepared,
                &context.issued().policy_revision,
                &context.issued().scope,
            )
            .map_err(invalid)?;
        let id = format!("{}/task-binding", key.execution_id);
        let event = self
            .ledger
            .event_by_id(&id)
            .map_err(invalid)?
            .ok_or_else(|| HookError::Integrity("execution ownership missing".into()))?;
        let linked: ExecutionBinding =
            serde_json::from_value(event.payload["binding"].clone()).map_err(invalid)?;
        linked.validate().map_err(invalid)?;
        if event.kind != LedgerEventKind::ExecutionBound
            || event.cursor == 0
            || event.event_id != id
            || event.idempotency_key != id
            || event.execution_id != key.execution_id
            || event.turn_id != key.turn_id
            || linked.execution_id != key.execution_id
            || linked.turn_id != key.turn_id
            || linked.session_id != key.session_id
            || event.payload != json!({"binding":linked})
        {
            return Err(HookError::Integrity("foreign execution ownership".into()));
        }
        let task = self
            .coordinator
            .snapshot(&linked.task_id)
            .map_err(invalid)?;
        let attempt = task
            .attempts
            .get(&linked.attempt_id)
            .ok_or_else(|| HookError::Integrity("admitted attempt missing".into()))?;
        if attempt.binding.invocation_id != linked.invocation_id
            || attempt.binding.execution.execution_id != key.execution_id
            || attempt.binding.execution.turn_id != key.turn_id
            || attempt.binding.execution.session_id != key.session_id
        {
            return Err(HookError::Integrity(
                "execution differs from exact attempt".into(),
            ));
        }
        let roots: Vec<_> = task
            .invocations
            .values()
            .filter(|v| v.definition.role == InvocationRole::Root)
            .collect();
        let [root] = roots.as_slice() else {
            return Err(HookError::Integrity("unique task root missing".into()));
        };
        let first = root
            .attempts
            .first()
            .and_then(|id| task.attempts.get(id))
            .ok_or_else(|| HookError::Integrity("root admitted session missing".into()))?;
        let logical = &first.binding.execution.session_id;
        if root.attempts.iter().any(|id| {
            task.attempts
                .get(id)
                .is_none_or(|a| &a.binding.execution.session_id != logical)
        }) {
            return Err(HookError::Integrity("root logical session changed".into()));
        }
        let store = AgentInvocationBindingStore::new(self.catalog.journal.clone());
        let (root_owner, _) = store
            .load_with_reference(&linked.task_id, &root.definition.invocation_id, logical)
            .map_err(invalid)?
            .ok_or_else(|| HookError::Integrity("root ownership missing".into()))?;
        if root_owner.context_kind != crate::BindingContextKind::Root
            || root_owner.snapshot.identity() != &first.binding.agent
            || root_owner.snapshot.identity() != &root.definition.agent
        {
            return Err(HookError::Integrity(
                "root logical ownership differs".into(),
            ));
        }
        let (owner, _) = store
            .load_with_reference(&linked.task_id, &linked.invocation_id, logical)
            .map_err(invalid)?
            .ok_or_else(|| HookError::Integrity("invocation ownership missing".into()))?;
        if owner.private_session_id != key.session_id
            || owner.snapshot.identity() != &attempt.binding.agent
            || context.issued().scope.agent_snapshot_digest.as_deref()
                != Some(owner.snapshot.digest())
        {
            return Err(HookError::Integrity(
                "private session or snapshot differs".into(),
            ));
        }
        let scope = HookScope::from_binding(&owner, key.execution_id.clone(), key.turn_id.clone())?;
        self.catalog.restore_for_scope(&scope)
    }

    fn coordinates(
        &self,
        binding: &VerifiedHookBinding,
        event: &HookEvent,
        context: &EffectHookContext,
    ) -> Result<(String, String), HookError> {
        let digest = hash(&(
            "kolyan.hooks.bridge.v1",
            binding.reference(),
            operation(context, event.phase())?,
        ))?;
        Ok((
            format!("agent.hooks.effect.{digest}"),
            format!("agent.hook.effect.{digest}"),
        ))
    }

    fn intent(
        &self,
        binding: &VerifiedHookBinding,
        event: &HookEvent,
        context: &EffectHookContext,
    ) -> Result<FactDraft, HookError> {
        let (_, id) = self.coordinates(binding, event, context)?;
        let receipt = if let HookPayload::AfterTool {
            receipt_event_id,
            receipt_cursor,
            ..
        } = &event.payload
        {
            let receipt = self
                .ledger
                .event_by_id(receipt_event_id)
                .map_err(invalid)?
                .ok_or_else(|| HookError::Integrity("actual receipt missing".into()))?;
            let key = &context.issued().scope.execution;
            if receipt.kind != LedgerEventKind::EffectReceipt
                || receipt.cursor != *receipt_cursor
                || receipt.execution_id != key.execution_id
                || receipt.turn_id != key.turn_id
                || receipt.event_id != *receipt_event_id
                || receipt.idempotency_key != *receipt_event_id
            {
                return Err(HookError::Integrity("actual receipt differs".into()));
            }
            Some(receipt)
        } else {
            None
        };
        let payload = json!({"binding":binding.reference(),"event":event,"issued":context.issued(),
            "effect_id":context.effect_id(),"input_digest":context.input_digest(),"receipt":receipt});
        encode(&payload)?;
        Ok(FactDraft {
            fact_id: format!("{id}.started"),
            subject: FactSubject {
                kind: "agent.hook.effect".into(),
                id,
            },
            kind: "agent.hook.effect.started".into(),
            schema_version: 1,
            critical: true,
            causes: vec![binding.reference().clone()],
            payload,
        })
    }

    pub(super) fn begin(
        &self,
        binding: &VerifiedHookBinding,
        event: &HookEvent,
        context: &EffectHookContext,
        receipt: Option<&CommittedEffectReceipt>,
    ) -> Result<(), HookError> {
        self.catalog
            .validate_current(binding, &self.policy, self.native.digest())?;
        let (stream, _) = self.coordinates(binding, event, context)?;
        if !self.catalog.journal.read(&stream, 0, 3)?.is_empty() {
            return Err(HookError::Interrupted);
        }
        let draft = self.intent(binding, event, context)?;
        if let Some(receipt) = receipt
            && draft.payload["receipt"] != serde_json::to_value(receipt.event()).map_err(invalid)?
        {
            return Err(HookError::Integrity(
                "opaque receipt differs from actual ledger".into(),
            ));
        }
        let ack = self
            .catalog
            .journal
            .append(&stream, 0, vec![draft.clone()])?;
        let stored = self.catalog.journal.read(&stream, 0, 3)?;
        if ack != stored
            || stored.len() != 1
            || stored[0].position != 1
            || stored[0].stream_id != stream
            || stored[0].draft != draft
        {
            return Err(HookError::Integrity(
                "bridge intent acknowledgement differs".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn complete(
        &self,
        binding: &VerifiedHookBinding,
        event: &HookEvent,
        context: &EffectHookContext,
        result: &HookDispatchResult,
    ) -> Result<(), HookError> {
        let (stream, id) = self.coordinates(binding, event, context)?;
        let rows = self.catalog.journal.read(&stream, 0, 3)?;
        let [started] = rows.as_slice() else {
            return Err(HookError::Interrupted);
        };
        if started.draft != self.intent(binding, event, context)?
            || started.position != 1
            || started.stream_id != stream
        {
            return Err(HookError::Integrity("bridge intent changed".into()));
        }
        let completed = match result {
            HookDispatchResult::Continued { completed }
            | HookDispatchResult::Denied { completed, .. } => completed,
        };
        let draft = completion(&id, reference(started), completed, result)?;
        let ack = self.catalog.journal.append(&stream, 1, vec![draft])?;
        let actual = self.catalog.journal.read(&stream, 1, 2)?;
        if ack != actual || actual.len() != 1 {
            return Err(HookError::Integrity(
                "bridge completion acknowledgement differs".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn verify_bridge(
        &self,
        binding: &VerifiedHookBinding,
        event: &HookEvent,
        context: &EffectHookContext,
    ) -> Result<HookDispatchResult, HookError> {
        let (stream, id) = self.coordinates(binding, event, context)?;
        let rows = self.catalog.journal.read(&stream, 0, 3)?;
        let [started, ended] = rows.as_slice() else {
            return Err(HookError::Interrupted);
        };
        let result =
            self.runtime
                .verify_dispatch(binding, event, &operation(context, event.phase())?)?;
        let completed = match &result {
            HookDispatchResult::Continued { completed }
            | HookDispatchResult::Denied { completed, .. } => completed,
        };
        if started.stream_id != stream
            || started.position != 1
            || started.draft != self.intent(binding, event, context)?
            || ended.stream_id != stream
            || ended.position != 2
            || ended.draft != completion(&id, reference(started), completed, &result)?
        {
            return Err(HookError::Integrity("bridge observation differs".into()));
        }
        Ok(result)
    }
}

fn completion(
    id: &str,
    started: FactRef,
    completed: &[FactRef],
    result: &HookDispatchResult,
) -> Result<FactDraft, HookError> {
    let payload = json!({"result":result});
    encode(&payload)?;
    Ok(FactDraft {
        fact_id: format!("{id}.completed"),
        subject: FactSubject {
            kind: "agent.hook.effect".into(),
            id: id.into(),
        },
        kind: "agent.hook.effect.completed".into(),
        schema_version: 1,
        critical: true,
        causes: std::iter::once(started)
            .chain(completed.iter().cloned())
            .collect(),
        payload,
    })
}
