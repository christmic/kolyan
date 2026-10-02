//! Exact execution-owned native hook consumer. No model opening, grants or replay.

mod proof;

use kolyan_ledger::{FactJournal, LedgerStore};
use kolyan_runtime::effect_hooks::{
    CommittedEffectReceipt, EffectHookContext, EffectHookDecision, EffectHookError,
    EffectHookFuture, EffectHookPort,
};
use kolyan_server::TaskCoordinator;

use super::{
    HookAccessPolicy, HookCatalog, HookDispatchResult, HookError, HookEvent, HookExecutionWindow,
    HookPayload, HookRuntime, NativeHookHost, VerifiedHookBinding, hash,
};

/// A host-installed consumer using exact persisted execution ownership.
/// Missing bindings fail closed; reconstruction never selects revisions or repairs proof.
#[derive(Clone)]
pub struct AgentEffectHookBridge<L, J> {
    ledger: L,
    coordinator: TaskCoordinator<J>,
    catalog: HookCatalog,
    policy: HookAccessPolicy,
    native: NativeHookHost,
    runtime: HookRuntime,
}

impl<L, J> AgentEffectHookBridge<L, J>
where
    L: LedgerStore + Clone + Send + Sync + 'static,
    J: FactJournal + Clone + Send + Sync + 'static,
{
    /// Dependencies are trusted host stores/configuration, never model DTO authority.
    /// The hook catalog must share the actual ownership journal; mismatches reject.
    pub fn new(
        ledger: L,
        coordinator: TaskCoordinator<J>,
        catalog: HookCatalog,
        policy: HookAccessPolicy,
        native: NativeHookHost,
    ) -> Self {
        let runtime = HookRuntime::new(catalog.clone(), policy.clone(), native.clone());
        Self {
            ledger,
            coordinator,
            catalog,
            policy,
            native,
            runtime,
        }
    }

    async fn resolve_binding(
        &self,
        context: &EffectHookContext,
    ) -> Result<VerifiedHookBinding, HookError> {
        let bridge = self.clone();
        let context = context.clone();
        blocking(move || bridge.resolve(&context)).await
    }

    fn event(
        &self,
        binding: &VerifiedHookBinding,
        context: &EffectHookContext,
        receipt: Option<&CommittedEffectReceipt>,
    ) -> Result<HookEvent, HookError> {
        let issued = context.issued();
        let payload = match receipt {
            None => HookPayload::BeforeTool {
                tool_name: issued.prepared.call().name.clone(),
                prepared_digest: issued.prepared.digest().into(),
                argument_digest: hash(&issued.prepared.call().arguments)?,
                scope: issued.scope.clone(),
            },
            Some(receipt) => HookPayload::AfterTool {
                tool_name: issued.prepared.call().name.clone(),
                prepared_digest: issued.prepared.digest().into(),
                scope: issued.scope.clone(),
                receipt_event_id: receipt.event().event_id.clone(),
                receipt_cursor: receipt.event().cursor,
                result_digest: hash(receipt.result())?,
            },
        };
        let event = HookEvent {
            schema_version: 1,
            scope: binding.scope().clone(),
            payload,
        };
        event.validate()?;
        Ok(event)
    }

    async fn dispatch(
        &self,
        context: EffectHookContext,
        receipt: Option<CommittedEffectReceipt>,
    ) -> Result<HookDispatchResult, HookError> {
        let window = HookExecutionWindow::at_deadline(
            context.control().clone(),
            context.window().deadline(),
        )?;
        let binding = self.resolve_binding(&context).await?;
        let event = self.event(&binding, &context, receipt.as_ref())?;
        let operation = proof::operation(&context, event.phase())?;
        let bridge = self.clone();
        let saved = binding.clone();
        let input = event.clone();
        let ctx = context.clone();
        blocking(move || bridge.begin(&saved, &input, &ctx, receipt.as_ref())).await?;
        let result = self
            .runtime
            .dispatch(&binding, event.clone(), operation.clone(), window)
            .await?;
        if context.control().is_cancelled() {
            return Err(HookError::Cancelled);
        }
        let bridge = self.clone();
        blocking(move || {
            bridge
                .catalog
                .validate_current(&binding, &bridge.policy, bridge.native.digest())?;
            let actual = bridge
                .runtime
                .verify_dispatch(&binding, &event, &operation)?;
            if serde_json::to_value(&actual).map_err(invalid)?
                != serde_json::to_value(&result).map_err(invalid)?
            {
                return Err(HookError::Integrity("native readback differs".into()));
            }
            bridge.complete(&binding, &event, &context, &actual)?;
            bridge.verify_bridge(&binding, &event, &context)?;
            Ok(actual)
        })
        .await
    }
}

impl<L, J> EffectHookPort for AgentEffectHookBridge<L, J>
where
    L: LedgerStore + Clone + Send + Sync + 'static,
    J: FactJournal + Clone + Send + Sync + 'static,
{
    fn before_effect(
        &self,
        context: EffectHookContext,
    ) -> EffectHookFuture<'_, EffectHookDecision> {
        Box::pin(async move {
            match self.dispatch(context, None).await.map_err(map_error)? {
                HookDispatchResult::Continued { .. } => Ok(EffectHookDecision::Continue),
                HookDispatchResult::Denied { reason, .. } => {
                    Ok(EffectHookDecision::Denied { reason })
                }
            }
        })
    }

    fn after_receipt(
        &self,
        context: EffectHookContext,
        receipt: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        Box::pin(async move {
            match self
                .dispatch(context, Some(receipt))
                .await
                .map_err(map_error)?
            {
                HookDispatchResult::Continued { .. } => Ok(()),
                HookDispatchResult::Denied { .. } => Err(EffectHookError::Host {
                    message: "observer denied".into(),
                }),
            }
        })
    }

    fn verify_observation(
        &self,
        context: EffectHookContext,
        receipt: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        Box::pin(async move {
            let binding = self.resolve_binding(&context).await.map_err(map_error)?;
            let before = self.event(&binding, &context, None).map_err(map_error)?;
            let after = self
                .event(&binding, &context, Some(&receipt))
                .map_err(map_error)?;
            let bridge = self.clone();
            blocking(move || {
                // Historical verification deliberately does not validate current policy/revocation.
                for event in [&before, &after] {
                    let result = bridge.verify_bridge(&binding, event, &context)?;
                    if !matches!(result, HookDispatchResult::Continued { .. }) {
                        return Err(HookError::Integrity(
                            "saved effect has denied hook proof".into(),
                        ));
                    }
                }
                Ok(())
            })
            .await
            .map_err(map_error)
        })
    }
}

fn invalid(error: impl std::fmt::Display) -> HookError {
    HookError::Integrity(error.to_string())
}
fn map_error(error: HookError) -> EffectHookError {
    match error {
        HookError::Cancelled => EffectHookError::Cancelled,
        HookError::Expired => EffectHookError::TimedOut,
        HookError::Interrupted => EffectHookError::Incomplete {
            message: error.to_string(),
        },
        other => EffectHookError::Host {
            message: other.to_string(),
        },
    }
}
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, HookError> + Send + 'static,
) -> Result<T, HookError> {
    tokio::task::spawn_blocking(f).await.map_err(invalid)?
}
