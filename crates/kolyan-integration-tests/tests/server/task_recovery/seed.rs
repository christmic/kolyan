//! Generate crash-window inputs through the public prepared Runtime contract.
//! Source generation is separate from measured recovery: no effects or receipts
//! are fabricated, and no private evidence hash/encoding is reproduced here.

use std::time::Duration;

use kolyan_ledger::InMemoryLedger;
use kolyan_runtime::DurableTurnDriver;
use serde::Deserialize;
use tokio::sync::Notify;

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedPlan {
    source: SeedSource,
    provider: String,
    events: Vec<LedgerEventKind>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SeedSource {
    PreparedRuntime,
}

struct PendingFixture {
    entered: Arc<Notify>,
}

impl ToolExecutor for PendingFixture {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            if call.name != "fixture.lookup" {
                return Err(kolyan_core::ToolError::Unavailable { name: call.name });
            }
            trusted_tools::prepare(call)
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            trusted_tools::validate(&invocation)?;
            // Runtime has durably recorded Started before invoking this port.
            // Dropping this pending future models interruption with no receipt,
            // not rollback evidence or a successful tool observation.
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

pub(super) async fn prepared_window(
    context: &Context<'_>,
    binding: &AttemptBinding,
    plan: &Value,
) -> Result<Vec<LedgerEvent>, Failure> {
    let plan: SeedPlan = serde_json::from_value(plan.clone())?;
    let SeedSource::PreparedRuntime = plan.source;
    assert_eq!(
        plan.events,
        [
            LedgerEventKind::EffectPrepared,
            LedgerEventKind::EffectAuthorized,
            LedgerEventKind::EffectStarted
        ]
    );
    let ledger = InMemoryLedger::default();
    let driver = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    let entered = Arc::new(Notify::new());
    let counters = Arc::new(Counters::default());
    let executor = TurnExecutor::with_tools(
        FixtureProvider {
            script: context.data["providers"][&plan.provider].clone(),
            counters: counters.clone(),
        },
        PendingFixture {
            entered: entered.clone(),
        },
    )
    .with_policy_engine(trusted_tools::policy());
    {
        let future = driver.start(
            executor,
            TurnRequest {
                turn_id: binding.execution.turn_id.clone(),
                model_request: serde_json::from_value(context.data["request"].clone())?,
                config: TurnConfig {
                    max_steps: context.data["max_steps"].as_u64().unwrap() as usize,
                    ..Default::default()
                },
            },
            &binding.execution.session_id,
            &binding.execution.execution_id,
        );
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut future => Err::<(), Failure>(format!("seed generation ended before its crash window: {result:?}").into()),
                _ = entered.notified() => Ok(()),
            }
        }).await??;
    }
    let events = ledger.events_after(0)?;
    write_jsonl(context.root.join("seed-source.execution.jsonl"), &events)?;
    write_jsonl(
        context.root.join("seed-source.requests.jsonl"),
        &counters.requests.lock().unwrap(),
    )?;
    assert_eq!(counters.models.load(Ordering::SeqCst), 1);
    assert_eq!(counters.tools.load(Ordering::SeqCst), 0);
    assert!(!events.iter().any(|event| matches!(
        event.kind,
        LedgerEventKind::EffectReceipt | LedgerEventKind::EffectUncertain
    )));
    let effect_id = context.effect_id(binding);
    let mut selected = Vec::new();
    for kind in plan.events {
        let found: Vec<_> = events.iter().filter(|event| event.kind == kind).collect();
        assert_eq!(found.len(), 1, "exactly one seed event per declared kind");
        let mut event = found[0].clone();
        assert_eq!(event.payload["effect_id"], effect_id);
        assert_eq!(event.execution_id, binding.execution.execution_id);
        assert_eq!(event.turn_id, binding.execution.turn_id);
        if event.kind == LedgerEventKind::EffectPrepared {
            assert_eq!(event.payload["binding_kind"], "prepared_tool_v1");
            let prepared: kolyan_policy::PreparedCall =
                serde_json::from_value(event.payload["input"]["prepared"].clone())?;
            let scope: kolyan_policy::ToolExecutionScope =
                serde_json::from_value(event.payload["input"]["scope"].clone())?;
            assert_eq!(scope.execution.session_id, binding.execution.session_id);
            assert_eq!(scope.execution.turn_id, binding.execution.turn_id);
            assert_eq!(scope.execution.execution_id, binding.execution.execution_id);
            assert_eq!(prepared.execution_binding(), &Value::Null);
        }
        // Destination journal chooses the cursor; retain every bound payload,
        // identity and idempotency key exactly as produced by Runtime.
        event.cursor = 0;
        selected.push(event);
    }
    write_jsonl(context.root.join("seed-effects.actual.jsonl"), &selected)?;
    Ok(selected)
}
