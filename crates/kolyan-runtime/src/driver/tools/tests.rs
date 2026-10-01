use super::*;
mod authority;
mod preparation;
mod scoped;
use kolyan_core::{ToolInvocation, ToolPreparationFuture};
use kolyan_ledger::InMemoryLedger;
use kolyan_model::ToolResult;
use preparation::{invocation_for, prepare_call};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct CountingTool(Arc<AtomicUsize>);
impl ToolExecutor for CountingTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move { prepare_call(call) })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &invocation.policy_revision,
                    &invocation.scope,
                )
                .map_err(|error| ToolError::PolicyDenied {
                    message: error.to_string(),
                })?;
            let call = invocation.prepared.call().clone();
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ToolResult {
                call_id: call.id,
                content: "persisted output".into(),
                is_error: false,
            })
        })
    }
}

fn key() -> RuntimeTurnKey {
    RuntimeTurnKey {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    }
}
fn call() -> ToolCall {
    ToolCall {
        id: "c".into(),
        name: "write".into(),
        arguments: json!({"content":"x"}),
    }
}

#[tokio::test]
async fn completed_receipt_replays_without_repeating_the_side_effect() {
    let ledger = InMemoryLedger::default();
    let count = Arc::new(AtomicUsize::new(0));
    let first = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()));
    let expected = first
        .execute_invocation(invocation_for(call()))
        .await
        .unwrap();
    drop(first);
    let reopened = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()));
    assert_eq!(
        reopened
            .execute_invocation(invocation_for(call()))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut changed = call();
    changed.arguments = json!({"content":"different"});
    assert!(
        reopened
            .execute_invocation(invocation_for(changed))
            .await
            .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn started_without_receipt_is_uncertain_and_never_reexecuted() {
    let ledger = InMemoryLedger::default();
    append_once(
        &ledger,
        "e",
        "t",
        "effect/step/c/started",
        LedgerEventKind::EffectStarted,
        json!({"effect_id":"step/c"}),
    )
    .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let tools = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()));
    assert!(
        tools
            .execute_invocation(invocation_for(call()))
            .await
            .unwrap_err()
            .to_string()
            .contains("uncertain")
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        ledger
            .events_after(0)
            .unwrap()
            .iter()
            .any(|e| e.kind == LedgerEventKind::EffectUncertain)
    );
}

#[tokio::test]
async fn cancelled_execution_cannot_start_a_tool_effect() {
    let ledger = InMemoryLedger::default();
    append_once(
        &ledger,
        "e",
        "t",
        "cancel",
        LedgerEventKind::ExecutionCancelled,
        Value::Null,
    )
    .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let tools = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()));
    assert_eq!(
        tools.execute_invocation(invocation_for(call())).await,
        Err(ToolError::Cancelled)
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        !ledger
            .events_after(0)
            .unwrap()
            .iter()
            .any(|e| e.kind == LedgerEventKind::EffectStarted)
    );
}
