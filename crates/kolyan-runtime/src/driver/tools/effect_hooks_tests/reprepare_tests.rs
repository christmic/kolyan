//! A real awaited host barrier changes the executor's current preparation.

use super::*;
use kolyan_policy::PreparedCall;
use std::sync::atomic::AtomicBool;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecheckPlan {
    window_ms: u64,
    cases: Vec<RecheckCase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecheckCase {
    id: String,
    mode: String,
    expected: String,
}

struct CurrentTool {
    mode: String,
    changed: Arc<AtomicBool>,
    control: kolyan_core::TurnControl,
    preparations: Arc<Mutex<Vec<Value>>>,
    effects: Arc<AtomicUsize>,
}

impl ToolExecutor for CurrentTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            let changed = self.changed.load(Ordering::SeqCst);
            self.preparations
                .lock()
                .unwrap()
                .push(json!({"event":"prepare_started","call":call,"after_barrier":changed}));
            let original = prepare_call(call.clone())?;
            let result = if changed {
                match self.mode.as_str() {
                    "revision" => PreparedCall::new(
                        call,
                        "changed-v2".into(),
                        original.claim().clone(),
                        original.requirements().clone(),
                    )
                    .map_err(|error| ToolError::InvalidBatch {
                        message: error.to_string(),
                    }),
                    "binding" => original
                        .with_execution_binding(json!({"scope":"foreign-host-resource"}))
                        .map_err(|error| ToolError::InvalidBatch {
                            message: error.to_string(),
                        }),
                    "error" => Err(ToolError::Failed {
                        message: "current prepare failed".into(),
                    }),
                    "cancel" => {
                        self.control.cancel();
                        std::future::pending().await
                    }
                    _ => std::future::pending().await,
                }
            } else {
                Ok(original)
            };
            self.preparations
                .lock()
                .unwrap()
                .push(json!({"event":"prepare_returned","result":result}));
            result
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            self.effects.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "unexpected effect".into(),
                is_error: false,
            }))
        })
    }
}

struct BarrierPort(Arc<AtomicBool>);
impl EffectHookPort for BarrierPort {
    fn before_effect(&self, _: EffectHookContext) -> EffectHookFuture<'_, EffectHookDecision> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            self.0.store(true, Ordering::SeqCst);
            Ok(EffectHookDecision::Continue)
        })
    }
    fn after_receipt(
        &self,
        _: EffectHookContext,
        _: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        Box::pin(async {
            Err(EffectHookError::Host {
                message: "unexpected observer".into(),
            })
        })
    }
    fn verify_observation(
        &self,
        _: EffectHookContext,
        _: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        Box::pin(async {
            Err(EffectHookError::Host {
                message: "unexpected saved receipt".into(),
            })
        })
    }
}

#[tokio::test]
async fn post_hook_current_preparation_dataset() {
    let input: Value = serde_json::from_str(include_str!("reprepare.json")).unwrap();
    let plan: RecheckPlan = serde_json::from_value(input.clone()).unwrap();
    let proof = tempfile::Builder::new()
        .prefix("kolyan-hook-reprepare-")
        .tempdir()
        .unwrap()
        .keep();
    let path = proof.join("actual.jsonl");
    let mut output = std::fs::File::create(&path).unwrap();
    writeln!(
        output,
        "{}",
        json!({"event":"plan","input":input,"attempts":1})
    )
    .unwrap();
    output.sync_all().unwrap();
    for case in &plan.cases {
        let ledger = InMemoryLedger::default();
        let preparations = Arc::new(Mutex::new(Vec::new()));
        let effects = Arc::new(AtomicUsize::new(0));
        let changed = Arc::new(AtomicBool::new(false));
        let mut invocation = invocation_for(call());
        invocation.window = ToolExecutionWindow::at_deadline(
            Instant::now() + Duration::from_millis(plan.window_ms),
        );
        let tools = DurableTools::new(
            ledger.clone(),
            key(),
            CurrentTool {
                mode: case.mode.clone(),
                changed: changed.clone(),
                control: invocation.control.clone(),
                preparations: preparations.clone(),
                effects: effects.clone(),
            },
        )
        .with_effect_hooks(Arc::new(BarrierPort(changed.clone())));
        let initial = tools.prepare(call()).await.unwrap();
        let authority = json!({"prepared":invocation.prepared,"grant":invocation.grant,"scope":invocation.scope,
            "policy_revision":invocation.policy_revision,"deadline":format!("{:?}",invocation.window.deadline())});
        let result = tools.execute_invocation(invocation).await;
        writeln!(output,"{}",json!({"event":"actual","id":case.id,"initial":initial,"authority":authority,
            "result":result_value(&result),"preparations":preparations.lock().unwrap().clone(),
            "barrier_passed":changed.load(Ordering::SeqCst),"effects":effects.load(Ordering::SeqCst),
            "ledger":ledger.execution_events_after("e",0).unwrap()})).unwrap();
        output.flush().unwrap();
        output.sync_all().unwrap();
    }
    drop(output);
    eprintln!("HOOK_REPREPARE_ACTUAL={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), plan.cases.len() + 1);
    for (row, case) in rows.iter().skip(1).zip(&plan.cases) {
        assert_eq!(row["result"]["kind"], case.expected, "{}", case.id);
        assert_eq!(row["effects"], 0, "{}", case.id);
        assert_eq!(row["barrier_passed"], true);
        assert!(
            !row["ledger"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["kind"] == "effect_authorized"
                    || event["kind"] == "effect_started")
        );
        assert_eq!(
            row["preparations"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| item["event"] == "prepare_started")
                .count(),
            2
        );
    }
}
