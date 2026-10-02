//! Actual DurableTools receipts and real stores; host hooks are scripted ports.

use super::*;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::effect_hooks::EffectHookFuture;
use kolyan_core::{ExternalWait, ToolExecutionWindow};
use kolyan_ledger::{FactDraft, FactError, FactJournal, FactRecord, InMemoryLedger};
use kolyan_model::ToolResult;
use serde::Deserialize;

mod authority;
mod port;
mod reprepare_tests;
mod stores;

use authority::{invocation_for, prepare_call};
use port::Port;
use stores::{Backend, Journal, JournalBackend, Store};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    hook_window_ms: u64,
    drop_wait_ms: u64,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: String,
    tool: String,
    replay: bool,
    expected: Vec<String>,
    effects: usize,
    before: usize,
    after: usize,
    verify: usize,
    receipt: usize,
}

struct Tool {
    outcome: String,
    effects: Arc<AtomicUsize>,
    store: Store,
}

impl ToolExecutor for Tool {
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
                .map_err(|error| ToolError::InvalidBatch {
                    message: error.to_string(),
                })?;
            if prepare_call(invocation.prepared.call().clone())? != invocation.prepared {
                return Err(ToolError::InvalidBatch {
                    message: "test preparation changed".into(),
                });
            }
            if !self
                .store
                .execution_events_after("e", 0)
                .unwrap()
                .iter()
                .any(|event| event.kind == LedgerEventKind::EffectStarted)
            {
                return Err(ToolError::InvalidBatch {
                    message: "effect entered without Started".into(),
                });
            }
            self.effects.fetch_add(1, Ordering::SeqCst);
            match self.outcome.as_str() {
                "error" => Err(ToolError::Failed {
                    message: "original tool failure".into(),
                }),
                "uncertain" => Err(ToolError::Uncertain {
                    message: "scripted effect has no definitive result".into(),
                }),
                "waiting" => Ok(ToolOutcome::AwaitingExternal(ExternalWait {
                    wait_id: "fixture-wait".into(),
                    kind: "fixture.scripted_wait".into(),
                    schema_version: 1,
                    binding: json!({"admission":"scripted-only"}),
                })),
                _ => Ok(ToolOutcome::Completed(ToolResult {
                    call_id: invocation.prepared.call().id.clone(),
                    content: "original output".into(),
                    is_error: false,
                })),
            }
        })
    }
}

struct WaitVerifier;
impl ExternalWaitVerifier for WaitVerifier {
    fn verify_wait(&self, context: ExternalWaitContext) -> crate::ExternalVerificationFuture<'_> {
        Box::pin(async move {
            context.validate(&context.issued.scope)?;
            if context.wait.kind != "fixture.scripted_wait" {
                return Err(ToolError::InvalidBatch {
                    message: "unsupported scripted wait".into(),
                });
            }
            Ok(())
        })
    }

    fn verify_result(
        &self,
        _: ExternalWaitContext,
        _: ToolResult,
    ) -> crate::ExternalVerificationFuture<'_> {
        Box::pin(async {
            Err(ToolError::InvalidBatch {
                message: "no result proof configured".into(),
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
        name: "scripted.write".into(),
        arguments: json!({"content":"x"}),
    }
}

fn result_value(result: &Result<ToolOutcome, ToolError>) -> Value {
    match result {
        Ok(outcome) => json!({"kind":classify(result),"outcome":outcome}),
        Err(error) => json!({"kind":classify(result),"error":error,"message":error.to_string()}),
    }
}

fn classify(result: &Result<ToolOutcome, ToolError>) -> &'static str {
    match result {
        Ok(ToolOutcome::Completed(_)) => "completed",
        Ok(ToolOutcome::AwaitingExternal(_)) => "waiting",
        Err(ToolError::InvalidBatch { .. }) => "invalid_batch",
        Err(ToolError::PolicyDenied { .. }) => "policy_denied",
        Err(ToolError::Cancelled) => "cancelled",
        Err(ToolError::TimedOut) => "timed_out",
        Err(ToolError::Uncertain { .. }) => "uncertain",
        Err(ToolError::Failed { .. }) => "failed",
        Err(ToolError::Unavailable { .. }) => "unavailable",
    }
}

#[tokio::test]
async fn effect_hook_consumer_dataset() {
    let input: Value = serde_json::from_str(include_str!("effect_hooks_tests/cases.json")).unwrap();
    let plan: Plan = serde_json::from_value(input.clone()).unwrap();
    let proof = tempfile::Builder::new()
        .prefix("kolyan-effect-hooks-")
        .tempdir()
        .unwrap()
        .keep();
    let path = proof.join("actual.jsonl");
    let mut output = std::fs::File::create(&path).unwrap();
    writeln!(
        output,
        "{}",
        json!({"event":"plan","backends":["memory","sqlite"],"input":input,"attempts":1})
    )
    .unwrap();
    output.flush().unwrap();
    output.sync_all().unwrap();
    let mut comparisons = Vec::new();
    for backend in ["memory", "sqlite"] {
        for case in &plan.cases {
            let directory = proof.join(format!("{backend}-{}", case.id));
            std::fs::create_dir(&directory).unwrap();
            let store = Store {
                mode: case.mode.clone(),
                backend: match backend {
                    "memory" => Backend::Memory(InMemoryLedger::default()),
                    _ => Backend::Sqlite(
                        kolyan_ledger::SqliteLedger::open(directory.join("ledger.sqlite")).unwrap(),
                    ),
                },
            };
            let journal = Arc::new(Journal {
                backend: match backend {
                    "memory" => JournalBackend::Memory(kolyan_ledger::MemoryFactJournal::default()),
                    _ => JournalBackend::Sqlite(
                        kolyan_ledger::SqliteFactJournal::open(directory.join("journal.sqlite"))
                            .unwrap(),
                    ),
                },
                fail_kind: match case.mode.as_str() {
                    "before_storage" => Some("fixture.hook.before"),
                    "after_storage" => Some("fixture.hook.after_completed"),
                    _ => None,
                },
            });
            let calls = Arc::new(Mutex::new(Vec::new()));
            let entered = Arc::new(tokio::sync::Notify::new());
            let make_port = || {
                Arc::new(Port {
                    mode: case.mode.clone(),
                    store: store.clone(),
                    journal: journal.clone(),
                    calls: calls.clone(),
                    after_entered: entered.clone(),
                })
            };
            let effects = Arc::new(AtomicUsize::new(0));
            let make_tools = |configured: bool| {
                let tools = DurableTools::new(
                    store.clone(),
                    key(),
                    Tool {
                        outcome: case.tool.clone(),
                        effects: effects.clone(),
                        store: store.clone(),
                    },
                )
                .with_wait_verifier(Arc::new(WaitVerifier));
                if configured {
                    tools.with_effect_hooks(make_port())
                } else {
                    tools
                }
            };
            let mut invocation = invocation_for(call());
            invocation.window = ToolExecutionWindow::at_deadline(if case.mode == "expired" {
                Instant::now() - Duration::from_millis(plan.hook_window_ms)
            } else {
                Instant::now() + Duration::from_millis(plan.hook_window_ms)
            });
            let original_authority = json!({"prepared":invocation.prepared,"grant":invocation.grant,
                "scope":invocation.scope,"policy_revision":invocation.policy_revision,
                "deadline":format!("{:?}",invocation.window.deadline())});
            let configured = !matches!(case.mode.as_str(), "unconfigured" | "replay_missing");
            let tools = make_tools(configured);
            let mut results = Vec::new();
            if case.mode == "after_drop" {
                {
                    let mut future = tools.execute_invocation(invocation);
                    tokio::select! {
                        biased;
                        observation = tokio::time::timeout(Duration::from_millis(plan.drop_wait_ms),entered.notified()) => {
                            results.push(json!({"kind":if observation.is_ok() {"dropped"} else {"drop_wait_timed_out"},
                                "detail":"caller dropped pending future","observed_after_started":observation.is_ok()}));
                        }
                        result = &mut future => results.push(result_value(&result)),
                    }
                }
            } else {
                results.push(result_value(&tools.execute_invocation(invocation).await));
            }
            drop(tools);
            if case.replay {
                // A new adapter/port reads the same real stores, never memory proof.
                let tools = make_tools(case.mode != "unconfigured");
                let result = tools.execute_invocation(invocation_for(call())).await;
                results.push(result_value(&result));
            }
            let events = store.execution_events_after("e", 0).unwrap();
            let facts = journal.read("scripted-effect-hook", 0, 64).unwrap();
            let observations = calls.lock().unwrap().clone();
            let row = json!({"event":"actual","id":case.id,"backend":backend,"attempt":1,
                "original_authority":original_authority,"results":results,"calls":observations,
                "ledger":events,"journal":facts,"effects":effects.load(Ordering::SeqCst)});
            writeln!(output, "{row}").unwrap();
            output.flush().unwrap();
            output.sync_all().unwrap();
            comparisons.push((row, case));
        }
    }
    drop(output);
    eprintln!("EFFECT_HOOK_ACTUAL={}", path.display());
    let physical: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(physical.len(), 1 + comparisons.len());
    for (physical, (row, case)) in physical.iter().skip(1).zip(comparisons) {
        assert_eq!(physical, &row);
        let label = format!("{}/{}", row["backend"], case.id);
        let kinds: Vec<_> = row["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|result| result["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, case.expected, "{label}");
        assert_eq!(row["effects"], case.effects, "{label}");
        for (phase, expected) in [
            ("before", case.before),
            ("after", case.after),
            ("verify", case.verify),
        ] {
            assert_eq!(
                row["calls"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|item| item["phase"] == phase)
                    .count(),
                expected,
                "{label}/{phase}"
            );
        }
        let receipts: Vec<_> = row["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "effect_receipt")
            .collect();
        assert_eq!(receipts.len(), case.receipt, "{label}");
        if case.effects == 0 {
            assert!(
                !row["ledger"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|event| event["kind"] == "effect_started"
                        || event["kind"] == "effect_authorized"),
                "{label}"
            );
        }
        for observed in row["calls"].as_array().unwrap() {
            if observed["phase"] == "before" {
                assert!(
                    !observed["ledger_at_call"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|event| event["kind"] == "effect_authorized"
                            || event["kind"] == "effect_started"),
                    "{label}"
                );
                assert_eq!(
                    observed["deadline"], row["original_authority"]["deadline"],
                    "{label}"
                );
                assert_eq!(
                    observed["context"]["issued"]["prepared"],
                    row["original_authority"]["prepared"],
                    "{label}"
                );
                assert_eq!(
                    observed["context"]["issued"]["grant"], row["original_authority"]["grant"],
                    "{label}"
                );
                assert_eq!(
                    observed["context"]["issued"]["scope"], row["original_authority"]["scope"],
                    "{label}"
                );
            }
            if observed["phase"] == "after" {
                assert!(
                    observed["ledger_at_call"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|event| event["kind"] == "effect_receipt"),
                    "{label}"
                );
            }
        }
        for receipt in receipts {
            if case.tool == "error" {
                assert_eq!(
                    receipt["payload"]["error"]["message"], "original tool failure",
                    "{label}"
                );
            } else {
                assert_eq!(
                    receipt["payload"]["output"]["content"], "original output",
                    "{label}"
                );
            }
            assert!(receipt["cursor"].as_u64().unwrap() > 0, "{label}");
        }
    }
}
