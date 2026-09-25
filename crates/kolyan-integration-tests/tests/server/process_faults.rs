//! Abrupt process exit at deterministic durability boundaries, never just a
//! dropped Rust object. The parent reopens storage in a different process.

use futures_util::stream;
use kolyan_core::{ToolError, ToolExecutor, ToolFuture, TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FileLedger, LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture, StopReason, ToolCall, ToolResult,
};
use kolyan_runtime::{
    AdmissionDecision, AdmissionPort, DurableTurnDriver, EffectDisposition, EffectExecutor,
    EffectGrant, EffectOutcome, EffectRequest, ExecutionKey, ExecutionRuntime,
    RuntimeExecutionError,
};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};
use std::{fs, path::PathBuf, process::Command};

#[derive(Clone)]
struct FaultLedger {
    inner: FileLedger,
    phase: String,
}

impl FaultLedger {
    fn before(&self, event: &LedgerEvent) {
        if self.phase == "before_receipt" && event.kind == LedgerEventKind::EffectReceipt {
            std::process::exit(77);
        }
    }
    fn after(&self, event: &LedgerEvent) {
        if (self.phase == "before_effect" && event.kind == LedgerEventKind::EffectStarted)
            || (self.phase == "after_receipt" && event.kind == LedgerEventKind::EffectReceipt)
        {
            std::process::exit(77);
        }
    }
}

impl LedgerStore for FaultLedger {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.before(&event);
        let event = self.inner.append(event)?;
        self.after(&event);
        Ok(event)
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.before(&event);
        let event = self.inner.append_unless_cancelled(event)?;
        self.after(&event);
        Ok(event)
    }
    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.events_after(cursor)
    }
    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.inner.claim(key)
    }
}

struct Provider;
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(ModelResponse {
                    id: "response".into(),
                    model: request.model,
                    content: vec![ContentBlock::ToolCall {
                        call: ToolCall {
                            id: "call".into(),
                            name: "effect.write".into(),
                            arguments: json!({"value":"once"}),
                        },
                    }],
                    structured_output: None,
                    stop_reason: StopReason::ToolUse,
                    usage: Default::default(),
                    metadata: Value::Null,
                })),
            ])) as ModelEventStream)
        })
    }
}

struct EffectTool(PathBuf);
impl ToolExecutor for EffectTool {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        Box::pin(async move {
            use std::io::Write;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.0)
                .map_err(|error| ToolError::Failed {
                    message: error.to_string(),
                })?;
            file.write_all(b"once").unwrap();
            file.sync_all().unwrap();
            Ok(ToolResult {
                call_id: call.id,
                content: "once".into(),
                is_error: false,
            })
        })
    }
}

#[tokio::test]
async fn crash_worker() {
    let Ok(root) = std::env::var("KOLYAN_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let phase = std::env::var("KOLYAN_CRASH_PHASE").unwrap();
    let ledger = FaultLedger {
        inner: FileLedger::open(root.join("ledger.jsonl")).unwrap(),
        phase,
    };
    let model_request = serde_json::from_value(json!({
        "request_id":"r", "model":{"provider":"fixture","model":"fixture"},
        "system":[],"messages":[],"tools":[],"tool_choice":"auto","output_format":null,
        "prompt_cache":null,"reasoning":null,"max_output_tokens":null,"extensions":{}
    }))
    .unwrap();
    let result = DurableTurnDriver::new(ledger, NoopTraceSink)
        .start(
            TurnExecutor::with_tools(Provider, EffectTool(root.join("effect.txt"))),
            TurnRequest {
                turn_id: "turn".into(),
                model_request,
                config: TurnConfig {
                    max_steps: 2,
                    ..Default::default()
                },
            },
            "session",
            "execution",
        )
        .await;
    panic!("fault did not terminate the child: {result:?}");
}

struct NeverReplay;
impl AdmissionPort for NeverReplay {
    fn decide(
        &self,
        _: &ExecutionKey,
        _: &EffectRequest,
    ) -> Result<AdmissionDecision, RuntimeExecutionError> {
        panic!("recovery must not seek new admission")
    }
}
impl EffectExecutor for NeverReplay {
    fn execute(
        &self,
        _: &ExecutionKey,
        _: &EffectRequest,
        _: &EffectGrant,
    ) -> Result<EffectOutcome, RuntimeExecutionError> {
        panic!("recovery must not replay a side effect")
    }
}

#[test]
fn crashed_tool_invocations_recover_from_the_same_effect_receipts() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/runtime_crash.json")).unwrap();
    for case in cases {
        let root = tempfile::Builder::new()
            .prefix("kolyan-crash-")
            .tempdir()
            .unwrap()
            .keep();
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "faults::crash_worker", "--nocapture"])
            .env("KOLYAN_CRASH_ROOT", &root)
            .env("KOLYAN_CRASH_PHASE", case["phase"].as_str().unwrap())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(77));
        assert_eq!(
            root.join("effect.txt").exists(),
            case["side_effect"].as_bool().unwrap()
        );
        let ledger = FileLedger::open(root.join("ledger.jsonl")).unwrap();
        let events = ledger.events_after(0).unwrap();
        assert_eq!(
            events
                .iter()
                .any(|event| event.kind == LedgerEventKind::EffectReceipt),
            case["receipt"].as_bool().unwrap()
        );
        let request: EffectRequest = serde_json::from_value(
            events
                .iter()
                .find(|event| event.kind == LedgerEventKind::EffectPrepared)
                .unwrap()
                .payload
                .clone(),
        )
        .unwrap();
        let runtime = ExecutionRuntime::new(ledger, NeverReplay, NeverReplay);
        let outcome = runtime
            .apply_effect(
                &ExecutionKey {
                    session_id: "session".into(),
                    turn_id: "turn".into(),
                    execution_id: "execution".into(),
                },
                &request,
            )
            .unwrap();
        match case["outcome"].as_str().unwrap() {
            "completed" => assert!(matches!(outcome, EffectDisposition::Completed { .. })),
            "uncertain" => assert!(matches!(outcome, EffectDisposition::Uncertain { .. })),
            _ => unreachable!(),
        }
        fs::write(
            root.join("outcome.json"),
            json!({"case":case,"outcome":format!("{outcome:?}")}).to_string(),
        )
        .unwrap();
        eprintln!("crash evidence: {}", root.display());
    }
}
