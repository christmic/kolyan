//! Abrupt process exit at deterministic durability boundaries, never just a
//! dropped Rust object. The parent reopens storage in a different process.

use futures_util::stream;
use kolyan_core::{
    ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture, TurnConfig,
    TurnExecutor, TurnRequest,
};
use kolyan_ledger::{FileLedger, LedgerError, LedgerEvent, LedgerEventKind, LedgerStore};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture, StopReason, ToolCall, ToolResult,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PathScope, PolicyEngine,
    PreparedCall, ResourceClaim, ToolManifest, ToolRequirements,
};
use kolyan_runtime::{
    AdmissionDecision, AdmissionPort, DurableTurnDriver, EffectDisposition, EffectExecutor,
    EffectGrant, EffectOutcome, EffectRequest, ExecutionKey, ExecutionRuntime,
    RuntimeExecutionError,
};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};
use std::sync::Arc;
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
    fn query(&self, query: &kolyan_ledger::LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.query(query)
    }
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
impl EffectTool {
    fn manifest(&self) -> ToolManifest {
        ToolManifest {
            tool_name: "effect.write".into(),
            capabilities: [Capability::FilesystemWrite].into(),
            effects: [Effect::Create].into(),
            path_scopes: vec![PathScope::new(self.0.to_string_lossy())],
            idempotency: Idempotency::NonIdempotent,
            approval: ApprovalMode::Never,
        }
    }

    fn policy(&self) -> Arc<PolicyEngine> {
        let mut policy = PolicyEngine::default();
        policy.register(self.manifest());
        Arc::new(policy)
    }
}

impl ToolExecutor for EffectTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            if call.name != "effect.write" {
                return Err(ToolError::Unavailable { name: call.name });
            }
            if call.arguments != json!({"value":"once"}) {
                return Err(ToolError::Failed {
                    message: "crash fixture only writes the literal once".into(),
                });
            }
            let declared = self.manifest();
            PreparedCall::new(
                call,
                "integration-fixture-v1".into(),
                InvocationClaim {
                    tool_name: declared.tool_name,
                    capabilities: declared.capabilities,
                    effects: declared.effects,
                    resource: ResourceClaim {
                        path: Some(self.0.to_string_lossy().into_owned()),
                    },
                    idempotency: declared.idempotency,
                },
                ToolRequirements {
                    process_sandbox: false,
                    max_output_bytes: 1024 * 1024,
                    timeout_ms: 30000,
                },
            )
            .map_err(|error| ToolError::Failed {
                message: error.to_string(),
            })
        })
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
            if self.prepare(call.clone()).await? != invocation.prepared {
                return Err(ToolError::PolicyDenied {
                    message: "crash fixture target or implementation changed".into(),
                });
            }
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
            Ok(kolyan_core::ToolOutcome::Completed(ToolResult {
                call_id: call.id,
                content: "once".into(),
                is_error: false,
            }))
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
    let tool = EffectTool(root.join("effect.txt"));
    let policy = tool.policy();
    let result = DurableTurnDriver::new(ledger, NoopTraceSink)
        .start(
            TurnExecutor::with_tools(Provider, tool).with_policy_engine(policy),
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
