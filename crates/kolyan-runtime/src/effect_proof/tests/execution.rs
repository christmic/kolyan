//! Scripted Core/Runtime execution supplies actual persisted facts; inspecting a
//! rebuilt SQLite store must not call the retained tool/provider again.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures_util::stream;
use kolyan_core::{
    ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture, TurnConfig,
    TurnExecutor, TurnRequest,
};
use kolyan_ledger::{InMemoryLedger, SqliteLedger};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRef, ModelRequest,
    ModelResponse, ProviderFuture, StopReason, TokenUsage, ToolCall, ToolChoice, ToolDefinition,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PolicyEngine, ResourceClaim,
    ToolManifest, ToolRequirements,
};
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use serde_json::json;

use super::*;
use crate::DurableTurnDriver;

#[derive(Deserialize)]
struct ExecutionCase {
    id: String,
    expected: String,
    provider_calls: usize,
}

struct ScriptedProvider(Arc<AtomicUsize>);
impl ModelProvider for ScriptedProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        let response = ModelResponse {
            id: request.request_id,
            model: request.model,
            content: if first {
                vec![ContentBlock::ToolCall {
                    call: ToolCall {
                        id: "native/调用".into(),
                        name: "file.write".into(),
                        arguments: json!({"path":"output.txt","content":"done"}),
                    },
                }]
            } else {
                vec![ContentBlock::Text {
                    text: "done".into(),
                }]
            },
            structured_output: None,
            stop_reason: if first {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

struct CountingTool {
    count: Arc<AtomicUsize>,
    case: String,
}
impl ToolExecutor for CountingTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            PreparedCall::new(
                call,
                "proof-execution-tool-v1".into(),
                InvocationClaim {
                    tool_name: "file.write".into(),
                    capabilities: [Capability::FilesystemWrite].into(),
                    effects: [Effect::Update].into(),
                    resource: ResourceClaim { path: None },
                    idempotency: Idempotency::NonIdempotent,
                },
                ToolRequirements {
                    process_sandbox: false,
                    max_output_bytes: 4096,
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
            self.count.fetch_add(1, Ordering::SeqCst);
            match self.case.as_str() {
                "failed_receipt" => Err(ToolError::Failed {
                    message: "definitive fixture failure".into(),
                }),
                "uncertain_effect" => Err(ToolError::Uncertain {
                    message: "unknown fixture disposition".into(),
                }),
                _ => Ok(ToolOutcome::Completed(ToolResult {
                    call_id: invocation.prepared.call().id.clone(),
                    content: "done".into(),
                    is_error: self.case == "completed_error_result",
                })),
            }
        })
    }
}

#[tokio::test]
async fn actual_runtime_sources_are_inspected_without_new_execution() {
    let cases: Vec<ExecutionCase> =
        serde_json::from_str(include_str!("execution_cases.json")).unwrap();
    let artifacts = tempfile::Builder::new()
        .prefix("kolyan-effect-proof-runtime-")
        .tempdir()
        .unwrap()
        .keep();
    let mut rows = Vec::new();
    for case in &cases {
        for sqlite in [false, true] {
            let counts = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let path = artifacts.join(format!("{}.db", case.id));
            let events = if sqlite {
                let ledger = SqliteLedger::open(&path).unwrap();
                execute(ledger.clone(), case, &counts).await;
                ledger.events_after(0).unwrap()
            } else {
                let ledger = InMemoryLedger::default();
                execute(ledger.clone(), case, &counts).await;
                ledger.events_after(0).unwrap()
            };
            let terminal = events
                .iter()
                .find(|event| {
                    matches!(
                        event.kind,
                        LedgerEventKind::TurnFailed
                            | LedgerEventKind::TurnCancelled
                            | LedgerEventKind::TurnTimedOut
                    ) || (event.kind == LedgerEventKind::TurnCompleted
                        && event.payload.get("reason").is_some())
                })
                .unwrap();
            let prepared = events
                .iter()
                .find(|event| event.kind == LedgerEventKind::EffectPrepared)
                .unwrap();
            let request = EffectProofRequest {
                execution: ExecutionKey {
                    session_id: "execution-session".into(),
                    turn_id: "execution-turn".into(),
                    execution_id: "execution-proof".into(),
                },
                effect_id: prepared.payload["effect_id"].as_str().unwrap().into(),
                terminal: coordinate(terminal),
                max_input_bytes: 65536,
                max_result_bytes: 4096,
            };
            let before = [
                counts.0.load(Ordering::SeqCst),
                counts.1.load(Ordering::SeqCst),
            ];
            let inner: Box<dyn LedgerStore> = if sqlite {
                Box::new(SqliteLedger::open(&path).unwrap())
            } else {
                let ledger = InMemoryLedger::default();
                for mut event in events.clone() {
                    event.cursor = 0;
                    ledger.append(event).unwrap();
                }
                Box::new(ledger)
            };
            let reader = support::ObservedLedger::new(inner, "actual-runtime");
            let first = inspect_effect_proof(&reader, &request);
            let second = inspect_effect_proof(&reader, &request);
            let first = observe(&first);
            let second = observe(&second);
            rows.push(
                json!({"case":case.id,"sqlite":sqlite,"expected":case.expected,
                "expected_provider_calls":case.provider_calls,"actual":first.0,"proof":first.1,
                "repeat_actual":second.0,"repeat_proof":second.1,"events_before":events,
                "events_after":reader.inner.events_after(0).unwrap(),"counts_before":before,
                "counts_after":[counts.0.load(Ordering::SeqCst),counts.1.load(Ordering::SeqCst)],
                "inspection_writes":reader.writes.load(Ordering::SeqCst),
                "inspection_unbounded_reads":reader.unbounded.load(Ordering::SeqCst)}),
            );
        }
    }
    let path = artifacts.join("actual.jsonl");
    std::fs::write(
        &path,
        rows.iter()
            .map(|row| serde_json::to_string(row).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
    println!(
        "actual Runtime effect proof: {} rows exported to {}",
        rows.len(),
        path.display()
    );
    let exported: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for row in exported {
        assert_eq!(row["actual"], row["expected"], "{}", row["case"]);
        assert_eq!(row["repeat_actual"], row["actual"]);
        assert_eq!(row["repeat_proof"], row["proof"]);
        assert_eq!(row["counts_before"], row["counts_after"]);
        assert_eq!(row["counts_before"][0], row["expected_provider_calls"]);
        assert_eq!(row["counts_before"][1], 1);
        assert_eq!(row["events_before"], row["events_after"]);
        assert_eq!(row["inspection_writes"], 0);
        assert_eq!(row["inspection_unbounded_reads"], 0);
    }
}

async fn execute<L: LedgerStore + Clone + 'static>(
    ledger: L,
    case: &ExecutionCase,
    counts: &(Arc<AtomicUsize>, Arc<AtomicUsize>),
) {
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    let executor = TurnExecutor::with_tools(
        ScriptedProvider(counts.0.clone()),
        CountingTool {
            count: counts.1.clone(),
            case: case.id.clone(),
        },
    )
    .with_policy_engine(Arc::new(policy));
    let request = TurnRequest {
        turn_id: "execution-turn".into(),
        model_request: ModelRequest {
            request_id: "execution-request".into(),
            model: ModelRef::new("scripted", "effect-proof"),
            system: vec![],
            messages: vec![],
            tools: vec![ToolDefinition {
                name: "file.write".into(),
                description: None,
                input_schema: json!({"type":"object","properties":{
                "path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}),
            }],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: Value::Null,
        },
        config: TurnConfig {
            max_steps: 3,
            max_tool_calls: Some(1),
            deadline: None,
        },
    };
    // The failure rows intentionally return errors after genuine terminal writes.
    let _result = DurableTurnDriver::new(ledger, NoopTraceSink)
        .start(executor, request, "execution-session", "execution-proof")
        .await;
}
