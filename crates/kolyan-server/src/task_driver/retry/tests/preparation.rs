//! Seed exact prepared authority through Runtime, not handwritten hash payloads.
//! The fixture executor never modifies files; interruption leaves Started without
//! a receipt and therefore provides no implicit NotCommitted evidence.

use std::time::Duration;

use futures_util::stream;
use kolyan_core::{
    ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture, TurnConfig, TurnExecutor,
    TurnRequest,
};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture, StopReason, TokenUsage, ToolCall,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PolicyEngine, PreparedCall,
    ResourceClaim, ToolManifest, ToolRequirements,
};
use kolyan_runtime::DurableTurnDriver;
use tokio::sync::Notify;

use super::*;

struct SeedProvider {
    call: ToolCall,
}

impl ModelProvider for SeedProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let call = self.call.clone();
        Box::pin(async move {
            let response = ModelResponse {
                id: request.request_id,
                model: request.model,
                content: vec![ContentBlock::ToolCall { call }],
                structured_output: None,
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
                metadata: serde_json::Value::Null,
            };
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

struct PendingWrite {
    entered: Arc<Notify>,
}

fn manifest() -> ToolManifest {
    ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Update].into(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    }
}

impl ToolExecutor for PendingWrite {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            assert_eq!(call.name, "file.write");
            assert_eq!(call.arguments, json!({"path":"safe/a","content":"result"}));
            let declared = manifest();
            Ok(PreparedCall::new(
                call,
                "r1".into(),
                InvocationClaim {
                    tool_name: declared.tool_name,
                    capabilities: declared.capabilities,
                    effects: declared.effects,
                    resource: ResourceClaim {
                        path: Some("safe/a".into()),
                    },
                    idempotency: declared.idempotency,
                },
                ToolRequirements {
                    process_sandbox: false,
                    max_output_bytes: 65536,
                    timeout_ms: 5000,
                },
            )
            .unwrap())
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
                .unwrap();
            assert_eq!(
                invocation.scope.execution,
                ExecutionKey {
                    session_id: "s".into(),
                    turn_id: "t".into(),
                    execution_id: "e".into()
                }
            );
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

pub(super) fn effect(harness: &Harness, id: &str) -> ReconciliationRequest {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(seed(harness, id))
}

async fn seed(harness: &Harness, id: &str) -> ReconciliationRequest {
    let call_id = id.rsplit_once('/').unwrap().1;
    let entered = Arc::new(Notify::new());
    let ledger = InMemoryLedger::default();
    let driver = DurableTurnDriver::new(ledger.clone(), NoopTraceSink);
    let mut policy = PolicyEngine::default();
    policy.register(manifest());
    let executor = TurnExecutor::with_tools(
        SeedProvider {
            call: ToolCall {
                id: call_id.into(),
                name: "file.write".into(),
                arguments: json!({"path":"safe/a","content":"result"}),
            },
        },
        PendingWrite {
            entered: entered.clone(),
        },
    )
    .with_policy_engine(Arc::new(policy));
    {
        let future = driver.start(
            executor,
            TurnRequest {
                turn_id: "t".into(),
                model_request: ModelRequest {
                    request_id: "seed".into(),
                    model: kolyan_model::ModelRef {
                        provider: "fixture".into(),
                        model: "offline".into(),
                    },
                    system: vec![],
                    messages: vec![],
                    tools: vec![kolyan_model::ToolDefinition {
                        name: "file.write".into(),
                        description: Some("Synthetic pending write for a recovery crash window".into()),
                        input_schema: json!({"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"],"additionalProperties":false}),
                    }],
                    tool_choice: kolyan_model::ToolChoice::Auto,
                    output_format: None,
                    prompt_cache: None,
                    reasoning: None,
                    max_output_tokens: None,
                    extensions: json!({}),
                },
                config: TurnConfig {
                    max_steps: 1,
                    ..Default::default()
                },
            },
            "s",
            "e",
        );
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut future => panic!("seed terminated before the started crash window: {result:?}"),
                _ = entered.notified() => {},
            }
        }).await.unwrap();
    }
    let events = ledger.events_after(0).unwrap();
    assert!(!events.iter().any(|event| matches!(
        event.kind,
        LedgerEventKind::EffectReceipt | LedgerEventKind::EffectUncertain
    )));
    let effect_id = format!("t-step-0/{call_id}");
    for kind in [
        LedgerEventKind::EffectPrepared,
        LedgerEventKind::EffectAuthorized,
        LedgerEventKind::EffectStarted,
    ] {
        let found: Vec<_> = events.iter().filter(|event| event.kind == kind).collect();
        assert_eq!(found.len(), 1);
        let mut event = found[0].clone();
        assert_eq!(event.payload["effect_id"], effect_id);
        event.cursor = 0;
        harness.service.ledger().append(event).unwrap();
    }
    ReconciliationRequest {
        reconciliation_id: "inspection".into(),
        execution: ExecutionKey {
            session_id: "s".into(),
            turn_id: "t".into(),
            execution_id: "e".into(),
        },
        effect_id,
    }
}
