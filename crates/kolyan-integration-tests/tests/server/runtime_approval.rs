use futures_util::stream;
#[path = "../common/approval_decision.rs"]
mod approval_decision;
use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FileLedger, LedgerStore};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelEvent, ModelEventStream, ModelProvider, ModelRef,
    ModelRequest, ModelResponse, ProviderFuture, StopReason, SystemInstruction, TokenUsage,
    ToolCall, ToolChoice,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyEngine, ToolManifest,
};
use kolyan_runtime::{DurableTurnDriver, DurableTurnResult};
use kolyan_server::{ExecutionRef, ExecutionServer, ExecutionState};
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::VecTraceSink;
use serde::Deserialize;
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize)]
struct Case {
    session_id: String,
    turn_id: String,
    execution_id: String,
    tool_call_id: String,
    path: String,
    content: String,
}

#[derive(Clone)]
struct ApprovalProvider {
    calls: Arc<Mutex<usize>>,
    tool_call: ToolCall,
}

impl ModelProvider for ApprovalProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let call_number = {
            let mut calls = self
                .calls
                .lock()
                .expect("provider counter must not be poisoned");
            let current = *calls;
            *calls += 1;
            current
        };
        let response = if call_number == 0 {
            ModelResponse {
                id: request.request_id.clone(),
                model: request.model,
                content: vec![ContentBlock::ToolCall {
                    call: self.tool_call.clone(),
                }],
                structured_output: None,
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
                metadata: Value::Null,
            }
        } else {
            ModelResponse {
                id: request.request_id.clone(),
                model: request.model,
                content: vec![ContentBlock::Text {
                    text: "approval completed".into(),
                }],
                structured_output: None,
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
                metadata: Value::Null,
            }
        };
        Box::pin(async move {
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

fn case() -> Case {
    serde_json::from_str(include_str!("../fixtures/server_approval_recovery.json"))
        .expect("server approval fixture must be valid")
}

fn execution(case: &Case) -> ExecutionRef {
    ExecutionRef {
        session_id: case.session_id.clone(),
        turn_id: case.turn_id.clone(),
        execution_id: case.execution_id.clone(),
    }
}

fn policy(root: &Path) -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Create, Effect::Update].into_iter().collect(),
        path_scopes: vec![PathScope::new(root.join("safe").to_string_lossy())],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Always,
    });
    policy.restrict_workspace(root.join("safe").to_string_lossy());
    Arc::new(policy)
}

fn executor(
    provider: ApprovalProvider,
    root: &Path,
    policy: Arc<PolicyEngine>,
) -> TurnExecutor<ApprovalProvider, PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>> {
    TurnExecutor::with_tools(
        provider,
        PolicyEnforcingTool::new(RestrictedFileTool::new(root), policy.clone()),
    )
    .with_policy_engine(policy)
}

fn request(case: &Case) -> TurnRequest {
    TurnRequest {
        turn_id: case.turn_id.clone(),
        model_request: ModelRequest {
            request_id: format!("{}-request", case.turn_id),
            model: ModelRef::new("fixture", "server-approval"),
            system: vec![SystemInstruction {
                text: "执行 file.write，等待审批后完成。".into(),
                cache: false,
            }],
            messages: vec![Message {
                role: MessageRole::User,
                content: vec![ContentBlock::Text {
                    text: "写入审批目标文件".into(),
                }],
            }],
            tools: RestrictedFileTool::tool_definitions(),
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(256),
            extensions: Value::Null,
        },
        config: TurnConfig {
            max_steps: 3,
            ..TurnConfig::default()
        },
    }
}

#[tokio::test]
async fn server_runtime_multi_step_approval_survives_server_rebuild() {
    let case = case();
    let root = std::env::temp_dir().join(format!(
        "kolyan-server-runtime-approval-{}",
        std::process::id()
    ));
    let safe = root.join("safe");
    fs::create_dir_all(&safe).expect("test workspace should exist");
    let ledger_path = root.join("ledger.jsonl");
    let policy = policy(&root);
    let provider = ApprovalProvider {
        calls: Arc::default(),
        tool_call: ToolCall {
            id: case.tool_call_id.clone(),
            name: "file.write".into(),
            arguments: json!({"path": case.path, "content": case.content}),
        },
    };
    let execution = execution(&case);

    let first_server = ExecutionServer::new(FileLedger::open(&ledger_path).unwrap());
    first_server.start(execution.clone()).unwrap();
    let first_driver = DurableTurnDriver::new(
        FileLedger::open(&ledger_path).unwrap(),
        VecTraceSink::default(),
    );
    let awaiting = first_driver
        .start(
            executor(provider.clone(), &root, policy.clone()),
            request(&case),
            &case.session_id,
            &case.execution_id,
        )
        .await
        .expect("first Runtime attempt should suspend for approval");
    let suspension = match awaiting {
        DurableTurnResult::Suspended { suspension, .. } => suspension,
        DurableTurnResult::Completed(_, _) => panic!("expected an approval checkpoint"),
    };
    assert_eq!(*provider.calls.lock().unwrap(), 1, "first model step only");
    assert!(
        !root.join(&case.path).exists(),
        "approval must precede side effect"
    );
    first_server.release(&case.execution_id);
    assert_eq!(
        first_server.state(&case.execution_id).unwrap(),
        ExecutionState::Suspended
    );
    drop(first_driver);
    drop(first_server);

    let second_server = ExecutionServer::new(FileLedger::open(&ledger_path).unwrap());
    second_server.resume(execution.clone()).unwrap();
    let second_driver = DurableTurnDriver::new(
        FileLedger::open(&ledger_path).unwrap(),
        VecTraceSink::default(),
    );
    let confirmation = approval_decision::confirm(
        second_driver.ledger(),
        kolyan_types::ExecutionKey {
            session_id: case.session_id.clone(),
            turn_id: case.turn_id.clone(),
            execution_id: case.execution_id.clone(),
        },
        &suspension,
    );
    let completed = second_driver
        .resume(
            executor(provider.clone(), &root, policy),
            &case.session_id,
            &case.execution_id,
            &suspension.checkpoint.checkpoint_id,
            kolyan_core::ResumeInput::ApprovalConfirmed(confirmation),
        )
        .await
        .expect("resumed Runtime attempt should complete");
    assert!(matches!(completed, DurableTurnResult::Completed(_, _)));
    second_server.release(&case.execution_id);
    assert_eq!(
        second_server.state(&case.execution_id).unwrap(),
        ExecutionState::Completed
    );
    assert_eq!(
        *provider.calls.lock().unwrap(),
        2,
        "one model call per Step"
    );
    assert_eq!(
        fs::read_to_string(root.join(&case.path)).unwrap(),
        case.content
    );

    let actual = write_and_compare_trajectory(&ledger_path, &case);
    assert!(!actual.as_os_str().is_empty());
    fs::remove_dir_all(root).expect("test workspace should be removed");
}

fn write_and_compare_trajectory(ledger_path: &Path, case: &Case) -> PathBuf {
    let ledger = FileLedger::open(ledger_path).unwrap();
    let records = ledger
        .events_after(0)
        .unwrap()
        .into_iter()
        .filter(|event| event.execution_id == case.execution_id)
        .map(|event| json!({"kind": event.kind}))
        .collect::<Vec<_>>();
    let actual = std::env::temp_dir().join(format!(
        "kolyan-server-trajectory-{}-{}.jsonl",
        std::process::id(),
        case.execution_id
    ));
    let content = records
        .iter()
        .map(|record| serde_json::to_string(record).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(&actual, content).expect("test should write the actual trajectory");
    let expected_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/expected/server/approval_recovery.jsonl");
    let mut offset = 0;
    for line in fs::read_to_string(expected_path).unwrap().lines() {
        let expected: Value = serde_json::from_str(line).unwrap();
        let found = records[offset..]
            .iter()
            .position(|record| record["kind"] == expected["kind"]);
        assert!(found.is_some(), "missing trajectory record {expected}");
        offset += found.unwrap() + 1;
    }
    fs::remove_file(&actual).expect("test trajectory should be temporary");
    actual
}
