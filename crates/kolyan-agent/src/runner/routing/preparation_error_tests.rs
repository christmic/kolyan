//! Actual routing and Turn feedback, with synthetic responses and no child execution.

mod events;
mod mapping_tests;

use std::fs::File;
use std::io::Write;
use std::sync::Mutex;

use futures_util::stream;
use kolyan_core::{
    ToolError, ToolErrorPolicy, TurnCheckpoint, TurnControl, TurnError, TurnEvent,
    TurnEventRecorder, TurnExecutor,
};
use kolyan_ledger::FactJournal;
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture, StopReason,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::*;
use crate::runner::tests::support::*;
use crate::{AgentDefinition, AgentDefinitionInput, AgentSelector, BindingContextKind};

#[derive(Deserialize)]
struct Base {
    definitions: Vec<AgentDefinitionInput>,
    limits: InvokePrepareLimits,
    call: ToolCall,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    pointer: Option<String>,
    value: Value,
    host: String,
    expected: String,
    reason: String,
    continue_batch: bool,
}

struct Script {
    call: ToolCall,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    responses: Arc<Mutex<Vec<ModelResponse>>>,
}

#[derive(Default)]
struct Records {
    events: Mutex<Vec<Value>>,
    full_events: Mutex<Vec<Value>>,
    checkpoints: Mutex<Vec<Value>>,
}

impl TurnEventRecorder for Records {
    fn record(&self, event: &TurnEvent) -> Result<(), TurnError> {
        let row = match event {
            TurnEvent::ToolExecutionFailed { call_id, error, .. } => {
                json!({"call_id":call_id,"failure":error})
            }
            TurnEvent::ToolExecutionStarted { call_id, .. } => json!({"started":call_id}),
            TurnEvent::ToolResult { result, .. } => json!({"result":result}),
            _ => json!({"diagnostic":format!("{event:?}")}),
        };
        self.events.lock().unwrap().push(row);
        self.full_events
            .lock()
            .unwrap()
            .push(events::serialize(event));
        Ok(())
    }
    fn record_checkpoint(&self, checkpoint: &TurnCheckpoint) -> Result<(), TurnError> {
        self.checkpoints.lock().unwrap().push(json!(checkpoint));
        Ok(())
    }
}

impl ModelProvider for Script {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let mut requests = self.requests.lock().unwrap();
        let index = requests.len();
        requests.push(request.clone());
        let content = match index {
            0 => vec![ContentBlock::ToolCall {
                call: self.call.clone(),
            }],
            1 => vec![ContentBlock::ToolCall {
                call: ToolCall {
                    id: "corrected-read".into(),
                    name: "file.read".into(),
                    arguments: json!({"path":"input.txt"}),
                },
            }],
            _ => vec![ContentBlock::Text {
                text: "Offline final answer".into(),
            }],
        };
        let response = ModelResponse {
            id: request.request_id,
            model: request.model,
            content,
            structured_output: None,
            stop_reason: if index < 2 {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: Default::default(),
            metadata: json!({"source":"synthetic_not_live"}),
        };
        self.responses.lock().unwrap().push(response.clone());
        Box::pin(async move {
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

fn category(error: &ToolError) -> &'static str {
    match error {
        ToolError::Failed { .. } => "failed",
        ToolError::PolicyDenied { .. } => "policy_denied",
        ToolError::InvalidBatch { .. } => "invalid_batch",
        _ => "other",
    }
}

fn export(rows: &[Value]) -> Vec<Value> {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-preparation-errors-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    for row in rows {
        writeln!(output, "{row}").unwrap();
    }
    output.sync_all().unwrap();
    println!("AGENT_PREPARATION_ERROR_TRACE={}", path.display());
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn typed_route_preparation_and_actual_feedback_export_all_before_comparison() {
    // Existing preparation input is the base SSOT; its cases remain untouched.
    let base: Base = serde_json::from_str(include_str!("../../invoke/tests/prepare.json")).unwrap();
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("preparation_error_cases.json")).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        let harness = Harness::new();
        let mut catalog = crate::AgentCatalog::new(8).unwrap();
        for definition in &base.definitions {
            catalog
                .register(AgentDefinition::new(definition.clone()).unwrap())
                .unwrap();
        }
        let mut permissions = base.definitions[0].permissions.clone();
        if case.host == "inline_disabled" {
            permissions.delegation.allow_inline = false;
        }
        if case.host == "self_disabled" {
            permissions.delegation.allow_self = false;
        }
        let mut runner = AgentRunner::new(
            harness.service.clone(),
            harness.runner.instances.clone(),
            harness.bindings.clone(),
            catalog,
            permissions.clone(),
            (
                Providers {
                    observations: harness.observations.clone(),
                    fail: false,
                    reject_context: false,
                    call_tool: false,
                },
                Tools(harness.observations.clone(), false),
            ),
            harness.runner.input_artifacts.clone(),
        )
        .unwrap()
        .with_delegation(AgentDelegationConfig {
            limits: base.limits.clone(),
            approval: ApprovalMode::Never,
        })
        .unwrap();
        let snapshot = runner
            .catalog
            .resolve(
                &AgentSelector::Named(crate::AgentKey::new("parent", "1").unwrap()),
                "preparation-instance",
                &permissions,
                &permissions,
            )
            .unwrap();
        let execution = execution(&case.id);
        let mut saved = AgentInvocationBinding {
            task_id: case.id.clone(),
            invocation_id: "root".into(),
            logical_session_id: "session".into(),
            private_session_id: "session".into(),
            context_kind: BindingContextKind::Root,
            snapshot: snapshot.clone(),
        };
        // Real ownership fact, not a grant or a Task admission. This test never executes agent.invoke.
        let ownership = harness.bindings.save(&saved).unwrap();
        let mut parent = AttemptBinding {
            attempt_id: "attempt".into(),
            invocation_id: "root".into(),
            execution: execution.clone(),
            agent: snapshot.identity().clone(),
            constraints_digest: snapshot.digest().into(),
            input_source: kolyan_server::InvocationInputSource::Standalone { fact: ownership },
        };
        match case.host.as_str() {
            "corrupt_binding" => saved.private_session_id = "foreign".into(),
            "invalid_limits" => runner.delegation.as_mut().unwrap().limits.max_children = 0,
            "invalid_permissions" => {
                runner
                    .host
                    .delegation
                    .named_targets
                    .insert(crate::AgentKey {
                        definition_id: "bad/key".into(),
                        revision: "1".into(),
                    });
            }
            "invalid_execution" => parent.execution.turn_id = "bad/turn".into(),
            "foreign_session" => parent.execution.session_id = "foreign".into(),
            "not_configured" => runner.delegation = None,
            _ => {}
        }
        let mut call = base.call.clone();
        if let Some(pointer) = &case.pointer {
            if pointer == "/unknown" {
                call.arguments["unknown"] = case.value.clone();
            } else if case.host == "long_error" {
                *call.arguments.pointer_mut(pointer).unwrap() = json!(["界".repeat(1500)]);
            } else if case.value.is_null() {
                call.arguments.as_object_mut().unwrap().remove("parallel");
            } else {
                *call.arguments.pointer_mut(pointer).unwrap() = case.value.clone();
            }
        }
        let original = call.clone();
        let mut set = Tools(harness.observations.clone(), false)
            .build(&snapshot, &execution)
            .unwrap();
        let mut policy = (*set.policy).clone();
        policy.register(crate::agent_invoke_manifest(ApprovalMode::Never));
        set.policy = Arc::new(policy);
        let routed = RoutedTools {
            runner: Arc::new(runner),
            saved,
            parent,
            environment: SnapshotTools {
                inner: set.executor,
                snapshot: snapshot.clone(),
                execution: execution.clone(),
                policy: set.policy.clone(),
            },
        };
        let prepared = routed.prepare(call.clone()).await;
        let actual = match &prepared {
            Ok(value) => json!({"category":"prepared","prepared":value}),
            Err(error) => {
                json!({"category":category(error),"error":error,"message":error.to_string()})
            }
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let responses = Arc::new(Mutex::new(Vec::new()));
        let records = Arc::new(Records::default());
        let mut outcome = Value::Null;
        if prepared.is_err() {
            let mut request = harness.request(&case.id, false).turn;
            request.config.max_steps = 3;
            request.config.max_tool_calls =
                Some(if case.host == "one_call_budget" { 1 } else { 2 });
            let executor = TurnExecutor::with_tools(
                Script {
                    call: call.clone(),
                    requests: requests.clone(),
                    responses: responses.clone(),
                },
                routed,
            )
            .with_policy_engine(set.policy)
            .with_execution_key(serde_json::from_value(json!(execution)).unwrap())
            .with_agent_snapshot_digest(snapshot.digest().into())
            .with_event_recorder(records.clone())
            .with_tool_dispatch_policy(kolyan_core::ToolDispatchPolicy {
                mode: kolyan_core::ToolDispatchMode::Serial,
                on_error: if case.continue_batch {
                    ToolErrorPolicy::ContinueBatch
                } else {
                    ToolErrorPolicy::FailTurn
                },
            });
            match executor
                .execute_with_events(request, TurnControl::default())
                .await
            {
                Ok(result) => {
                    outcome = json!(format!("{:?}", result.result.outcome));
                }
                Err(error) => {
                    outcome = json!({"error":error.to_string(),
                    "budget_exceeded":matches!(error, TurnError::ToolBudgetExceeded)})
                }
            }
        }
        let instance_stream = format!("agent-instances:{:x}", Sha256::digest(b"unit-host"));
        let instances = harness
            .service
            .coordinator()
            .journal()
            .read(&instance_stream, 0, 1024)
            .unwrap();
        let task_facts = harness
            .service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .unwrap();
        rows.push(json!({"fixture_id":case.id,"source":"offline_actual_routing_and_turn_not_task_admission",
            "original":original,"after":call,"actual":actual,"events":*records.events.lock().unwrap(),
            "full_events":*records.full_events.lock().unwrap(),"responses":*responses.lock().unwrap(),
            "checkpoints":*records.checkpoints.lock().unwrap(),"instances":instances,"task_facts":task_facts,"outcome":outcome,
            "requests":*requests.lock().unwrap(),"effects":*harness.observations.effects.lock().unwrap()}));
    }
    let actual = export(&rows);
    for (case, row) in cases.iter().zip(actual) {
        events::compare(&row);
        assert_eq!(row["original"], row["after"], "{}", case.id);
        assert_eq!(row["instances"], json!([]), "{}", case.id);
        assert_eq!(row["task_facts"], json!([]), "{}", case.id);
        assert!(
            !row["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["started"] == "invoke-1")
        );
        assert_eq!(
            row["actual"]["category"], case.expected,
            "{}: {row}",
            case.id
        );
        if case.expected == "prepared" {
            continue;
        }
        let message = row["actual"]["message"].as_str().unwrap();
        assert!(message.contains(&case.reason), "{}: {message}", case.id);
        let continued = case.continue_batch && case.expected != "invalid_batch";
        let budget_stop = case.host == "one_call_budget";
        assert_eq!(
            row["requests"].as_array().unwrap().len(),
            if budget_stop {
                2
            } else if continued {
                3
            } else {
                1
            },
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["effects"].as_array().unwrap().len(),
            usize::from(continued && !budget_stop),
            "{}",
            case.id
        );
        if budget_stop {
            assert_eq!(row["outcome"]["budget_exceeded"], true);
        }
        for event in row["events"].as_array().unwrap() {
            if event["call_id"] == "invoke-1" {
                assert_eq!(event["failure"], row["actual"]["error"]);
            }
        }
        if continued {
            let request: ModelRequest = serde_json::from_value(row["requests"][1].clone()).unwrap();
            assert!(request.messages.iter().flat_map(|message| &message.content).any(|block|
                matches!(block, ContentBlock::ToolResult { result } if result.call_id == "invoke-1"
                    && result.is_error && result.content == message)), "{}", case.id);
        }
        if case.host == "long_error" {
            let payload = &row["actual"]["error"]["Failed"]["message"];
            assert!(payload.as_str().unwrap().len() <= 1024 + "invalid invocation input: ".len());
        }
    }
}
