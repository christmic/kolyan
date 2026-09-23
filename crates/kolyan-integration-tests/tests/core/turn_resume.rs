#[path = "../common/mod.rs"]
mod common;
mod turn_resume_live;
mod turn_resume_races;
mod turn_resume_support;

use futures_util::stream;
use kolyan_core::*;
use kolyan_model::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use turn_resume_support::*;

#[derive(Clone, Deserialize)]
struct Case {
    name: String,
    batches: Vec<Vec<ToolCall>>,
    #[serde(default)]
    approvals: Vec<String>,
    #[serde(default)]
    denied: Vec<String>,
    #[serde(default)]
    fail_calls: Vec<String>,
    tool_failure_kind: Option<String>,
    max_steps: usize,
    max_tool_calls: Option<usize>,
    deadline_ms: Option<u64>,
    tool_timeout_ms: Option<u64>,
    #[serde(default)]
    provider_delay_ms: u64,
    #[serde(default)]
    tool_delay_ms: u64,
    #[serde(default)]
    resume_delay_ms: u64,
    #[serde(default)]
    cancel_locally_on_resume: bool,
    cancel_at: Option<Trigger>,
    fail_at: Option<Trigger>,
    dispatch: ToolDispatchPolicy,
    expected: Expected,
}

#[derive(Clone, Deserialize)]
struct Expected {
    outcome: String,
    model_calls: usize,
    tool_calls: usize,
    approvals: usize,
    max_parallel: usize,
    #[serde(default)]
    context_errors: usize,
}

struct Scripted {
    batches: Mutex<std::collections::VecDeque<Vec<ToolCall>>>,
    delay: u64,
}
impl ModelProvider for Scripted {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(self.delay)).await;
            let calls = self
                .batches
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra model call");
            let terminal = calls.is_empty();
            let content = if terminal {
                vec![ContentBlock::Text {
                    text: "done".into(),
                }]
            } else {
                calls
                    .into_iter()
                    .map(|call| ContentBlock::ToolCall { call })
                    .collect()
            };
            let response = ModelResponse {
                id: request.request_id,
                model: request.model,
                content,
                structured_output: None,
                stop_reason: if terminal {
                    StopReason::EndTurn
                } else {
                    StopReason::ToolUse
                },
                usage: TokenUsage::default(),
                metadata: Value::Null,
            };
            Ok(Box::pin(stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::ReasoningDelta("fixture planning".into())),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

#[derive(Default)]
struct ToolStats {
    active: usize,
    max_active: usize,
}
#[derive(Clone)]
struct FixtureTool {
    fail_calls: Vec<String>,
    failure_kind: Option<String>,
    delay: u64,
    stats: Arc<Mutex<ToolStats>>,
}
impl ToolExecutor for FixtureTool {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        Box::pin(async move {
            {
                let mut stats = self.stats.lock().unwrap();
                stats.active += 1;
                stats.max_active = stats.max_active.max(stats.active);
            }
            tokio::time::sleep(Duration::from_millis(self.delay)).await;
            self.stats.lock().unwrap().active -= 1;
            if self.fail_calls.contains(&call.id) {
                if self.failure_kind.as_deref() == Some("policy_denied") {
                    return Err(ToolError::PolicyDenied {
                        message: "tool enforcement rejected grant".into(),
                    });
                }
                return Err(ToolError::Failed {
                    message: "fixture failure".into(),
                });
            }
            Ok(ToolResult {
                call_id: call.id,
                content: call.arguments.to_string(),
                is_error: false,
            })
        })
    }
}

fn request(case: &Case) -> TurnRequest {
    TurnRequest {
        turn_id: case.name.clone(),
        model_request: ModelRequest {
            request_id: case.name.clone(),
            model: ModelRef::new("fixture", "fixture"),
            system: vec![],
            messages: vec![],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: None,
            extensions: Value::Null,
        },
        config: TurnConfig {
            max_steps: case.max_steps,
            max_tool_calls: case.max_tool_calls,
            deadline: case.deadline_ms.map(Duration::from_millis),
        },
    }
}

#[tokio::test]
async fn data_driven_resume_and_boundary_contracts() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/turn_resume_contracts.json")).unwrap();
    for case in cases {
        let records = Records::default();
        let gate = Arc::new(MemoryGate::new(
            records.clone(),
            case.cancel_at.clone(),
            case.fail_at.clone(),
        ));
        let provider = RecordingProvider {
            inner: Arc::new(Scripted {
                batches: Mutex::new(case.batches.clone().into()),
                delay: case.provider_delay_ms,
            }),
            records: records.clone(),
        };
        let tools = FixtureTool {
            fail_calls: case.fail_calls.clone(),
            failure_kind: case.tool_failure_kind.clone(),
            delay: case.tool_delay_ms,
            stats: Arc::default(),
        };
        let policy = policy(&case.approvals, &case.denied);
        let build = || {
            TurnExecutor::with_tools(
                provider.clone(),
                RecordingTool {
                    inner: tools.clone(),
                    records: records.clone(),
                },
            )
            .with_policy_engine(policy.clone())
            .with_boundary_control(gate.clone())
        };
        let mut executor = build().with_tool_dispatch_policy(case.dispatch);
        if let Some(timeout) = case.tool_timeout_ms {
            executor = executor.with_tool_timeout(Duration::from_millis(timeout));
        }
        let mut result = executor.start_resumable(request(&case)).await;
        drop(executor);
        let mut approvals = 0;
        let mut prior_batch: Option<(String, usize)> = None;
        while let Ok(ResumableTurn::AwaitingApproval(approval)) = result {
            approvals += 1;
            assert!(approvals < 30, "unbounded approval loop");
            let step_id = approval.continuation.steps.last().unwrap().step_id.clone();
            if let Some((prior_step, calls)) = &prior_batch
                && *prior_step == step_id
            {
                assert_eq!(
                    records.count("tool_start"),
                    *calls,
                    "batch effects before all approvals"
                );
            }
            prior_batch = Some((step_id, records.count("tool_start")));
            records.push(json!({"event":"suspended","approval":approval}));
            let encoded = serde_json::to_vec(&approval).unwrap();
            let restored: ApprovalRequest = serde_json::from_slice(&encoded).unwrap();
            tokio::time::sleep(Duration::from_millis(case.resume_delay_ms)).await;
            let id = restored.approval_id.clone();
            let control = TurnControl::default();
            if case.cancel_locally_on_resume {
                control.cancel();
            }
            result = build()
                .resume_approval_with_control(restored, &id, control)
                .await;
        }
        let outcome = match result {
            Ok(ResumableTurn::Completed(execution)) => {
                let mut step = String::new();
                let mut failures = std::collections::HashSet::new();
                for event in &execution.events {
                    match event {
                        TurnEvent::StepStarted { step_id, .. } => step = step_id.clone(),
                        TurnEvent::ToolExecutionFailed { call_id, .. } => assert!(
                            failures.insert((step.clone(), call_id.clone())),
                            "duplicate tool failure event"
                        ),
                        _ => {}
                    }
                }
                assert!(
                    !gate.cancel(),
                    "completion must win against late cancellation"
                );
                assert_eq!(execution.result.steps.len(), case.expected.model_calls);
                format!("{:?}", execution.result.end_reason)
            }
            Err(error) => format!("{:?}", error.end_reason()),
            _ => unreachable!(),
        };
        records.push(json!({"event":"outcome","reason":outcome}));
        let file = records.write(&case.name);
        assert_eq!(
            outcome,
            case.expected.outcome,
            "{}: {}",
            case.name,
            file.display()
        );
        assert_eq!(
            records.count("model_request"),
            case.expected.model_calls,
            "{}",
            case.name
        );
        assert_eq!(
            records.count("tool_start"),
            case.expected.tool_calls,
            "{}",
            case.name
        );
        assert_eq!(approvals, case.expected.approvals, "{}", case.name);
        assert_eq!(
            tools.stats.lock().unwrap().max_active,
            case.expected.max_parallel,
            "{}",
            case.name
        );
        let requests = records
            .all()
            .into_iter()
            .filter(|row| row["event"] == "model_request")
            .collect::<Vec<_>>();
        for (index, row) in requests.iter().enumerate() {
            assert_eq!(row["step_id"], format!("{}-step-{index}", case.name));
            let request: ModelRequest = serde_json::from_value(row["request"].clone()).unwrap();
            for (message_index, message) in request.messages.iter().enumerate() {
                let calls = message
                    .content
                    .iter()
                    .filter_map(|block| {
                        if let ContentBlock::ToolCall { call } = block {
                            Some(&call.id)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                if !calls.is_empty() {
                    let results = request.messages[message_index + 1..]
                        .iter()
                        .take_while(|message| message.role != MessageRole::Assistant)
                        .flat_map(|message| &message.content)
                        .filter_map(|block| {
                            if let ContentBlock::ToolResult { result } = block {
                                Some(&result.call_id)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(calls, results, "incomplete or reordered context");
                }
            }
        }
        if case.expected.context_errors > 0 {
            let request: ModelRequest =
                serde_json::from_value(requests.last().unwrap()["request"].clone()).unwrap();
            assert_eq!(request.messages.iter().flat_map(|m| &m.content).filter(|block| matches!(block, ContentBlock::ToolResult {result} if result.is_error)).count(), case.expected.context_errors);
        }
        let expected = include_str!("../expected/turn/turn_resume_contracts.jsonl")
            .lines()
            .filter_map(|line| {
                let mut value: Value = serde_json::from_str(line).unwrap();
                if value["case"] != case.name {
                    return None;
                }
                value.as_object_mut().unwrap().remove("case");
                Some(value.to_string())
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!expected.is_empty());
        compare(&file, &expected, &case.name);
        eprintln!("PASS deterministic {}", case.name);
    }
}
