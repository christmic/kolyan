//! Actual dispatcher windows and preserving forwarding; no model or OS network.

use super::*;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Deserialize;
use serde_json::json;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    turn_ms: Option<u64>,
    tool_ms: Option<u64>,
    grant_ms: u64,
    winner: String,
    cancel: bool,
}

#[derive(Default)]
struct Observations {
    rows: Mutex<Vec<Value>>,
    deadlines: Mutex<Vec<Instant>>,
    entered: Mutex<Vec<Instant>>,
    requests: Mutex<Vec<ModelRequest>>,
    events: Mutex<Vec<Value>>,
    drops: AtomicUsize,
}

impl TurnEventRecorder for Observations {
    fn record(&self, event: &TurnEvent) -> Result<(), TurnError> {
        let value = match event {
            TurnEvent::Started { turn_id } => json!({"kind":"started","turn_id":turn_id}),
            TurnEvent::StepStarted { turn_id, step_id } => {
                json!({"kind":"step_started","turn_id":turn_id,"step_id":step_id})
            }
            TurnEvent::StepCompleted { turn_id, step } => {
                json!({"kind":"step_completed","turn_id":turn_id,"step":step})
            }
            TurnEvent::ToolCallRequested { turn_id, call } => {
                json!({"kind":"tool_call_requested","turn_id":turn_id,"call":call})
            }
            TurnEvent::ToolExecutionStarted {
                turn_id,
                call_id,
                name,
            } => {
                json!({"kind":"tool_execution_started","turn_id":turn_id,"call_id":call_id,"name":name})
            }
            TurnEvent::ToolResult { turn_id, result } => {
                json!({"kind":"tool_result","turn_id":turn_id,"result":result})
            }
            TurnEvent::ToolExecutionFailed {
                turn_id,
                call_id,
                name,
                error,
            } => {
                json!({"kind":"tool_execution_failed","turn_id":turn_id,"call_id":call_id,"name":name,"error":format!("{error:?}"),"message":error.to_string()})
            }
            TurnEvent::ApprovalRequested {
                turn_id,
                call_id,
                name,
            } => {
                json!({"kind":"approval_requested","turn_id":turn_id,"call_id":call_id,"name":name})
            }
            TurnEvent::ToolAwaitingExternal {
                turn_id,
                call_id,
                wait,
            } => {
                json!({"kind":"tool_awaiting_external","turn_id":turn_id,"call_id":call_id,"wait":wait})
            }
            TurnEvent::Failed { turn_id, error } => {
                json!({"kind":"failed","turn_id":turn_id,"error":error})
            }
            TurnEvent::Cancelled { turn_id } => json!({"kind":"cancelled","turn_id":turn_id}),
            TurnEvent::TimedOut { turn_id } => json!({"kind":"timed_out","turn_id":turn_id}),
            TurnEvent::Completed { turn_id, outcome } => {
                json!({"kind":"completed","turn_id":turn_id,"outcome":format!("{outcome:?}")})
            }
        };
        self.events
            .lock()
            .unwrap()
            .push(json!({"kind":"turn","event":value}));
        Ok(())
    }
}

impl StepEventRecorder for Observations {
    fn record(&self, event: &StepEvent) -> Result<(), StepEventRecordError> {
        let value = match event {
            StepEvent::Started { step_id } => json!({"kind":"started","step_id":step_id}),
            StepEvent::TextDelta { step_id, text } => {
                json!({"kind":"text_delta","step_id":step_id,"text":text})
            }
            StepEvent::ReasoningDelta { step_id, text } => {
                json!({"kind":"reasoning_delta","step_id":step_id,"text":text})
            }
            StepEvent::ToolCallStarted { step_id, id, name } => {
                json!({"kind":"tool_call_started","step_id":step_id,"id":id,"name":name})
            }
            StepEvent::ToolCallArgumentsDelta { step_id, id, delta } => {
                json!({"kind":"tool_call_arguments_delta","step_id":step_id,"id":id,"delta":delta})
            }
            StepEvent::ToolCallCompleted { step_id, call } => {
                json!({"kind":"tool_call_completed","step_id":step_id,"call":call})
            }
            StepEvent::Usage { step_id, usage } => {
                json!({"kind":"usage","step_id":step_id,"usage":usage})
            }
            StepEvent::Provider { step_id, metadata } => {
                json!({"kind":"provider","step_id":step_id,"metadata":metadata})
            }
            StepEvent::Completed(result) => json!({"kind":"completed","result":result}),
            StepEvent::Cancelled { step_id } => json!({"kind":"cancelled","step_id":step_id}),
            StepEvent::TimedOut { step_id } => json!({"kind":"timed_out","step_id":step_id}),
        };
        self.events
            .lock()
            .unwrap()
            .push(json!({"kind":"step","event":value}));
        Ok(())
    }
}

struct RequestRecorder {
    inner: ScriptedProvider,
    observed: Arc<Observations>,
}

impl ModelProvider for RequestRecorder {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.observed.requests.lock().unwrap().push(request.clone());
        self.inner.stream(request)
    }
}

struct Forward {
    inner: PendingTool,
    observed: Arc<Observations>,
}

fn authority(invocation: &ToolInvocation) -> Value {
    json!({
        "prepared": invocation.prepared,
        "grant": invocation.grant,
        "scope": invocation.scope,
        "policy_revision": invocation.policy_revision,
        "control_cancelled": invocation.control.is_cancelled()
    })
}

impl ToolExecutor for Forward {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        self.inner.prepare(call)
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        let deadline = invocation.window.deadline();
        self.observed.entered.lock().unwrap().push(Instant::now());
        let budget_before = invocation.window.remaining();
        self.observed.deadlines.lock().unwrap().push(deadline);
        self.observed.rows.lock().unwrap().push(json!({
            "event": "forward",
            "authority": authority(&invocation),
            "remaining_ns": budget_before.as_nanos().to_string(),
            "deadline": format!("{deadline:?}")
        }));
        // Forward the complete live object, not a new grant-duration window.
        self.inner.execute_invocation(invocation)
    }
}

struct PendingTool {
    case: Case,
    observed: Arc<Observations>,
}

struct PendingDrop(Arc<Observations>);

impl Drop for PendingDrop {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl ToolExecutor for PendingTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            let original = fixture_prepare(call)?;
            let mut requirements = original.requirements().clone();
            requirements.timeout_ms = self.case.grant_ms;
            PreparedCall::new(
                original.call().clone(),
                original.tool_revision().into(),
                original.claim().clone(),
                requirements,
            )
            .map_err(|error| ToolError::InvalidBatch {
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
            let current = self.prepare(invocation.prepared.call().clone()).await?;
            if current != invocation.prepared {
                return Err(ToolError::PolicyDenied {
                    message: "window fixture preparation changed".into(),
                });
            }
            let _drop = PendingDrop(self.observed.clone());
            self.observed
                .deadlines
                .lock()
                .unwrap()
                .push(invocation.window.deadline());
            self.observed.rows.lock().unwrap().push(json!({
                "event": "inner",
                "authority": authority(&invocation),
                "remaining_ns": invocation.window.remaining().as_nanos().to_string(),
                "deadline": format!("{:?}", invocation.window.deadline())
            }));
            if self.case.cancel {
                invocation.control.cancel();
            }
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn dispatcher_window_contract_dataset() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("window.json")).unwrap();
    let mut actual = Vec::new();
    let mut checks = Vec::new();
    for case in cases {
        let observed = Arc::new(Observations::default());
        let call = ToolCall {
            id: "window-call".into(),
            name: "shell.query".into(),
            arguments: json!({"command":"count_lines"}),
        };
        let tools = Forward {
            inner: PendingTool {
                case: case.clone(),
                observed: observed.clone(),
            },
            observed: observed.clone(),
        };
        let provider = RequestRecorder {
            inner: ScriptedProvider {
                calls: Arc::new(AtomicUsize::new(0)),
                saw_tool_result: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                tool_call: call,
            },
            observed: observed.clone(),
        };
        let executor = TurnExecutor::with_tools(provider, tools)
            .with_execution_key(fixture_key(&case.id))
            .with_policy_engine(fixture_policy())
            .with_event_recorder(observed.clone())
            .with_step_event_recorder(observed.clone());
        let executor = match case.tool_ms {
            Some(ms) => executor.with_tool_timeout(Duration::from_millis(ms)),
            None => executor,
        };
        let turn_request = TurnRequest {
            turn_id: case.id.clone(),
            model_request: request(),
            config: TurnConfig {
                max_steps: 2,
                deadline: case.turn_ms.map(Duration::from_millis),
                ..TurnConfig::default()
            },
        };
        let anchor = TurnDeadline::capture(turn_request.config.deadline, None).unwrap();
        let turn_cutoff = anchor.instant();
        let before = Instant::now();
        let result = executor
            .start_resumable_with_control_and_deadline(turn_request, TurnControl::default(), anchor)
            .await;
        let finished = Instant::now();
        let rows = observed.rows.lock().unwrap().clone();
        let deadlines = observed.deadlines.lock().unwrap().clone();
        let entered = observed.entered.lock().unwrap().first().copied();
        let events = observed.events.lock().unwrap().clone();
        let root_matches = match case.winner.as_str() {
            "turn" => matches!(
                result,
                Err(TurnError::TimedOut) | Err(TurnError::Tool(ToolError::TimedOut))
            ),
            "cancel" => matches!(
                result,
                Err(TurnError::Cancelled) | Err(TurnError::Tool(ToolError::Cancelled))
            ),
            _ => matches!(result, Err(TurnError::Tool(ToolError::TimedOut))),
        };
        let expected_ms = match case.winner.as_str() {
            "tool" => case.tool_ms.unwrap(),
            _ => case.grant_ms,
        };
        let deadline_valid = deadlines.first().is_some_and(|deadline| {
            if case.winner == "turn" {
                Some(*deadline) == turn_cutoff
            } else {
                *deadline >= before + Duration::from_millis(expected_ms)
                    && entered.is_some_and(|entry| {
                        *deadline <= entry + Duration::from_millis(expected_ms)
                    })
            }
        });
        let same_deadline = deadlines.len() == 2 && deadlines[0] == deadlines[1];
        let same_authority = rows.len() == 2 && rows[0]["authority"] == rows[1]["authority"];
        let budget_decreased = rows.len() == 2
            && rows[1]["remaining_ns"]
                .as_str()
                .unwrap()
                .parse::<u128>()
                .unwrap()
                <= rows[0]["remaining_ns"]
                    .as_str()
                    .unwrap()
                    .parse::<u128>()
                    .unwrap();
        let expired = deadlines.first().is_some_and(|deadline| {
            case.cancel
                || ToolExecutionWindow::at_deadline(*deadline)
                    .remaining()
                    .is_zero()
        });
        let checks_row = json!({
            "root_matches":root_matches,"deadline_valid":deadline_valid,
            "same_deadline":same_deadline,"same_authority":same_authority,
            "budget_decreased":budget_decreased,"expired":expired,
            "future_dropped_once":observed.drops.load(Ordering::SeqCst)==1,
            "full_response":events.iter().any(|event| {
                event["kind"]=="step" && event["event"]["kind"]=="completed"
                    && event["event"]["result"]["response"]["content"][0]["call"]
                        == rows.first().map(|row|row["authority"]["prepared"]["call"].clone()).unwrap_or(Value::Null)
            })
        });
        let cause = result.as_ref().err().map(|error|json!({
            "typed":format!("{error:?}"), "message":error.to_string(),
            "causal_message":kolyan_model::ProviderError::describe(error),
            "end_reason":format!("{:?}",error.end_reason()),
            "tool_error":match error {TurnError::Tool(tool)=>serde_json::to_value(tool).unwrap(),_=>Value::Null}
        }));
        actual.push(json!({
            "id":case.id,"winner":case.winner,"source_request":observed.requests.lock().unwrap().clone(),
            "actual":rows,"events":events,"elapsed_ns":finished.duration_since(before).as_nanos().to_string(),
            "result":format!("{result:?}"),"cause":cause,"checks":checks_row
        }));
        checks.push(checks_row);
    }
    let expired = ToolExecutionWindow::at_deadline(Instant::now() - Duration::from_secs(1));
    actual.push(
        json!({"id":"expired-window","remaining_ns":expired.remaining().as_nanos().to_string()}),
    );
    let path =
        std::env::temp_dir().join(format!("kolyan-tool-window-{}.jsonl", std::process::id()));
    let mut file = std::fs::File::create(&path).unwrap();
    for row in &actual {
        writeln!(file, "{row}").unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    eprintln!("TOOL_WINDOW_ACTUAL={}", path.display());
    let physical: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(physical, actual);
    assert!(expired.remaining().is_zero());
    for (row, check) in physical.iter().zip(checks) {
        for (field, passed) in check.as_object().unwrap() {
            assert_eq!(passed, true, "{}: {field}", row["id"]);
        }
    }
}
