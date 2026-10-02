use super::*;

#[derive(Deserialize)]
struct Plan {
    watchdog_ms: u64,
    deadline_ms: u64,
    aggregation: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: Mode,
    cancelled: bool,
    deadline: Deadline,
    expected: String,
    calls: usize,
    stream_drops: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Deadline {
    None,
    Expired,
    Future,
}

#[derive(Debug, Serialize)]
struct Actual {
    id: String,
    request: Value,
    provider_requests: Vec<ModelRequest>,
    full_result: Value,
    result: String,
    calls: usize,
    stream_drops: usize,
    watchdog: bool,
}

#[tokio::test]
async fn aggregation_requires_bounded_verified_completion() {
    let plan: Plan = serde_json::from_str(include_str!("additional.json")).unwrap();
    let path = evidence_path("aggregation");
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    eprintln!("STEP_LIVENESS_EVIDENCE {}", path.display());
    let mut rows = Vec::new();
    for case in &plan.aggregation {
        let state = Arc::new(Observed::default());
        let executor = StepExecutor::new(ScriptProvider {
            state: state.clone(),
            mode: case.mode,
        });
        let control = StepControl::default();
        if case.cancelled {
            control.cancel();
        }
        let deadline = match case.deadline {
            Deadline::None => None,
            Deadline::Expired => Some(Instant::now() - Duration::from_secs(1)),
            Deadline::Future => Some(Instant::now() + Duration::from_millis(plan.deadline_ms)),
        };
        let input = StepRequest {
            step_id: case.id.clone(),
            model_request: request(),
            options: StepExecutionOptions {
                deadline,
                ..Default::default()
            },
        };
        let full_request = capture::request_value(&input);
        let result = tokio::time::timeout(
            Duration::from_millis(plan.watchdog_ms),
            executor.execute_with_control(input, control),
        )
        .await;
        let watchdog = result.is_err();
        let full_result = match &result {
            Ok(Ok(result)) => serde_json::json!({"kind":"completed","result":result}),
            Ok(Err(error)) => capture::error_value(error),
            Err(error) => serde_json::json!({"kind":"watchdog","message":error.to_string()}),
        };
        let label = match result {
            Ok(Ok(result)) => match result.outcome {
                StepOutcome::FinalAnswer => "final_answer".into(),
                _ => "unexpected_outcome".into(),
            },
            Ok(Err(error)) => event_label(Err(error)),
            Err(_) => "watchdog".into(),
        };
        let actual = Actual {
            id: case.id.clone(),
            request: full_request,
            provider_requests: state.requests.lock().unwrap().clone(),
            full_result,
            result: label,
            calls: state.calls.load(Ordering::SeqCst),
            stream_drops: state.stream_drops.load(Ordering::SeqCst),
            watchdog,
        };
        serde_json::to_writer(&mut output, &actual).unwrap();
        writeln!(output).unwrap();
        output.flush().unwrap();
        rows.push(actual);
    }
    for (case, actual) in plan.aggregation.iter().zip(&rows) {
        assert_eq!(actual.provider_requests.len(), actual.calls);
        for received in &actual.provider_requests {
            assert_eq!(
                serde_json::to_value(received).unwrap(),
                actual.request["model_request"]
            );
        }
        assert!(!actual.watchdog, "{actual:?}");
        assert_eq!(actual.result, case.expected, "{actual:?}");
        assert_eq!(actual.calls, case.calls, "{actual:?}");
        assert_eq!(actual.stream_drops, case.stream_drops, "{actual:?}");
        assert_eq!(
            actual.request["model_request"],
            serde_json::to_value(request()).unwrap()
        );
        if actual.full_result["kind"] == "completed" {
            let result: StepResult =
                serde_json::from_value(actual.full_result["result"].clone()).unwrap();
            assert_eq!(result.step_id, actual.id);
            assert_eq!(result.response.id, "liveness-response");
            assert_eq!(
                serde_json::to_value(result).unwrap(),
                actual.full_result["result"]
            );
        } else {
            assert_eq!(actual.full_result["kind"], "error");
            assert!(actual.full_result["error_type"].is_string());
            assert!(
                actual.full_result["message"]
                    .as_str()
                    .is_some_and(|message| !message.is_empty())
            );
        }
    }
}
