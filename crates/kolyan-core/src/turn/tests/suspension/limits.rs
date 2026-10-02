use super::*;

#[derive(Default)]
struct Events(Mutex<Vec<TurnEvent>>);
impl TurnEventRecorder for Events {
    fn record(&self, event: &TurnEvent) -> Result<(), TurnError> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
}

#[tokio::test]
async fn completed_full_envelope_over_limit_halts_before_feedback_or_next_model() {
    let mut tools = ScenarioTools::new(&[("large", Behavior::Oversized)]);
    tools.output_limit = 128;
    let (executor, requests) = executor(
        vec![call("large", "file.read", "a")],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let events = Arc::new(Events::default());
    let error = executor
        .with_event_recorder(events.clone())
        .start_resumable(input(1))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        TurnError::Tool(ToolError::InvalidBatch { .. })
    ));
    assert_eq!(tools.executed(), ["large"]);
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(
        !events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(event, TurnEvent::ToolResult { .. }))
    );
}

#[tokio::test]
async fn grant_timeout_alone_bounds_a_hanging_executor_without_turn_or_config_timeout() {
    let mut tools = ScenarioTools::new(&[("hang", Behavior::Hang)]);
    tools.timeout_ms = 10;
    let (executor, requests) = executor(
        vec![call("hang", "file.read", "a")],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let executor = executor.with_tool_dispatch_policy(ToolDispatchPolicy {
        mode: ToolDispatchMode::Serial,
        on_error: ToolErrorPolicy::FailTurn,
    });
    assert_eq!(executor.tool_timeout(), None);
    assert_eq!(input(1).config.deadline, None);
    let error = tokio::time::timeout(Duration::from_secs(1), executor.start_resumable(input(1)))
        .await
        .expect("issued timeout must bound tool future")
        .unwrap_err();
    assert!(matches!(error, TurnError::Tool(ToolError::TimedOut)));
    assert_eq!(tools.executed(), ["hang"]);
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn uncertain_and_invalid_wait_are_fatal_even_under_continue_batch() {
    for behavior in [Behavior::Uncertain, Behavior::InvalidWait] {
        let tools = ScenarioTools::new(&[("risk", behavior)]);
        let (executor, requests) = executor(
            vec![
                call("risk", "file.write", "shared"),
                call("tail", "file.read", "shared"),
            ],
            tools.clone(),
            ToolDispatchMode::Serial,
            false,
        );
        let events = Arc::new(Events::default());
        let error = executor
            .with_event_recorder(events.clone())
            .start_resumable(input(2))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            TurnError::Tool(ToolError::Uncertain { .. })
        ));
        assert_eq!(tools.executed(), ["risk"]);
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert!(
            !events
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, TurnEvent::ToolResult { .. }))
        );
    }
}

#[tokio::test]
async fn parallel_uncertainty_drains_current_stage_but_never_admits_conflict_tail() {
    let tools = ScenarioTools::new(&[("risk", Behavior::Uncertain)]);
    let (executor, requests) = executor(
        vec![
            call("risk", "file.write", "a"),
            call("sibling", "file.read", "b"),
            call("tail", "file.read", "a"),
        ],
        tools.clone(),
        ToolDispatchMode::Parallel,
        false,
    );
    let events = Arc::new(Events::default());
    let error = executor
        .with_event_recorder(events.clone())
        .start_resumable(input(3))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        TurnError::Tool(ToolError::Uncertain { .. })
    ));
    assert_eq!(tools.executed(), ["risk", "sibling"]);
    assert_eq!(requests.lock().unwrap().len(), 1);
    let events = events.0.lock().unwrap();
    assert!(events.iter().any(
        |event| matches!(event, TurnEvent::ToolResult { result, .. } if result.call_id == "sibling")
    ));
    assert!(!events.iter().any(
        |event| matches!(event, TurnEvent::ToolResult { result, .. } if result.call_id == "risk")
    ));
}

#[tokio::test]
async fn requested_budget_counts_preparation_failed_calls_and_serial_tail_not_only_effects() {
    let tools = ScenarioTools::new(&[
        ("failed", Behavior::PrepareFailure),
        ("child", Behavior::Wait),
    ]);
    let (executor, requests) = executor(
        vec![
            call("failed", "file.read", "a"),
            call("child", "file.read", "b"),
            call("tail", "file.read", "c"),
        ],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let error = executor.start_resumable(input(2)).await.unwrap_err();
    assert!(matches!(error, TurnError::ToolBudgetExceeded));
    assert_eq!(tools.executed().len(), 0);
    assert_eq!(requests.lock().unwrap().len(), 1);
    let (executor, requests) = super::executor(
        vec![
            call("failed", "file.read", "a"),
            call("child", "file.read", "b"),
            call("tail", "file.read", "c"),
        ],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let waiting = suspended(executor.start_resumable(input(3)).await.unwrap());
    assert_eq!(waiting.checkpoint.budget.tool_calls_used, 2);
    assert!(matches!(
        waiting.checkpoint.calls[0].state,
        CheckpointCallState::Completed { issued: None, .. }
    ));
    assert!(matches!(
        waiting.checkpoint.calls[2].state,
        CheckpointCallState::Ready
    ));
    let mut lower = waiting.checkpoint.clone();
    lower.budget.max_tool_calls = Some(2);
    assert!(lower.validate(&lower.scope).is_err());
    let checkpoint = resolve(&executor, waiting, &["child"]);
    assert_eq!(checkpoint.budget.tool_calls_used, 2);
    let scope = checkpoint.scope.clone();
    assert!(matches!(
        executor
            .resume_checkpoint_with_control(checkpoint, scope, TurnControl::default())
            .await
            .unwrap(),
        ResumableTurn::Completed(_)
    ));
    assert_eq!(tools.executed(), ["child", "tail"]);
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[test]
fn getters_and_absolute_deadline_tightening_preserve_source_configuration() {
    let (executor, _) = executor(
        vec![],
        ScenarioTools::new(&[]),
        ToolDispatchMode::Parallel,
        false,
    );
    let executor = executor
        .with_agent_snapshot_digest("a".repeat(64))
        .with_tool_timeout(Duration::from_millis(250));
    assert_eq!(
        executor.agent_snapshot_digest(),
        Some("a".repeat(64).as_str())
    );
    assert_eq!(
        executor.tool_dispatch_policy().mode,
        ToolDispatchMode::Parallel
    );
    assert_eq!(executor.tool_timeout(), Some(Duration::from_millis(250)));
    let now = now_ms();
    let executor = executor
        .with_absolute_deadline_at_ms(now + 1000)
        .with_absolute_deadline_at_ms(now + 2000);
    let mut request = input(1);
    request.config.deadline = Some(Duration::from_millis(100));
    let state = executor.new_run_state(request).unwrap();
    assert!(state.deadline_at_ms.unwrap() <= now + 1000);
    assert!(state.deadline.unwrap() <= Instant::now() + Duration::from_millis(100));
    let state = executor.new_run_state(input(1)).unwrap();
    assert_eq!(state.deadline_at_ms, Some(now + 1000));
}

#[tokio::test]
async fn denied_and_preparation_failure_feedback_cannot_loop_without_charging_requested_budget() {
    for denied in [true, false] {
        let tools = ScenarioTools::new(&[(
            "repeat",
            if denied {
                Behavior::Complete
            } else {
                Behavior::PrepareFailure
            },
        )]);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut policy = (*fixture_policy()).clone();
        if denied {
            policy.deny_tool("file.read");
        }
        let executor = TurnExecutor::with_tools(
            BatchProvider {
                batch: vec![call("repeat", "file.read", "a")],
                requests: requests.clone(),
                repeat: true,
            },
            tools.clone(),
        )
        .with_execution_key(fixture_key("suspension"))
        .with_policy_engine(Arc::new(policy))
        .with_tool_dispatch_policy(ToolDispatchPolicy {
            mode: ToolDispatchMode::Serial,
            on_error: ToolErrorPolicy::ContinueBatch,
        });
        let error = executor.start_resumable(input(1)).await.unwrap_err();
        assert!(matches!(error, TurnError::ToolBudgetExceeded));
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert_eq!(tools.executed().len(), 0);
        let second_request = &requests.lock().unwrap()[1];
        assert!(second_request.messages.iter().flat_map(|message| &message.content).any(|block| matches!(block, ContentBlock::ToolResult { result } if result.call_id == "repeat" && result.is_error)));
    }
}

#[tokio::test]
async fn parallel_uncertainty_overrides_an_earlier_ordinary_fatal_error() {
    let tools = ScenarioTools::new(&[("failed", Behavior::Failure), ("risk", Behavior::Uncertain)]);
    let (executor, requests) = executor(
        vec![
            call("failed", "file.write", "a"),
            call("risk", "file.write", "b"),
            call("tail", "file.read", "b"),
        ],
        tools.clone(),
        ToolDispatchMode::Parallel,
        false,
    );
    let executor = executor.with_tool_dispatch_policy(ToolDispatchPolicy {
        mode: ToolDispatchMode::Parallel,
        on_error: ToolErrorPolicy::FailTurn,
    });
    let error = executor.start_resumable(input(3)).await.unwrap_err();
    assert!(matches!(
        error,
        TurnError::Tool(ToolError::Uncertain { .. })
    ));
    assert_eq!(tools.executed(), ["failed", "risk"]);
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn recording_failure_cannot_mask_uncertain_as_ordinary_failure() {
    struct RefuseUncertain;
    impl TurnEventRecorder for RefuseUncertain {
        fn record(&self, event: &TurnEvent) -> Result<(), TurnError> {
            if matches!(
                event,
                TurnEvent::ToolExecutionFailed {
                    error: ToolError::Uncertain { .. },
                    ..
                } | TurnEvent::Failed { .. }
            ) {
                return Err(TurnError::BoundaryControl {
                    message: "uncertainty recording failed".into(),
                });
            }
            Ok(())
        }
    }
    let tools = ScenarioTools::new(&[("risk", Behavior::Uncertain)]);
    let (executor, requests) = executor(
        vec![call("risk", "file.write", "a")],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let error = executor
        .with_event_recorder(Arc::new(RefuseUncertain))
        .start_resumable(input(1))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        TurnError::Tool(ToolError::Uncertain { .. })
    ));
    assert_eq!(tools.executed(), ["risk"]);
    assert_eq!(requests.lock().unwrap().len(), 1);
}
