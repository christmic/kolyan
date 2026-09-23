use super::*;
use futures_util::StreamExt;

#[tokio::test]
async fn cancellation_preserves_completed_tool_events() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/turn_resume_contracts.json")).unwrap();
    let case = cases
        .into_iter()
        .find(|case| case.name == "cancel_between_tools")
        .unwrap();
    for mode in [ToolDispatchMode::Serial, ToolDispatchMode::Parallel] {
        let records = Records::default();
        let gate = Arc::new(MemoryGate::new(
            records.clone(),
            case.cancel_at.clone(),
            None,
        ));
        let provider = RecordingProvider {
            inner: Arc::new(Scripted {
                batches: Mutex::new(case.batches.clone().into()),
                delay: 0,
            }),
            records: records.clone(),
        };
        let executor = TurnExecutor::with_tools(
            provider,
            FixtureTool {
                fail_calls: vec![],
                delay: 1,
                stats: Arc::default(),
            },
        )
        .with_boundary_control(gate)
        .with_tool_dispatch_policy(ToolDispatchPolicy {
            mode,
            ..Default::default()
        });
        let events = executor
            .execute_event_stream(request(&case), TurnControl::default())
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Ok(TurnEvent::ToolResult { .. })))
                .count(),
            case.expected.tool_calls
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Ok(TurnEvent::Cancelled { .. })))
                .count(),
            1
        );
        assert!(matches!(events.last(), Some(Err(TurnError::Cancelled))));
    }
}

#[test]
fn cancellation_and_completion_are_atomically_ordered() {
    use futures_util::FutureExt;
    for _ in 0..32 {
        let gate = Arc::new(MemoryGate::default());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let cancel = {
            let gate = gate.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                gate.cancel()
            })
        };
        let complete = {
            let gate = gate.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                gate.admit(TurnBoundary {
                    turn_id: "race".into(),
                    kind: TurnBoundaryKind::Terminal {
                        reason: TurnEndReason::FinalAnswer,
                    },
                })
                .now_or_never()
                .unwrap()
            })
        };
        barrier.wait();
        match (cancel.join().unwrap(), complete.join().unwrap()) {
            (true, Err(TurnError::Cancelled)) | (false, Ok(())) => {}
            other => panic!("inconsistent terminal ordering: {other:?}"),
        }
    }
}
