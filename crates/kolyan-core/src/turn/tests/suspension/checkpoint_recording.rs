//! The host checkpoint barrier precedes effects without adding an event variant.

use super::*;

struct CheckpointRecorder {
    snapshots: Mutex<Vec<TurnCheckpoint>>,
    started: Mutex<Vec<String>>,
    fail_at: Option<usize>,
}

impl CheckpointRecorder {
    fn new(fail_at: Option<usize>) -> Self {
        Self {
            snapshots: Mutex::new(vec![]),
            started: Mutex::new(vec![]),
            fail_at,
        }
    }
}

impl TurnEventRecorder for CheckpointRecorder {
    fn record(&self, event: &TurnEvent) -> Result<(), TurnError> {
        if let TurnEvent::ToolExecutionStarted { call_id, .. } = event {
            let snapshots = self.snapshots.lock().unwrap();
            let latest = snapshots.last().expect("checkpoint must precede admission");
            let call = latest
                .calls
                .iter()
                .find(|item| item.call.id == *call_id)
                .unwrap();
            assert!(matches!(call.state, CheckpointCallState::Ready));
            assert!(!call.charged);
            assert!(call.prepared.is_some());
            latest.validate(&latest.scope).unwrap();
            self.started.lock().unwrap().push(call_id.clone());
        }
        Ok(())
    }

    fn record_checkpoint(&self, checkpoint: &TurnCheckpoint) -> Result<(), TurnError> {
        checkpoint.validate(&checkpoint.scope).unwrap();
        let mut snapshots = self.snapshots.lock().unwrap();
        if self.fail_at == Some(snapshots.len()) {
            return Err(TurnError::BoundaryControl {
                message: "checkpoint publication refused".into(),
            });
        }
        snapshots.push(checkpoint.clone());
        Ok(())
    }
}

#[tokio::test]
async fn checkpoint_barrier_precedes_each_serial_stage_with_completed_history() {
    let tools = ScenarioTools::new(&[("head", Behavior::Complete), ("tail", Behavior::Complete)]);
    let (executor, requests) = executor(
        vec![
            call("head", "file.read", "a"),
            call("tail", "file.read", "b"),
        ],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let recorder = Arc::new(CheckpointRecorder::new(None));
    let outcome = executor
        .with_event_recorder(recorder.clone())
        .start_resumable(input(2))
        .await
        .unwrap();
    assert!(matches!(outcome, ResumableTurn::Completed(_)));
    assert_eq!(tools.executed(), ["head", "tail"]);
    assert_eq!(*recorder.started.lock().unwrap(), ["head", "tail"]);
    assert_eq!(requests.lock().unwrap().len(), 2);
    let snapshots = recorder.snapshots.lock().unwrap();
    assert_eq!(snapshots.len(), 2);
    assert!(
        snapshots[0]
            .calls
            .iter()
            .all(|item| matches!(item.state, CheckpointCallState::Ready))
    );
    assert_eq!(snapshots[0].budget.tool_calls_used, 0);
    assert!(matches!(
        snapshots[1].calls[0].state,
        CheckpointCallState::Completed { .. }
    ));
    assert!(snapshots[1].calls[0].charged);
    assert_eq!(snapshots[1].budget.tool_calls_used, 1);
    assert_eq!(snapshots[0].steps, snapshots[1].steps);
}

#[tokio::test]
async fn checkpoint_publication_failure_blocks_first_or_later_effect_and_next_model() {
    for fail_at in [0, 1] {
        let tools =
            ScenarioTools::new(&[("head", Behavior::Complete), ("tail", Behavior::Complete)]);
        let (executor, requests) = executor(
            vec![
                call("head", "file.read", "a"),
                call("tail", "file.read", "b"),
            ],
            tools.clone(),
            ToolDispatchMode::Serial,
            false,
        );
        let recorder = Arc::new(CheckpointRecorder::new(Some(fail_at)));
        let error = executor
            .with_event_recorder(recorder.clone())
            .start_resumable(input(2))
            .await
            .unwrap_err();
        assert!(
            matches!(error, TurnError::BoundaryControl { ref message } if message == "checkpoint publication refused")
        );
        let expected: Vec<String> = if fail_at == 0 {
            vec![]
        } else {
            vec!["head".into()]
        };
        assert_eq!(tools.executed(), expected);
        assert_eq!(*recorder.started.lock().unwrap(), expected);
        assert_eq!(recorder.snapshots.lock().unwrap().len(), fail_at);
        assert_eq!(requests.lock().unwrap().len(), 1);
    }
}
