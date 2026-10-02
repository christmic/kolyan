//! The host sees the final history projection before any immutable input is saved.
use super::*;
use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_model::{ContentBlock, ModelRef, ModelRequest, ProviderFuture, ToolChoice};

struct NeverProvider;
impl ModelProvider for NeverProvider {
    fn stream(&self, _: ModelRequest) -> ProviderFuture<'_> {
        panic!("rejected preparation must not invoke a model")
    }
}
struct RejectHook {
    seen: Arc<Mutex<Vec<TurnRequest>>>,
}
impl TurnPreparationHook for RejectHook {
    fn prepare(&self, execution: &ExecutionRef, request: &TurnRequest) -> Result<(), ServerError> {
        assert_eq!(execution.session_id, "s");
        assert_eq!(execution.turn_id, request.turn_id);
        self.seen.lock().unwrap().push(request.clone());
        Err(StorageError::Conflict("host rejected final binding".into()).into())
    }
}

#[tokio::test]
async fn final_request_hook_sees_history_and_rejection_preserves_session_and_ledger() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(directory.path()).unwrap();
    store.create("s").unwrap();
    let text = |value: &str| Message {
        role: kolyan_model::MessageRole::User,
        content: vec![ContentBlock::Text { text: value.into() }],
    };
    let history = text("prior private context");
    store
        .append_turn(
            "s",
            SessionTurn {
                turn_id: "prior".into(),
                execution_id: "prior-e".into(),
                status: SessionTurnStatus::Completed,
            },
            vec![history.clone()],
        )
        .unwrap();
    let before = store.load("s").unwrap();
    let ledger = InMemoryLedger::default();
    let seen = Arc::new(Mutex::new(vec![]));
    let service = SessionExecutionService::new(
        ExecutionService::new(ledger.clone(), VecTraceSink::default()),
        SessionService::new(store.clone()),
    )
    .with_preparation_hook(Arc::new(RejectHook { seen: seen.clone() }));
    let input = text("current input");
    let request = TurnRequest {
        turn_id: "t".into(),
        config: TurnConfig {
            max_steps: 2,
            ..TurnConfig::default()
        },
        model_request: ModelRequest {
            request_id: "r".into(),
            model: ModelRef::new("fixture", "model"),
            system: vec![],
            messages: vec![input.clone()],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(32),
            extensions: Value::Null,
        },
    };
    assert!(
        service
            .start(TurnExecutor::new(NeverProvider), request, "s", "e")
            .await
            .is_err()
    );
    let saved = seen.lock().unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].model_request.messages, vec![history, input]);
    assert_eq!(saved[0].config.max_steps, 2);
    assert_eq!(store.load("s").unwrap(), before);
    assert!(ledger.events_after(0).unwrap().is_empty());
}
