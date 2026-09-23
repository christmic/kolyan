//! Real-provider SessionExecutionService matrix.
//!
//! Each configured model runs two independent Turns through the Session
//! boundary. The first Turn is completed, the process-facing services are
//! rebuilt, and the second Turn must receive the persisted first-turn context.

#[path = "common/mod.rs"]
mod common;

use common::{
    ModelMatrixEntry, build_anthropic_provider, build_openai_provider, build_request, has_api_key,
    has_api_key_anthropic, load_config, load_fixture, require_api_key, require_api_key_anthropic,
};
use futures_util::stream::StreamExt;
use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::FileLedger;
use kolyan_model::{MessageRole, ModelEventStream, ModelProvider, ModelRequest, ProviderFuture};
use kolyan_server::{ExecutionService, SessionExecutionService, SessionService};
use kolyan_storage::{FileSessionStore, SessionStore, SessionTurnStatus};
use kolyan_trace::VecTraceSink;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct RecordingProvider<P> {
    inner: P,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl<P> RecordingProvider<P> {
    fn new(inner: P) -> (Self, Arc<Mutex<Vec<ModelRequest>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner,
                requests: requests.clone(),
            },
            requests,
        )
    }
}

impl<P: ModelProvider> ModelProvider for RecordingProvider<P> {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.requests
            .lock()
            .expect("session live request recorder lock")
            .push(request.clone());
        let future = self.inner.stream(request);
        Box::pin(async move {
            let stream = future.await?;
            Ok(Box::pin(stream.map(|event| event)) as ModelEventStream)
        })
    }
}

#[tokio::test]
#[ignore = "live Session matrix: requires project-local provider API keys"]
async fn session_execution_matrix_carries_context_across_rebuilt_services() {
    let config = load_config();
    let fixture = load_fixture("text");
    let mut executed = 0usize;

    if has_api_key(&config.minimax_openai) {
        let key = require_api_key(&config.minimax_openai);
        let provider = build_openai_provider(&config.minimax_openai, &key);
        for entry in config.minimax_openai.model_matrix.clone() {
            run_case(&provider, &entry, "minimax", "openai", &fixture).await;
            executed += 1;
        }
    }
    if has_api_key_anthropic(&config.minimax_anthropic) {
        let key = require_api_key_anthropic(&config.minimax_anthropic);
        let provider = build_anthropic_provider(&config.minimax_anthropic, &key);
        for entry in config.minimax_anthropic.model_matrix.clone() {
            run_case(&provider, &entry, "minimax", "anthropic", &fixture).await;
            executed += 1;
        }
    }
    if has_api_key(&config.qwen_openai) {
        let key = require_api_key(&config.qwen_openai);
        let provider = build_openai_provider(&config.qwen_openai, &key);
        for entry in config.qwen_openai.model_matrix.clone() {
            run_case(&provider, &entry, "qwen", "openai", &fixture).await;
            executed += 1;
        }
    }
    if has_api_key_anthropic(&config.qwen_anthropic) {
        let key = require_api_key_anthropic(&config.qwen_anthropic);
        let provider = build_anthropic_provider(&config.qwen_anthropic, &key);
        for entry in config.qwen_anthropic.model_matrix.clone() {
            run_case(&provider, &entry, "qwen", "anthropic", &fixture).await;
            executed += 1;
        }
    }
    assert_eq!(
        executed, 21,
        "the configured Session matrix must cover all 21 entries"
    );
}

async fn run_case<P>(
    provider: &P,
    entry: &ModelMatrixEntry,
    family: &str,
    surface: &str,
    fixture: &common::Fixture,
) where
    P: ModelProvider + Clone + 'static,
{
    let label = format!("session/{family}/{surface}/{}", entry.model);
    let root = std::env::temp_dir().join(format!(
        "kolyan-session-live-{}-{}-{}-{}",
        family,
        surface,
        entry.model.replace(['/', ':'], "_"),
        std::process::id()
    ));
    let ledger_path = root.join("ledger.jsonl");
    let session_path = root.join("sessions");
    let store = FileSessionStore::new(&session_path).expect("SessionStore should open");
    store.create("live-session").expect("Session should create");
    let (provider, requests) = RecordingProvider::new(provider.clone());

    let first = SessionExecutionService::new(
        ExecutionService::new(
            FileLedger::open(&ledger_path).expect("Ledger should open"),
            VecTraceSink::default(),
        ),
        SessionService::new(store),
    );
    let first_request = TurnRequest {
        turn_id: format!("{label}/turn-1"),
        model_request: build_request(
            family,
            &entry.model,
            fixture,
            format!("{label}/request-1"),
            entry.max_output_tokens,
        ),
        config: TurnConfig {
            max_steps: 2,
            ..TurnConfig::default()
        },
    };
    first
        .start(
            TurnExecutor::new(provider.clone()),
            first_request,
            "live-session",
            format!("{label}/execution-1"),
        )
        .await
        .unwrap_or_else(|error| panic!("[{label}] first Session Turn failed: {error}"));
    drop(first);

    let reopened = SessionExecutionService::new(
        ExecutionService::new(
            FileLedger::open(&ledger_path).expect("Ledger should reopen"),
            VecTraceSink::default(),
        ),
        SessionService::new(
            FileSessionStore::new(&session_path).expect("SessionStore should reopen"),
        ),
    );
    let second_request = TurnRequest {
        turn_id: format!("{label}/turn-2"),
        model_request: build_request(
            family,
            &entry.model,
            fixture,
            format!("{label}/request-2"),
            entry.max_output_tokens,
        ),
        config: TurnConfig {
            max_steps: 2,
            ..TurnConfig::default()
        },
    };
    reopened
        .start(
            TurnExecutor::new(provider),
            second_request,
            "live-session",
            format!("{label}/execution-2"),
        )
        .await
        .unwrap_or_else(|error| panic!("[{label}] second Session Turn failed: {error}"));

    let captured = requests.lock().expect("request recorder lock");
    assert_eq!(captured.len(), 2, "[{label}] expected two model calls");
    assert!(
        captured[1].messages.len() > captured[0].messages.len(),
        "[{label}] second Turn did not receive persisted context"
    );
    assert!(
        captured[1]
            .messages
            .iter()
            .any(|message| message.role == MessageRole::Assistant),
        "[{label}] persisted assistant context missing"
    );
    let session = FileSessionStore::new(&session_path)
        .expect("SessionStore should reopen for assertion")
        .load("live-session")
        .expect("Session should load");
    assert_eq!(session.turns.len(), 2, "[{label}] turn count");
    assert!(
        session
            .turns
            .iter()
            .all(|turn| turn.status == SessionTurnStatus::Completed),
        "[{label}] non-terminal Session turn"
    );
    std::fs::remove_dir_all(root).expect("live Session fixture should clean up");
}
