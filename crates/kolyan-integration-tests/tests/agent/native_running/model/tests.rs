//! Pure Provider-port tests; never a real-model acceptance receipt.

use super::super::super::{data, harness};
use super::*;
use kolyan_model::ModelEvent;
use serde::Deserialize;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fake {
    opening_hangs: bool,
    streaming_hangs: bool,
    request: Mutex<Option<ModelRequest>>,
}
impl ModelProvider for Fake {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        *self.request.lock().unwrap() = Some(request);
        Box::pin(async move {
            if self.opening_hangs {
                return std::future::pending().await;
            }
            let events = stream::iter([Ok(ModelEvent::Started)]);
            if self.streaming_hangs {
                Ok(Box::pin(events.chain(stream::pending())) as ModelEventStream)
            } else {
                let call = kolyan_model::ToolCall {
                    id: "sdk/native/🦀".into(),
                    name: "shell".into(),
                    arguments: json!({"command":"printf untouched"}),
                };
                Ok(
                    Box::pin(events.chain(stream::iter([Ok(ModelEvent::ToolCallCompleted(call))])))
                        as ModelEventStream,
                )
            }
        })
    }
}

fn request() -> ModelRequest {
    harness::request(
        &data::dataset().turns[0],
        &ModelRef::new("fixture", "clock"),
        8192,
    )
}

fn setup(
    opening_hangs: bool,
    streaming_hangs: bool,
) -> (tempfile::TempDir, BoundedModel, Arc<Fake>) {
    let root = tempfile::tempdir().unwrap();
    let inner = Arc::new(Fake {
        opening_hangs,
        streaming_hangs,
        request: Mutex::new(None),
    });
    let provider = BoundedModel {
        inner: inner.clone(),
        wait: Duration::from_millis(20),
        evidence: Arc::new(Evidence::new(&root.path().join("actual.jsonl"))),
    };
    (root, provider, inner)
}

#[tokio::test]
async fn model_open_wait_bound_is_explicit_fixture_failure() {
    let (_root, provider, _) = setup(true, false);
    let error = match provider.stream(request()).await {
        Ok(_) => panic!("opening unexpectedly completed"),
        Err(error) => error,
    };
    assert_eq!(error.phase, ProviderErrorPhase::Open);
    assert_eq!(error.kind, ProviderErrorKind::Other);
    assert_eq!(
        provider.evidence.rows()[1]["event"],
        "fixture_model_timeout"
    );
}

#[tokio::test]
async fn model_stream_wait_bound_retains_original_event_then_one_failure() {
    let (_root, provider, _) = setup(false, true);
    let mut events = provider.stream(request()).await.unwrap();
    assert!(matches!(events.next().await, Some(Ok(ModelEvent::Started))));
    let error = events.next().await.unwrap().unwrap_err();
    assert_eq!(error.phase, ProviderErrorPhase::Stream);
    assert!(events.next().await.is_none());
    assert_eq!(provider.evidence.rows().len(), 2);
}

#[tokio::test]
async fn model_wrapper_preserves_actual_request_and_events() {
    let (_root, provider, inner) = setup(false, false);
    let request = request();
    let expected = serde_json::to_value(&request).unwrap();
    let mut events = provider.stream(request).await.unwrap();
    assert!(matches!(events.next().await, Some(Ok(ModelEvent::Started))));
    let call = events.next().await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(call).unwrap(),
        json!({"ToolCallCompleted":{
        "id":"sdk/native/🦀","name":"shell","arguments":{"command":"printf untouched"}}})
    );
    assert!(events.next().await.is_none());
    assert_eq!(
        serde_json::to_value(inner.request.lock().unwrap().as_ref().unwrap()).unwrap(),
        expected
    );
    assert_eq!(provider.evidence.rows().len(), 1);
    assert_eq!(provider.evidence.rows()[0]["event"], "model_wait_started");
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorCase {
    stream_error: bool,
    kind: ProviderErrorKind,
    phase: ProviderErrorPhase,
    message: String,
    provider: String,
    status: u16,
}

struct ErrorFake {
    case: ErrorCase,
    calls: AtomicUsize,
}
impl ModelProvider for ErrorFake {
    fn stream(&self, _: ModelRequest) -> ProviderFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let error = ProviderError {
                kind: self.case.kind,
                phase: self.case.phase,
                message: self.case.message.clone(),
                provider: Some(self.case.provider.clone()),
                status: Some(self.case.status),
                diagnostics: None,
            };
            if self.case.stream_error {
                Ok(Box::pin(stream::iter([Err(error)])) as ModelEventStream)
            } else {
                Err(error)
            }
        })
    }
}

#[tokio::test]
async fn original_provider_failures_keep_fields_and_are_not_retried() {
    let cases: Vec<ErrorCase> = serde_json::from_str(include_str!("error_cases.json")).unwrap();
    for case in cases {
        let root = tempfile::tempdir().unwrap();
        let inner = Arc::new(ErrorFake {
            case: case.clone(),
            calls: AtomicUsize::new(0),
        });
        let provider = BoundedModel {
            inner: inner.clone(),
            wait: Duration::from_secs(1),
            evidence: Arc::new(Evidence::new(&root.path().join("actual.jsonl"))),
        };
        let error = match provider.stream(request()).await {
            Ok(mut events) => events.next().await.unwrap().unwrap_err(),
            Err(error) => error,
        };
        assert_eq!(error.kind, case.kind);
        assert_eq!(error.phase, case.phase);
        assert_eq!(error.message, case.message);
        assert_eq!(error.provider.as_deref(), Some(case.provider.as_str()));
        assert_eq!(error.status, Some(case.status));
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        let records = provider.evidence.rows();
        assert_eq!(records[1]["event"], "original_provider_error");
        assert_eq!(records[1]["status"], case.status);
        assert!(
            records
                .iter()
                .all(|record| record["event"] != "fixture_model_timeout")
        );
    }
}
