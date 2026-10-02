//! Binding preserves actual validation and recording, not just configuration labels.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

struct Counting<P> {
    inner: P,
    opens: Arc<AtomicUsize>,
}

impl<P: ModelProvider> ModelProvider for Counting<P> {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.stream(request)
    }
}

struct Reject;

impl StepValidator for Reject {
    fn validate(&self, _: &ModelRequest, _: &StepResult) -> Result<(), StepValidationError> {
        Err(StepValidationError {
            message: "original validator rejected completion".into(),
        })
    }
}

#[tokio::test]
async fn mapped_provider_retains_actual_validator_and_recorder() {
    let recorder = Arc::new(CapturingRecorder::default());
    let source =
        StepExecutor::with_validator(DeltaProvider, Reject).with_event_recorder(recorder.clone());
    let validator = source.validator.clone();
    let observer = source.event_recorder.clone().unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let mapped = source
        .try_map_provider(|inner| {
            Ok::<_, ()>(Counting {
                inner,
                opens: opens.clone(),
            })
        })
        .unwrap();
    assert!(Arc::ptr_eq(&validator, &mapped.validator));
    assert!(Arc::ptr_eq(
        &observer,
        mapped.event_recorder.as_ref().unwrap()
    ));
    let result = mapped
        .execute(StepRequest {
            step_id: "mapped-step".into(),
            model_request: request(),
            options: StepExecutionOptions::default(),
        })
        .await;
    assert!(matches!(result, Err(StepError::Validation(_))));
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    let events = recorder.events.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StepEvent::TextDelta { text, .. } if text == "he"))
    );
    assert!(!events.iter().any(|e| matches!(e, StepEvent::Completed(_))));
}

#[test]
fn rejected_binding_preserves_error_and_opens_nothing() {
    let opens = Arc::new(AtomicUsize::new(0));
    let source = StepExecutor::new(Counting {
        inner: DeltaProvider,
        opens: opens.clone(),
    });
    let calls = AtomicUsize::new(0);
    let result = source.try_map_provider(|_| {
        calls.fetch_add(1, Ordering::SeqCst);
        Err::<DeltaProvider, _>("attempt binding refused")
    });
    assert!(matches!(result, Err("attempt binding refused")));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}
