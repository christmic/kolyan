use super::*;

use std::task::Context;

use futures_util::task::{ArcWake, waker};

#[derive(Deserialize)]
struct Plan {
    broadcast: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    cancel_at: CancelAt,
    calls: usize,
    stream_drops: usize,
    first_woken: bool,
    second_woken: bool,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CancelAt {
    BeforeOpening,
    BeforeRegistration,
    BetweenRegistrations,
    AfterRegistration,
}

#[derive(Debug, Serialize)]
struct Actual {
    id: String,
    first_request: Value,
    second_request: Value,
    provider_requests: Vec<ModelRequest>,
    first_observations: Vec<Value>,
    second_observations: Vec<Value>,
    first_registration: String,
    second_registration: String,
    first_woken: bool,
    second_woken: bool,
    first_events: Vec<String>,
    second_events: Vec<String>,
    calls: usize,
    stream_drops: usize,
    sticky: bool,
}

#[derive(Default)]
struct WakeCount(std::sync::atomic::AtomicUsize);

impl ArcWake for WakeCount {
    fn wake_by_ref(owner: &Arc<Self>) {
        owner.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll(stream: &mut StepEventStream, wake: &Arc<WakeCount>, full: &mut Vec<Value>) -> String {
    let waker = waker(wake.clone());
    match stream.as_mut().poll_next(&mut Context::from_waker(&waker)) {
        Poll::Ready(Some(event)) => capture::observe(event, full),
        Poll::Ready(None) => "eof".into(),
        Poll::Pending => "pending".into(),
    }
}

#[tokio::test]
async fn cancellation_broadcast_and_registration_matrix() {
    let plan: Plan = serde_json::from_str(include_str!("additional.json")).unwrap();
    let path = evidence_path("broadcast");
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    eprintln!("STEP_LIVENESS_EVIDENCE {}", path.display());
    let mut rows = Vec::new();
    for case in &plan.broadcast {
        let state = Arc::new(Observed::default());
        let executor = StepExecutor::new(ScriptProvider {
            state: state.clone(),
            mode: Mode::Pending,
        });
        let control = StepControl::default();
        if matches!(case.cancel_at, CancelAt::BeforeOpening) {
            control.cancel();
        }
        let input = |suffix: &str| StepRequest {
            step_id: format!("{}-{suffix}", case.id),
            model_request: request(),
            options: StepExecutionOptions::default(),
        };
        let first_input = input("first");
        let second_input = input("second");
        let first_request = capture::request_value(&first_input);
        let second_request = capture::request_value(&second_input);
        let mut first = executor
            .start_with_control(first_input, control.clone())
            .await
            .unwrap();
        let mut second = executor
            .start_with_control(second_input, control.clone())
            .await
            .unwrap();
        let first_waker = Arc::new(WakeCount::default());
        let second_waker = Arc::new(WakeCount::default());
        if matches!(case.cancel_at, CancelAt::BeforeRegistration) {
            control.cancel();
        }
        let mut first_observations = Vec::new();
        let mut second_observations = Vec::new();
        let mut first_events = vec![poll(
            &mut first.stream,
            &first_waker,
            &mut first_observations,
        )];
        let mut second_events = vec![poll(
            &mut second.stream,
            &second_waker,
            &mut second_observations,
        )];
        let first_registration = poll(&mut first.stream, &first_waker, &mut first_observations);
        if first_registration != "pending" {
            first_events.push(first_registration.clone());
        }
        if matches!(case.cancel_at, CancelAt::BetweenRegistrations) {
            control.cancel();
        }
        let second_registration = poll(&mut second.stream, &second_waker, &mut second_observations);
        if second_registration != "pending" {
            second_events.push(second_registration.clone());
        }
        if matches!(case.cancel_at, CancelAt::AfterRegistration) {
            control.cancel();
        }
        // Inspect actual wake counts before another manual poll: the test must
        // prove broadcast, not rescue an unwoken consumer by polling it again.
        let first_woken = first_waker.0.load(Ordering::SeqCst) > 0;
        let second_woken = second_waker.0.load(Ordering::SeqCst) > 0;
        control.cancel();
        for (stream, wake, events, observations) in [
            (
                &mut first.stream,
                &first_waker,
                &mut first_events,
                &mut first_observations,
            ),
            (
                &mut second.stream,
                &second_waker,
                &mut second_events,
                &mut second_observations,
            ),
        ] {
            if events.last().is_none_or(|event| event != "cancelled") {
                events.push(poll(stream, wake, observations));
            }
            events.push(poll(stream, wake, observations));
        }
        let actual = Actual {
            id: case.id.clone(),
            first_request,
            second_request,
            provider_requests: state.requests.lock().unwrap().clone(),
            first_observations,
            second_observations,
            first_registration,
            second_registration,
            first_woken,
            second_woken,
            first_events,
            second_events,
            calls: state.calls.load(Ordering::SeqCst),
            stream_drops: state.stream_drops.load(Ordering::SeqCst),
            sticky: control.is_cancelled(),
        };
        serde_json::to_writer(&mut output, &actual).unwrap();
        writeln!(output).unwrap();
        output.flush().unwrap();
        rows.push(actual);
    }
    for (case, actual) in plan.broadcast.iter().zip(&rows) {
        assert_eq!(actual.provider_requests.len(), actual.calls);
        for received in &actual.provider_requests {
            assert_eq!(
                serde_json::to_value(received).unwrap(),
                actual.first_request["model_request"]
            );
        }
        assert_eq!(actual.calls, case.calls, "{actual:?}");
        assert_eq!(actual.stream_drops, case.stream_drops, "{actual:?}");
        assert_eq!(actual.first_woken, case.first_woken, "{actual:?}");
        assert_eq!(actual.second_woken, case.second_woken, "{actual:?}");
        assert_eq!(
            actual.first_events,
            ["started", "cancelled", "eof"],
            "{actual:?}"
        );
        assert_eq!(
            actual.second_events,
            ["started", "cancelled", "eof"],
            "{actual:?}"
        );
        assert!(actual.sticky, "{actual:?}");
        for (input, events) in [
            (&actual.first_request, &actual.first_observations),
            (&actual.second_request, &actual.second_observations),
        ] {
            assert_eq!(
                input["model_request"],
                serde_json::to_value(request()).unwrap()
            );
            assert_eq!(events.len(), 2);
            assert_eq!(
                events[0],
                serde_json::json!({"kind":"started","step_id":input["step_id"]})
            );
            assert_eq!(
                events[1],
                serde_json::json!({"kind":"cancelled","step_id":input["step_id"]})
            );
        }
    }
}
