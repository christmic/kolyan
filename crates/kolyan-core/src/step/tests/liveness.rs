use super::*;

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use support::{Mode, Observed, RejectRecorder, RejectValidator, ScriptProvider, event_label};

mod aggregation;
mod broadcast;
mod capture;
mod support;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    watchdog_ms: u64,
    deadline_ms: u64,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: Mode,
    action: Action,
    expected: Vec<String>,
    calls: usize,
    opening_drops: usize,
    stream_drops: usize,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    PreCancel,
    PreDeadline,
    OpeningCancel,
    Deadline,
    Cancel,
    Both,
    Recording,
    Validation,
    DropOpening,
    DropStream,
    Collect,
}

#[derive(Debug, Serialize)]
struct Actual {
    id: String,
    request: Value,
    provider_requests: Vec<ModelRequest>,
    observations: Vec<Value>,
    events: Vec<String>,
    calls: usize,
    opening_polls: usize,
    opening_drops: usize,
    stream_polls: usize,
    stream_drops: usize,
    watchdog: bool,
    reached_pending: bool,
}

impl Actual {
    fn capture(
        case: &Case,
        state: &Observed,
        events: Vec<String>,
        request: Value,
        observations: Vec<Value>,
        watchdog: bool,
        pending: bool,
    ) -> Self {
        Self {
            id: case.id.clone(),
            request,
            provider_requests: state.requests.lock().unwrap().clone(),
            observations,
            events,
            calls: state.calls.load(Ordering::SeqCst),
            opening_polls: state.opening_polls.load(Ordering::SeqCst),
            opening_drops: state.opening_drops.load(Ordering::SeqCst),
            stream_polls: state.stream_polls.load(Ordering::SeqCst),
            stream_drops: state.stream_drops.load(Ordering::SeqCst),
            watchdog,
            reached_pending: pending,
        }
    }
}

#[tokio::test]
async fn standalone_liveness_fault_matrix() {
    let plan: Plan = serde_json::from_str(include_str!("liveness.json")).unwrap();
    let mut rows = Vec::new();
    let path = evidence_path("matrix");
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    eprintln!("STEP_LIVENESS_EVIDENCE {}", path.display());
    for case in &plan.cases {
        let actual = observe(case, &plan).await;
        serde_json::to_writer(&mut output, &actual).unwrap();
        writeln!(output).unwrap();
        output.flush().unwrap();
        rows.push(actual);
    }
    capture::content_regression().await;
    for actual in &rows {
        assert_eq!(actual.provider_requests.len(), actual.calls);
        for received in &actual.provider_requests {
            assert_eq!(
                serde_json::to_value(received).unwrap(),
                actual.request["model_request"]
            );
        }
        assert_eq!(
            actual.request["model_request"],
            serde_json::to_value(request()).unwrap()
        );
        assert_eq!(
            actual.observations.len(),
            actual.events.iter().filter(|label| *label != "eof").count()
        );
        for observation in &actual.observations {
            match observation["kind"].as_str() {
                Some("completed") => {
                    let result: StepResult =
                        serde_json::from_value(observation["result"].clone()).unwrap();
                    assert_eq!(result.step_id, actual.id);
                    assert_eq!(serde_json::to_value(result).unwrap(), observation["result"]);
                }
                Some("error") => {
                    assert!(observation["error_type"].is_string());
                    assert!(
                        observation["message"]
                            .as_str()
                            .is_some_and(|message| !message.is_empty())
                    );
                    assert!(observation["causal_message"].is_string());
                }
                _ => assert_eq!(observation["step_id"], actual.id),
            }
        }
    }
    let failures: Vec<_> = plan
        .cases
        .iter()
        .zip(&rows)
        .filter_map(|(case, actual)| {
            let pending_required = matches!(
                case.action,
                Action::OpeningCancel | Action::DropOpening | Action::Cancel | Action::DropStream
            );
            (actual.events != case.expected
                || actual.calls != case.calls
                || actual.opening_drops != case.opening_drops
                || actual.stream_drops != case.stream_drops
                || actual.watchdog
                || (pending_required && !actual.reached_pending))
                .then(|| format!("{}: {actual:?}", case.id))
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} failures after all {} rows exported:\n{}",
        failures.len(),
        rows.len(),
        failures.join("\n")
    );
}

async fn observe(case: &Case, plan: &Plan) -> Actual {
    let state = Arc::new(Observed::default());
    let provider = ScriptProvider {
        state: state.clone(),
        mode: case.mode,
    };
    let mut executor = if matches!(case.action, Action::Validation) {
        StepExecutor::with_validator(provider, RejectValidator)
    } else {
        StepExecutor::new(provider)
    };
    if matches!(case.action, Action::Recording) {
        executor = executor.with_event_recorder(Arc::new(RejectRecorder));
    }
    let control = StepControl::default();
    if matches!(case.action, Action::PreCancel | Action::Both) {
        control.cancel();
    }
    let deadline = match case.action {
        Action::PreDeadline | Action::Both => Some(Instant::now() - Duration::from_secs(1)),
        Action::Deadline => Some(Instant::now() + Duration::from_millis(plan.deadline_ms)),
        _ => None,
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
    let mut observations = Vec::new();
    let mut opening = Box::pin(executor.start_with_control(input, control.clone()));
    let mut pending = false;
    if matches!(case.action, Action::OpeningCancel | Action::DropOpening) {
        pending = futures_util::poll!(opening.as_mut()).is_pending();
        if matches!(case.action, Action::DropOpening) {
            drop(opening);
            return Actual::capture(
                case,
                &state,
                Vec::new(),
                full_request,
                observations,
                false,
                pending,
            );
        }
        control.cancel();
    }
    let mut execution =
        match tokio::time::timeout(Duration::from_millis(plan.watchdog_ms), opening).await {
            Ok(Ok(execution)) => execution,
            Ok(Err(error)) => {
                let label = capture::observe(Err(error), &mut observations);
                return Actual::capture(
                    case,
                    &state,
                    vec![label],
                    full_request,
                    observations,
                    false,
                    pending,
                );
            }
            Err(_) => {
                return Actual::capture(
                    case,
                    &state,
                    Vec::new(),
                    full_request,
                    observations,
                    true,
                    pending,
                );
            }
        };
    let mut events = Vec::new();
    if matches!(case.action, Action::Cancel | Action::DropStream) {
        if let Some(event) = execution.stream.next().await {
            events.push(capture::observe(event, &mut observations));
        }
        if matches!(case.mode, Mode::CompletedPending)
            && let Some(event) = execution.stream.next().await
        {
            events.push(capture::observe(event, &mut observations));
        }
        let mut next = Box::pin(execution.stream.next());
        pending = futures_util::poll!(next.as_mut()).is_pending();
        drop(next);
        if matches!(case.action, Action::DropStream) {
            drop(execution);
            return Actual::capture(
                case,
                &state,
                events,
                full_request,
                observations,
                false,
                pending,
            );
        }
        control.cancel();
    }
    let collection = tokio::time::timeout(Duration::from_millis(plan.watchdog_ms), async {
        while let Some(event) = execution.stream.next().await {
            events.push(capture::observe(event, &mut observations));
        }
        events.push("eof".into());
    })
    .await;
    // Retain the execution object while observing drops: a terminal must release
    // resources itself, not rely on this test dropping the public handle.
    Actual::capture(
        case,
        &state,
        events,
        full_request,
        observations,
        collection.is_err(),
        pending,
    )
}

pub(super) fn evidence_path(label: &str) -> std::path::PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "kolyan-step-liveness-{label}-{}-{stamp}-{}.jsonl",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}
