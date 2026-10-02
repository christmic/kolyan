//! Test-owned complete observations, independent of the semantic event labels.

use super::*;

use serde_json::{Value, json};

pub(super) fn item(item: &Result<StepEvent, StepError>) -> Value {
    match item {
        Ok(event) => event_value(event),
        Err(error) => error_value(error),
    }
}

pub(super) fn event_value(event: &StepEvent) -> Value {
    match event {
        StepEvent::Started { step_id } => json!({"kind":"started","step_id":step_id}),
        StepEvent::TextDelta { step_id, text } => {
            json!({"kind":"text_delta","step_id":step_id,"text":text})
        }
        StepEvent::ReasoningDelta { step_id, text } => {
            json!({"kind":"reasoning_delta","step_id":step_id,"text":text})
        }
        StepEvent::ToolCallStarted { step_id, id, name } => {
            json!({"kind":"tool_call_started","step_id":step_id,"id":id,"name":name})
        }
        StepEvent::ToolCallArgumentsDelta { step_id, id, delta } => {
            json!({"kind":"tool_call_arguments_delta","step_id":step_id,"id":id,"delta":delta})
        }
        StepEvent::ToolCallCompleted { step_id, call } => {
            json!({"kind":"tool_call_completed","step_id":step_id,"call":call})
        }
        StepEvent::Usage { step_id, usage } => {
            json!({"kind":"usage","step_id":step_id,"usage":usage})
        }
        StepEvent::Provider { step_id, metadata } => {
            json!({"kind":"provider","step_id":step_id,"metadata":metadata})
        }
        StepEvent::Completed(result) => json!({"kind":"completed","result":result}),
        StepEvent::Cancelled { step_id } => json!({"kind":"cancelled","step_id":step_id}),
        StepEvent::TimedOut { step_id } => json!({"kind":"timed_out","step_id":step_id}),
    }
}

pub(super) fn error_value(error: &StepError) -> Value {
    let (error_type, detail) = match error {
        StepError::Provider(cause) => (
            "provider",
            json!({
                "kind":cause.kind,"phase":cause.phase,"message":cause.message,
                "provider":cause.provider,"status":cause.status,"diagnostics":cause.diagnostics
            }),
        ),
        StepError::Protocol { message } => ("protocol", json!({"message":message})),
        StepError::InvalidRequest { message } => ("invalid_request", json!({"message":message})),
        StepError::Validation(cause) => ("validation", json!({"message":cause.message})),
        StepError::Recording(cause) => ("recording", json!({"message":cause.message})),
        StepError::Cancelled => ("cancelled", Value::Null),
        StepError::TimedOut => ("timed_out", Value::Null),
    };
    json!({"kind":"error","error_type":error_type,
        "message":error.to_string(),"causal_message":ProviderError::describe(error),"detail":detail})
}

pub(super) fn request_value(input: &StepRequest) -> Value {
    json!({"step_id":input.step_id,"model_request":input.model_request,
        "options":{"deadline_configured":input.options.deadline.is_some(),
        "context_version":input.options.context_version,"trace_id":input.options.trace_id}})
}

pub(super) fn observe(item: Result<StepEvent, StepError>, full: &mut Vec<Value>) -> String {
    full.push(self::item(&item));
    event_label(item)
}

pub(super) async fn content_regression() {
    // Exercise the real Step mapping with the existing rich DeltaProvider. No
    // production serializer, Recorder, or original fixture is changed.
    let input = StepRequest {
        step_id: "complete-content-observation".into(),
        model_request: request(),
        options: StepExecutionOptions::default(),
    };
    let full_request = request_value(&input);
    let executor = StepExecutor::new(super::super::DeltaProvider);
    let mut stream = executor.execute_stream(input).await.unwrap();
    let mut events = Vec::new();
    while let Some(item) = stream.next().await {
        events.push(self::item(&item));
    }
    let actual =
        json!({"case_id":"complete_content_observation","request":full_request,"events":events});
    let path = evidence_path("complete-content");
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    serde_json::to_writer(&mut output, &actual).unwrap();
    writeln!(output).unwrap();
    output.flush().unwrap();
    eprintln!("STEP_LIVENESS_EVIDENCE {}", path.display());

    assert_eq!(
        actual["request"]["model_request"],
        serde_json::to_value(request()).unwrap()
    );
    assert_eq!(
        actual["events"][0],
        json!({"kind":"started","step_id":"complete-content-observation"})
    );
    assert_eq!(actual["events"][1]["text"], "he");
    assert_eq!(actual["events"][2]["text"], "think");
    assert_eq!(actual["events"][3]["usage"]["output_tokens"], 2);
    let completed = &actual["events"][4]["result"];
    let decoded: StepResult = serde_json::from_value(completed.clone()).unwrap();
    assert_eq!(decoded.step_id, "complete-content-observation");
    assert_eq!(decoded.response.id, "response-delta");
    assert_eq!(
        decoded.response.content,
        vec![kolyan_model::ContentBlock::Text {
            text: "hello".into()
        }]
    );
    assert_eq!(decoded.outcome, StepOutcome::FinalAnswer);
    assert_eq!(serde_json::to_value(decoded).unwrap(), *completed);
}
