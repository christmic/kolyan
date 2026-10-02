//! Protocol-level differential; expectations come from the running official SDK harness.

use futures_util::StreamExt;
use kolyan_protocol_anthropic::{
    AnthropicClient, AnthropicConfig, AnthropicError, MessageCreateRequest,
};
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig, OpenAiError, ResponseCreateRequest};
use serde_json::{Value, json};
use std::{fs, time::Duration};

#[tokio::test]
#[ignore = "run sdk_transport.py to start the loopback fixture and official SDK oracle"]
async fn same_http_faults_match_official_sdk() {
    let path = std::env::var("KOLYAN_SDK_TRANSPORT_REPORT").unwrap();
    let cases: Vec<Value> = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let mut failures = vec![];
    let mut results = vec![];
    for case in cases {
        let actual = if case["protocol"] == "openai" {
            openai(&case).await
        } else {
            anthropic(&case).await
        };
        if actual != case["expected"] {
            failures.push(case["label"].clone());
        }
        results.push(json!({"label":case["label"],"expected":case["expected"],"actual":actual}));
    }
    let result_path = std::path::Path::new(&path).with_file_name("rust-results.json");
    fs::write(&result_path, serde_json::to_vec_pretty(&results).unwrap()).unwrap();
    assert!(
        failures.is_empty(),
        "differential failures {failures:?}; {}",
        result_path.display()
    );
    println!(
        "PASS {} official SDK HTTP/SSE differentials: {}",
        results.len(),
        result_path.display()
    );
}

async fn openai(case: &Value) -> Value {
    let client = OpenAiClient::new(OpenAiConfig {
        base_url: case["base_url"].as_str().unwrap().into(),
        api_key: "fixture".into(),
        timeout: Duration::from_millis(100),
        transport_retries: 0,
        http_retry: Default::default(),
        diagnostics: false,
    })
    .unwrap();
    let request = ResponseCreateRequest {
        model: "fixture".into(),
        input: json!("hello"),
        instructions: None,
        max_output_tokens: None,
        tools: vec![],
        tool_choice: None,
        text: None,
        reasoning: None,
        prompt_cache_key: None,
        prompt_cache_retention: None,
        stream: true,
    };
    let mut events = vec![];
    let error = match client.stream_response(&request).await {
        Err(error) => Some(openai_error(error)),
        Ok(mut stream) => {
            let mut error = None;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => {
                        events.push(json!({"type":event.kind,"delta":event.fields.get("delta")}))
                    }
                    Err(err) => {
                        error = Some(openai_error(err));
                        break;
                    }
                }
            }
            error
        }
    };
    json!({"events":events,"error":error})
}

async fn anthropic(case: &Value) -> Value {
    let client = AnthropicClient::new(AnthropicConfig {
        base_url: case["base_url"].as_str().unwrap().into(),
        api_key: "fixture".into(),
        version: "2023-06-01".into(),
        timeout: Duration::from_millis(100),
        transport_retries: 0,
        http_retry: Default::default(),
        diagnostics: false,
    })
    .unwrap();
    let request = MessageCreateRequest {
        model: "fixture".into(),
        max_tokens: 128,
        messages: vec![json!({"role":"user","content":"hello"})],
        system: None,
        tools: vec![],
        tool_choice: None,
        thinking: None,
        output_config: None,
        stream: true,
    };
    let mut events = vec![];
    let error = match client.stream_message(&request).await {
        Err(error) => Some(anthropic_error(error)),
        Ok(mut stream) => {
            let mut error = None;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => {
                        events.push(json!({"type":event.kind,"delta":event.fields.get("delta")}))
                    }
                    Err(err) => {
                        error = Some(anthropic_error(err));
                        break;
                    }
                }
            }
            error
        }
    };
    json!({"events":events,"error":error})
}

fn openai_error(error: OpenAiError) -> String {
    match error {
        OpenAiError::Count(failure) => format!("count:{failure}"),
        OpenAiError::Configuration(_) => "configuration".into(),
        OpenAiError::OpeningBudgetExhausted => "transport".into(),
        OpenAiError::RetriedError { source, .. } => openai_error(*source),
        OpenAiError::Api(_) => "api".into(),
        OpenAiError::Decode(_) => "json".into(),
        OpenAiError::Framing(_) => "utf8".into(),
        OpenAiError::Transport { .. } => "transport".into(),
        OpenAiError::Http { status, .. } => format!("http:{status}"),
    }
}

fn anthropic_error(error: AnthropicError) -> String {
    match error {
        AnthropicError::Count(failure) => format!("count:{failure}"),
        AnthropicError::Configuration(_) => "configuration".into(),
        AnthropicError::OpeningBudgetExhausted => "transport".into(),
        AnthropicError::RetriedError { source, .. } => anthropic_error(*source),
        AnthropicError::Api(_) => "api".into(),
        AnthropicError::Decode(_) => "json".into(),
        AnthropicError::Framing(_) => "utf8".into(),
        AnthropicError::Transport { .. } => "transport".into(),
        AnthropicError::Http { status, .. } => format!("http:{status}"),
    }
}
