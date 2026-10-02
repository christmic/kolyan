//! Mapper-only diagnostics; constructed protocol errors are not HTTP evidence.

use std::{
    fs,
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use kolyan_protocol_openai::{OpenAiError, RetryReport};

use super::*;

#[tokio::test]
async fn terminal_opening_report_preserves_provider_classification() {
    let plan: Value = serde_json::from_str(include_str!("opening_diagnostic_cases.json")).unwrap();
    let directory = std::env::temp_dir().join(format!(
        "kolyan-openai-provider-diagnostics-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("actual.jsonl");
    println!("OPENAI_PROVIDER_DIAGNOSTIC_TRACE={}", path.display());
    let mut output = fs::File::create(&path).unwrap();
    // Invalid URL parsing produces an SDK report without opening a socket.
    // Cloned observations below are mapper fixtures, not claimed HTTP evidence.
    let mut config = kolyan_protocol_openai::OpenAiConfig::new("not-a-secret");
    config.base_url = "not-an-absolute-url".into();
    config.transport_retries = 0;
    let client = OpenAiClient::new(config).unwrap();
    let request = ResponseCreateRequest {
        model: "fixture".into(),
        input: json!([]),
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
    let source_error = match client.stream_response(&request).await {
        Err(error) => error,
        Ok(_) => panic!("invalid URL unexpectedly opened a stream"),
    };
    let template = source_error.opening_report().unwrap().observations()[0].clone();
    for case in plan["cases"].as_array().unwrap() {
        let source = match case["root"].as_str().unwrap() {
            "http" => OpenAiError::Http {
                status: u16::try_from(case["status"].as_u64().unwrap()).unwrap(),
                body: "original current cause".into(),
            },
            "budget" => OpenAiError::OpeningBudgetExhausted,
            "api" => OpenAiError::Api("original stream cause".into()),
            other => panic!("unknown mapper root {other}"),
        };
        let mut report = RetryReport::default();
        for (index, status) in case["statuses"].as_array().unwrap().iter().enumerate() {
            let mut observation = template.clone();
            observation.attempt = u32::try_from(index + 1).unwrap();
            observation.status = status.as_u64().map(|value| u16::try_from(value).unwrap());
            observation.decision = serde_json::from_value(case["decision"].clone()).unwrap();
            report.push(observation).unwrap();
        }
        if !case["stop"].is_null() {
            report.finish(serde_json::from_value(case["stop"].clone()).unwrap());
        }
        let mapped = openai_error(OpenAiError::RetriedError {
            source: Box::new(source),
            report,
        });
        writeln!(output, "{}", json!({"id":case["id"],"kind":format!("{:?}",mapped.kind),
            "phase":format!("{:?}",mapped.phase),"status":mapped.status,"diagnostics":mapped.diagnostics,
            "message":mapped.message})).unwrap();
        output.sync_all().unwrap();
    }
    let actual = fs::read_to_string(path).unwrap();
    assert_eq!(plan["schema_version"], 1);
    assert_eq!(
        actual.lines().count(),
        plan["cases"].as_array().unwrap().len()
    );
    for (line, case) in actual.lines().zip(plan["cases"].as_array().unwrap()) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["kind"], case["kind"], "{row}");
        assert_eq!(row["phase"], case["phase"], "{row}");
        assert_eq!(row["status"], case["status"], "{row}");
        assert_eq!(
            !row["diagnostics"].is_null(),
            case["diagnostic"].as_bool().unwrap(),
            "{row}"
        );
        if !row["diagnostics"].is_null() {
            assert_eq!(row["diagnostics"]["kind"], case["diagnostic_kind"], "{row}");
            assert_eq!(
                row["diagnostics"]["report"]["terminal_stop"], case["stop"],
                "{row}"
            );
            let observations = row["diagnostics"]["report"]["observations"]
                .as_array()
                .unwrap();
            assert_eq!(
                observations.len(),
                case["statuses"].as_array().unwrap().len(),
                "{row}"
            );
            for (observation, status) in observations
                .iter()
                .zip(case["statuses"].as_array().unwrap())
            {
                assert_eq!(observation["status"], *status, "{row}");
                assert_eq!(observation["decision"], case["decision"], "{row}");
            }
        }
        assert!(
            row["message"].as_str().unwrap().contains("terminal cause"),
            "{row}"
        );
        if case["root"] == "http" {
            assert!(
                row["message"]
                    .as_str()
                    .unwrap()
                    .contains("original current cause"),
                "{row}"
            );
        }
    }
}
