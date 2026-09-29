//! Match real model calls, actual tool receipts and next-request context.

use kolyan_model::{ContentBlock, ModelRequest, ToolResult};
use serde_json::Value;

pub(super) fn save_ledger(directory: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    use kolyan_ledger::LedgerStore;
    use std::io::Write;

    let ledger = kolyan_ledger::SqliteLedger::open(directory.join("ledger.sqlite"))?;
    let mut output = std::fs::File::create(directory.join("ledger.jsonl"))?;
    for event in ledger.events_after(0)? {
        writeln!(output, "{}", serde_json::to_string(&event)?)?;
    }
    output.sync_all()?;
    Ok(())
}

pub(super) fn assert_effects(events: &[Value], scenario: &str) {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/server_process.json")).unwrap();
    let expected = &fixture["effects"][scenario];
    let expected_name = expected["input"]["name"].as_str().unwrap();
    let ranges = expected["tool_count_ranges"].as_object().unwrap();
    let receipts = events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["kind"] == "effect_receipt")
        .collect::<Vec<_>>();
    assert!(
        receipts
            .iter()
            .all(|(_, event)| ranges
                .contains_key(event["payload"]["input"]["name"].as_str().unwrap())),
        "{scenario}: unexpected tool receipt"
    );
    for (name, range) in ranges {
        let count = receipts
            .iter()
            .filter(|(_, event)| event["payload"]["input"]["name"] == *name)
            .count() as u64;
        assert!(
            count >= range["min"].as_u64().unwrap() && count <= range["max"].as_u64().unwrap(),
            "{scenario}: {name} receipt count {count} outside configured range"
        );
    }
    for (index, event) in receipts {
        let payload = &event["payload"];
        let result: ToolResult = serde_json::from_value(payload["output"].clone()).unwrap();
        assert_eq!(payload["output"]["is_error"], false);
        assert_eq!(payload["receipt"]["status"], "Completed");
        assert!(!result.call_id.is_empty());
        assert_receipt_is_in_model_context(events, index, payload, &result, scenario);
        if payload["input"]["name"] != expected_name {
            continue;
        }
        let mut actual_input = payload["input"].clone();
        let mut expected_input = expected["input"].clone();
        if let Some(mode) = expected["content_match"].as_str() {
            let actual = actual_input["arguments"]["content"]
                .as_str()
                .unwrap()
                .to_owned();
            let wanted = expected_input["arguments"]["content"]
                .as_str()
                .unwrap()
                .to_owned();
            assert_content(&actual, &wanted, mode);
            actual_input["arguments"]["content"] = Value::String(wanted.clone());
            expected_input["arguments"]["content"] = Value::String(wanted);
            assert_eq!(
                payload["output"]["content"],
                format!("wrote {} bytes", actual.len()),
                "{scenario}: output must report actual bytes"
            );
        } else if let Some(mode) = expected["output_match"].as_str() {
            assert_content(
                payload["output"]["content"].as_str().unwrap(),
                expected["output"].as_str().unwrap(),
                mode,
            );
        } else {
            assert_eq!(
                payload["output"]["content"], expected["output"],
                "{scenario}: actual tool output"
            );
        }
        assert_eq!(
            actual_input, expected_input,
            "{scenario}: actual tool input"
        );
    }
}

fn assert_receipt_is_in_model_context(
    events: &[Value],
    index: usize,
    payload: &Value,
    result: &ToolResult,
    scenario: &str,
) {
    let model_call_exists = events[..index].iter()
            .filter(|event| event["kind"] == "step_completed")
            .any(|event| {
                let step: kolyan_core::StepResult = serde_json::from_value(event["payload"]["step"].clone()).unwrap();
                step.response.content.iter().any(|block| matches!(block, ContentBlock::ToolCall {call}
                    if call.id == result.call_id && call.name == payload["input"]["name"] && call.arguments == payload["input"]["arguments"]))
            });
    assert!(
        model_call_exists,
        "{scenario}: receipt must bind an actual model-produced call"
    );
    let next = events[index + 1..]
        .iter()
        .find(|event| event["kind"] == "model_requested")
        .expect("tool result must reach the next model request");
    let request: ModelRequest = serde_json::from_value(next["payload"]["request"].clone()).unwrap();
    assert!(request.messages.iter().flat_map(|message| &message.content)
        .any(|block| matches!(block, ContentBlock::ToolResult {result: next_result} if next_result == result)),
        "{scenario}: next request lost or changed the actual tool result");
}

pub(super) fn assert_content(actual: &str, expected: &str, mode: &str) {
    match mode {
        "optional_final_newline" => assert_eq!(
            actual
                .strip_suffix("\r\n")
                .or_else(|| actual.strip_suffix('\n'))
                .unwrap_or(actual),
            expected
        ),
        _ => panic!("unknown content comparison mode: {mode}"),
    }
}

#[test]
fn content_comparison_only_allows_one_final_line_ending() {
    assert_content("proof", "proof", "optional_final_newline");
    assert_content("proof\n", "proof", "optional_final_newline");
    assert_content("proof\r\n", "proof", "optional_final_newline");
    assert!(
        std::panic::catch_unwind(|| {
            assert_content("proof\n\n", "proof", "optional_final_newline")
        })
        .is_err()
    );
}
