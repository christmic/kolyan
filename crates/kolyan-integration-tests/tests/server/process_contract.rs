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
    for (index, event) in events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["kind"] == "effect_receipt")
    {
        let payload = &event["payload"];
        assert_eq!(
            payload["input"], expected["input"],
            "{scenario}: actual tool input"
        );
        assert_eq!(
            payload["output"]["content"], expected["output"],
            "{scenario}: actual tool output"
        );
        assert_eq!(payload["output"]["is_error"], false);
        assert_eq!(payload["receipt"]["status"], "Completed");
        let result: ToolResult = serde_json::from_value(payload["output"].clone()).unwrap();
        assert!(!result.call_id.is_empty());
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
        let request: ModelRequest =
            serde_json::from_value(next["payload"]["request"].clone()).unwrap();
        assert!(request.messages.iter().flat_map(|message| &message.content)
            .any(|block| matches!(block, ContentBlock::ToolResult {result: next_result} if *next_result == result)),
            "{scenario}: next request lost or changed the actual tool result");
    }
}
