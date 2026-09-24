use super::*;
use serde_json::{Value, json};

fn format(schema: Value) -> OutputFormat {
    OutputFormat {
        name: "test".into(),
        schema,
        strict: true,
    }
}

#[test]
fn validates_output_without_repairing_or_discarding_invalid_values() {
    let validator = OutputValidator::new(Some(&format(json!({"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false})))).unwrap();
    let mut response = ModelResponse {
        id: "test".into(),
        model: crate::ModelRef::new("test", "test"),
        content: vec![],
        structured_output: None,
        stop_reason: StopReason::EndTurn,
        usage: crate::TokenUsage::default(),
        metadata: Value::Null,
    };
    for output in [
        None,
        Some(json!({})),
        Some(json!({"ok":"true"})),
        Some(json!({"ok":true,"extra":1})),
    ] {
        response.structured_output = output;
        assert!(validator.validate(&response).is_err());
    }
    response.structured_output = Some(json!({"ok":true}));
    assert!(validator.validate(&response).is_ok());
    response.structured_output = None;
    for reason in [
        StopReason::ToolUse,
        StopReason::Refusal,
        StopReason::MaxOutputTokens,
    ] {
        response.stop_reason = reason;
        assert!(validator.validate(&response).is_ok());
    }
}

#[test]
fn invalid_schemas_and_external_references_fail_without_io() {
    for schema in [
        json!({"type":"not-a-type"}),
        json!({"$ref":"https://example.invalid/schema"}),
        json!({"$ref":"file:///not-allowed"}),
    ] {
        assert!(OutputValidator::new(Some(&format(schema))).is_err());
    }
    assert!(
        OutputValidator::new(Some(&format(
            json!({"$defs":{"s":{"type":"string"}},"$ref":"#/$defs/s"})
        )))
        .is_ok()
    );
}
