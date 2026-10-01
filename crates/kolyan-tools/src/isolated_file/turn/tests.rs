use super::*;

#[test]
fn complete_result_budget_includes_escaped_call_id_and_json_content() {
    let id = "quote\"\\\n";
    let cap = raw_cap(id, 4096).unwrap();
    let content = "\\".repeat(cap);
    let result = ToolResult {
        call_id: id.into(),
        content,
        is_error: false,
    };
    assert!(serde_json::to_vec(&result).unwrap().len() <= 4096);
    assert!(raw_cap(id, 1).is_err());
}

#[test]
fn cancellation_and_timeout_keep_typed_outcomes() {
    assert!(matches!(
        tool_error(IsolatedFileError::Sandbox(SandboxError::Cancelled)),
        ToolError::Cancelled
    ));
    assert!(matches!(
        tool_error(IsolatedFileError::Sandbox(SandboxError::Timeout)),
        ToolError::TimedOut
    ));
}
