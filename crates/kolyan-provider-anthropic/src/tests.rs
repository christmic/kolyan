use super::*;

#[test]
fn relaxed_json_parser_accepts_fenced_object() {
    assert_eq!(
        parse_json_relaxed("```json\n{\"city\":\"Shanghai\"}\n```"),
        Some(json!({"city": "Shanghai"}))
    );
}

#[test]
fn relaxed_json_parser_rejects_plain_text() {
    assert_eq!(parse_json_relaxed("not json"), None);
}
