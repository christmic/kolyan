use super::*;
use serde_json::json;

#[test]
fn identifiers_are_bounded_and_path_safe() {
    for valid in ["s", "turn_01-a", &"a".repeat(128)] {
        assert!(id(valid).is_ok());
    }
    for invalid in ["", "../s", "s/t", "会话", &"a".repeat(129)] {
        assert!(id(invalid).is_err());
    }
}

#[test]
fn input_dtos_reject_unknown_fields_and_decisions() {
    assert!(
        serde_json::from_value::<Start>(json!({"turn_id":"t","input":"hello","model":"override"}))
            .is_err()
    );
    assert!(serde_json::from_value::<Create>(json!({"session_id":"s","key":"secret"})).is_err());
    assert!(
        serde_json::from_value::<Decision>(json!({"decision":"approve","approval_id":"a"}))
            .is_err()
    );
    assert!(serde_json::from_value::<Decision>(json!({"decision":"retry"})).is_err());
    assert!(serde_json::from_value::<Empty>(json!({"cancel":true})).is_err());
}

#[test]
fn safe_problem_uses_standard_content_type_and_auth_header() {
    let response = Problem(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/problem+json"
    );
    assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
}
