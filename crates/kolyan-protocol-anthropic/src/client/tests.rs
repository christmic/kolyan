use super::*;
use futures_util::FutureExt;

#[test]
fn http_failures_preserve_status_and_bound_body() {
    for status in [401, 403, 429, 500, 503] {
        let response = http::Response::builder()
            .status(status)
            .body(vec![b'x'; 8192])
            .unwrap();
        let error = check_status(response.into())
            .now_or_never()
            .unwrap()
            .unwrap_err();
        let AnthropicError::Http {
            status: actual,
            body,
        } = error
        else {
            panic!("HTTP errors must not become transport errors");
        };
        assert_eq!(actual, status);
        assert_eq!(body.len(), 4096);
    }
}

#[test]
fn successful_response_is_not_consumed() {
    let response = http::Response::builder()
        .status(200)
        .body("payload")
        .unwrap();
    let response = check_status(response.into())
        .now_or_never()
        .unwrap()
        .unwrap();
    assert_eq!(response.text().now_or_never().unwrap().unwrap(), "payload");
}
