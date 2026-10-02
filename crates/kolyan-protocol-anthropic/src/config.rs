use std::time::Duration;

#[derive(Clone)]
pub struct AnthropicConfig {
    pub base_url: String,
    pub api_key: String,
    pub version: String,
    pub timeout: Duration,
    /// Include bounded response diagnostics in stream transport errors.
    /// Disabled by default because response fragments may contain user data.
    pub diagnostics: bool,
    /// Number of retries for transport failures before an HTTP response (0..=16).
    /// The default one immediate transport retry is retained independently of HTTP policy.
    pub transport_retries: u8,
    /// Explicit bounded HTTP-opening policy. Status retries are disabled by default.
    pub http_retry: kolyan_protocol_http::HttpRetryPolicy,
}

impl AnthropicConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.anthropic.com".into(),
            api_key: api_key.into(),
            version: "2023-06-01".into(),
            timeout: Duration::from_secs(120),
            diagnostics: false,
            transport_retries: 1,
            http_retry: Default::default(),
        }
    }
}
