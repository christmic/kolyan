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
    /// Number of retries for transport failures before an HTTP response.
    pub transport_retries: u8,
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
        }
    }
}
