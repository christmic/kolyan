use std::time::Duration;

#[derive(Clone)]
pub struct AnthropicConfig {
    pub base_url: String,
    pub api_key: String,
    pub version: String,
    pub timeout: Duration,
}

impl AnthropicConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.anthropic.com".into(),
            api_key: api_key.into(),
            version: "2023-06-01".into(),
            timeout: Duration::from_secs(120),
        }
    }
}
