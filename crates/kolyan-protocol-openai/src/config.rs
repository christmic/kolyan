use std::time::Duration;

#[derive(Clone)]
pub struct OpenAiConfig {
    pub base_url: String,
    pub api_key: String,
    pub timeout: Duration,
}

impl OpenAiConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.openai.com".into(),
            api_key: api_key.into(),
            timeout: Duration::from_secs(120),
        }
    }
}
