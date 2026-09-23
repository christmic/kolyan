use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseDiagnostics {
    pub status: u16,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub received_bytes: usize,
    pub tail_preview: Option<String>,
}

#[derive(Debug, Error)]
pub enum AnthropicError {
    #[error("Anthropic transport error: {source}; diagnostics: {diagnostics:?}")]
    Transport {
        source: reqwest::Error,
        diagnostics: Option<ResponseDiagnostics>,
    },
    #[error("Anthropic HTTP error {status}: {body}")]
    Http { status: u16, body: String },
    #[error("Anthropic response decode error: {0}")]
    Decode(#[from] serde_json::Error),
}

impl From<reqwest::Error> for AnthropicError {
    fn from(source: reqwest::Error) -> Self {
        Self::Transport {
            source,
            diagnostics: None,
        }
    }
}
