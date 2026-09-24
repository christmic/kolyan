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
pub enum OpenAiError {
    #[error("OpenAI SSE framing error: {0}")]
    Framing(#[from] kolyan_protocol_sse::DecodeError),
    #[error("OpenAI transport error: {source}; diagnostics: {diagnostics:?}")]
    Transport {
        source: reqwest::Error,
        diagnostics: Option<ResponseDiagnostics>,
    },
    #[error("OpenAI HTTP error {status}: {body}")]
    Http { status: u16, body: String },
    #[error("OpenAI response decode error: {0}")]
    Decode(#[from] serde_json::Error),
}

impl From<reqwest::Error> for OpenAiError {
    fn from(source: reqwest::Error) -> Self {
        Self::Transport {
            source,
            diagnostics: None,
        }
    }
}
