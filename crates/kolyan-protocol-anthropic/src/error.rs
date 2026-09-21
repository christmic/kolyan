use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnthropicError {
    #[error("Anthropic transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("Anthropic HTTP error {status}: {body}")]
    Http { status: u16, body: String },
    #[error("Anthropic response decode error: {0}")]
    Decode(#[from] serde_json::Error),
}
