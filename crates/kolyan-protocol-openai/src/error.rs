use thiserror::Error;

#[derive(Debug, Error)]
pub enum OpenAiError {
    #[error("OpenAI transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("OpenAI HTTP error {status}: {body}")]
    Http { status: u16, body: String },
    #[error("OpenAI response decode error: {0}")]
    Decode(#[from] serde_json::Error),
}
