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
    #[error(transparent)]
    Count(#[from] crate::CountFailure),
    #[error("invalid OpenAI client configuration: {0}")]
    Configuration(String),
    #[error("OpenAI opening budget exhausted before successful response headers")]
    OpeningBudgetExhausted,
    #[error("OpenAI local opening report: {report:?}; terminal cause: {source}")]
    RetriedError {
        #[source]
        source: Box<OpenAiError>,
        report: kolyan_protocol_http::RetryReport,
    },
    #[error("OpenAI stream API error: {0}")]
    Api(String),
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

impl OpenAiError {
    /// Classification uses the original cause, not the local report wrapper.
    pub fn root(&self) -> &Self {
        match self {
            Self::RetriedError { source, .. } => source.root(),
            other => other,
        }
    }

    /// A retry report proves multiple actual sends, not just a failed opening.
    pub fn retry_report(&self) -> Option<&kolyan_protocol_http::RetryReport> {
        self.opening_report().filter(|report| report.was_retried())
    }

    /// All retained opening diagnostics, including one send or a pre-send stop.
    pub fn opening_report(&self) -> Option<&kolyan_protocol_http::RetryReport> {
        match self {
            Self::RetriedError { report, .. } => Some(report),
            _ => None,
        }
    }

    pub(crate) fn with_retry_report(self, report: &kolyan_protocol_http::RetryReport) -> Self {
        if report.attempts() > 0 || report.terminal_stop().is_some() {
            Self::RetriedError {
                source: Box::new(self),
                report: report.clone(),
            }
        } else {
            self
        }
    }
}

impl From<reqwest::Error> for OpenAiError {
    fn from(source: reqwest::Error) -> Self {
        Self::Transport {
            source,
            diagnostics: None,
        }
    }
}
