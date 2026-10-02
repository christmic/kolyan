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
    #[error(transparent)]
    Count(#[from] crate::CountFailure),
    #[error("Anthropic opening budget exhausted")]
    OpeningBudgetExhausted,
    #[error("invalid Anthropic opening configuration: {0}")]
    Configuration(String),
    #[error("Anthropic local opening report: {report:?}; terminal cause: {source}")]
    RetriedError {
        #[source]
        source: Box<AnthropicError>,
        report: kolyan_protocol_http::RetryReport,
    },
    #[error("Anthropic stream API error: {0}")]
    Api(String),
    #[error("Anthropic SSE framing error: {0}")]
    Framing(#[from] kolyan_protocol_sse::DecodeError),
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

impl AnthropicError {
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

    /// Terminal original cause; retry metadata never changes error classification.
    pub fn root_cause(&self) -> &Self {
        let mut cause = self;
        while let Self::RetriedError { source, .. } = cause {
            cause = source;
        }
        cause
    }

    /// Actual retry evidence only: at least two sends occurred.
    pub fn retry_report(&self) -> Option<&kolyan_protocol_http::RetryReport> {
        self.opening_report().filter(|report| report.was_retried())
    }

    /// Complete local opening diagnostics, including one send or a pre-send stop.
    pub fn opening_report(&self) -> Option<&kolyan_protocol_http::RetryReport> {
        match self {
            Self::RetriedError { report, .. } => Some(report),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for AnthropicError {
    fn from(source: reqwest::Error) -> Self {
        Self::Transport {
            source,
            diagnostics: None,
        }
    }
}
