//! Bounded local diagnostics; no headers, bodies, credentials or SDK wire metadata.

use serde::Serialize;
use thiserror::Error;

use crate::{RetryDecision, StopReason};

const MAX_OBSERVATIONS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RetryObservation {
    pub attempt: u32,
    pub status: Option<u16>,
    pub error_code: Option<String>,
    pub request_id: Option<String>,
    pub fields_truncated: bool,
    pub decision: Option<RetryDecision>,
}

impl RetryObservation {
    /// Strings are bounded UTF-8 prefixes with explicit truncation, never raw envelopes.
    pub fn new(
        attempt: u32,
        status: Option<u16>,
        error_code: Option<&str>,
        request_id: Option<&str>,
        decision: Option<RetryDecision>,
    ) -> Self {
        let mut truncated = false;
        let error_code = error_code.map(|text| bounded(text, 128, &mut truncated));
        let request_id = request_id.map(|text| bounded(text, 256, &mut truncated));
        Self {
            attempt,
            status,
            error_code,
            request_id,
            fields_truncated: truncated,
            decision,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RetryReport {
    observations: Vec<RetryObservation>,
    terminal_stop: Option<StopReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("retry report capacity or attempt sequence exceeded")]
pub struct ReportError;

impl RetryReport {
    /// Exactly one observation per actual send, including transport failures and success.
    pub fn push(&mut self, observation: RetryObservation) -> Result<(), ReportError> {
        if self.observations.len() >= MAX_OBSERVATIONS
            || observation.attempt as usize != self.observations.len() + 1
            || observation
                .error_code
                .as_ref()
                .is_some_and(|text| text.len() > 128)
            || observation
                .request_id
                .as_ref()
                .is_some_and(|text| text.len() > 256)
        {
            return Err(ReportError);
        }
        self.observations.push(observation);
        Ok(())
    }
    pub fn observations(&self) -> &[RetryObservation] {
        &self.observations
    }
    pub fn attempts(&self) -> usize {
        self.observations.len()
    }
    /// Record why opening ended without changing any actual-send decision or count.
    pub fn finish(&mut self, reason: StopReason) {
        self.terminal_stop = Some(reason);
    }
    pub fn terminal_stop(&self) -> Option<StopReason> {
        self.terminal_stop
    }
    /// Successful streams emit retry metadata only when this is true.
    /// Failure diagnostics also retain a single send or a pre-send terminal stop.
    pub fn was_retried(&self) -> bool {
        self.attempts() > 1
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseWithRetryReport<T> {
    pub response: T,
    pub retry_report: RetryReport,
}

fn bounded(text: &str, limit: usize, truncated: &mut bool) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    *truncated = true;
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
