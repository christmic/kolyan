//! HTTP eligibility and SDK-profile delay calculation with caller-owned clock/count/RNG.

use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_RETRIES: u32 = 16;
const MAX_WINDOW_MS: u64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryProfile {
    OpenAi,
    Anthropic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyConfig {
    max_retries: u32,
    max_elapsed_ms: u64,
    max_server_delay_ms: u64,
    profile: RetryProfile,
}

/// Validated host settings; HTTP status retries are disabled by default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PolicyConfig", into = "PolicyConfig")]
pub struct HttpRetryPolicy {
    config: PolicyConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid bounded HTTP retry policy")]
pub struct PolicyError;

#[derive(Debug, Clone, Copy, Default)]
pub struct RetryHeaders<'a> {
    pub should_retry: Option<&'a str>,
    pub retry_after_ms: Option<&'a str>,
    pub retry_after: Option<&'a str>,
}

/// retries_used counts ALL earlier transport/status retries, not this category only.
#[derive(Debug, Clone, Copy)]
pub struct RetryInput<'a> {
    pub retries_used: u32,
    pub elapsed_ms: u64,
    pub now_unix_ms: u64,
    /// A random multiplier in [.75, 1]; never substitute the clock for randomness.
    pub jitter: f64,
    pub status: u16,
    /// Exact structured code from the bounded envelope, never a diagnostic preview.
    pub error_code: Option<&'a str>,
    pub headers: RetryHeaders<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitBasis {
    RetryAfterMs,
    RetryAfterSeconds,
    RetryAfterDate,
    Backoff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopReason {
    Disabled,
    Exhausted,
    PermanentQuota,
    UnknownRateLimit,
    HeaderDenied,
    Ineligible,
    ElapsedLimit,
    ServerDelayLimit,
    InvalidInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryDecision {
    Stop { reason: StopReason },
    Retry { delay_ms: u64, basis: WaitBasis },
}

impl HttpRetryPolicy {
    /// Up to 16 retries and a 24-hour host window; delays are never shortened to fit.
    pub fn new(
        max_retries: u32,
        max_elapsed_ms: u64,
        max_server_delay_ms: u64,
        profile: RetryProfile,
    ) -> Result<Self, PolicyError> {
        PolicyConfig {
            max_retries,
            max_elapsed_ms,
            max_server_delay_ms,
            profile,
        }
        .try_into()
    }

    pub fn max_retries(&self) -> u32 {
        self.config.max_retries
    }
    pub fn max_elapsed_ms(&self) -> u64 {
        self.config.max_elapsed_ms
    }
    pub fn max_server_delay_ms(&self) -> u64 {
        self.config.max_server_delay_ms
    }
    pub fn profile(&self) -> RetryProfile {
        self.config.profile
    }

    /// Remaining opening budget; caller must also bound each request timeout by it.
    pub fn remaining_ms(&self, elapsed_ms: u64) -> Option<u64> {
        self.config
            .max_elapsed_ms
            .checked_sub(elapsed_ms)
            .filter(|value| *value > 0)
    }

    pub fn decide(&self, input: RetryInput<'_>) -> RetryDecision {
        use StopReason::*;
        let stop = |reason| RetryDecision::Stop { reason };
        if matches!(
            input.error_code,
            Some("Throttling.AllocationQuota" | "insufficient_quota")
        ) {
            return stop(PermanentQuota);
        }
        if self.config.max_retries == 0 {
            return stop(Disabled);
        }
        if !(100..=599).contains(&input.status)
            || !input.jitter.is_finite()
            || !(0.75..=1.0).contains(&input.jitter)
        {
            return stop(InvalidInput);
        }
        if input.retries_used >= self.config.max_retries {
            return stop(Exhausted);
        }
        if (200..300).contains(&input.status) {
            return stop(Ineligible);
        }
        if input.status == 429
            && !matches!(
                input.error_code,
                Some(
                    "rate_limit_exceeded"
                        | "rate_limit_error"
                        | "Throttling.RateLimit"
                        | "Throttling.RateLimitExceeded"
                )
            )
        {
            return stop(UnknownRateLimit);
        }
        match input.headers.should_retry {
            Some("false") => return stop(HeaderDenied),
            Some("true") => {}
            _ if !(matches!(input.status, 408 | 409 | 429) || input.status >= 500) => {
                return stop(Ineligible);
            }
            _ => {}
        }
        let hint = server_hint(input.headers, input.now_unix_ms);
        if self.config.profile == RetryProfile::OpenAi && hint.is_some_and(|(ms, _)| ms > 120_000) {
            return stop(ServerDelayLimit);
        }
        let (delay_ms, basis) = match hint {
            Some((ms, basis)) => {
                if ms > self.config.max_server_delay_ms {
                    return stop(ServerDelayLimit);
                }
                (ms, basis)
            }
            None => (
                ((500_u64.saturating_mul(1_u64 << input.retries_used.min(4))).min(8_000) as f64
                    * input.jitter)
                    .ceil() as u64,
                WaitBasis::Backoff,
            ),
        };
        // Equality would leave no time to actually open the next request.
        if self
            .remaining_ms(input.elapsed_ms)
            .is_none_or(|remaining| delay_ms >= remaining)
        {
            return stop(ElapsedLimit);
        }
        RetryDecision::Retry { delay_ms, basis }
    }
}

impl Default for HttpRetryPolicy {
    fn default() -> Self {
        Self {
            config: PolicyConfig {
                max_retries: 0,
                max_elapsed_ms: 0,
                max_server_delay_ms: 0,
                profile: RetryProfile::OpenAi,
            },
        }
    }
}

impl TryFrom<PolicyConfig> for HttpRetryPolicy {
    type Error = PolicyError;
    fn try_from(config: PolicyConfig) -> Result<Self, Self::Error> {
        if config.max_retries > MAX_RETRIES
            || config.max_elapsed_ms > MAX_WINDOW_MS
            || config.max_server_delay_ms > config.max_elapsed_ms
            || (config.max_retries > 0
                && (config.max_elapsed_ms == 0 || config.max_server_delay_ms == 0))
        {
            return Err(PolicyError);
        }
        Ok(Self { config })
    }
}

impl From<HttpRetryPolicy> for PolicyConfig {
    fn from(policy: HttpRetryPolicy) -> Self {
        policy.config
    }
}

fn positive_ms(text: &str, multiplier: f64) -> Option<u64> {
    let parsed = text.trim().parse::<f64>().ok()?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return None;
    }
    let value = parsed * multiplier;
    // Saturation is only a sentinel for host-cap refusal, never a shortened wait.
    Some(value.ceil() as u64)
}

fn server_hint(headers: RetryHeaders<'_>, now_unix_ms: u64) -> Option<(u64, WaitBasis)> {
    if let Some(ms) = headers
        .retry_after_ms
        .and_then(|text| positive_ms(text, 1.0))
    {
        return Some((ms, WaitBasis::RetryAfterMs));
    }
    let text = headers.retry_after?;
    if let Some(ms) = positive_ms(text, 1_000.0) {
        return Some((ms, WaitBasis::RetryAfterSeconds));
    }
    let date = httpdate::parse_http_date(text).ok()?;
    let now = UNIX_EPOCH.checked_add(Duration::from_millis(now_unix_ms))?;
    let delay = date.duration_since(now).ok()?;
    let ms = u64::try_from(delay.as_millis()).ok()?;
    (ms > 0).then_some((ms, WaitBasis::RetryAfterDate))
}
