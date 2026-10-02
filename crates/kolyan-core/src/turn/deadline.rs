//! One checked execution window, shared by admission and live Core execution.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;

/// Immutable time value, not an authorization proof or a serializable Instant.
#[derive(Debug, Clone)]
pub struct TurnDeadline {
    duration: Option<Duration>,
    sample: Option<ClockSample>,
    instant: Option<Instant>,
    unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TurnDeadlineError {
    #[error("clock precedes Unix epoch")]
    ClockBeforeUnixEpoch,
    #[error("Unix deadline overflow")]
    UnixOverflow,
    #[error("monotonic deadline overflow")]
    InstantOverflow,
    #[error("request duration differs from its deadline anchor")]
    DurationMismatch,
}

#[derive(Debug, Clone)]
struct ClockSample {
    before: Instant,
    after: Instant,
    unix: Duration,
}

impl TurnDeadline {
    /// Capture once before I/O. No limits means no wall-clock read.
    pub fn capture(
        duration: Option<Duration>,
        absolute_ceiling_at_ms: Option<u64>,
    ) -> Result<Self, TurnDeadlineError> {
        if duration.is_none() && absolute_ceiling_at_ms.is_none() {
            return Ok(Self {
                duration,
                sample: None,
                instant: None,
                unix_ms: None,
            });
        }
        Self::from_sample(
            duration,
            absolute_ceiling_at_ms,
            ClockSample::capture()?,
            |instant, duration| instant.checked_add(duration),
        )
    }

    /// Intersect a ceiling without renewing any existing execution window.
    pub fn tighten_absolute(mut self, ceiling: Option<u64>) -> Result<Self, TurnDeadlineError> {
        let Some(ceiling) = ceiling else {
            return Ok(self);
        };
        let sample = match self.sample.take() {
            Some(sample) => sample,
            None => ClockSample::capture()?,
        };
        let instant =
            sample.map_external(ceiling, |instant, duration| instant.checked_add(duration))?;
        self.instant = Some(self.instant.map_or(instant, |saved| saved.min(instant)));
        self.unix_ms = Some(self.unix_ms.map_or(ceiling, |saved| saved.min(ceiling)));
        self.sample = Some(sample);
        Ok(self)
    }

    /// A downstream layer cannot replace the original relative limit.
    pub fn validate_duration(&self, duration: Option<Duration>) -> Result<(), TurnDeadlineError> {
        if self.duration == duration {
            Ok(())
        } else {
            Err(TurnDeadlineError::DurationMismatch)
        }
    }

    /// None is unlimited; Some ZERO is exhausted, never a renewed budget.
    pub fn remaining(&self) -> Option<Duration> {
        self.instant
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    /// The exact effective ceiling to persist in Runtime input admission.
    pub fn deadline_at_ms(&self) -> Option<u64> {
        self.unix_ms
    }

    pub(super) fn instant(&self) -> Option<Instant> {
        self.instant
    }

    pub(super) fn restore(unix_ms: Option<u64>) -> Result<Self, TurnDeadlineError> {
        let Some(unix_ms) = unix_ms else {
            return Self::capture(None, None);
        };
        let sample = ClockSample::capture()?;
        let instant =
            sample.map_external(unix_ms, |instant, duration| instant.checked_add(duration))?;
        // Preserve durable identity. The restored live mapping cannot exceed
        // this fixed cutoff, and never reconstructs a relative request budget.
        Ok(Self {
            duration: None,
            sample: Some(sample),
            instant: Some(instant),
            unix_ms: Some(unix_ms),
        })
    }

    fn from_sample(
        duration: Option<Duration>,
        ceiling: Option<u64>,
        sample: ClockSample,
        add: impl Fn(Instant, Duration) -> Option<Instant> + Copy,
    ) -> Result<Self, TurnDeadlineError> {
        let relative = duration
            .map(|limit| {
                let precise = sample
                    .unix
                    .checked_add(limit)
                    .ok_or(TurnDeadlineError::UnixOverflow)?;
                sample.conservative_unix(precise)
            })
            .transpose()?;
        let unix_ms = match (relative, ceiling) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let relative_instant = duration
            .map(|limit| add(sample.before, limit).ok_or(TurnDeadlineError::InstantOverflow))
            .transpose()?;
        let mapped = relative
            .map(|value| sample.map_unix(value, add))
            .transpose()?;
        let instant = match (relative_instant, mapped) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let external = ceiling
            .map(|value| sample.map_external(value, add))
            .transpose()?;
        let instant = match (instant, external) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Ok(Self {
            duration,
            sample: Some(sample),
            instant,
            unix_ms,
        })
    }
}

impl ClockSample {
    fn conservative_unix(&self, deadline: Duration) -> Result<u64, TurnDeadlineError> {
        // Debit the entire clock-sampling span before flooring. Mapping from
        // `after` then preserves the original window, while serialization is
        // no later than that live mapping for every position of the wall sample.
        let precise = deadline.saturating_sub(self.after.duration_since(self.before));
        u64::try_from(precise.as_millis()).map_err(|_| TurnDeadlineError::UnixOverflow)
    }

    fn capture() -> Result<Self, TurnDeadlineError> {
        let before = Instant::now();
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TurnDeadlineError::ClockBeforeUnixEpoch);
        let after = Instant::now();
        Self::from_reading(before, after, unix)
    }

    fn from_reading(
        before: Instant,
        after: Instant,
        unix: Result<Duration, TurnDeadlineError>,
    ) -> Result<Self, TurnDeadlineError> {
        Ok(Self {
            before,
            after,
            unix: unix?,
        })
    }

    fn map_unix(
        &self,
        unix_ms: u64,
        add: impl Fn(Instant, Duration) -> Option<Instant>,
    ) -> Result<Instant, TurnDeadlineError> {
        // This timestamp already paid the full sampling span. Starting at
        // `after` avoids serializing a later cutoff than the initial live one.
        add(
            self.after,
            Duration::from_millis(unix_ms).saturating_sub(self.unix),
        )
        .ok_or(TurnDeadlineError::InstantOverflow)
    }

    fn map_external(
        &self,
        unix_ms: u64,
        add: impl Fn(Instant, Duration) -> Option<Instant>,
    ) -> Result<Instant, TurnDeadlineError> {
        // The wall sample lies between before and after. Starting at before
        // conservatively maps its declared cutoff without rewriting that identity.
        add(
            self.before,
            Duration::from_millis(unix_ms).saturating_sub(self.unix),
        )
        .ok_or(TurnDeadlineError::InstantOverflow)
    }
}

#[cfg(test)]
mod tests;
