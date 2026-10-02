//! Real owned preparation is counted once, durably admitted, then consumed once.

use std::sync::Arc;

use kolyan_model::{
    CountSource, ModelProvider, ModelRequest, PreparedModelGeneration, PreparedModelProvider,
    ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderFuture,
};

use super::{ModelOpeningAttempt, OpeningAccountingPolicy, OpeningAdmissionError as Error};
use crate::ModelOpeningAccounting;

pub struct OpeningModelProvider<P> {
    provider: P,
    attempt: ModelOpeningAttempt,
    policy: OpeningAccountingPolicy,
}

impl<P: PreparedModelProvider> OpeningModelProvider<P>
where
    P::Prepared: 'static,
{
    pub fn new(
        provider: P,
        attempt: ModelOpeningAttempt,
        policy: OpeningAccountingPolicy,
    ) -> Result<Self, Error> {
        match &policy {
            OpeningAccountingPolicy::ProviderReported { count_timeout, .. }
                if count_timeout.is_zero() =>
            {
                return Err(Error::InvalidConfiguration(
                    "positive count timeout required".into(),
                ));
            }
            OpeningAccountingPolicy::WireBytes {
                policy_revision, ..
            } if policy_revision.trim().is_empty()
                || policy_revision.len() > 128
                || policy_revision.chars().any(char::is_control) =>
            {
                return Err(Error::InvalidConfiguration(
                    "invalid wire policy revision".into(),
                ));
            }
            _ => {}
        }
        Ok(Self {
            provider,
            attempt,
            policy,
        })
    }
}

impl<P: PreparedModelProvider> ModelProvider for OpeningModelProvider<P>
where
    P::Prepared: 'static,
{
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            let result = async {
                let requested = self.attempt.begin(&request)?;
                let prepared = self.provider.prepare_generation(&request)?;
                self.attempt.check_live()?;
                let wire = prepared.wire();
                if wire.neutral_digest() != kolyan_model::digest_json(&request)? || wire.identity().model() != &request.model {
                    return Err(Error::BindingMismatch("prepared source/model differs".into()));
                }
                let accounting = match &self.policy {
                    OpeningAccountingPolicy::WireBytes { policy_revision, max_generation_wire_bytes } => {
                        if wire.generation_wire_bytes() > *max_generation_wire_bytes {
                            return Err(Error::AccountingRejected("generation byte budget exceeded".into()));
                        }
                        ModelOpeningAccounting::WireBytes { policy_revision: policy_revision.clone(), max_generation_wire_bytes: *max_generation_wire_bytes }
                    }
                    OpeningAccountingPolicy::ProviderReported { max_input_tokens, count_timeout } => {
                        if !wire.coverage().is_complete() { return Err(Error::AccountingRejected("incomplete count coverage".into())); }
                        let timeout = self.attempt.remaining().map_or(*count_timeout, |n| n.min(*count_timeout));
                        if timeout.is_zero() { return Err(Error::DeadlineExceeded); }
                        let count = tokio::select! {
                            biased;
                            () = self.attempt.inner.config.control.cancelled() => return Err(Error::Cancelled),
                            result = tokio::time::timeout(timeout, self.provider.count_prepared(&prepared, timeout)) =>
                                match result {
                                    Ok(result) => result?,
                                    Err(_) => {
                                        self.attempt.check_live()?;
                                        return Err(Error::CountTimedOut);
                                    }
                                },
                        };
                        self.attempt.check_live()?;
                        let expected = wire.reported_count(count.input_tokens())?;
                        if count.source() != CountSource::ProviderReported || count.identity() != wire.identity()
                            || count.neutral_digest() != wire.neutral_digest()
                            || count.generation_wire_digest() != wire.generation_wire_digest()
                            || count.count_input_digest() != wire.count_input_digest()
                            || count.counter_revision() != expected.counter_revision()
                            || count.counter_revision().is_empty() || count.counter_revision().len() > 128
                            || count.counter_revision().chars().any(char::is_control) {
                            return Err(Error::BindingMismatch("actual count report differs from prepared plan".into()));
                        }
                        if count.input_tokens() > *max_input_tokens {
                            return Err(Error::AccountingRejected("reported input budget exceeded".into()));
                        }
                        ModelOpeningAccounting::ProviderReported { counter_revision: count.counter_revision().into(), input_tokens: count.input_tokens(), max_input_tokens: *max_input_tokens }
                    }
                };
                self.attempt.check_live()?;
                // Move, not clone/re-map, the actual SDK plan into a blocking storage
                // worker and return that same owner alongside its private permit.
                let attempt = self.attempt.clone();
                let prepared = tokio::task::spawn_blocking(move || {
                    let permit = attempt.prepare_and_admit(&request, requested, prepared.wire(), accounting)?;
                    attempt.consume_permit(permit)?;
                    Ok::<_, Error>(prepared)
                }).await.map_err(|e| Error::Worker(e.to_string()))
                    .and_then(|r| r)?;
                self.attempt.check_live()?;
                let generation = self.provider.stream_prepared(prepared);
                tokio::select! {
                    biased;
                    () = self.attempt.inner.config.control.cancelled() => Err(Error::Cancelled),
                    result = async {
                        match self.attempt.remaining() {
                            Some(remaining) => tokio::time::timeout(remaining, generation).await
                                .map_err(|_| Error::DeadlineExceeded)?.map_err(Error::Provider),
                            None => generation.await.map_err(Error::Provider),
                        }
                    } => result,
                }
            }.await;
            result.map_err(|error| provider_error(self.attempt.retain(error)))
        })
    }
}

fn provider_error(error: Arc<Error>) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Other,
        ProviderErrorPhase::Open,
        error.to_string(),
    )
}
