//! Agent-side request preparation before every actual Provider stream opening.
//! The supplied recorder must acknowledge the exact source and preparation before
//! the underlying provider is invoked. This is not evidence of network completion.
//! Planning, protocol mapping, events and retries remain owned by the inner provider.
//! Any preparation that changes the request is rejected: the runner must prepare
//! its initial request before Turn admission so Core and Provider observe the same
//! neutral input. A future explicit projection hook is required for compaction.

use std::sync::Arc;

use kolyan_model::{
    ModelDescriptor, ModelProvider, ModelRequest, ProviderError, ProviderErrorKind,
    ProviderErrorPhase, ProviderFuture,
};
use thiserror::Error;

use crate::context::{
    ContextError, ContextPolicy, ContextTokenCounter, PreparedContext, prepare_context,
};

/// Typed preparation evidence. Rejections retain the actual unmodified source,
/// the original typed error and its provenance where preparation supplied one.
#[derive(Debug, Clone, PartialEq)]
pub enum ContextRecord {
    Prepared {
        source: ModelRequest,
        prepared: PreparedContext,
    },
    Rejected {
        source: ModelRequest,
        failure: Arc<ContextError>,
        /// A rejected projection proposal is evidence only, never a sent request.
        preparation: Option<PreparedContext>,
    },
}

/// Recorder failures are blocking admission failures, not transient Provider errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("context recording failed: {message}")]
pub struct ContextRecordError {
    pub message: String,
}

/// Host-owned evidence port; implementations must acknowledge complete recording
/// or fail. Durability and redaction belong to the implementation. This synchronous
/// port must not perform unbounded blocking work on the model execution thread.
pub trait ContextRecorder: Send + Sync {
    fn record(&self, record: &ContextRecord) -> Result<(), ContextRecordError>;
}

/// Compose with an existing Provider without replacing its wire planning or retry
/// policy. Host-selected Strict/Inspect mode is immutable for this wrapper instance.
/// No production tokenizer, compaction, persistence backend or default recorder is
/// provided. A recorder success means preparation was observed, not a model call.
pub struct ContextPreparingProvider<P> {
    inner: P,
    descriptor: ModelDescriptor,
    policy: ContextPolicy,
    counter: Arc<dyn ContextTokenCounter + Send + Sync>,
    recorder: Arc<dyn ContextRecorder>,
}

impl<P> ContextPreparingProvider<P> {
    /// Every stream invocation independently validates this explicit configuration
    /// against its actual request. Invalid configuration is recorded and fails closed.
    pub fn new(
        inner: P,
        descriptor: ModelDescriptor,
        policy: ContextPolicy,
        counter: Arc<dyn ContextTokenCounter + Send + Sync>,
        recorder: Arc<dyn ContextRecorder>,
    ) -> Self {
        Self {
            inner,
            descriptor,
            policy,
            counter,
            recorder,
        }
    }
}

impl<P: ModelProvider> ModelProvider for ContextPreparingProvider<P> {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            match prepare_context(
                &request,
                &self.descriptor,
                &self.policy,
                self.counter.as_ref(),
            ) {
                Ok(prepared) => {
                    if prepared.request != request {
                        let failure = Arc::new(ContextError::Invalid(
                            "request projection is forbidden at the Provider boundary; prepare the initial request before Turn admission".into(),
                        ));
                        self.recorder
                            .record(&ContextRecord::Rejected {
                                source: request,
                                failure: failure.clone(),
                                preparation: Some(prepared),
                            })
                            .map_err(record_error)?;
                        return Err(preparation_error(&failure));
                    }
                    let forwarded = request.clone();
                    let record = ContextRecord::Prepared {
                        source: request,
                        prepared,
                    };
                    self.recorder.record(&record).map_err(record_error)?;
                    // Return the original stream object: no buffering, event changes,
                    // provider error conversion or extra retry loop is introduced.
                    self.inner.stream(forwarded).await
                }
                Err(failure) => {
                    let failure = Arc::new(failure);
                    let record = ContextRecord::Rejected {
                        source: request,
                        failure: failure.clone(),
                        preparation: None,
                    };
                    self.recorder.record(&record).map_err(record_error)?;
                    Err(preparation_error(&failure))
                }
            }
        })
    }
}

fn preparation_error(error: &ContextError) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        ProviderErrorPhase::Validate,
        error.to_string(),
    )
}

fn record_error(error: ContextRecordError) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        ProviderErrorPhase::Validate,
        error.to_string(),
    )
}

#[cfg(test)]
mod tests;
