//! Adapter capabilities for counting and consuming one production generation plan.

use std::{future::Future, pin::Pin, time::Duration};

use crate::{
    ModelProvider, ModelRequest, PreparedContextWire, ProviderError, ProviderFuture,
    ProviderInputCount,
};

pub type ProviderCountFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ProviderInputCount, ProviderError>> + Send + 'a>>;

/// Read-only evidence from an adapter-owned plan; not a durable opening permission.
pub trait PreparedModelGeneration: Send + Sync {
    fn wire(&self) -> &PreparedContextWire;
}

/// Adapters use their existing mapper, counter and streaming pipeline. No fallback
/// is provided: a caller must explicitly choose its accounting and opening policy.
pub trait PreparedModelProvider: ModelProvider {
    type Prepared: PreparedModelGeneration;

    fn prepare_generation(&self, request: &ModelRequest) -> Result<Self::Prepared, ProviderError>;

    fn count_prepared<'a>(
        &'a self,
        prepared: &'a Self::Prepared,
        timeout: Duration,
    ) -> ProviderCountFuture<'a>;

    fn stream_prepared(&self, prepared: Self::Prepared) -> ProviderFuture<'_>;
}
