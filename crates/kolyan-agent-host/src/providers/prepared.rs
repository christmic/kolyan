//! Forward opaque SDK plans without remapping, recounting or opening a second loop.

use std::time::Duration;

use kolyan_model::{
    ModelRequest, PreparedContextWire, PreparedModelGeneration, PreparedModelProvider,
    ProviderCountFuture, ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderFuture,
};
use kolyan_provider_anthropic::PreparedAnthropicGeneration;
use kolyan_provider_openai::PreparedOpenAiGeneration;

use super::HostProvider;

/// Adapter-owned generation bodies are not Clone or Deserialize. Each variant
/// retains its own mapping identity, accounting owner and registered count profile;
/// an OpenAI measurement can never substitute for an Anthropic measurement.
pub enum HostPreparedGeneration {
    Openai(PreparedOpenAiGeneration),
    Anthropic(PreparedAnthropicGeneration),
}

impl PreparedModelGeneration for HostPreparedGeneration {
    fn wire(&self) -> &PreparedContextWire {
        match self {
            Self::Openai(prepared) => prepared.wire(),
            Self::Anthropic(prepared) => prepared.wire(),
        }
    }
}

impl PreparedModelProvider for HostProvider {
    type Prepared = HostPreparedGeneration;

    fn prepare_generation(&self, request: &ModelRequest) -> Result<Self::Prepared, ProviderError> {
        match self {
            Self::Openai(provider) => provider
                .prepare_generation(request)
                .map(HostPreparedGeneration::Openai),
            Self::Anthropic(provider) => provider
                .prepare_generation(request)
                .map(HostPreparedGeneration::Anthropic),
        }
    }

    fn count_prepared<'a>(
        &'a self,
        prepared: &'a Self::Prepared,
        timeout: Duration,
    ) -> ProviderCountFuture<'a> {
        match (self, prepared) {
            (Self::Openai(provider), HostPreparedGeneration::Openai(prepared)) => {
                PreparedModelProvider::count_prepared(provider, prepared, timeout)
            }
            (Self::Anthropic(provider), HostPreparedGeneration::Anthropic(prepared)) => {
                PreparedModelProvider::count_prepared(provider, prepared, timeout)
            }
            _ => Box::pin(async { Err(protocol_mismatch()) }),
        }
    }

    fn stream_prepared(&self, prepared: Self::Prepared) -> ProviderFuture<'_> {
        match (self, prepared) {
            (Self::Openai(provider), HostPreparedGeneration::Openai(prepared)) => {
                provider.stream_prepared(prepared)
            }
            (Self::Anthropic(provider), HostPreparedGeneration::Anthropic(prepared)) => {
                provider.stream_prepared(prepared)
            }
            _ => Box::pin(async { Err(protocol_mismatch()) }),
        }
    }
}

fn protocol_mismatch() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        ProviderErrorPhase::Validate,
        "prepared generation belongs to a different Host protocol",
    )
}
