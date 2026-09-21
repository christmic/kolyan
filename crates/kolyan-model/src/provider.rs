use crate::{ModelEvent, ModelRequest, ModelResponse, ProviderError};
use futures_core::Stream;
use futures_util::StreamExt;
use std::pin::Pin;

pub type ModelEventStream = Pin<Box<dyn Stream<Item = Result<ModelEvent, ProviderError>> + Send>>;
pub type ProviderFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ModelEventStream, ProviderError>> + Send + 'a>>;

pub trait ModelProvider: Send + Sync {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_>;
}

pub async fn aggregate_stream(
    mut stream: ModelEventStream,
) -> Result<ModelResponse, ProviderError> {
    while let Some(event) = stream.next().await {
        if let ModelEvent::Completed(response) = event? {
            return Ok(response);
        }
    }
    Err(ProviderError::new(
        crate::ProviderErrorKind::Protocol,
        crate::ProviderErrorPhase::Stream,
        "provider stream ended without a completed response",
    ))
}
