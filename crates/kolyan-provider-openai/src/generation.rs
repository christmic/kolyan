//! Owned production planning and fixed-body generation. No count, admission or new SSE loop.
use crate::*;
use kolyan_model::{PlannedRequest, PreparedContextWire};

/// One owned generation plan. No Clone or Deserialize; wire access is read-only.
pub struct PreparedOpenAiGeneration {
    plan: PlannedRequest,
    wire: PreparedContextWire,
    original: ModelRequest,
    planned_request_digest: String,
}
impl PreparedOpenAiGeneration {
    pub fn wire(&self) -> &PreparedContextWire {
        &self.wire
    }
}
impl OpenAiProvider {
    /// Prepare without network I/O or selection, retaining the planned validator input.
    pub fn prepare_generation(
        &self,
        request: &ModelRequest,
    ) -> Result<PreparedOpenAiGeneration, ProviderError> {
        let (plan, _, wire) = self.plan_wire(request)?;
        let planned_request_digest = kolyan_model::digest_json(&plan.request)?;
        Ok(PreparedOpenAiGeneration {
            plan,
            wire,
            original: request.clone(),
            planned_request_digest,
        })
    }
    /// Consume exactly this prepared body using the existing SDK opening and SSE pipeline.
    /// This is not a durable send permission; dropping releases local waiting only.
    pub fn stream_prepared(&self, prepared: PreparedOpenAiGeneration) -> ProviderFuture<'_> {
        let client = self.client.clone();
        Box::pin(async move {
            let PreparedOpenAiGeneration {
                plan,
                wire,
                original,
                planned_request_digest,
            } = prepared;
            let identity = self.mapping_identity(wire.identity().model().clone())?;
            wire.verify_generation_for(&self.accounting_owner, &identity, &self.count_profile)?;
            if kolyan_model::digest_json(&original)? != wire.neutral_digest()
                || kolyan_model::digest_json(&plan.request)? != planned_request_digest
            {
                return Err(crate::accounting::count_error(
                    "prepared neutral digest mismatch",
                ));
            }
            let request = plan.request;
            let validator = kolyan_model::OutputValidator::new(request.output_format.as_ref())?;
            let response = client
                .stream_response_body(wire.generation_body())
                .await
                .map_err(openai_error)?;
            let retry_metadata = response.retry_report().was_retried().then(|| {
                Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                    provider: request.model.provider.clone(),
                    raw: Some(
                        json!({"kind":"local_http_opening_retry","report":response.retry_report()}),
                    ),
                }))
            });
            let model = request.model.clone();
            let model_for_map = model.clone();
            let mut call_ids = BTreeMap::new();
            let mut finalized = BTreeMap::new();
            let mapped = response.map(move |event| {
                event.map_err(openai_error).and_then(|mut event| {
                    recover_finalized_output(&mut event, &mut finalized)?;
                    map_stream_event(event, &model_for_map, &mut call_ids)
                })
            });
            let mapped =
                mapped.flat_map(|event| futures_util::stream::iter(finalized_call_events(event)));
            let stream = require_terminal(mapped);
            let stream = stream.map(move |event| {
                event.and_then(|event| {
                    if let ModelEvent::Completed(response) = &event {
                        validator.validate(response)?;
                    }
                    Ok(event)
                })
            });
            let audit = plan.decisions;
            let prefix = futures_util::stream::iter((!audit.is_empty()).then(|| {
                Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                    provider: request.model.provider,
                    raw: Some(json!({"kind":"request_planning","decisions":audit})),
                }))
            }));
            Ok(Box::pin(
                prefix
                    .chain(futures_util::stream::iter(retry_metadata))
                    .chain(stream),
            ) as _)
        })
    }
}
impl kolyan_model::PreparedModelGeneration for PreparedOpenAiGeneration {
    fn wire(&self) -> &PreparedContextWire {
        PreparedOpenAiGeneration::wire(self)
    }
}

impl kolyan_model::PreparedModelProvider for OpenAiProvider {
    type Prepared = PreparedOpenAiGeneration;

    fn prepare_generation(&self, request: &ModelRequest) -> Result<Self::Prepared, ProviderError> {
        OpenAiProvider::prepare_generation(self, request)
    }

    fn count_prepared<'a>(
        &'a self,
        prepared: &'a Self::Prepared,
        timeout: std::time::Duration,
    ) -> kolyan_model::ProviderCountFuture<'a> {
        Box::pin(OpenAiProvider::count_prepared(
            self,
            prepared.wire(),
            timeout,
        ))
    }

    fn stream_prepared(&self, prepared: Self::Prepared) -> ProviderFuture<'_> {
        OpenAiProvider::stream_prepared(self, prepared)
    }
}
#[cfg(test)]
mod tests;
