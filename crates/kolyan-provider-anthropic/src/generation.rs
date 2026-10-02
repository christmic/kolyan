//! Owned production planning and fixed-body generation. No count, admission or new SSE loop.
use crate::*;
use kolyan_model::{PlannedRequest, PreparedContextWire};

/// One owned generation plan. No Clone or Deserialize; wire access is read-only.
pub struct PreparedAnthropicGeneration {
    plan: PlannedRequest,
    wire: PreparedContextWire,
    original: ModelRequest,
    planned_request_digest: String,
}
impl PreparedAnthropicGeneration {
    pub fn wire(&self) -> &PreparedContextWire {
        &self.wire
    }
}
impl AnthropicProvider {
    /// Prepare without network I/O or selection, retaining the planned validator input.
    pub fn prepare_generation(
        &self,
        request: &ModelRequest,
    ) -> Result<PreparedAnthropicGeneration, ProviderError> {
        let (plan, _, wire) = self.plan_wire(request)?;
        let planned_request_digest = kolyan_model::digest_json(&plan.request)?;
        Ok(PreparedAnthropicGeneration {
            plan,
            wire,
            original: request.clone(),
            planned_request_digest,
        })
    }
    /// Consume exactly this prepared body using the existing SDK opening and SSE pipeline.
    /// This is not a durable send permission; dropping releases local waiting only.
    pub fn stream_prepared(&self, prepared: PreparedAnthropicGeneration) -> ProviderFuture<'_> {
        let client = self.client.clone();
        Box::pin(async move {
            let PreparedAnthropicGeneration {
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
                .stream_message_body(wire.generation_body())
                .await
                .map_err(anthropic_error)?;
            let retry_report = response.retry_report().clone();
            let model = request.model.clone();
            let structured = request.output_format.is_some();
            let stream = map_stream(
                response.map(|event| event.map_err(anthropic_error)),
                model,
                structured,
                validator,
            );
            let audit = plan.decisions;
            let mut metadata = Vec::new();
            if !audit.is_empty() {
                metadata.push(Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                    provider: request.model.provider.clone(),
                    raw: Some(json!({"kind":"request_planning","decisions":audit})),
                })));
            }
            if retry_report.was_retried() {
                metadata.push(Ok(ModelEvent::Provider(kolyan_model::ProviderMetadata {
                    provider: request.model.provider,
                    raw: Some(json!({"kind":"local_http_opening_retry","report":retry_report})),
                })));
            }
            let prefix = futures_util::stream::iter(metadata);
            Ok(Box::pin(prefix.chain(stream)) as _)
        })
    }
}
#[cfg(test)]
mod tests;
