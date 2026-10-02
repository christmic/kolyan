//! One generation mapping path and an explicit, profile-gated count projection.
use crate::AnthropicProvider;
use kolyan_model::{
    ContextProtocol, CountCoverage, CountProfile, MappingIdentity, ModelRequest, PlannedRequest,
    PreparedContextWire, ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderInputCount,
};
use serde_json::{Map, Value};
use std::{sync::Arc, time::Duration};

impl AnthropicProvider {
    /// Register exact endpoint/model/revision support; no accuracy is inferred.
    pub fn with_count_profile(mut self, profile: CountProfile) -> Self {
        self.count_profile = profile;
        self
    }
    /// Plan once through the generation mapper without HTTP or hidden selection.
    pub fn prepare_wire(
        &self,
        request: &ModelRequest,
    ) -> Result<PreparedContextWire, ProviderError> {
        Ok(self.plan_wire(request)?.2)
    }
    /// Verify preparation ownership/profile/coverage, then send one bounded count request.
    pub async fn count_prepared(
        &self,
        prepared: &PreparedContextWire,
        timeout: Duration,
    ) -> Result<ProviderInputCount, ProviderError> {
        let identity = self.mapping_identity(prepared.identity().model().clone())?;
        prepared.verify_for(&self.accounting_owner, &identity, &self.count_profile)?;
        let dto = serde_json::from_value(prepared.count_body().clone())
            .map_err(|_| count_error("count projection does not match the protocol DTO"))?;
        if serde_json::to_value(&dto).map_err(|_| count_error("count DTO serialization failed"))?
            != *prepared.count_body()
        {
            return Err(count_error(
                "count DTO would change the admitted projection",
            ));
        }
        let response = self
            .client
            .count_tokens(&dto, timeout)
            .await
            .map_err(crate::anthropic_error)?;
        prepared.reported_count(response.input_tokens)
    }
    fn mapping_identity(
        &self,
        model: kolyan_model::ModelRef,
    ) -> Result<MappingIdentity, ProviderError> {
        MappingIdentity::new(
            self.client.count_endpoint_identity(),
            ContextProtocol::AnthropicMessages,
            model,
            "anthropic-messages-mapping-v1".into(),
            "messages-count-tokens-coverage-v1".into(),
        )
    }
    pub(crate) fn plan_wire(
        &self,
        original: &ModelRequest,
    ) -> Result<
        (
            PlannedRequest,
            kolyan_protocol_anthropic::MessageCreateRequest,
            PreparedContextWire,
        ),
        ProviderError,
    > {
        kolyan_model::context_json_bytes(original)?;
        let plan = match &self.planner {
            Some(planner) => planner.plan(original)?,
            None => PlannedRequest::unconfigured(original.clone())?,
        };
        let request = &plan.request;
        if request
            .prompt_cache
            .as_ref()
            .is_some_and(|cache| cache.key.is_some() || cache.retention.is_some())
        {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                ProviderErrorPhase::Open,
                "Anthropic cache mapping supports breakpoints, not key or retention",
            ));
        }
        if self.planner.is_some() && request.max_output_tokens.is_none() {
            return Err(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                ProviderErrorPhase::Open,
                "Anthropic requires supported max_output_tokens from request or parameter table default",
            ));
        }
        kolyan_model::OutputValidator::new(request.output_format.as_ref())?;
        let mut wire = Self::request(request);
        if plan.omitted("tool_choice") {
            wire.tool_choice = None;
        }
        let wire_bytes = kolyan_model::context_json_bytes(&wire)?;
        let extension_bytes = kolyan_model::context_json_bytes(&plan.wire_extensions)?;
        if !plan.wire_extensions.is_empty()
            && wire_bytes + extension_bytes - 1 > kolyan_model::MAX_CONTEXT_JSON_BYTES
        {
            return Err(count_error(
                "generation wire exceeds 16 MiB serialized JSON ceiling",
            ));
        }
        let mut body =
            serde_json::to_value(&wire).map_err(|_| count_error("wire serialization failed"))?;
        body.as_object_mut()
            .ok_or_else(|| count_error("wire must be an object"))?
            .extend(plan.wire_extensions.clone());
        let (count_body, coverage) = project_count(&body)?;
        let prepared = PreparedContextWire::new(
            Arc::clone(&self.accounting_owner),
            self.mapping_identity(request.model.clone())?,
            self.count_profile.clone(),
            original,
            body,
            count_body,
            coverage,
        )?;
        Ok((plan, wire, prepared))
    }
}

fn project_count(body: &Value) -> Result<(Value, CountCoverage), ProviderError> {
    let object = body
        .as_object()
        .ok_or_else(|| count_error("generation wire is not an object"))?;
    let mut projected = Map::new();
    let mut unsupported = Vec::new();
    for (key, value) in object {
        match key.as_str() {
            "model" | "messages" | "system" | "tools" | "tool_choice" | "thinking"
            | "output_config" | "cache_control" => {
                projected.insert(key.clone(), value.clone());
            }
            "stream" | "max_tokens" => {}
            _ => {
                unsupported.push(key.clone());
            }
        }
    }

    if !supported_content(&Value::Object(projected.clone())) {
        unsupported.push("unsupported_modality_or_server_tool".into());
    }
    Ok((Value::Object(projected), CountCoverage::new(unsupported)))
}

// Inspect only protocol content, not user-supplied tool JSON schemas.
fn supported_content(body: &Value) -> bool {
    let content_keys = ["messages", "system"];
    for key in content_keys {
        if let Some(content) = body.get(key)
            && !content_nodes(content)
        {
            return false;
        }
    }
    body.get("tools")
        .and_then(Value::as_array)
        .is_none_or(|tools| {
            tools.iter().all(|tool| {
                tool.get("type").is_none()
                    || tool.get("type").and_then(Value::as_str) == Some("custom")
            })
        })
}

fn content_nodes(root: &Value) -> bool {
    let mut stack = vec![root];
    while let Some(value) = stack.pop() {
        match value {
            Value::Array(values) => stack.extend(values),
            Value::Object(object) => {
                let allowed: &[&str] =
                    match object.get("type").and_then(Value::as_str).unwrap_or("") {
                        "text" => &["type", "text", "cache_control"],
                        "tool_use" => &["type", "id", "name", "input", "cache_control"],
                        "tool_result" => &[
                            "type",
                            "tool_use_id",
                            "content",
                            "is_error",
                            "cache_control",
                        ],
                        "thinking" => &["type", "thinking", "signature"],
                        "redacted_thinking" => &["type", "data"],
                        _ => &["role", "content"],
                    };
                if object.keys().any(|key| !allowed.contains(&key.as_str())) {
                    return false;
                }
                if let Some(kind) = object.get("type").and_then(Value::as_str)
                    && !matches!(
                        kind,
                        "text" | "tool_use" | "tool_result" | "thinking" | "redacted_thinking"
                    )
                {
                    return false;
                }
                if let Some(content) = object.get("content") {
                    stack.push(content);
                }
            }
            _ => {}
        }
    }
    true
}

fn count_error(message: &str) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Unsupported,
        ProviderErrorPhase::Validate,
        message,
    )
}
#[cfg(test)]
mod tests;
