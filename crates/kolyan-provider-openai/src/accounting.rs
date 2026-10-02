//! One generation mapping path and an explicit, profile-gated count projection.
use crate::OpenAiProvider;
use kolyan_model::{
    ContextProtocol, CountCoverage, CountProfile, MappingIdentity, ModelRequest, PlannedRequest,
    PreparedContextWire, ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderInputCount,
};
use serde_json::{Map, Value};
use std::{sync::Arc, time::Duration};

impl OpenAiProvider {
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
            .count_input_tokens(&dto, timeout)
            .await
            .map_err(crate::openai_error)?;
        prepared.reported_count(response.input_tokens)
    }
    fn mapping_identity(
        &self,
        model: kolyan_model::ModelRef,
    ) -> Result<MappingIdentity, ProviderError> {
        MappingIdentity::new(
            self.client.count_endpoint_identity(),
            ContextProtocol::OpenAiResponses,
            model,
            "openai-responses-mapping-v1".into(),
            "responses-input-tokens-coverage-v1".into(),
        )
    }
    pub(crate) fn plan_wire(
        &self,
        original: &ModelRequest,
    ) -> Result<
        (
            PlannedRequest,
            kolyan_protocol_openai::ResponseCreateRequest,
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
        crate::validate_request(request)?;
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
            "conversation"
            | "input"
            | "instructions"
            | "model"
            | "parallel_tool_calls"
            | "personality"
            | "previous_response_id"
            | "reasoning"
            | "text"
            | "tool_choice"
            | "tools"
            | "truncation" => {
                projected.insert(key.clone(), value.clone());
            }
            "stream" | "max_output_tokens" => {}
            _ => {
                unsupported.push(key.clone());
            }
        }
    }
    if object.contains_key("conversation") || object.contains_key("previous_response_id") {
        unsupported.push("remote_history".into());
    }
    if !supported_content(&Value::Object(projected.clone())) {
        unsupported.push("unsupported_modality_or_server_tool".into());
    }
    Ok((Value::Object(projected), CountCoverage::new(unsupported)))
}

// Inspect only protocol content, not user-supplied tool JSON schemas.
fn supported_content(body: &Value) -> bool {
    let content_keys = ["input"];
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
            tools
                .iter()
                .all(|tool| tool.get("type").and_then(Value::as_str) == Some("function"))
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
                        "message" => &["type", "role", "content", "id", "status", "phase"],
                        "input_text" | "output_text" | "summary_text" => {
                            &["type", "text", "annotations", "logprobs"]
                        }
                        "function_call" => {
                            &["type", "id", "call_id", "name", "arguments", "status"]
                        }
                        "function_call_output" => &["type", "id", "call_id", "output", "status"],
                        "reasoning" => &["type", "id", "summary", "encrypted_content", "status"],
                        _ => &["role", "content"],
                    };
                if object.keys().any(|key| !allowed.contains(&key.as_str())) {
                    return false;
                }
                if let Some(kind) = object.get("type").and_then(Value::as_str)
                    && !matches!(
                        kind,
                        "message"
                            | "input_text"
                            | "output_text"
                            | "function_call"
                            | "function_call_output"
                            | "reasoning"
                            | "summary_text"
                    )
                {
                    return false;
                }
                if let Some(content) = object.get("content") {
                    stack.push(content);
                }
                if let Some(summary) = object.get("summary") {
                    stack.push(summary);
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
