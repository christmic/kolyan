//! Immutable wire accounting. Reported numbers never confer token authority.

mod encoding;

use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{ModelRef, ModelRequest, ProviderError, ProviderErrorKind, ProviderErrorPhase};

pub use encoding::{MAX_CONTEXT_JSON_BYTES, context_json_bytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextProtocol {
    OpenAiResponses,
    AnthropicMessages,
}

/// Describes where a number came from, not its accuracy or budget authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CountSource {
    ProviderReported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MappingIdentity {
    endpoint_id: String,
    protocol: ContextProtocol,
    model: ModelRef,
    mapping_revision: String,
    coverage_revision: String,
}

impl MappingIdentity {
    pub fn new(
        endpoint_id: String,
        protocol: ContextProtocol,
        model: ModelRef,
        mapping_revision: String,
        coverage_revision: String,
    ) -> Result<Self, ProviderError> {
        if endpoint_id.len() != 64
            || !endpoint_id.bytes().all(|b| b.is_ascii_hexdigit())
            || !identity_text(&model.provider, 128)
            || !identity_text(&model.model, 512)
            || !identity_text(&mapping_revision, 128)
            || !identity_text(&coverage_revision, 128)
        {
            return Err(accounting_error("invalid accounting identity"));
        }
        Ok(Self {
            endpoint_id,
            protocol,
            model,
            mapping_revision,
            coverage_revision,
        })
    }
    pub fn endpoint_id(&self) -> &str {
        &self.endpoint_id
    }
    pub fn protocol(&self) -> ContextProtocol {
        self.protocol
    }
    pub fn model(&self) -> &ModelRef {
        &self.model
    }
    pub fn mapping_revision(&self) -> &str {
        &self.mapping_revision
    }
    pub fn coverage_revision(&self) -> &str {
        &self.coverage_revision
    }
}

/// Host registration is explicit; it does not assert an endpoint's count is exact.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CountProfile {
    registration: Option<CountRegistration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CountRegistration {
    identity: MappingIdentity,
    counter_revision: String,
}

impl CountProfile {
    pub fn registered(
        identity: MappingIdentity,
        counter_revision: String,
    ) -> Result<Self, ProviderError> {
        if !identity_text(&counter_revision, 128) {
            return Err(accounting_error("invalid counter revision"));
        }
        Ok(Self {
            registration: Some(CountRegistration {
                identity,
                counter_revision,
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CountCoverage {
    unsupported_fields: Vec<String>,
}
impl CountCoverage {
    pub fn new(mut unsupported_fields: Vec<String>) -> Self {
        unsupported_fields.sort();
        unsupported_fields.dedup();
        Self { unsupported_fields }
    }
    pub fn is_complete(&self) -> bool {
        self.unsupported_fields.is_empty()
    }
    pub fn unsupported_fields(&self) -> &[String] {
        &self.unsupported_fields
    }
}

/// No Deserialize implementation. The opaque owner is not exposed by any getter.
#[derive(Debug, Clone)]
pub struct PreparedContextWire {
    owner: Arc<()>,
    identity: MappingIdentity,
    profile: CountProfile,
    neutral_digest: String,
    generation_wire_digest: String,
    generation_wire_bytes: u64,
    generation_body: Value,
    count_body: Value,
    count_input_digest: String,
    coverage: CountCoverage,
}

impl PreparedContextWire {
    /// Provider adapters supply an unexposed instance token and the actual mapped body.
    pub fn new(
        owner: Arc<()>,
        identity: MappingIdentity,
        profile: CountProfile,
        request: &ModelRequest,
        generation_body: Value,
        count_body: Value,
        coverage: CountCoverage,
    ) -> Result<Self, ProviderError> {
        if identity.model() != &request.model
            || !generation_body.is_object()
            || !count_body.is_object()
            || generation_body.get("model").and_then(Value::as_str) != Some(&request.model.model)
            || count_body.get("model").and_then(Value::as_str) != Some(&request.model.model)
        {
            return Err(accounting_error("prepared wire model or shape mismatch"));
        }
        let neutral_digest = encoding::digest_bounded(request, "neutral source")?;
        let (generation_wire_bytes, generation_wire_digest) =
            encoding::measure_digest(&generation_body, "generation wire")?;
        let (_, count_input_digest) = encoding::measure_digest(&count_body, "count input")?;
        Ok(Self {
            owner,
            identity,
            profile,
            neutral_digest,
            generation_wire_digest,
            generation_wire_bytes,
            generation_body,
            count_input_digest,
            count_body,
            coverage,
        })
    }
    pub fn identity(&self) -> &MappingIdentity {
        &self.identity
    }
    pub fn neutral_digest(&self) -> &str {
        &self.neutral_digest
    }
    pub fn generation_wire_digest(&self) -> &str {
        &self.generation_wire_digest
    }
    pub fn generation_wire_bytes(&self) -> u64 {
        self.generation_wire_bytes
    }
    pub fn generation_body(&self) -> &Value {
        &self.generation_body
    }
    pub fn count_body(&self) -> &Value {
        &self.count_body
    }
    pub fn count_input_digest(&self) -> &str {
        &self.count_input_digest
    }
    pub fn coverage(&self) -> &CountCoverage {
        &self.coverage
    }
    /// Validate generation binding without requiring count support or coverage.
    /// Unsupported profiles can still generate; changing a profile invalidates preparation.
    pub fn verify_generation_for(
        &self,
        owner: &Arc<()>,
        identity: &MappingIdentity,
        profile: &CountProfile,
    ) -> Result<(), ProviderError> {
        if !Arc::ptr_eq(&self.owner, owner)
            || &self.identity != identity
            || &self.profile != profile
        {
            return Err(accounting_error(
                "prepared wire belongs to a different provider identity or profile",
            ));
        }
        let (bytes, wire_digest) =
            encoding::measure_digest(&self.generation_body, "generation wire")?;
        if wire_digest != self.generation_wire_digest
            || encoding::measure_digest(&self.count_body, "count input")?.1
                != self.count_input_digest
            || bytes != self.generation_wire_bytes
        {
            return Err(accounting_error(
                "prepared wire digest or byte size mismatch",
            ));
        }
        Ok(())
    }
    /// Checks instance, endpoint/model/revisions, current profile and count coverage before I/O.
    pub fn verify_for(
        &self,
        owner: &Arc<()>,
        identity: &MappingIdentity,
        profile: &CountProfile,
    ) -> Result<(), ProviderError> {
        self.verify_generation_for(owner, identity, profile)?;
        match &profile.registration {
            Some(CountRegistration {
                identity: registered,
                ..
            }) if registered == identity => {}
            _ => return Err(accounting_error("count profile unsupported or mismatched")),
        }
        if !self.coverage.is_complete() {
            return Err(accounting_error("count coverage incomplete"));
        }
        Ok(())
    }
    pub fn reported_count(&self, input_tokens: u64) -> Result<ProviderInputCount, ProviderError> {
        let Some(CountRegistration {
            counter_revision,
            identity,
        }) = &self.profile.registration
        else {
            return Err(accounting_error("count profile unsupported"));
        };
        if identity != &self.identity || !self.coverage.is_complete() {
            return Err(accounting_error("count coverage incomplete"));
        }
        Ok(ProviderInputCount {
            source: CountSource::ProviderReported,
            identity: self.identity.clone(),
            neutral_digest: self.neutral_digest.clone(),
            generation_wire_digest: self.generation_wire_digest.clone(),
            count_input_digest: self.count_input_digest.clone(),
            counter_revision: counter_revision.clone(),
            input_tokens,
        })
    }
}

/// Untrusted provider report. Official exactness requires separate host identity/coverage proof.
#[derive(Debug, Clone, Serialize)]
pub struct ProviderInputCount {
    source: CountSource,
    identity: MappingIdentity,
    neutral_digest: String,
    generation_wire_digest: String,
    count_input_digest: String,
    counter_revision: String,
    input_tokens: u64,
}
impl ProviderInputCount {
    pub fn source(&self) -> CountSource {
        self.source
    }
    pub fn identity(&self) -> &MappingIdentity {
        &self.identity
    }
    pub fn input_tokens(&self) -> u64 {
        self.input_tokens
    }
    pub fn neutral_digest(&self) -> &str {
        &self.neutral_digest
    }
    pub fn generation_wire_digest(&self) -> &str {
        &self.generation_wire_digest
    }
    pub fn count_input_digest(&self) -> &str {
        &self.count_input_digest
    }
    pub fn counter_revision(&self) -> &str {
        &self.counter_revision
    }
}

/// Hash the actual configured base and protocol/version; never expose credentials or URL.
pub fn endpoint_identity(base: &str, protocol_version: &str) -> String {
    let mut hash = Sha256::new();
    hash.update((base.len() as u64).to_be_bytes());
    hash.update(base.as_bytes());
    hash.update(protocol_version.as_bytes());
    format!("{:x}", hash.finalize())
}
pub fn digest_json(value: &impl Serialize) -> Result<String, ProviderError> {
    encoding::digest_bounded(value, "accounting JSON")
}
fn identity_text(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}
fn accounting_error(message: &str) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Unsupported,
        ProviderErrorPhase::Validate,
        message,
    )
}
#[cfg(test)]
mod tests;
