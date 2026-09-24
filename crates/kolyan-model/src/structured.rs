//! Shared output contract validation; schema resolution never performs external I/O.

use crate::{
    ModelResponse, OutputFormat, ProviderError, ProviderErrorKind, ProviderErrorPhase, StopReason,
};

/// Compile once before opening the provider request, validate terminal output once.
pub struct OutputValidator(Option<jsonschema::Validator>);

impl OutputValidator {
    /// Invalid schemas fail before network I/O. Remote/file references are disabled.
    pub fn new(format: Option<&OutputFormat>) -> Result<Self, ProviderError> {
        let validator = format
            .map(|format| {
                jsonschema::validator_for(&format.schema).map_err(|_| {
                    ProviderError::new(
                        ProviderErrorKind::InvalidRequest,
                        ProviderErrorPhase::Open,
                        "invalid or externally referenced output schema",
                    )
                })
            })
            .transpose()?;
        Ok(Self(validator))
    }

    /// Tool handoffs, refusals and token-limit stops need not contain final JSON.
    /// A successful final answer must contain JSON satisfying the requested schema.
    pub fn validate(&self, response: &ModelResponse) -> Result<(), ProviderError> {
        let Some(validator) = &self.0 else {
            return Ok(());
        };
        if response.stop_reason != StopReason::EndTurn {
            return Ok(());
        }
        let output = response
            .structured_output
            .as_ref()
            .ok_or_else(|| Self::error("missing or invalid structured output"))?;
        if !validator.is_valid(output) {
            return Err(Self::error(
                "structured output does not satisfy the requested schema",
            ));
        }
        Ok(())
    }

    fn error(message: &str) -> ProviderError {
        ProviderError::new(
            ProviderErrorKind::Protocol,
            ProviderErrorPhase::Decode,
            message,
        )
    }
}

#[cfg(test)]
mod tests;
