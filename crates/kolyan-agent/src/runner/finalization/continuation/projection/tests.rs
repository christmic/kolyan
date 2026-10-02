//! Test-only access to the actual private archive decoder, never a shadow DTO.
pub(in crate::runner) fn decode(value: serde_json::Value) -> Result<serde_json::Value, String> {
    serde_json::from_value::<super::Body>(value)
        .and_then(serde_json::to_value)
        .map_err(|error| error.to_string())
}
