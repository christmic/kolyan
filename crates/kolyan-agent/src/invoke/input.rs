//! Strict decoding with parser-reported paths, not coercion or permission grants.

use super::{AgentInvokeInput, InvokePrepareError};

const MAX_ERROR_BYTES: usize = 1024;
const TRUNCATION: &str = " [truncated]";

pub(super) fn decode(value: serde_json::Value) -> Result<AgentInvokeInput, InvokePrepareError> {
    serde_path_to_error::deserialize(value).map_err(|error| {
        let mut message = error.to_string();
        if message.len() > MAX_ERROR_BYTES {
            let mut end = MAX_ERROR_BYTES - TRUNCATION.len();
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
            message.push_str(TRUNCATION);
        }
        InvokePrepareError::Invalid(message)
    })
}

#[cfg(test)]
mod tests;
