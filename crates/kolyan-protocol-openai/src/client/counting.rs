//! Single-send count HTTP operation. Timeout includes the entire bounded body.
use crate::{InputTokenCountRequest, InputTokenCountResponse, OpenAiClient, OpenAiError};
use sha2::{Digest, Sha256};
use std::time::Duration;

impl OpenAiClient {
    /// Opaque SHA-256 binding to the exact configured base URL and protocol version.
    /// Includes userinfo/query bytes without returning them; this is not authentication
    /// or encryption and does not promise secrecy against guesses of low-entropy input.
    pub fn count_endpoint_identity(&self) -> String {
        let mut hash = Sha256::new();
        hash.update((self.config.base_url.len() as u64).to_be_bytes());
        hash.update(self.config.base_url.as_bytes());
        hash.update("responses/v1".as_bytes());
        format!("{:x}", hash.finalize())
    }
    /// No retries. Dropping this future cancels local waiting, not remote processing.
    pub async fn count_input_tokens(
        &self,
        request: &InputTokenCountRequest,
        timeout: Duration,
    ) -> Result<InputTokenCountResponse, OpenAiError> {
        if timeout.is_zero()
            || std::time::Instant::now().checked_add(timeout).is_none()
            || request.model.is_empty()
        {
            return Err(OpenAiError::Configuration(
                "count requires a model and a positive timeout".into(),
            ));
        }
        tokio::time::timeout(timeout, async {
            let body = encode_count_request(request)?;
            let mut response = self
                .count_http
                .post(format!(
                    "{}/v1/responses/input_tokens",
                    self.config.base_url
                ))
                .bearer_auth(&self.config.api_key)
                .header("content-type", "application/json")
                .timeout(timeout)
                .body(body)
                .send()
                .await?;
            let status = response.status();
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if chunk.len() > 64 * 1024 - bytes.len() {
                    return Err(crate::CountFailure::ResponseLimit.into());
                }
                bytes.extend_from_slice(&chunk);
            }
            if !status.is_success() {
                return Err(OpenAiError::Http {
                    status: status.as_u16(),
                    body: String::from_utf8_lossy(&bytes).into_owned(),
                });
            }
            serde_json::from_slice(&bytes).map_err(OpenAiError::from)
        })
        .await
        .map_err(|_| OpenAiError::from(crate::CountFailure::Timeout))?
    }
}

// Bound the direct SDK entry point too, not only the Provider's prepared input.
fn encode_count_request(request: &impl serde::Serialize) -> Result<Vec<u8>, OpenAiError> {
    struct LimitedBuffer(Vec<u8>);
    impl std::io::Write for LimitedBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > 16 * 1024 * 1024 - self.0.len() {
                return Err(std::io::Error::other("count input exceeds 16 MiB"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut buffer = LimitedBuffer(Vec::new());
    serde_json::to_writer(&mut buffer, request).map_err(|_| {
        OpenAiError::Configuration("count input serialization failed or exceeds 16 MiB".into())
    })?;
    Ok(buffer.0)
}
#[cfg(test)]
mod tests;
