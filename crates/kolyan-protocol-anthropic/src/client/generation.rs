//! Bounded exact-body encoding shared by direct and prepared generation.
use crate::AnthropicError;
pub(super) fn encode_body(body: &serde_json::Value) -> Result<Vec<u8>, AnthropicError> {
    struct Buffer(Vec<u8>);
    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > 16 * 1024 * 1024 - self.0.len() {
                return Err(std::io::Error::other("generation body exceeds 16 MiB"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut buffer = Buffer(Vec::new());
    serde_json::to_writer(&mut buffer, body).map_err(|_| {
        AnthropicError::Configuration(
            "generation body serialization failed or exceeds 16 MiB".into(),
        )
    })?;
    Ok(buffer.0)
}
