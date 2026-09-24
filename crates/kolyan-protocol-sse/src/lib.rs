//! Bounded SSE framing shared by protocol clients, independent of model events.

use thiserror::Error;

const MAX_EVENT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub struct Event {
    pub kind: String,
    pub data: String,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("SSE event exceeds the byte limit")]
    TooLarge,
    #[error("invalid SSE UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("SSE stream ended with an unfinished frame")]
    Truncated,
}

/// Holds incomplete bytes until a full line can be decoded without loss.
#[derive(Default)]
pub struct Decoder {
    line: Vec<u8>,
    event_bytes: usize,
    kind: String,
    data: Vec<String>,
}

impl Decoder {
    /// Return complete events; callers must terminate the stream on error.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>, DecodeError> {
        let mut events = Vec::new();
        for &byte in bytes {
            self.event_bytes += 1;
            if self.event_bytes > MAX_EVENT_BYTES {
                return Err(DecodeError::TooLarge);
            }
            if byte != b'\n' {
                self.line.push(byte);
                continue;
            }
            let line = std::mem::take(&mut self.line);
            let line = line.strip_suffix(b"\r").unwrap_or(&line);
            let text = std::str::from_utf8(line)?;
            if text.is_empty() {
                if !self.data.is_empty() {
                    events.push(Event {
                        kind: std::mem::take(&mut self.kind),
                        data: self.data.join("\n"),
                    });
                }
                self.kind.clear();
                self.data.clear();
                self.event_bytes = 0;
            } else if !text.starts_with(':') {
                let (field, value) = text.split_once(':').unwrap_or((text, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "event" => self.kind = value.to_owned(),
                    "data" => self.data.push(value.to_owned()),
                    _ => {}
                }
            }
        }
        Ok(events)
    }

    /// EOF is not a delimiter: an unfinished event must not become a response.
    pub fn finish(&self) -> Result<(), DecodeError> {
        if self.line.is_empty() && self.data.is_empty() && self.kind.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::Truncated)
        }
    }
}

#[cfg(test)]
mod tests;
